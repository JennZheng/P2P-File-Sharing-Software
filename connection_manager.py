# Manage TCP connections, sending/receiving data, and connection failures.
import socket
import threading
import messages
import message_handler

class ConnectionManager:
    def __init__(self, peer_id, host, port, logger, state):
        self.peer_id = peer_id
        self.host = host
        self.port = port

        # store EventLogger instance
        self.logger = logger
        self.state = state

        # the socket that accepts incoming connections
        self.server_socket = None

        # stores the sockets that are connected to other peers
        self.connections = {}

        # track whether network manager is running
        self.is_running = False
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._threads = []
        self._pending_sockets = set()

    def _start_thread(self, target, args=()):
        with self._lock:
            thread = threading.Thread(target=target, args=args, daemon=True)
            self._threads.append(thread)
            thread.start()

    def start_listening(self):
        # create a TCP/IP socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #allow port to be resused after restarting program

        try:
            self.server_socket.bind((self.host, self.port)) # might need to change this?
            self.server_socket.listen()
            self.server_socket.settimeout(0.2)
            self.port = self.server_socket.getsockname()[1]
            self.is_running = True
            print(f"[*] Server listening on {self.host}:{self.port}")

            # run accepted loop
            self._start_thread(self.accept_after_peers)
        except OSError:
            self.is_running = False
            self.server_socket.close()
            raise

    # send our handshake message, read the other peer's, return their id
    def exchange_handshake(self, sock):
        sock.settimeout(5)
        # send our id
        my_id = self.peer_id
        sock.sendall(messages.encode_handshake(my_id))

        # read other peer's id
        received_id = messages.decode_handshake(self.read_exact(sock, 32)) # change this later so  message_handler calls for messages.decode
        sock.settimeout(None)
        return received_id

    def _register_connection(self, remote_peer_id, sock, incoming=False):
        with self._lock:
            if not self.is_running:
                raise ConnectionError("connection manager is stopped")
            if remote_peer_id == self.peer_id or remote_peer_id in self.connections:
                raise ValueError("self or duplicate peer connection")
            self.connections[remote_peer_id] = sock
            self._pending_sockets.discard(sock)
            # Peers must receive UNCHOKE before requesting pieces.
            self.state.mark_choked_by(remote_peer_id)
            self.state.choke_peer(remote_peer_id)
            if incoming:
                self.logger.connection_received(remote_peer_id)
            else:
                self.logger.connection_made(remote_peer_id)
            self._start_thread(self._listen_to_peer_loop, (remote_peer_id, sock))

    
    # connecting to a peer before us - we start the connection
    def connect_to_before_peers(self, host, port, expected_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with self._lock:
            self._pending_sockets.add(sock)
        try:
            sock.settimeout(5)
            sock.connect((host, port))
            received_id = self.exchange_handshake(sock)
            if received_id != expected_id:
                raise ValueError(f"expected {expected_id}, got {received_id}")
            self._register_connection(received_id, sock)
            return sock
        except Exception:
            with self._lock:
                self._pending_sockets.discard(sock)
            sock.close()
            raise

    # letting peers after us connect to us - they start the connection
    def accept_after_peers(self):
        while self.is_running:
            try:
                sock, _ = self.server_socket.accept() # waiting for someone to connect to us
                with self._lock:
                    if not self.is_running:
                        sock.close()
                        break
                    self._pending_sockets.add(sock)
                    self._start_thread(self._handle_incoming_handshake, (sock,))
            except socket.timeout:
                continue
            except OSError: # server socket closed, stop quietly
                break

    # proccess incoming peer identitiy without locking up accept loop
    def _handle_incoming_handshake(self, sock):  
        try:
            # send our id once accepted, and read other peer's id, we have no expected_id, peer is telling us who they are 
            received_id = self.exchange_handshake(sock)
            self._register_connection(received_id, sock, incoming=True)
        except Exception as e:
            print(f"[-] Handshake failed on incoming connection: {e}")
            sock.close()
        finally:
            with self._lock:
                self._pending_sockets.discard(sock)

    # Continuously read incoming data traffic stream and map them to team's codecs
    def _listen_to_peer_loop(self, remote_peer_id, sock):
        try:
            while self.is_running:
                # Read 4-byte message length prefix
                length_bytes = self.read_exact(sock, 4)
                message_length = int.from_bytes(length_bytes, byteorder="big")
                if message_length < 1:
                    raise ValueError("message must include a type byte")

                # Read remaining body (message_length includes the 1-byte type and payload)
                rest_of_message = self.read_exact(sock, message_length)

                # Reconstruct full frame packet
                full_packet = length_bytes + rest_of_message

                # Pass verified full frame block straight to message_handler
                message_handler.handle_message(remote_peer_id, full_packet, self.state, self.logger)
        except (OSError, ValueError, IndexError, NotImplementedError) as e:
            if self.is_running:
                print(f"[-] Connection broken with Peer {remote_peer_id}: {e}")
        finally:
            self.disconnect_peer(remote_peer_id, sock)

    # Safely tears down broken network pipes and erase them from register memory
    def disconnect_peer(self, remote_peer_id, expected_socket=None):
        with self._lock:
            sock = self.connections.get(remote_peer_id)
            if sock is None or (expected_socket is not None and sock is not expected_socket):
                return
            del self.connections[remote_peer_id]
            self.state.remove_disconnected_peer(remote_peer_id)
        self._close_socket(sock)
        print(f"[*] Connection and registers scrubbed for Peer {remote_peer_id}")

    @staticmethod
    def _close_socket(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def stop(self):
        with self._lock:
            self.is_running = False
            if self.server_socket is not None:
                self._close_socket(self.server_socket)
            for sock in self._pending_sockets:
                self._close_socket(sock)
            self._pending_sockets.clear()
            peer_ids = list(self.connections)
            threads = list(self._threads)
        for remote_peer_id in peer_ids:
            self.disconnect_peer(remote_peer_id)
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=1)

    # helper method to help send messages easily
    # Ex: conn_mgr.send_protocol_message(1002, message.INTERSTED)
    def send_protocol_message(self, remote_peer_id, message_type, message_payload=b''):
        with self._lock:
            sock = self.connections.get(remote_peer_id)
        if not sock:
            return False
        try:
            packet_bytes = messages.encode_message(message_type, message_payload)
            # Concurrent sends must not interleave bytes from different frames.
            with self._send_lock:
                sock.sendall(packet_bytes)
            return True
        except socket.error:
            self.disconnect_peer(remote_peer_id, sock)
            return False

    # read n bytes from socket, sock
    def read_exact(self, sock, n):
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data)) # ask only for the bytes still missing
            if not chunk: # empty means the other side closed
                raise ConnectionError("connection closed early")
            data += chunk
        return data
