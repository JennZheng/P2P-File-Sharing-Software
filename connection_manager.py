# Manage TCP connections, sending/receiving data, and connection failures.
import socket
import threading
import messages
import message_handler

HANDSHAKE_SIZE = 32       # bytes in a handshake message
HANDSHAKE_TIMEOUT = 5     # seconds to wait for connect/handshake

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

    # ------------------------------------------------------------------
    # Starting up
    # ------------------------------------------------------------------

    # Start listening for incoming peer connections
    def start_listening(self):
        # create a TCP/IP socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #allow port to be resused after restarting program

        try:
            self.server_socket.bind((self.host, self.port)) # might need to change this?
            self.server_socket.listen()
            self.server_socket.settimeout(0.2)
            self.port = self.server_socket.getsockname()[1]
        except OSError:
            self._close_socket(self.server_socket)
            self.server_socket = None
            raise

        self.is_running = True
        print(f"[*] Server listening on {self.host}:{self.port}")
        # run accepted loop
        self._start_thread(self._accept_loop)

    # connecting to a peer before us - we start the connection
    def connect_to_before_peers(self, host, port, expected_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(HANDSHAKE_TIMEOUT)
            sock.connect((host, port))
        except OSError:
            sock.close()
            raise
        self._setup_peer(sock, expected_id=expected_id, incoming=False)
        return sock

    # ------------------------------------------------------------------
    # Accepting incoming connections (peers that started after us)
    # ------------------------------------------------------------------
    def _accept_loop(self):
        while self.is_running:
            try:
                sock, _ = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:  # server socket was closed -> stop quietly
                break
            # Handshake in its own thread so one slow peer can't block accept()
            self._start_thread(self._handle_incoming, (sock,))

    def _handle_incoming(self, sock):
        try:
            self._setup_peer(sock, incoming=True)  # peer tells us who they are
        except Exception as e:
            print(f"[-] Handshake failed on incoming connection: {e}")
    
    # ------------------------------------------------------------------
    # Shared setup used by BOTH incoming and outgoing connections
    # ------------------------------------------------------------------
    def _setup_peer(self, sock, expected_id=None, incoming=False):
        """Handshake, verify, register, and start listening to the peer.
        Closes the socket and re-raises if anything goes wrong."""
        try:
            remote_id = self._exchange_handshake(sock)
            if expected_id is not None and remote_id != expected_id:
                raise ValueError(f"Expected Peer {expected_id}, got Peer {remote_id}")
            self._register_connection(remote_id, sock, incoming)
        except Exception:
            self._close_socket(sock)
            raise
        self._start_thread(self._listen_to_peer_loop, (remote_id, sock))
        return remote_id
    
    # send our handshake and recieve other peer's handshake
    def _exchange_handshake(self, sock):
        sock.settimeout(HANDSHAKE_TIMEOUT)
        sock.sendall(messages.encode_handshake(self.peer_id))

        # read other peer's id
        data = self._read_exact(sock, HANDSHAKE_SIZE)
        received_id = messages.decode_handshake(data) # change this later so  message_handler calls for messages.decode
        sock.settimeout(None)
        return received_id

     # Register a successfully connected peer.
    def _register_connection(self, remote_peer_id, sock, incoming=False):
        with self._lock:
            if not self.is_running:
                raise ConnectionError("Connection manager is stopped")
            if remote_peer_id == self.peer_id:
                raise ValueError("Cannot connect to yourself")
            if remote_peer_id in self.connections:
                raise ValueError("Peer is already connected")
    
            self.connections[remote_peer_id] = sock
    
            # New connections begin in the choked state.
            self.state.mark_choked_by(remote_peer_id)
            self.state.choke_peer(remote_peer_id)
    
            if incoming:
                self.logger.connection_received(remote_peer_id)
            else:
                self.logger.connection_made(remote_peer_id)
    
    # ------------------------------------------------------------------
    # Receiving
    # ------------------------------------------------------------------
    
    # Continuously read incoming data traffic stream and map them to team's codecs
    def _listen_to_peer_loop(self, remote_peer_id, sock):
        try:
            while self.is_running:
                # Read 4-byte message length prefix
                length_bytes = self._read_exact(sock, 4)
                message_length = int.from_bytes(length_bytes, byteorder="big")
                if message_length < 1:
                    raise ValueError("message must include a type byte")

                # message_length covers the 1-byte type plus the payload
                body = self._read_exact(sock, message_length)
                message_handler.handle_message(
                    remote_peer_id, length_bytes + body, self.state, self.logger
                )
        except (OSError, ValueError, IndexError, NotImplementedError) as e:
            if self.is_running:
                print(f"[-] Connection broken with Peer {remote_peer_id}: {e}")
        finally:
            self.disconnect_peer(remote_peer_id, sock)

    # read n bytes from socket, sock
    def _read_exact(self, sock, n):
        data = b""
        while len(data) < n:
            chunk = sock.recv(n - len(data)) # ask only for the bytes still missing
            if not chunk: # empty means the other side closed
                raise ConnectionError("connection closed early")
            data += chunk
        return data

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------

    # Ex: conn_mgr.send_protocol_message(1002, message.INTERSTED)
    def send_protocol_message(self, remote_peer_id, message_type, message_payload=b''):
        with self._lock:
            sock = self.connections.get(remote_peer_id)
        if not sock:
            return False
        try:
            packet_bytes = messages.encode_message(message_type, message_payload)
            with self._send_lock:
                sock.sendall(packet_bytes)
            return True
        except OSError:
            self.disconnect_peer(remote_peer_id, sock)
            return False

    # ------------------------------------------------------------------
    # Shutting down
    # ------------------------------------------------------------------

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

    def stop(self):
        with self._lock:
            self.is_running = False
            if self.server_socket is not None:
                self._close_socket(self.server_socket)
            peer_ids = list(self.connections)
            threads = list(self._threads)
        for remote_peer_id in peer_ids:
            self.disconnect_peer(remote_peer_id)
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=1)

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    def _start_thread(self, target, args=()):
        thread = threading.Thread(target=target, args=args, daemon=True)
        with self._lock:
            self._threads.append(thread)
        thread.start()

    @staticmethod
    def _close_socket(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()