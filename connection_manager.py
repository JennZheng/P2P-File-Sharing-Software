# Manage TCP connections, sending/receiving data, and connection failures.
import socket
import threading
import messages
import message_handler

class ConnectionManager:
    def __init__(self, peer_id, host, port, logger):
        self.peer_id = peer_id
        self.host = host
        self.port = port

        # store EventLogger instance
        self.logger = logger

        # the socket that accepts incoming connections
        self.server_socket = None

        # stores the sockets that are connected to other peers
        self.connections = {}

        # track whether network manager is running
        self.is_running = False

    def start_listening(self):
        self.is_running = True
        # create a TCP/IP socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #allow port to be resused after restarting program

        try:
            self.server_socket.bind((self.host, self.port)) # might need to change this?
            self.server_socket.listen()
            print(f"[*] Server listening on {self.host}:{self.port}")

            # run accepted loop
            accept_thread = threading.Thread(target=self.accept_after_peers, daemon=True)
            accept_thread.start()
        except Exception as e:
            print(f"[-] Failed to start server socket: {e}")

    # send our handshake message, read the other peer's, return their id
    def exchange_handshake(self, sock):
        # send our id
        my_id = self.peer_id
        sock.sendall(messages.encode_handshake(my_id))

        # read other peer's id
        received_id = messages.decode_handshake(self.read_exact(sock, 32)) # change this later so  message_handler calls for messages.decode
        return received_id

    
    # connecting to a peer before us - we start the connection
    def connect_to_before_peers(self, host, port, expected_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port)) # connect to peer with tcp handshake

        # send our id, read other peer's id
        received_id = self.exchange_handshake(sock)

        if received_id != expected_id: # received = who I reached, expected = who I expected to reach
            sock.close()
            raise ValueError(f"expected {expected_id}, got {received_id}")

        self.connections[received_id] = sock 

        #Log outbound connection success
        self.logger.connection_made(received_id)

        #spin up background thread to listen for data from this peer
        peer_thread = threading.Thread(target=self._listen_to_peer_loop, args=(received_id, sock), daemon=True)
        peer_thread.start()

        return sock

    # letting peers after us connect to us - they start the connection
    def accept_after_peers(self):
        while self.is_running:
            try:
                sock, _ = self.server_socket.accept() # waiting for someone to connect to us

                #offload incoming handshake as background thread to prevent server freeze
                handshake_thread = threading.Thread(target=self._handle_incoming_handshake, args=(sock,), daemon=True)
                handshake_thread.start()
            except OSError: # server socket closed, stop quietly
                break

    # proccess incoming peer identitiy without locking up accept loop
    def _handle_incoming_handshake(self, sock):  
        try:
            # send our id once accepted, and read other peer's id, we have no expected_id, peer is telling us who they are 
            received_id = self.exchange_handshake(sock)
            self.connections[received_id] = sock

            #log inbound connection success via EventLogger
            self.logger.connection_received(received_id)

            # spin up background thread to listen for data from newly accepted peer
            peer_thread = threading.Thread(target=self._listen_to_peer_loop, args=(received_id, sock), daemon=True)
            peer_thread.start()
        except Exception as e:
            print(f"[-] Handshake failed on incoming connection: {e}")
            sock.close()

    # Continuously read incoming data traffic stream and map them to team's codecs
    def _listen_to_peer_loop(self, remote_peer_id, sock):
        while self.is_running:
            try:
                # Read 4-byte message length prefix
                length_bytes = self.read_exact(sock, 4)
                message_length = int.from_bytes(length_bytes, byteorder="big")

                # Read remaining body (message_length includes the 1-byte type and payload)
                rest_of_message = self.read_exact(sock, message_length)

                # Reconstruct full frame packet
                full_packet = length_bytes + rest_of_message

                # Pass verified full frame block straight to message_handler
                message_handler.handle_message(remote_peer_id, full_packet, self.logger)
            except (socket.error, ConnectionError) as e:
                print(f"[-] Connection broken with Peer {remote_peer_id}: {e}")
                break
        self.disconnect_peer(remote_peer_id)

    # Safely tears down broken network pipes and erase them from register memory
    def disconnect_peer(self, remote_peer_id):
        if remote_peer_id in self.connections:
            try:
                self.connections[remote_peer_id].close()
            except Exception:
                pass
            del self.connections[remote_peer_id]
            print(f"[*] Connection and registers scrubbed for Peer {remote_peer_id}")

    # helper method to help send messages easily
    # Ex: conn_mgr.send_protocol_message(1002, message.INTERSTED)
    def send_protocol_message(self, remote_peer_id, message_type, message_payload=b''):
        sock = self.connections.get(remote_peer_id)
        if not sock:
            return False
        try:
            packet_bytes = messages.encode_message(message_type, message_payload)
            sock.sendall(packet_bytes)
            return True
        except socket.error:
            self.disconnect_peer(remote_peer_id)
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