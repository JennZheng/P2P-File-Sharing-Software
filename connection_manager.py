# Manage TCP connections, sending/receiving data, and connection failures.
import socket
import threading
import messages

class ConnectionManager:
    def __init__(self, peer_id, host, port):
        self.peer_id = peer_id
        self.host = host
        self.port = port

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
        received_id = messages.decode_handshake(self.read_exact(sock, 32))
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
        return sock

    # letting peers after us connect to us - they start the connection
    def accept_after_peers(self):
        while self.is_running:
            try:
                sock, _ = self.server_socket.accept() # waiting for someone to connect to us
            except OSError: # server socket closed, stop quietly
                break

            # send our id once accepted, and read other peer's id, we have no expected_id, peer is telling us who they are 
            received_id = self.exchange_handshake(sock)
            self.connections[received_id] = sock

    # Milestone 3: potential other functions
    # read_exact(sock, n): read n bytes from a socket








    

