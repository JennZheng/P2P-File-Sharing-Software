# Manage TCP connections, sending/receiving data, and connection failures.
import socket
import threading

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
        self.server_socket.setsockopt(socket.SQL_SOCKET, socket.SO_REUSEADDR, 1) #allow port to be resused after restarting program

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen()
            print(f"[*] Server listening on {self.host}:{self.port}")

            # run accepted loop
            accept_thread = threading.Thread(target=self.accept_loop, daemon=True)
            accept_thread.start()
        except Exception as e:
            print(f"[-] Failed to start server socket: {e}")






    

