# coordinates getting pieces from other peers

# Some preliminary notes/timeline for my understanding:
"""
A is interested in B's pieces.
- A sends INTERESTED to B
- B adds A to its interested peers

A is selected as a newly preferred peer by B
- B sends UNCHOKE to A

A receives UNCHOKED, and sends a REQUEST message with the piece index that it wants
- A marks B as UNCHOKED
- A picks a piece from B that A does not have (randomly)
- A records it as a pending request
- A sends REQUEST to B with that piece index

B receives REQUEST message
- B checks A is not choked
- B reads the piece from its file
- B sends PIECE message to A with the index and data

A receives PIECE message
- A writes this piece to its fiel
- A updates B's download rate and clears the pending request.
- A logs the download
- A sends HAVE to all neighbors

A sends the next REQUEST message to B, unless B has otherwise choked A

"""

import random
import messages

class TransferManager:
    # initialize the object and store any info needed
    def __init__(self, peer_state, piece_store, connection_manager, event_logger):
         # PeerState instance: bitfields, choke status, pending requests, download rates, and more of peers
        self.peer_state = peer_state

        # PieceStore instance: reads and writes pieces in the file
        self.piece_store = piece_store

        # ConnectionManager instance: sends actual messages to peers
        self.connection_manager = connection_manager

        # EventLogger instance: writes the log events
        self.event_logger = event_logger

    # Any time after a peer sends us BITFIELD message or HAVE message, we need to update and share our interests (INTERESTED or NOT_INTERESTED)
    def send_interest(self, peer_id):
        # 1. understand what pieces the peer/neighbor has
        neighbor = self.peer_state.get_neighbor_bitfield(peer_id)

        # 2. check if we do not have neighbor info yet
        if neighbor is None:
            return

        # 3. check if there is at least 1 piece we are interested in
        wanted_piece_exists = False
        for piece_index in range(self.peer_state.my_bitfield.num_pieces):
            if neighbor.has_piece(piece_index) and not self.peer_state.my_bitfield.has_piece(piece_index):
                wanted_piece_exists = True
                break # break out of loop as one piece is enough to send interested message

        # 4. send INTERESTED/NOT_INTERESTED
        if wanted_piece_exists:
            self.connection_manager.send_protocol_message(peer_id, messages.INTERESTED)
        else:
            self.connection_manager.send_protocol_message(peer_id, messages.NOT_INTERESTED)

    # requests (REQUEST) a missing piece from peer as long as peer has piece we lack, are unchoked by that peer, and have not requested that piece yet
    def request_piece(self, peer_id):
        pass

    # hanldes a REQUEST message on the receiver end: reads the piece from the store and sends the PIECE if the peer isn't choked
    # sends piece
    def receive_request_message(self, peer_id, payload):
        pass

    # handles a PIECE message on the receiver end: writes it, updates info and sends HAVE to all neighbors, checks completion, requests the next piece
    # reads piece
    def receive_piece_message(self, peer_id, payload):
        pass

    # clears pending requests to a peer that choked us so those pieces can be requested from anothdr peer
    def clear_pending_requests(self, peer_id):
        pass