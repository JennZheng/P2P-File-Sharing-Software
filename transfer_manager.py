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
        # 1. do not request from a peer that we are choked by
        if self.peer_state.is_choked_by(peer_id):
            return

        # 2. get pieces this peer HAS that we DO NOT HAVE (& have not already requested)
        missing_pieces = self.peer_state.get_missing_pieces_from_peer(peer_id)

        # 3. check if there are remaining pieces to request
        if len(missing_pieces) == 0:
            return

        # 4. pick a random piece from our missing_pieces list
        piece_index = random.choice(missing_pieces)

        # 5. add a pending request so we do not request same piece again
        self.peer_state.add_pending_request(piece_index, peer_id)

        # 6. send REQUEST with the 4-byte piece index as the payload (in big endian)
        self.connection_manager.send_protocol_message(peer_id, messages.REQUEST, piece_index.to_bytes(4, "big"))

    # handles a REQUEST message on the receiver end: reads the piece from the store and sends the PIECE if the peer isn't choked
    # sends piece
    def receive_request_message(self, peer_id, payload):
        # 1. do not send piece to a peer that we are choking
        if self.peer_state.am_choking(peer_id):
            return

        # 2. store the requested piece index from the payload
        piece_index = int.from_bytes(payload, "big")

        # 3. read and store the piece's data from our file
        piece_data = self.piece_store.read_piece(piece_index)

        # 4. send PIECE message: 4-byte piece index + the piece data
        piece_index_bytes = piece_index.to_bytes(4, "big")
        self.connection_manager.send_protocol_message(peer_id, messages.PIECE, piece_index_bytes + piece_data)

    # handles a PIECE message on the receiver end: writes it, updates info and sends HAVE to all neighbors, checks completion, requests the next piece
    # reads piece
    def receive_piece_message(self, peer_id, payload):
        # 1. load piece index (first 4 bytes) & piece data (5th byte forward)
        piece_index = int.from_bytes(payload[:4], "big")
        piece_data = payload[4:]

        # 2. set the request to not pending 
        self.peer_state.remove_pending_request(piece_index)

        # 3. write the piece data to our file 
        # returns True only if it is a new piece has been written bc duplicates may happen (not requiring a new write)
        is_new_piece = self.piece_store.write_piece(piece_index, piece_data)

        if is_new_piece:
            # 4. update bitfield with our newly acquired data
            self.peer_state.my_bitfield.mark_piece(piece_index)

            # 5. update download rate of the peer that gave us this data
            self.peer_state.update_download_rate(peer_id, len(piece_data))

            # 6. log download event with new piece count
            self.event_logger.piece_downloaded(peer_id, piece_index, self.piece_store.piece_count)

            # 7. send HAVE message to all neighbors - to update that we now have this piece
            for neighbor_id in list(self.connection_manager.connections):
                self.connection_manager.send_protocol_message(neighbor_id, messages.HAVE, payload[:4])

            # 8. log again if we now have the whole file
            if self.piece_store.is_complete():
                self.event_logger.download_complete()

        # 9. keep requesting pieces from this peer
        self.request_piece(peer_id)

    # clears pending requests to a peer that choked us so those pieces can be requested from anothdr peer
    def clear_pending_requests(self, peer_id):
        # 1. loop over a copy of the list since we remove entries (& alter list) while looping
        for piece_index, piece_owner in list(self.peer_state.pending_requests.items()):
            
            # 2. only remove piece requests to peer that choked us
            if piece_owner == peer_id:
                self.peer_state.remove_pending_request(piece_index)