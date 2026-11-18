# coordinates getting pieces from other peers

import random
import messages


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
    def __init__(self, state, store, conn_manager, logger):
        pass

    # Any time after a peer sends us BITFIELD message or HAVE message, we need to update and share our interests (INTERESTED or NOT_INTERESTED)
    def send_interest(self, remote_peer_id):
        pass

    # requests (REQUEST) a missing piece from peer as long as peer has piece we lack, are unchoked by that peer, and have not requested that piece yet
    def request_piece(self, remote_peer_id):
        pass

    # hanldes a REQUEST message on the receiver end: reads the piece from the store and sends the PIECE if the peer isn't choked
    # sends piece
    def receive_request_message(self, remote_peer_id, payload):
        pass

    # handles a PIECE message on the receiver end: writes it, updates info and sends HAVE to all neighbors, checks completion, requests the next piece
    # reads piece
    def receive_piece_message(self, remote_peer_id, payload):
        pass

    # clears pending requests to a peer that choked us so those pieces can be requested from anothdr peer
    def clear_pending_requests(self, remote_peer_id):
        pass