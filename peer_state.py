# represents the current state of a peer

# PeerState stores information about local pieces,
# neighboring peers, interest status, choke status,
# download statistics, and outstanding requests.

from threading import RLock

class PeerState:
  
    def __init__(self, peer_id, my_bitfield):
        self.peer_id = peer_id

        # Pieces currently owned by this peer
        self.my_bitfield = my_bitfield

        # Tracks pieces owned by neighboring peers
        self.neighbor_bitfields = {}

        # Peers interested in downloading from us
        self.interested_peers = set()

        # Current preferred neighbors
        self.preferred_neighbors = set()

        # Current optimistic unchoke neighbor
        self.optimistic_neighbor = None

        # Download totals from peers used by scheduler ranking (to determine preferred neighbors)
        self.download_rates = {}

        # Prevents duplicate piece requests
        self.pending_requests = {}

        # Peers currently choking us
        self.choked_by = set()

        # Peers we are currently choking
        self.choked_peers = set()

        # Peers known to have the complete file
        self.completed_peers = set()

        # Protect shared state across threads
        self._lock = RLock()

    # Called after receiving a BITFIELD message
    def update_neighbor_bitfield(self, peer_id, bitfield):
        if bitfield.num_pieces != self.my_bitfield.num_pieces:
            raise ValueError("neighbor bitfield has the wrong number of pieces")
        with self._lock:
            self.neighbor_bitfields[peer_id] = bitfield

            if bitfield.is_complete():
                self.completed_peers.add(peer_id)
            else:
                self.completed_peers.discard(peer_id)

    def remove_disconnected_peer(self, peer_id):
        with self._lock:
            self.neighbor_bitfields.pop(peer_id, None)
            self.interested_peers.discard(peer_id)
            self.preferred_neighbors.discard(peer_id)
            if self.optimistic_neighbor == peer_id:
                self.optimistic_neighbor = None
            self.download_rates.pop(peer_id, None)
            for piece_index, owner in list(self.pending_requests.items()):
                if owner == peer_id:
                    del self.pending_requests[piece_index]
            self.choked_by.discard(peer_id)
            self.choked_peers.discard(peer_id)
            self.completed_peers.discard(peer_id)

    # Returns a neighbor's bitfield if known
    def get_neighbor_bitfield(self, peer_id):
        with self._lock:
            return self.neighbor_bitfields.get(peer_id)

    # Called after receiving a HAVE message, updates neighbor bitfield for newly acquired piece
    def update_neighbor_piece(self, peer_id, piece_index):
        with self._lock:
            bitfield = self.neighbor_bitfields.get(peer_id)

            if bitfield is not None:
                bitfield.mark_piece(piece_index)

                if bitfield.is_complete():
                    self.completed_peers.add(peer_id)

    # Peer sent INTERESTED
    def add_interested_peer(self, peer_id):
        with self._lock:
            self.interested_peers.add(peer_id)

    # Peer sent NOT_INTERESTED
    def remove_interested_peer(self, peer_id):
        with self._lock:
            self.interested_peers.discard(peer_id)

    # Return a copy to avoid accidental modification
    def get_interested_peers(self):
        with self._lock:
            return set(self.interested_peers)

    # Updated by choke_manager
    def set_preferred_neighbors(self, peer_ids):
        with self._lock:
            self.preferred_neighbors = set(peer_ids)

    def get_preferred_neighbors(self):
        with self._lock:
            return set(self.preferred_neighbors)

    # Updated by choke_manager
    def set_optimistic_neighbor(self, peer_id):
        with self._lock:
            self.optimistic_neighbor = peer_id

    def get_optimistic_neighbor(self):
        with self._lock:
            return self.optimistic_neighbor

    # Called after successfully receiving piece data
    def update_download_rate(self, peer_id, bytes_received):
        with self._lock:
            current = self.download_rates.get(peer_id, 0)
            self.download_rates[peer_id] = current + bytes_received

    def get_download_rate(self, peer_id):
        with self._lock:
            return self.download_rates.get(peer_id, 0)

    # Return a copy of current download statistics
    def get_all_download_rates(self):
        with self._lock:
            return dict(self.download_rates)

    # Called by scheduler after each interval
    def reset_download_rates(self):
        with self._lock:
            self.download_rates.clear()

    # Record an on-going piece request
    def add_pending_request(self, piece_index, peer_id):
        with self._lock:
            self.pending_requests[piece_index] = peer_id

    # Request completed or cancelled
    def remove_pending_request(self, piece_index):
        with self._lock:
            self.pending_requests.pop(piece_index, None)

    # Check if a piece already has an active request
    def is_piece_requested(self, piece_index):
        with self._lock:
            return piece_index in self.pending_requests

    # Neighbor is no longer uploading to us
    def mark_choked_by(self, peer_id):
        with self._lock:
            self.choked_by.add(peer_id)

    # Neighbor allows requests again
    def mark_unchoked_by(self, peer_id):
        with self._lock:
            self.choked_by.discard(peer_id)

    def is_choked_by(self, peer_id):
        with self._lock:
            return peer_id in self.choked_by

    # Record peers we are choking
    def choke_peer(self, peer_id):
        with self._lock:
            self.choked_peers.add(peer_id)

    def unchoke_peer(self, peer_id):
        with self._lock:
            self.choked_peers.discard(peer_id)

    def am_choking(self, peer_id):
        with self._lock:
            return peer_id in self.choked_peers

    def mark_completed_peer(self, peer_id):
        with self._lock:
            self.completed_peers.add(peer_id)

    def is_peer_complete(self, peer_id):
        with self._lock:
            return peer_id in self.completed_peers

    def get_completed_peers(self):
        with self._lock:
            return set(self.completed_peers)

    # Returns pieces a neighbor has that we still need
    def get_missing_pieces_from_peer(self, peer_id):
        with self._lock:
            neighbor = self.neighbor_bitfields.get(peer_id)

            if neighbor is None:
                return []

            missing = []

            for piece_index in range(self.my_bitfield.num_pieces):
                if (neighbor.has_piece(piece_index) and not self.my_bitfield.has_piece(piece_index) and piece_index not in self.pending_requests):
                    missing.append(piece_index)

            return missing
