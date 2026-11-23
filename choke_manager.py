import random
import threading
import time
import config
import messages

# Selects preferred neighbors and optimistic neighbors.

# Preferred neighbors:
# - Recomputed every UnchokingInterval seconds
# - Chosen from interested peers
# - Based on download rate while downloading
# - Chosen randomly once the peer has the full file

# Optimistic neighbor:
# - Recomputed every OptimisticUnchokingInterval seconds
# - Randomly selected from interested, currently choked peers

# The scheduler updates PeerState and sends CHOKE/UNCHOKE
# messages so upload permissions match the current selections.

class ChokeManager:

    def __init__(self, peer_state, connection_manager, logger):
      # Holds peer information used for scheduling decisions
      self.peer_state = peer_state

      # Sends CHOKE and UNCHOKE messages
      self.connection_manager = connection_manager

      # Records scheduler events in the peer log
      self.logger = logger

      self.is_running = False
      self._thread = None

    # Start the scheduler in the background
    def start(self):      
      # Avoid creating duplicate scheduler threads
      if self.is_running:
        return

      self.is_running = True
      self._thread = threading.Thread(target=self.run, daemon=True )
      self._thread.start()

    # Stop the scheduler thread
    def stop(self):
      self.is_running = False

      # Give the scheduler time to exit cleanly
      if self._thread is not None:
        self._thread.join(timeout = 1)

    # Chooses preferred neighbors for the next interval
    def select_preferred_neighbors(self):
      # Only interested peers can become preferred neighbors
      interested_peers = list(self.peer_state.get_interested_peers())
      k = config.NumberOfPreferredNeighbors

      # If nobody is currently requesting pieces from us
      if not interested_peers:
        self.peer_state.set_preferred_neighbors(set())
        self.logger.preferred_neighbors([])
        return

      # If we already have the whole file, choose randomly
      if self.peer_state.my_bitfield.is_complete():
        preferred = set(random.sample(interested_peers, min(k, len(interested_peers))))
        
      # Choose based on download rates
      else:
        # Rank peers by recent contribution
        rates = self.peer_state.get_all_download_rates()

        # Break equal-rate ties randomly
        random.shuffle(interested_peers)

        # Higher download rates get priority
        interested_peers.sort(key=lambda peer_id: rates.get(peer_id, 0), reverse=True)

        # Keep only the top k peers
        preferred = set(interested_peers[:k])

      # Save current preferred neighbors
      self.peer_state.set_preferred_neighbors(preferred)
      self.logger.preferred_neighbors(sorted(preferred))

      # Apply any choke/unchoke changes
      self._apply_upload_permissions()

      # Start fresh for the next interval
      self.peer_state.reset_download_rates()

    # Choose one extra peer to temporarily unchoke
    def select_optimistic_neighbor(self):
      interested_peers = self.peer_state.get_interested_peers()

      # Preferred peers already have upload access
      preferred = self.peer_state.get_preferred_neighbors()

      candidates = []

      # Populate candidates with neighbors that are elgible. 
      for peer_id in interested_peers:
        # Preferred neighbors can't also be optimistic neighbors
        if peer_id in preferred:
          continue

        # Only consider peers that are currently choked
        if self.peer_state.am_choking(peer_id):
          candidates.append(peer_id)

      # If no eligible peer exists
      if not candidates:
        self.peer_state.set_optimistic_neighbor(None)
        return

      # Randomly choose one additional peer
      optimistic_peer = random.choice(candidates)

      # Save current optimistic neighbor
      self.peer_state.set_optimistic_neighbor(optimistic_peer)
      self.logger.optimistic_neighbor(optimistic_peer)

      # Apply any choke/unchoke changes
      self._apply_upload_permissions()

    # Make actual choke states match scheduler decisions
    def _apply_upload_permissions(self):
      preferred = self.peer_state.get_preferred_neighbors()
      optimistic = self.peer_state.get_optimistic_neighbor()

      # Peers in this set may currently download from us
      upload_set = set(preferred)

      # Optimistic neighbor is also added to upload_set
      if optimistic is not None:
        upload_set.add(optimistic)

      connected_peers = list(self.connection_manager.connections.keys())

      for peer_id in connected_peers:
        # This peer should currently have upload access
        if peer_id in upload_set:
          # Unchoke peers that are currently choked
          if self.peer_state.am_choking(peer_id):
            self.connection_manager.send_protocol_message(peer_id, messages.UNCHOKE)

            # Record local choke state
            self.peer_state.unchoke_peer(peer_id)

        # This peer should not have upload access
        else:
          # Only choke peers that are currently unchoked
          if not self.peer_state.am_choking(peer_id):
            self.connection_manager.send_protocol_message(peer_id, messages.CHOKE)

            # Record local choke state
            self.peer_state.choke_peer(peer_id)

    # Runs the scheduler timers
    def run(self):
      preferred_time = time.time()
      optimistic_time = time.time()

      while self.is_running:
        current_time = time.time()

        # Time to choose preferred neighbors again
        if (current_time - preferred_time >= config.UnchokingInterval):
          self.select_preferred_neighbors()
          preferred_time = current_time

        # Time to choose a new optimistic neighbor
        if (current_time - optimistic_time >= config.OptimisticUnchokingInterval):
          self.select_optimistic_neighbor()
          optimistic_time = current_time

        # Prevent busy-waiting
        time.sleep(0.5)
