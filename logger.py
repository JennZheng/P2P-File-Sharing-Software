# Write the required events to one log file per peer 

from datetime import datetime
from pathlib import Path
from threading import RLock


class EventLogger:
    #Append complete, timestamped lines safely from multiple peer threads

    def __init__(self, peer_id, base_dir="."):
        if type(peer_id) is not int or peer_id <= 0:
            raise ValueError("peer_id must be a positive integer")
        self.peer_id = peer_id
        # logs belong in the working directory 
        self.path = Path(base_dir) / f"log_peer_{peer_id}.log"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # prevent different threads from mixing their log entries together
        self._lock = RLock()
        # append preserves entries from earlier runs instead of erasing them 
        self._file = self.path.open("a", encoding="utf-8")

    def _write(self, event):
        # all events share the timestamp, local peer ID, and newline format 
        with self._lock:
            # include local date/time to the second and the UTC offset 
            timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
            self._file.write(f"[{timestamp}]: Peer {self.peer_id} {event}\n")
            # make each entry visible immediately while the peer is still running 
            self._file.flush()

    def connection_made(self, neighbor_id):
        # our peer initiated this connection to an earlier peer 
        self._write(f"makes a connection to Peer {neighbor_id}.")

    def connection_received(self, neighbor_id):
        # our peer accepted a connection initiated by another peer 
        self._write(f"is connected from Peer {neighbor_id}.")

    def preferred_neighbors(self, neighbor_ids):
        # the required format separates preferred peer IDs with commas 
        neighbors = ",".join(str(peer_id) for peer_id in neighbor_ids)
        self._write(f"has the preferred neighbors {neighbors}.")

    def optimistic_neighbor(self, neighbor_id):
        self._write(f"has the optimistically unchoked neighbor {neighbor_id}.")

    def unchoked_by(self, neighbor_id):
        # log received UNCHOKE messages: this neighbor allows us to request pieces 
        self._write(f"is unchoked by {neighbor_id}.")

    def choked_by(self, neighbor_id):
        # log received CHOKE messages: this neighbor stops uploading to us 
        self._write(f"is choked by {neighbor_id}.")

    def have_received(self, neighbor_id, piece_index):
        # this neighbor announces that it now owns the specified piece 
        self._write(
            f"received the 'have' message from {neighbor_id} "
            f"for the piece {piece_index}."
        )

    def interested_received(self, neighbor_id):
        self._write(f"received the 'interested' message from {neighbor_id}.")

    def not_interested_received(self, neighbor_id):
        self._write(f"received the 'not interested' message from {neighbor_id}.")

    def piece_downloaded(self, neighbor_id, piece_index, piece_count):
        # the handler calls this after storage succeeds, with the updated count
        self._write(
            f"has downloaded the piece {piece_index} from {neighbor_id}. "
            f"Now the number of pieces it has is {piece_count}."
        )

    def download_complete(self):
        # the handler calls this once when our peer finishes its own download
        # the peer still serves uploads until every peer has the complete file
        self._write("has downloaded the complete file.")

    def close(self):
        with self._lock:
            self._file.close()

    def __enter__(self):
        # support 'with EventLogger(...) as events' for automatic cleanup
        with self._lock:
            if self._file.closed:
                raise ValueError("event logger is closed")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # release the log file when the with block ends, including on exceptions 
        self.close()
