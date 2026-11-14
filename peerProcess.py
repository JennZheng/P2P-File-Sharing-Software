# this is where the main program will run
# aka reads config, creates PeerState, starts netwroking, starts peer
# we might need to name this peerProcess.py based on the project description

# peer_process.py
# entry point: python peer_process.py 1001
import sys
import time
import config
from bitfield import Bitfield
from connection_manager import ConnectionManager
from logger import EventLogger
from peer_state import PeerState

def main():
     # make sure an id was given
    if len(sys.argv) != 2:
        print("usage: python peer_process.py <peer_id>")
        return 1

    # read our id from the command line - python peer_process.py 1001
    try:
        my_id = int(sys.argv[1])
        my_entry, peers_before_me, peers_after_me = config.find_peers(my_id)
    except ValueError as error:
        print(error)
        return 1

    # find our own entry, the peers before us (we connect to), and the peers after us (they connect to us)
    my_peer_id, my_host, my_port, i_have_file = my_entry

    state = PeerState(my_id, Bitfield(config.NumPieces, i_have_file))
    with EventLogger(my_id) as logger:
        manager = ConnectionManager(my_id, my_host, my_port, logger, state)
        try:
            # Listen first, so later peers can connect to us.
            manager.start_listening()
            for before_peer_id, before_host, before_port, _ in peers_before_me:
                manager.connect_to_before_peers(before_host, before_port, before_peer_id)

            # Temporary networking demo; transfer completion is not implemented yet.
            time.sleep(10)
            print("my connections:", list(manager.connections))
        finally:
            # Stop workers before their logger is closed.
            manager.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
