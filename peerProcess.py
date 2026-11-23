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
from transfer_manager import TransferManager
from file_manager import PieceStore
from choke_manager import ChokeManager

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
    store = PieceStore(my_id, config.FileName, config.FileSize, config.PieceSize,
                 has_file=i_have_file, base_dir="project_config_file_small")
    try:
        if i_have_file:
            state.mark_completed_peer(my_id)

        logger = EventLogger(my_id)
        try:
            manager = ConnectionManager(my_id, my_host, my_port, logger, state, None)
            try:
                chokeManager = ChokeManager(state, manager, logger)
                try:
                    transferManager = TransferManager(state, store, manager, logger)
                    manager.transferManager = transferManager
                    # Listen first, so later peers can connect to us.
                    manager.start_listening()
                    chokeManager.start()
                    for before_peer_id, before_host, before_port, _ in peers_before_me:
                        manager.connect_to_before_peers(before_host, before_port, before_peer_id)

                    peer_ids = [peer[0] for peer in config.peers]
                    # Keep serving pieces until every configured peer has finished.
                    while not state.all_peers_complete(peer_ids):
                        transferManager.request_from_neighbors()
                        time.sleep(0.1)
                finally:
                    # Stop scheduling messages before shutting down networking.
                    chokeManager.stop()
            finally:
                # Stop workers before closing their logger and storage.
                manager.stop()
        finally:
            logger.close()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
