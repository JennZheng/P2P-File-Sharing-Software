# this is where the main program will run
# aka reads config, creates PeerState, starts netwroking, starts peer
# we might need to name this peerProcess.py based on the project description

# peer_process.py
# entry point: python peer_process.py 1001
import sys
import time
import config
from connection_manager import ConnectionManager

def main():
     # make sure an id was given
    if len(sys.argv) != 2:
        print("usage: python peer_process.py <peer_id>")
        return

    # read our id from the command line - python peer_process.py 1001
    my_id = int(sys.argv[1])

    # find our own entry, the peers before us (we connect to), and the peers after us (they connect to us)
    my_entry, peers_before_me, peers_after_me = config.find_peers(my_id)
    my_peer_id, my_host, my_port, i_have_file = my_entry

    # create the logger and the connection manage
    manager = ConnectionManager(my_id, my_host, my_port)

    # listen first, so later peers can connect to us
    manager.start_listening()

    # connect to every peer listed before us
    for before_peer in peers_before_me:
        before_peer_id, before_host, before_port, before_has_file = before_peer
        manager.connect_to_before_peers(before_host, before_port, before_peer_id)

    time.sleep(10) # for the sake of testing and keeping the process alive until thread can finish - needs to be replaced with loop later
    print("my connections:", list(manager.connections))


if __name__ == "__main__":
    main()