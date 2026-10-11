# reads the cfg files (Common and PeerInfo) and locates peers
import math

COMMON_CONFIG_PATH = "project_config_file_small/Common.cfg"
PEER_INFO_PATH = "project_config_file_small/PeerInfo.cfg"

# -----Common.cfg-----
common = {}

# Assign configuration variables
with open(COMMON_CONFIG_PATH) as f:
    for line in f:
        key, value = line.split()
        common[key] = value

# Assign from dictionary
NumberOfPreferredNeighbors = int(common["NumberOfPreferredNeighbors"])
UnchokingInterval = int(common["UnchokingInterval"])
OptimisticUnchokingInterval = int(common["OptimisticUnchokingInterval"])
FileName = common["FileName"]
FileSize = int(common["FileSize"])
PieceSize = int(common["PieceSize"])

# Calculate the number of pieces present
NumPieces = math.ceil(FileSize / PieceSize)


# -----PeerInfo.cfg-----
peers = []

# Read in each peer from file
# Format: tuple of ([peer ID] [hostname] [listening port] [has file or not])
with open(PEER_INFO_PATH) as f:
    for line in f:
        peer_id, hostname, port, has_file = line.split()
        peers.append((int(peer_id), hostname, int(port), has_file == "1"))

# find neighboring peers (before & after me) for connection initialization
# peers before me -> I connect to them
# peers after me -> They connect to me
def find_peers(my_id):
    for i, peer in enumerate(peers):
        if peer[0] == my_id:
            return peer, peers[:i], peers[i + 1:] # returns self, peers before you, peers after you

    raise ValueError(f"Peer ID {my_id} not found in PeerInfo.cfg")


# -----Testing purposes only-----
if __name__ == "__main__":
    # Common.cfg values
    print("NumberOfPreferredNeighbors:", NumberOfPreferredNeighbors)
    print("UnchokingInterval:", UnchokingInterval)
    print("OptimisticUnchokingInterval:", OptimisticUnchokingInterval)
    print("FileName:", FileName)
    print("FileSize:", FileSize)
    print("PieceSize:", PieceSize)
    print("NumPieces:", NumPieces)

    # PeerInfo.cfg, should match the file's order
    for peer in peers:
        print(peer)

    # Neighbor split for the third peer in the file
    me, before, after = find_peers(peers[2][0])
    print("me:", me[0])

    print("I connect to:")
    for p in before:
        print(p[0])

    print("they connect to me:")
    for p in after:
        print(p[0])
