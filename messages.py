# Protocol Codec: Encodes the bytes peers send to each other and decodes the bytes peers receive
# Defines actual protocol messages

# -----Handshake Message-----
# 32 bytes total
# [18-byte handshake header] [10-byte zero bits] [4-byte peer ID]

HANDSHAKE_HEADER_BYTES = b"P2PFILESHARINGPROJ"
def encode_handshake(peer_id):
    return HANDSHAKE_HEADER_BYTES + bytes(10) + peer_id.to_bytes(4, "big") # use big endian

def decode_handshake(data):
    if len(data) != 32:
        raise ValueError("Handshake must be 32 bytes.") # must be exactly 32 bytes total
    
    # verify handshake header
    if data[:18] != HANDSHAKE_HEADER_BYTES:
        raise ValueError("Wrong handshake header.")
    
    # verify and return peer id -- !! double check this 
    return int.from_bytes(data[28:32], "big")


# -----Actual Message-----
# [4-byte message length] [1-byte message type] [variable size message payload]