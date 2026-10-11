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
CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7

def encode_message(message_type, message_payload=b''): # default payload is empty bytes object
    length = 1 + len(message_payload) # 4-byte message length is not included
    message_length_bytes = length.to_bytes(4, byteorder="big")
    message_type_bytes = bytes([message_type])

    final_message_bytes = message_length_bytes + message_type_bytes + message_payload

    return final_message_bytes

# data = FULL frame = 4-byte length + type + payload
def decode_message(data):
    if len(data) < 5:
        raise ValueError("Message is too short") # at minimum 5 bytes

    length = int.from_bytes(data[:4], byteorder="big") # 4-byte message length

    if length != len(data) - 4:
        raise ValueError("Invalid message length.")

    message_type = data[4] # 1-byte message type
    message_payload = data[5:] # varaible size message payload

    if message_type not in range(8):
        raise ValueError("Invalid message type.") # must be one of 8 listed above

    if message_type in (CHOKE, UNCHOKE, INTERESTED, NOT_INTERESTED):
        if message_payload:
            raise ValueError("Unexpected payload.") # no payload for these types
    elif message_type in (HAVE, REQUEST):
        if len(message_payload) != 4:
            raise ValueError("Expected a 4-byte piece index.") # 4-byte payload only for these types
    elif message_type == PIECE:
        if len(message_payload) <= 4:
            raise ValueError("Piece message must contain an index and data.") # double check if empty data is permitted?

    return message_type, message_payload


# -----Testing purposes only-----
if __name__ == "__main__":
    # Test handshake
    handshake = encode_handshake(1001)
    print(handshake)
    print(decode_handshake(handshake)) # 1001

    # Test interested message
    message = encode_message(INTERESTED)
    print(message)
    print(decode_message(message))  # (2, b'')

    # Test HAVE message with piece index 5
    message = encode_message(HAVE, bytes.fromhex("00000005"))
    print(message)
    print(decode_message(message))  # (4, b'\x00\x00\x00\x05')