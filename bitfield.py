# Track piece ownership and encode the protocol's packed bitfield 

from threading import RLock


class Bitfield:
    # one bit per piece, with piece zero in the highest bit of byte zero 

    def __init__(self, num_pieces, has_file=False):
        # reject invalid configuration before allocating the ownership bits 
        if type(num_pieces) is not int or num_pieces <= 0:
            raise ValueError("num_pieces must be a positive integer")
        if type(has_file) is not bool:
            raise ValueError("has_file must be a boolean")
        self.num_pieces = num_pieces
        # each byte holds eight pieces. Adding seven rounds up for a partial byte
        self._bits = bytearray((num_pieces + 7) // 8)
        self._count = 0
        # keep the bits and count consistent when multiple threads update them
        self._lock = RLock()
        if has_file:
            # a seed owns every real piece; spare bits remain zero
            for index in range(num_pieces):
                self.mark_piece(index)

    def _check_index(self, index):
        # piece indices start at zero and end at num_pieces - 1 
        if type(index) is not int or not 0 <= index < self.num_pieces:
            raise IndexError("piece index is outside the bitfield")

    def has_piece(self, index):
        self._check_index(index)
        with self._lock:
            # index // 8 chooses the bytes, 7 - index % 8 chooses its bit 
            # for example, piece 0 uses 10000000 and piece 7 uses 00000001 
            return bool(self._bits[index // 8] & (1 << (7 - index % 8)))

    def mark_piece(self, index):
        # return True only when this call adds a previously missing piece 
        self._check_index(index)
        with self._lock:
            # a mask has a single 1 at the position belonging to this piece
            mask = 1 << (7 - index % 8)
            if self._bits[index // 8] & mask:
                # receiving the same piece twice can not increase the count
                return False
            # OR turns this bit on while preserving all other ownership bits 
            self._bits[index // 8] |= mask
            self._count += 1
            return True

    @property
    def piece_count(self):
        with self._lock:
            return self._count

    def is_complete(self):
        return self.piece_count == self.num_pieces

    def to_bytes(self):
        # return an immutable snapshot suitable for a BITFIELD payload 
        with self._lock:
            # return a copy so callers cannot change our internal bytearray 
            return bytes(self._bits)

    @classmethod
    def from_bytes(cls, num_pieces, payload):
        # decode a neighbor's payload, rejecting wrong lengths or spare bits
        result = cls(num_pieces)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError("bitfield payload must be bytes-like")
        payload = bytes(payload)
        if len(payload) != len(result._bits):
            raise ValueError("bitfield payload has the wrong length")
        # unused bits are at the low end of the last byte 
        # for ten pieces, the last byte may use only its two highest bits 
        spare_bits = len(payload) * 8 - num_pieces
        if spare_bits and payload[-1] & ((1 << spare_bits) - 1):
            raise ValueError("unused bitfield bits must be zero")
        result._bits[:] = payload
        # count the 1 bits to recover how many pieces this neighbor owns 
        result._count = sum(byte.bit_count() for byte in payload)
        return result
