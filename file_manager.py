# Store one peer's pieces in its own file, independently of networking 

import os
from pathlib import Path
from threading import RLock

from bitfield import Bitfield


class PieceStore:

    def __init__(self, peer_id, file_name, file_size, piece_size,
                 has_file=False, base_dir="."):
        # configuration must describe a positive peer ID and nonempty file 
        for name, value in (("peer_id", peer_id), ("file_size", file_size),
                            ("piece_size", piece_size)):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(has_file) is not bool:
            raise ValueError("has_file must be a boolean")
        # keep the file inside this peer's directory by rejecting path names 
        if (not isinstance(file_name, str) or not file_name
                or file_name in (".", "..") or Path(file_name).name != file_name):
            raise ValueError("file_name must be a single file name")

        self.peer_id = peer_id
        self.file_size = file_size
        self.piece_size = piece_size
        # round up because the final piece may contain fewer bytes 
        self.num_pieces = (file_size + piece_size - 1) // piece_size
        # each peer has a separate copy, such as peer_1001/thefile 
        self.path = Path(base_dir) / f"peer_{peer_id}" / file_name
        # reading and writing share a file cursor 
        self._lock = RLock()
        self._bitfield = Bitfield(self.num_pieces, has_file)

        if has_file:
            # open seeds read-only to preserve the supplied complete file 
            self._file = self.path.open("rb")
            # check the actual opened file against Common.cfg's FileSize 
            actual_size = os.fstat(self._file.fileno()).st_size
            if actual_size != file_size:
                self._file.close()
                raise ValueError(
                    f"seed file has {actual_size} bytes; expected {file_size}"
                )
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # start a fresh download. Existing partial contents are removed 
            self._file = self.path.open("w+b")
            try:
                # reserve the final length, but keep all ownership bits zero 
                # file size alone does not mean any pieces have been downloaded 
                self._file.truncate(file_size)
                self._file.flush()
            except Exception:
                # release the file if initialization fails, then report the error 
                self._file.close()
                raise

    def _check_open(self):
        if self._file.closed:
            raise ValueError("piece store is closed")

    def expected_piece_size(self, index):
        if type(index) is not int or not 0 <= index < self.num_pieces:
            raise IndexError("piece index is outside the file")
        # the last piece uses only the bytes remaining after its starting offset 
        return min(self.piece_size, self.file_size - index * self.piece_size)

    def has_piece(self, index):
        return self._bitfield.has_piece(index)

    @property
    def piece_count(self):
        return self._bitfield.piece_count

    def is_complete(self):
        return self._bitfield.is_complete()

    def bitfield_bytes(self):
        # this is the BITFIELD payload 
        return self._bitfield.to_bytes()

    def read_piece(self, index):
        size = self.expected_piece_size(index)
        with self._lock:
            self._check_open()
            if not self.has_piece(index):
                # do not upload empty space reserved for missing pieces 
                raise ValueError("cannot read a piece that is not owned")
            # pieces are stored at their final offsets, even if received out of order 
            self._file.seek(index * self.piece_size)
            data = self._file.read(size)
            if len(data) != size:
                # detect an unexpectedly shortened file 
                raise OSError("stored piece is shorter than expected")
            return data

    def write_piece(self, index, data):
        # the caller removes the four-byte piece index before passing the content 
        size = self.expected_piece_size(index)
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("piece data must be bytes-like")
        data = bytes(data)
        # reject incomplete or oversized content before changing the file 
        if len(data) != size:
            raise ValueError(f"piece {index} requires {size} bytes")
        with self._lock:
            self._check_open()
            if self.has_piece(index):
                # false tells the handler not to count or announce a duplicate 
                return False
            self._file.seek(index * self.piece_size)
            if self._file.write(data) != size:
                raise OSError("could not write the complete piece")
            # make the bytes available before publishing ownership to other threads 
            # if writing or flushing fails, the piece remains marked as missing 
            self._file.flush()
            self._bitfield.mark_piece(index)
            # the handler can now log this download and broadcast a HAVE message 
            return True

    def close(self):
        with self._lock:
            self._file.close()

    def __enter__(self):
        # support 'with PieceStore(...) as store' for automatic cleanup 
        with self._lock:
            self._check_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # close the file even if an exception occurred inside the with block 
        self.close()
