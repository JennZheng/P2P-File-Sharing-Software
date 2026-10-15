from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from bitfield import Bitfield
from file_manager import PieceStore
from logger import EventLogger
import messages


class StorageAndLoggingTests(unittest.TestCase):
    def setUp(self):
        # each test gets a temporary folder, removed automatically afterward 
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)

    def make_store(self, peer_id=1002, has_file=False, file_size=10, piece_size=4):
        # most tests use abcdefghij, split into abcd / efgh / ij 
        store = PieceStore(peer_id, "sample", file_size, piece_size,
                           has_file=has_file, base_dir=self.folder)
        self.addCleanup(store.close)
        return store

    def test_bitfield_encoding(self):
        field = Bitfield(10)
        for index in (0, 7, 8):
            self.assertTrue(field.mark_piece(index))
        self.assertFalse(field.mark_piece(8))  # a duplicate must not count twice 
        self.assertEqual(field.piece_count, 3)
        # pieces 0, 7, 8 give binary 10000001 10000000 
        self.assertEqual(field.to_bytes(), b"\x81\x80")
        decoded = Bitfield.from_bytes(10, field.to_bytes())
        self.assertEqual([i for i in range(10) if decoded.has_piece(i)], [0, 7, 8])
        # complete bitfields have to keep unused bits zero even at byte boundaries 
        for count, expected in ((1, b"\x80"), (8, b"\xff"), (10, b"\xff\xc0")):
            self.assertEqual(Bitfield(count, True).to_bytes(), expected)
            self.assertTrue(Bitfield.from_bytes(count, expected).is_complete())

    def test_invalid_bitfields(self):
        with self.assertRaises(ValueError):
            Bitfield(0)
        with self.assertRaises(IndexError):
            Bitfield(10).mark_piece(10)  # only indices 0 through 9 exist
        # ten pieces need two bytes with six unused bits set to zero 
        for payload in (b"", b"\x80", b"\x80\x00\x00", b"\x80\x01"):
            with self.assertRaises(ValueError):
                Bitfield.from_bytes(10, payload)

    def test_download_and_duplicates(self):
        store = self.make_store()
        self.assertEqual(store.path, self.folder / "peer_1002" / "sample")
        self.assertFalse(store.is_complete())
        with self.assertRaises(ValueError):
            store.read_piece(0)  # reserved file space is not an owned piece
        # receive the short final piece first to check out of order storage
        for index, data in ((2, b"ij"), (0, b"abcd"), (1, b"efgh")):
            self.assertTrue(store.write_piece(index, data))
            self.assertEqual(store.read_piece(index), data)
        self.assertFalse(store.write_piece(0, b"xxxx"))
        self.assertEqual(store.piece_count, 3)
        self.assertTrue(store.is_complete())
        self.assertEqual(store.bitfield_bytes(), b"\xe0")  # binary 11100000 
        self.assertEqual(store.path.read_bytes(), b"abcdefghij")

    def test_invalid_pieces_and_failed_save(self):
        store = self.make_store()
        for index, data, error in ((-1, b"abcd", IndexError),
                                   (3, b"abcd", IndexError),
                                   (0, b"abc", ValueError),
                                   (2, b"ijkl", ValueError), (0, 4, TypeError)):
            with self.assertRaises(error):
                store.write_piece(index, data)
        # temporarily make flush fail to imitate a disk error then restore it
        with patch.object(store._file, "flush", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                store.write_piece(0, b"abcd")
        self.assertEqual(store.piece_count, 0)  # failed saves shouldn't count 
        self.assertTrue(store.write_piece(0, b"abcd"))  # a retry can succeed 

    def test_seed_file(self):
        with self.assertRaises(FileNotFoundError):
            self.make_store(peer_id=1001, has_file=True)
        path = self.folder / "peer_1001" / "sample"
        path.parent.mkdir()
        path.write_bytes(b"short")
        with self.assertRaises(ValueError):
            self.make_store(peer_id=1001, has_file=True)
        # a correctly sized seed starts complete and keeps its original bytes
        path.write_bytes(b"abcdefghij")
        seed = self.make_store(peer_id=1001, has_file=True)
        self.assertTrue(seed.is_complete())
        self.assertEqual(seed.read_piece(2), b"ij")
        self.assertFalse(seed.write_piece(0, b"xxxx"))
        self.assertEqual(path.read_bytes(), b"abcdefghij")

    def test_piece_lengths(self):
        # check both a full sized final piece and a file smaller than one piece 
        for size, lengths in ((8, [4, 4]), (2, [2])):
            with self.make_store(file_size=size) as store:
                self.assertEqual(store.num_pieces, len(lengths))
                for index, length in enumerate(lengths):
                    self.assertEqual(store.expected_piece_size(index), length)
                    store.write_piece(index, b"x" * length)
                self.assertTrue(store.is_complete())
                self.assertEqual(store.path.read_bytes(), b"x" * size)

    def test_message_integration(self):
        store = self.make_store()
        # a PIECE payload has a four-byte index followed by the actual content 
        payload = (2).to_bytes(4, "big") + b"ij"
        kind, received = messages.decode_message(messages.encode_message(messages.PIECE, payload))
        self.assertEqual(kind, messages.PIECE)
        store.write_piece(int.from_bytes(received[:4], "big"), received[4:])
        kind, received = messages.decode_message(
            messages.encode_message(messages.BITFIELD, store.bitfield_bytes()))
        self.assertEqual(kind, messages.BITFIELD)
        self.assertTrue(Bitfield.from_bytes(3, received).has_piece(2))

    def test_concurrent_storage_and_logging(self):
        store = self.make_store(file_size=16, piece_size=2)
        with EventLogger(1002, self.folder) as logger:
            def save(index):
                logger.have_received(1001, index)
                return store.write_piece(index, bytes([index]) * 2)
            # four threads imitate neighbors sending eight pieces, each three times 
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(save, list(reversed(range(8))) * 3))
            self.assertEqual(sum(results), 8)  # only eight writes should be new 
            self.assertEqual(store.piece_count, 8)
            self.assertTrue(store.is_complete())
            self.assertEqual(store.path.read_bytes(), b"".join(bytes([i]) * 2 for i in range(8)))
            lines = logger.path.read_text().splitlines()
            self.assertEqual(len(lines), 24)  # no concurrent log entry was lost
            for index in range(8):
                event = f"Peer 1002 received the 'have' message from 1001 for the piece {index}."
                self.assertEqual(sum(line.split("]: ", 1)[1] == event for line in lines), 3)

    def test_required_log_events(self):
        with EventLogger(1001, self.folder) as logger:
            logger.connection_made(1002)
            logger.connection_received(1003)
            logger.preferred_neighbors([1002, 1003])
            logger.optimistic_neighbor(1004)
            logger.unchoked_by(1002)
            logger.choked_by(1003)
            logger.have_received(1002, 7)
            logger.interested_received(1003)
            logger.not_interested_received(1004)
            logger.piece_downloaded(1002, 7, 8)
            logger.download_complete()
            self.assertEqual(logger.path, self.folder / "log_peer_1001.log")
            lines = logger.path.read_text().splitlines()  # visible before close 
        expected = [
            "makes a connection to Peer 1002.", "is connected from Peer 1003.",
            "has the preferred neighbors 1002,1003.",
            "has the optimistically unchoked neighbor 1004.",
            "is unchoked by 1002.", "is choked by 1003.",
            "received the 'have' message from 1002 for the piece 7.",
            "received the 'interested' message from 1003.",
            "received the 'not interested' message from 1004.",
            "has downloaded the piece 7 from 1002. Now the number of pieces it has is 8.",
            "has downloaded the complete file.",
        ]
        self.assertEqual(len(lines), 11)
        for line, event in zip(lines, expected):
            self.assertRegex(line, r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{4}\]: ")
            self.assertEqual(line.split("]: ", 1)[1], f"Peer 1001 {event}")
        # opening the logger again should append rather than erase old entries 
        with EventLogger(1001, self.folder) as logger:
            logger.download_complete()
        self.assertEqual(len(logger.path.read_text().splitlines()), 12)


if __name__ == "__main__":
    unittest.main()
