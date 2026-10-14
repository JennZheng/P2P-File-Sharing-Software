# check wire format, file reconstruction, failure handling, and required logs 

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from bitfield import Bitfield
from file_manager import PieceStore
from logger import EventLogger
import messages


class BitfieldTests(unittest.TestCase):
    def test_wire_order_and_duplicate_marking(self):
        field = Bitfield(10)
        for index in (0, 7, 8):
            self.assertTrue(field.mark_piece(index))
        self.assertFalse(field.mark_piece(8))
        self.assertEqual(field.piece_count, 3)
        self.assertEqual(field.to_bytes(), b"\x81\x80")
        decoded = Bitfield.from_bytes(10, field.to_bytes())
        self.assertEqual([i for i in range(10) if decoded.has_piece(i)], [0, 7, 8])
        self.assertEqual(decoded.piece_count, 3)

    def test_seed_padding_and_byte_boundary(self):
        for count, expected in ((1, b"\x80"), (8, b"\xff"),
                                (10, b"\xff\xc0")):
            with self.subTest(count=count):
                field = Bitfield(count, has_file=True)
                self.assertEqual(field.to_bytes(), expected)
                self.assertTrue(Bitfield.from_bytes(count, expected).is_complete())

    def test_rejects_malformed_payloads(self):
        for payload in (b"", b"\x80", b"\x80\x00\x00", b"\x80\x01"):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    Bitfield.from_bytes(10, payload)
        with self.assertRaises(TypeError):
            Bitfield.from_bytes(10, 2)

    def test_rejects_invalid_indices_and_sizes(self):
        field = Bitfield(10)
        for index in (-1, 10, 1.5, True):
            with self.subTest(index=index):
                with self.assertRaises(IndexError):
                    field.mark_piece(index)
                with self.assertRaises(IndexError):
                    field.has_piece(index)
        for count in (0, -1, 1.5, True):
            with self.assertRaises(ValueError):
                Bitfield(count)


class PieceStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base_dir = Path(self.temp.name)

    def store(self, **kwargs):
        options = dict(peer_id=1002, file_name="sample", file_size=10,
                       piece_size=4, base_dir=self.base_dir)
        options.update(kwargs)
        store = PieceStore(**options)
        self.addCleanup(store.close)
        return store

    def seed_file(self, data):
        path = self.base_dir / "peer_1001" / "sample"
        path.parent.mkdir()
        path.write_bytes(data)
        return path

    def test_out_of_order_download_reconstructs_exact_file(self):
        store = self.store()
        self.assertEqual(store.path, self.base_dir / "peer_1002" / "sample")
        self.assertEqual(store.bitfield_bytes(), b"\x00")
        self.assertFalse(store.is_complete())
        with self.assertRaises(ValueError):
            store.read_piece(0)
        for index, data in ((2, b"ij"), (0, b"abcd"), (1, b"efgh")):
            self.assertTrue(store.write_piece(index, data))
            self.assertEqual(store.read_piece(index), data)
        self.assertTrue(store.is_complete())
        self.assertEqual(store.piece_count, 3)
        self.assertEqual(store.bitfield_bytes(), b"\xe0")
        self.assertEqual(store.path.read_bytes(), b"abcdefghij")

    def test_duplicate_does_not_count_or_overwrite(self):
        store = self.store()
        self.assertTrue(store.write_piece(0, b"abcd"))
        self.assertFalse(store.write_piece(0, b"xxxx"))
        self.assertEqual(store.piece_count, 1)
        self.assertEqual(store.read_piece(0), b"abcd")

    def test_invalid_writes_never_publish_ownership(self):
        store = self.store()
        for index, data, error in ((-1, b"abcd", IndexError),
                                   (3, b"abcd", IndexError),
                                   (0, b"abc", ValueError),
                                   (2, b"ijkl", ValueError),
                                   (0, 4, TypeError)):
            with self.subTest(index=index, data=data):
                with self.assertRaises(error):
                    store.write_piece(index, data)
        self.assertEqual(store.piece_count, 0)

    def test_failed_flush_does_not_publish_ownership_and_can_retry(self):
        store = self.store()
        with patch.object(store._file, "flush", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                store.write_piece(0, b"abcd")
        self.assertFalse(store.has_piece(0))
        self.assertEqual(store.piece_count, 0)
        self.assertTrue(store.write_piece(0, b"abcd"))

    def test_seed_is_readable_and_unchanged(self):
        path = self.seed_file(b"abcdefghij")
        store = self.store(peer_id=1001, has_file=True)
        self.assertTrue(store.is_complete())
        self.assertEqual(store.read_piece(2), b"ij")
        self.assertFalse(store.write_piece(0, b"xxxx"))
        self.assertEqual(path.read_bytes(), b"abcdefghij")

    def test_missing_or_wrong_size_seed_is_rejected(self):
        with self.assertRaises(FileNotFoundError):
            self.store(peer_id=1001, has_file=True)
        path = self.seed_file(b"short")
        with self.assertRaisesRegex(ValueError, "5 bytes; expected 10"):
            self.store(peer_id=1001, has_file=True)
        self.assertEqual(path.read_bytes(), b"short")

    def test_exact_multiple_and_smaller_than_piece(self):
        for file_size, piece_size, lengths in ((8, 4, [4, 4]), (2, 4, [2])):
            with self.subTest(file_size=file_size):
                with self.store(file_size=file_size, piece_size=piece_size) as store:
                    self.assertEqual(store.num_pieces, len(lengths))
                    for index, length in enumerate(lengths):
                        self.assertEqual(store.expected_piece_size(index), length)
                        store.write_piece(index, b"x" * length)
                    self.assertTrue(store.is_complete())
                    self.assertEqual(store.path.read_bytes(), b"x" * file_size)

    def test_concurrent_writes_and_duplicates(self):
        store = self.store(file_size=64, piece_size=2)
        def save(index):
            return store.write_piece(index, bytes([index]) * 2)
        indices = list(reversed(range(32))) * 3
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(save, indices))
        self.assertEqual(sum(results), 32)
        self.assertEqual(store.piece_count, 32)
        self.assertTrue(store.is_complete())
        self.assertEqual(store.path.read_bytes(),
                         b"".join(bytes([i]) * 2 for i in range(32)))

    def test_closed_store_and_unsafe_filename(self):
        store = self.store()
        store.close()
        with self.assertRaises(ValueError):
            store.write_piece(0, b"abcd")
        with self.assertRaises(ValueError):
            store.read_piece(0)
        for name in ("", "..", "../sample", "/tmp/sample"):
            with self.assertRaises(ValueError):
                self.store(file_name=name)

    def test_bitfield_and_piece_message_integration(self):
        store = self.store()
        payload = (2).to_bytes(4, "big") + b"ij"
        kind, received = messages.decode_message(messages.encode_message(messages.PIECE, payload))
        self.assertEqual(kind, messages.PIECE)
        store.write_piece(int.from_bytes(received[:4], "big"), received[4:])
        kind, received = messages.decode_message(
            messages.encode_message(messages.BITFIELD, store.bitfield_bytes()))
        self.assertEqual(kind, messages.BITFIELD)
        self.assertTrue(Bitfield.from_bytes(store.num_pieces, received).has_piece(2))


class EventLoggerTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def test_all_required_events_and_immediate_flush(self):
        with EventLogger(1001, self.temp.name) as logger:
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
            self.assertEqual(logger.path, Path(self.temp.name) / "log_peer_1001.log")
            lines = logger.path.read_text().splitlines()
        expected = [
            "makes a connection to Peer 1002.",
            "is connected from Peer 1003.",
            "has the preferred neighbors 1002,1003.",
            "has the optimistically unchoked neighbor 1004.",
            "is unchoked by 1002.", "is choked by 1003.",
            "received the 'have' message from 1002 for the piece 7.",
            "received the 'interested' message from 1003.",
            "received the 'not interested' message from 1004.",
            "has downloaded the piece 7 from 1002. Now the number of pieces it has is 8.",
            "has downloaded the complete file.",
        ]
        self.assertEqual(len(lines), len(expected))
        for line, event in zip(lines, expected):
            self.assertRegex(line, r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{4}\]: ")
            self.assertEqual(line.split("]: ", 1)[1], f"Peer 1001 {event}")

    def test_concurrent_lines_and_append_on_reopen(self):
        with EventLogger(1001, self.temp.name) as logger:
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(logger.interested_received, range(1002, 1102)))
            path = logger.path
        with EventLogger(1001, self.temp.name) as logger:
            logger.download_complete()
        lines = path.read_text().splitlines()
        self.assertEqual(len(lines), 101)
        actual = {line.split("]: ", 1)[1] for line in lines[:-1]}
        self.assertEqual(actual, {
            f"Peer 1001 received the 'interested' message from {i}."
            for i in range(1002, 1102)
        })
        self.assertTrue(lines[-1].endswith("has downloaded the complete file."))


if __name__ == "__main__":
    unittest.main()
