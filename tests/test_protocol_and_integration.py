import io
import socket
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from unittest.mock import patch

import config
import messages
import peer_process
from bitfield import Bitfield
from connection_manager import ConnectionManager
from logger import EventLogger
from message_handler import handle_message
from peer_state import PeerState


class ProtocolTests(unittest.TestCase):
    def test_handshake_round_trip_and_invalid_frames(self):
        frame = messages.encode_handshake(1001)
        self.assertEqual(len(frame), 32)
        self.assertEqual(frame[18:28], bytes(10))
        self.assertEqual(messages.decode_handshake(frame), 1001)
        for bad in (frame[:-1], frame + b"x", b"X" + frame[1:]):
            with self.subTest(frame=bad), self.assertRaises(ValueError):
                messages.decode_handshake(bad)

    def test_every_message_type_round_trips(self):
        index = (2).to_bytes(4, "big")
        for kind, payload in (
            (messages.CHOKE, b""), (messages.UNCHOKE, b""),
            (messages.INTERESTED, b""), (messages.NOT_INTERESTED, b""),
            (messages.HAVE, index), (messages.BITFIELD, b"\xe0"),
            (messages.REQUEST, index), (messages.PIECE, index + b"ij"),
        ):
            with self.subTest(kind=kind):
                frame = messages.encode_message(kind, payload)
                self.assertEqual(int.from_bytes(frame[:4], "big"), 1 + len(payload))
                self.assertEqual(messages.decode_message(frame), (kind, payload))

    def test_invalid_message_frames(self):
        frames = [b"", b"\0\0\0\0", b"\0\0\0\x02\0",
                  b"\0\0\0\x01\xff"]
        frames.extend(messages.encode_message(kind, b"x") for kind in range(4))
        frames.extend(messages.encode_message(kind, b"abc")
                      for kind in (messages.HAVE, messages.REQUEST, messages.PIECE))
        frames.append(messages.encode_message(messages.PIECE, bytes(4)))
        for frame in frames:
            with self.subTest(frame=frame), self.assertRaises(ValueError):
                messages.decode_message(frame)

    def test_configuration_and_peer_order(self):
        self.assertEqual(config.NumPieces,
                         (config.FileSize + config.PieceSize - 1) // config.PieceSize)
        for i, entry in enumerate(config.peers):
            self.assertEqual(config.find_peers(entry[0]),
                             (entry, config.peers[:i], config.peers[i + 1:]))
        with self.assertRaises(ValueError):
            config.find_peers(-1)


class StateAndHandlerTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.logger = EventLogger(1002, temp.name)
        self.addCleanup(self.logger.close)
        self.state = PeerState(1002, Bitfield(3))

    def deliver(self, kind, payload=b""):
        handle_message(1001, messages.encode_message(kind, payload),
                       self.state, self.logger)

    def test_choke_and_interest_messages_update_real_state_and_logs(self):
        self.deliver(messages.CHOKE)
        self.assertTrue(self.state.is_choked_by(1001))
        self.deliver(messages.UNCHOKE)
        self.assertFalse(self.state.is_choked_by(1001))
        self.deliver(messages.INTERESTED)
        self.assertEqual(self.state.get_interested_peers(), {1001})
        self.deliver(messages.NOT_INTERESTED)
        self.assertEqual(self.state.get_interested_peers(), set())
        events = self.logger.path.read_text()
        for event in ("is choked by 1001", "is unchoked by 1001",
                      "received the 'interested'", "received the 'not interested'"):
            self.assertIn(event, events)

    def test_bitfield_have_and_request_selection(self):
        self.state.my_bitfield.mark_piece(0)
        self.deliver(messages.BITFIELD, b"\xc0")
        self.assertEqual(self.state.get_missing_pieces_from_peer(1001), [1])
        self.state.add_pending_request(1, 1001)
        self.assertEqual(self.state.get_missing_pieces_from_peer(1001), [])
        self.deliver(messages.HAVE, (2).to_bytes(4, "big"))
        self.assertEqual(self.state.get_missing_pieces_from_peer(1001), [2])
        self.assertTrue(self.state.is_peer_complete(1001))
        self.assertIn("for the piece 2.", self.logger.path.read_text())
        self.state.remove_pending_request(1)
        self.assertEqual(self.state.get_missing_pieces_from_peer(1001), [1, 2])

    def test_have_without_initial_bitfield(self):
        self.deliver(messages.HAVE, (1).to_bytes(4, "big"))
        self.assertEqual(self.state.get_missing_pieces_from_peer(1001), [1])

    def test_invalid_metadata_does_not_change_state(self):
        for kind, payload in ((messages.BITFIELD, b""),
                              (messages.BITFIELD, b"\x01"),
                              (messages.HAVE, (3).to_bytes(4, "big"))):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.deliver(kind, payload)
        self.assertIsNone(self.state.get_neighbor_bitfield(1001))
        with self.assertRaises(ValueError):
            self.state.update_neighbor_bitfield(1001, Bitfield(4))

    def test_reconnected_peer_completion_is_refreshed(self):
        self.deliver(messages.BITFIELD, b"\xe0")
        self.assertTrue(self.state.is_peer_complete(1001))
        self.deliver(messages.BITFIELD, b"\x80")
        self.assertFalse(self.state.is_peer_complete(1001))

    def test_unimplemented_transfers_are_reported(self):
        for kind, payload in ((messages.REQUEST, bytes(4)),
                              (messages.PIECE, bytes(4) + b"abcd")):
            with self.subTest(kind=kind), self.assertRaises(NotImplementedError):
                self.deliver(kind, payload)

    def test_concurrent_statistics_and_disconnect_cleanup(self):
        self.state.update_neighbor_bitfield(1001, Bitfield(3, True))
        self.state.add_interested_peer(1001)
        self.state.set_preferred_neighbors({1001, 1003})
        self.state.set_optimistic_neighbor(1001)
        self.state.mark_choked_by(1001)
        self.state.choke_peer(1001)
        self.state.add_pending_request(0, 1001)
        self.state.add_pending_request(1, 1003)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.state.update_download_rate(1001, 2), range(100)))
        self.assertEqual(self.state.get_download_rate(1001), 200)
        snapshot = self.state.get_all_download_rates()
        snapshot[1001] = 0
        self.assertEqual(self.state.get_download_rate(1001), 200)
        self.state.remove_disconnected_peer(1001)
        self.assertIsNone(self.state.get_neighbor_bitfield(1001))
        self.assertFalse(self.state.is_piece_requested(0))
        self.assertTrue(self.state.is_piece_requested(1))
        self.assertEqual(self.state.get_interested_peers(), set())
        self.assertEqual(self.state.get_preferred_neighbors(), {1003})
        self.assertIsNone(self.state.get_optimistic_neighbor())
        self.assertFalse(self.state.is_choked_by(1001))
        self.assertFalse(self.state.am_choking(1001))
        self.assertFalse(self.state.is_peer_complete(1001))
        self.assertEqual(self.state.get_download_rate(1001), 0)


class EntryPointTests(unittest.TestCase):
    def test_startup_passes_logger_and_state_and_stops_workers(self):
        with TemporaryDirectory() as folder:
            logger = EventLogger(1001, folder)
            with patch.object(sys, "argv", ["peer_process.py", "1001"]), \
                 patch.object(peer_process, "EventLogger", return_value=logger), \
                 patch.object(peer_process, "ConnectionManager") as manager_type, \
                 patch.object(peer_process.time, "sleep"):
                self.assertEqual(peer_process.main(), 0)
                args = manager_type.call_args.args
                self.assertIs(args[3], logger)
                self.assertIsInstance(args[4], PeerState)
                self.assertTrue(args[4].my_bitfield.is_complete())
                manager_type.return_value.start_listening.assert_called_once()
                manager_type.return_value.stop.assert_called_once()
            self.assertTrue(logger._file.closed)

    def test_startup_failure_still_cleans_up(self):
        with TemporaryDirectory() as folder:
            logger = EventLogger(1002, folder)
            with patch.object(sys, "argv", ["peer_process.py", "1002"]), \
                 patch.object(peer_process, "EventLogger", return_value=logger), \
                 patch.object(peer_process, "ConnectionManager") as manager_type:
                manager_type.return_value.connect_to_before_peers.side_effect = OSError("refused")
                with self.assertRaises(OSError):
                    peer_process.main()
                self.assertFalse(manager_type.call_args.args[4].my_bitfield.is_complete())
                manager_type.return_value.stop.assert_called_once()
            self.assertTrue(logger._file.closed)

    def test_invalid_peer_ids(self):
        for args in ([], ["abc"], ["999999"]):
            with self.subTest(args=args), \
                 patch.object(sys, "argv", ["peer_process.py"] + args), \
                 patch.object(peer_process, "ConnectionManager") as manager_type:
                self.assertEqual(peer_process.main(), 1)
                manager_type.assert_not_called()


class NetworkIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.managers = []
        self.loggers = []
        self.thread_errors = []
        errors = patch.object(threading, "excepthook", self.thread_errors.append)
        errors.start()
        self.addCleanup(errors.stop)
        output = patch.object(sys, "stdout", io.StringIO())
        output.start()
        self.addCleanup(output.stop)
        self.addCleanup(self.close_peers)

    def close_peers(self):
        for manager in self.managers:
            manager.stop()
        for logger in self.loggers:
            logger.close()
        self.assertEqual(self.thread_errors, [], "uncaught background worker exception")
        for manager in self.managers:
            self.assertFalse(any(thread.is_alive() for thread in manager._threads))

    def make_peer(self, peer_id, complete=False):
        logger = EventLogger(peer_id, self.temp.name)
        self.loggers.append(logger)
        state = PeerState(peer_id, Bitfield(3, complete))
        manager = ConnectionManager(peer_id, "127.0.0.1", 0, logger, state)
        self.managers.append(manager)
        manager.start_listening()
        return manager

    def wait_for(self, condition):
        deadline = time.monotonic() + 3
        while not condition():
            if time.monotonic() >= deadline:
                self.fail("timed out waiting for peer state")
            threading.Event().wait(0.005)

    def connect_pair(self):
        seed = self.make_peer(1001, True)
        downloader = self.make_peer(1002)
        downloader.connect_to_before_peers(seed.host, seed.port, 1001)
        self.wait_for(lambda: 1002 in seed.connections)
        return seed, downloader

    def test_real_tcp_handshake_messages_and_logging(self):
        seed, downloader = self.connect_pair()
        self.assertTrue(downloader.state.is_choked_by(1001))
        self.assertTrue(seed.state.am_choking(1002))
        self.assertTrue(seed.send_protocol_message(1002, messages.BITFIELD,
                                                  seed.state.my_bitfield.to_bytes()))
        self.wait_for(lambda: downloader.state.is_peer_complete(1001))
        self.assertEqual(downloader.state.get_missing_pieces_from_peer(1001), [0, 1, 2])
        self.assertTrue(downloader.send_protocol_message(1001, messages.INTERESTED))
        self.wait_for(lambda: 1002 in seed.state.get_interested_peers())
        self.assertTrue(seed.send_protocol_message(1002, messages.UNCHOKE))
        self.wait_for(lambda: not downloader.state.is_choked_by(1001))
        self.assertTrue(seed.send_protocol_message(1002, messages.CHOKE))
        self.wait_for(lambda: downloader.state.is_choked_by(1001))
        self.assertTrue(downloader.send_protocol_message(1001, messages.NOT_INTERESTED))
        self.wait_for(lambda: not seed.state.get_interested_peers())
        self.assertIn("is connected from Peer 1002", seed.logger.path.read_text())
        self.assertIn("makes a connection to Peer 1001", downloader.logger.path.read_text())

    def test_fragmented_and_combined_frames(self):
        seed, downloader = self.connect_pair()
        sock = seed.connections[1002]
        frame = messages.encode_message(messages.BITFIELD, b"\x80")
        for byte in frame:
            sock.sendall(bytes([byte]))
        combined = b"".join(messages.encode_message(messages.HAVE, i.to_bytes(4, "big"))
                            for i in (1, 2))
        sock.sendall(combined)
        self.wait_for(lambda: downloader.state.is_peer_complete(1001))

    def test_malformed_frame_disconnects_and_releases_requests(self):
        seed, downloader = self.connect_pair()
        downloader.state.add_pending_request(0, 1001)
        seed.connections[1002].sendall(b"\0\0\0\x01\xff")
        self.wait_for(lambda: 1001 not in downloader.connections)
        self.assertFalse(downloader.state.is_piece_requested(0))
        self.assertFalse(downloader.send_protocol_message(1001, messages.INTERESTED))

    def test_zero_length_frame_disconnects(self):
        seed, downloader = self.connect_pair()
        seed.connections[1002].sendall(bytes(4))
        self.wait_for(lambda: 1001 not in downloader.connections)

    def test_wrong_handshake_identity_is_rejected(self):
        seed = self.make_peer(1001)
        downloader = self.make_peer(1002)
        with self.assertRaisesRegex(ValueError, "expected 1003, got 1001"):
            downloader.connect_to_before_peers(seed.host, seed.port, 1003)
        self.assertEqual(downloader.connections, {})

    def test_connection_failure_does_not_leak_socket(self):
        downloader = self.make_peer(1002)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            with self.assertRaises(OSError):
                downloader.connect_to_before_peers("127.0.0.1", reservation.getsockname()[1], 1001)
        self.assertEqual(downloader.connections, {})
        self.assertEqual(downloader._pending_sockets, set())

    def test_busy_listening_port_fails_cleanly(self):
        first = self.make_peer(1001)
        second = ConnectionManager(1002, first.host, first.port, first.logger,
                                   PeerState(1002, Bitfield(3)))
        self.managers.append(second)
        with self.assertRaises(OSError):
            second.start_listening()
        self.assertFalse(second.is_running)
        self.assertEqual(second.server_socket.fileno(), -1)

    def test_stop_closes_unfinished_handshakes(self):
        seed = self.make_peer(1001)
        with socket.create_connection((seed.host, seed.port), timeout=2) as stalled:
            self.wait_for(lambda: bool(seed._pending_sockets))
            seed.stop()
            self.assertEqual(seed._pending_sockets, set())
            self.assertFalse(any(thread.is_alive() for thread in seed._threads))

    def test_three_peers_form_a_full_mesh(self):
        peers = [self.make_peer(i) for i in (1001, 1002, 1003)]
        for i, peer in enumerate(peers):
            for before in peers[:i]:
                peer.connect_to_before_peers(before.host, before.port, before.peer_id)
        self.wait_for(lambda: all(len(peer.connections) == 2 for peer in peers))
        for peer in peers:
            for neighbor in peers:
                if neighbor is not peer:
                    self.assertTrue(peer.send_protocol_message(neighbor.peer_id, messages.INTERESTED))
        self.wait_for(lambda: all(len(peer.state.get_interested_peers()) == 2 for peer in peers))


if __name__ == "__main__":
    unittest.main()
