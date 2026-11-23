import unittest
import threading
from unittest.mock import patch

import messages
import message_handler
from connection_manager import ConnectionManager


# Simple stand-ins for peer_state.py and logger.py
class FakeState:
    def mark_choked_by(self, peer_id):
        pass

    def choke_peer(self, peer_id):
        pass

    def remove_disconnected_peer(self, peer_id):
        pass


class FakeLogger:
    def connection_made(self, peer_id):
        pass

    def connection_received(self, peer_id):
        pass


class TestConnectionManager(unittest.TestCase):

    def setUp(self):
        # Port 0 lets the operating system choose an available port.
        self.peer1 = ConnectionManager(
            1001, "127.0.0.1", 0, FakeLogger(), FakeState()
        )

        self.peer2 = ConnectionManager(
            1002, "127.0.0.1", 0, FakeLogger(), FakeState()
        )

        self.peer1.start_listening()
        self.peer2.start_listening()

    def tearDown(self):
        # Always close sockets after each test.
        self.peer1.stop()
        self.peer2.stop()

    def test_peer_connection_and_message(self):
        # Used to tell the test when a message arrives.
        message_received = threading.Event()
        received_packets = []

        # Temporarily replace the real handler so we can check
        # whether ConnectionManager delivers the packet correctly.
        def fake_handler(peer_id, packet, state, logger):
            received_packets.append((peer_id, packet))
            message_received.set()

        with patch.object(
            message_handler, "handle_message", side_effect=fake_handler
        ):
            # Peer 1002 connects to Peer 1001.
            self.peer2.connect_to_before_peers(
                "127.0.0.1",
                self.peer1.port,
                expected_id=1001
            )

            # Check that both peers registered the connection.
            self.assertIn(1002, self.peer1.connections)
            self.assertIn(1001, self.peer2.connections)

            # Peer 1002 sends a CHOKE message to Peer 1001.
            self.assertTrue(
                self.peer2.send_protocol_message(1001, 0)
            )

            # Wait up to 3 seconds for the message to arrive.
            self.assertTrue(
                message_received.wait(timeout=3),
                "Peer 1001 did not receive the message"
            )

            # Verify the received peer ID and packet contents.
            expected_packet = messages.encode_message(0)

            self.assertEqual(received_packets[0][0], 1002)
            self.assertEqual(received_packets[0][1], expected_packet)


if __name__ == "__main__":
    unittest.main()