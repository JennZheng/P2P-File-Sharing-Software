# recieves the message and determines what should be done with it
import messages
from bitfield import Bitfield

# will be called by connection_manager when full message frame arrives
def handle_massage(remote_peer_id, full_packet, state):
    try:
        # use codec to parse message type and payload
        msg_type, payload= messages.decode_message(full_packet)
        logger = state.logger

        # route message type to correct helper function/state logic
        if msg_type == messages.CHOKE:
            logger.chokec_by(remote_peer_id)
            if remote_peer_id in state.remote_peers:
                state.remote_peers[remote_peer_id]["am choked"] = True

        elif msg_type == messages.UNCHOKE:
            logger.unchoked_by(remote_peer_id)
            if remote_peer_id in state.remote_peers:
                state.remote_peers[remote_peer_id]["am_choked"] = False
            # Remember to trigger Logic to start requesting missing piecies from this peer

    except Exception as e:
        print(f"[-] Fatal operational crash inside message handler for Peer {remote_peer_id}: {e}")