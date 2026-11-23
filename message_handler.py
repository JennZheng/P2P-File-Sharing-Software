# receives a complete frame and updates the shared peer state
import messages
from bitfield import Bitfield

# will be called by connection_manager when full message frame arrives
def handle_message(remote_peer_id, full_packet, state, logger, transfer_manager):
    msg_type, payload = messages.decode_message(full_packet)

    if msg_type == messages.CHOKE:
        state.mark_choked_by(remote_peer_id)
        logger.choked_by(remote_peer_id)
        transfer_manager.clear_pending_requests(remote_peer_id)
    elif msg_type == messages.UNCHOKE:
        state.mark_unchoked_by(remote_peer_id)
        logger.unchoked_by(remote_peer_id)
        transfer_manager.request_piece(remote_peer_id)
        # request scheduling belongs to the unfinished transfer manager
    elif msg_type == messages.INTERESTED:
        state.add_interested_peer(remote_peer_id)
        logger.interested_received(remote_peer_id)
    elif msg_type == messages.NOT_INTERESTED:
        state.remove_interested_peer(remote_peer_id)
        logger.not_interested_received(remote_peer_id)
    elif msg_type == messages.BITFIELD:
        field = Bitfield.from_bytes(state.my_bitfield.num_pieces, payload)
        state.update_neighbor_bitfield(remote_peer_id, field)
        transfer_manager.send_interest(remote_peer_id)
        transfer_manager.request_piece(remote_peer_id)
    elif msg_type == messages.HAVE:
        piece_index = int.from_bytes(payload, "big")
        if not 0 <= piece_index < state.my_bitfield.num_pieces:
            raise ValueError("HAVE piece index is outside the bitfield")
        if state.get_neighbor_bitfield(remote_peer_id) is None:
            state.update_neighbor_bitfield(
                remote_peer_id, Bitfield(state.my_bitfield.num_pieces))
        state.update_neighbor_piece(remote_peer_id, piece_index)
        logger.have_received(remote_peer_id, piece_index)
        transfer_manager.send_interest(remote_peer_id)
        transfer_manager.request_piece(remote_peer_id)
    elif msg_type == messages.REQUEST:
        transfer_manager.receive_request_message(remote_peer_id, payload)
    elif msg_type == messages.PIECE:
        transfer_manager.receive_piece_message(remote_peer_id, payload)
