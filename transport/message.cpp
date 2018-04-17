#include "message.h"
#include "helper.h"
#include "manager.h"

Message::Message(Type type, uint32_t dest, uint64_t txn_id, int size, char * data)
	: _msg_type(type)
	, _txn_id(txn_id)
	, _data_size(size)
	, _data(data)
{
	_dest_node_id = dest;
	_src_node_id = g_node_id; 
}

Message::Message(Message * msg)
{
	memcpy(this, msg, sizeof(Message));
	_data = (char *) MALLOC(_data_size);
	memcpy(_data, msg->get_data(), _data_size);
}

Message::Message(char * packet)
{
	memcpy(this, packet, sizeof(Message));
	if (_data_size > 0) {
		_data = (char *) MALLOC(_data_size);
		memcpy(_data, packet + sizeof(Message), _data_size);
	} else 
		_data = NULL;
}

Message::~Message()
{
	if (_data_size > 0)
		FREE(_data, _data_size);
}

uint32_t 
Message::get_packet_len()
{ 
	return sizeof(Message) + _data_size; 
}

void
Message::to_packet(char * packet)
{
	memcpy(packet, this, sizeof(Message));
	if (_data_size > 0)
		memcpy(packet + sizeof(Message), _data, _data_size);
}

string  
Message::get_name(Type type)
{
	switch(type) {
	case CLIENT_REQ:		return "CLIENT_REQ";
	case CLIENT_RESP:		return "CLIENT_RESP";
	case REQ: 				return "REQ";
	case RESP_COMMIT: 		return "RESP_COMMIT";
	case RESP_ABORT:		return "RESP_ABORT";
	case LOCK_REQ:			return "LOCK_REQ";
	case LOCK_COMMIT:		return "LOCK_COMMIT";
	case LOCK_ABORT:		return "LOCK_ABORT";
	case PREPARE_REQ:		return "PREPARE_REQ";
	case AMEND_REQ:			return "AMEND_REQ";
	case PREPARED_COMMIT:	return "PREPARED_COMMIT";
	case PREPARED_ABORT:	return "PREPARED_ABORT";
	case COMMITTED:			return "COMMITTED";
	case COMMIT_REQ:		return "COMMIT_REQ";
	case ABORT_REQ:			return "ABORT_REQ";
	case ACK:				return "ACK";
	case TERMINATE:			return "TERMINATE";
	case DUMMY:				return "DUMMY";
	case LOCAL_COPY_RESP:	return "LOCAL_COPY_RESP";
	case LOCAL_COPY_NACK:	return "LOCAL_COPY_NACK";
	case TCM_TS_SYNC_REQ:	return "TCM_TS_SYNC_REQ";
	default:				assert(false);
	}
}

bool 
Message::is_response(Type type)
{
	return (type == RESP_COMMIT) 
		|| (type == RESP_ABORT)
		|| (type == PREPARED_COMMIT)
		|| (type == PREPARED_ABORT)
		|| (type == ACK);
}
