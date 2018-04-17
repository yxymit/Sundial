#include "input_thread.h"
#include "global.h"
#include "helper.h"
#include "message.h"
#include "transport.h"
#include "workload.h"
#include "manager.h"
#include "server_thread.h"

InputThread::InputThread(uint64_t thd_id)
	: Thread(thd_id, INPUT_THREAD)
{
	for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++) {
		_msg_count[i] = 0;
		_msg_size[i] = 0;
	}
	_transport = transport[thd_id % g_num_input_threads];
}

void InputThread::dealwithMsg(Message * msg, uint64_t t1)
{
    static uint64_t num_termination_received=0;
    if (msg) {
		if (msg->get_type() == Message::DUMMY) {
			DELETE(Message, msg);
			return;
		}
		else if (msg->get_type() == Message::TCM_TS_SYNC_REQ) {
			uint64_t ts;
			ts = *(uint64_t *)msg->get_data();
			glob_manager->update_global_gc_ts(msg->get_src_node_id(), ts);
			delete msg;
			return;
		}
		_last_input_time = get_sys_clock();
		INC_FLOAT_STATS(time_recv_msg, get_sys_clock() - t1);
		uint64_t t2 = get_sys_clock();
		INC_FLOAT_STATS(bytes_received, msg->get_packet_len());
		stats->_stats[GET_THD_ID]->_msg_count[msg->get_type()] ++;
		stats->_stats[GET_THD_ID]->_msg_size[msg->get_type()] += msg->get_packet_len();

		if (msg->get_type() == Message::TERMINATE) {
			num_termination_received ++;
			if (num_termination_received == g_num_server_nodes - 1)
				glob_manager->set_remote_done();
		} else {
			uint32_t queue_id = 0;
			queue_id = glob_manager->txnid_to_server_thread(msg->get_txn_id());
			
			M_ASSERT(queue_id % g_num_input_threads == GET_THD_ID % g_num_input_threads, 
					"queue_id=%d, thd_id=%ld\n", queue_id, GET_THD_ID);
		
			uint64_t tt = get_sys_clock();
			bool success = input_queues[ queue_id ]->push((uint64_t)msg);
			while (!success) {
				PAUSE
				success = input_queues[ queue_id ]->push((uint64_t)msg);
			}
			INC_FLOAT_STATS(time_debug7, get_sys_clock() - tt);
		}
		INC_FLOAT_STATS(time_write_queue, get_sys_clock() - t2);
	} else {
		PAUSE10
		INC_FLOAT_STATS(time_input_idle, get_sys_clock() - t1);
	}
}

RC
InputThread::run()
{
	global_sync();
	printf("Node %d sync done!\n", g_node_id);

	glob_manager->init_rand( get_thd_id() );
	glob_manager->set_thd_id( get_thd_id() );
	assert( glob_manager->get_thd_id() == get_thd_id() );
	pthread_barrier_wait( &global_barrier );

	if (g_num_nodes == 1) {
		cout << "running on single node" << endl;
		return RCOK;
	}
	Message * msg;
	_start_time = get_sys_clock();
	_last_input_time = _start_time;
	while (!glob_manager->is_sim_done())
	{
		uint64_t t1 = get_sys_clock();
		msg = _transport->recvMsg();
        dealwithMsg(msg, t1);
	}
	return RCOK;
}

void 
InputThread::global_sync()
{
	// wait for message from each remote node.
	uint32_t num_msg_received = 0;
	while (num_msg_received < g_num_nodes - 1) {
	    uint64_t t1 = get_sys_clock();
        Message * msg = _transport->recvMsg();
		if (msg && msg->get_txn_id() == 0) {  // I assume that everyone will not send any other messages after they send their single msg whose txn_id=0
			num_msg_received ++;
		} else 
			dealwithMsg(msg, t1);
		PAUSE;
	}
}

void 
InputThread::measure_bw()
{
	// TODO right now, only measure from Node 0 to Node 1
	if (g_num_nodes == 1 || g_node_id != 1) 
		return;
	Message * msg = NULL;	
	do{
		msg = _transport->recvMsg();
		if (msg)
			FREE(msg, sizeof(Message));
	} while (!msg || msg->get_type() != Message::TERMINATE);
}
