#include <iomanip> 
#include "output_thread.h"
#include "global.h"
#include "helper.h"
#include "message.h"
#include "transport.h"
#include "workload.h"
#include "manager.h"

OutputThread::OutputThread(uint64_t thd_id)
	: Thread(thd_id, OUTPUT_THREAD)
{ 
	for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++) {
		_msg_count[i] = 0;
		_msg_size[i] = 0;
	}
	_transport = transport[thd_id % g_num_input_threads];
	_terminated = false;
}

RC
OutputThread::run()
{
	global_sync();
	glob_manager->init_rand( get_thd_id() );
	glob_manager->set_thd_id( get_thd_id() );
	assert( glob_manager->get_thd_id() == get_thd_id() );
	pthread_barrier_wait( &global_barrier );

	Message * msg;
	bool sim_done = false;
	// Stats
	uint64_t total_bytes_sent = 0;
#if ENABLE_MSG_BUFFER
	uint64_t last_send_time = get_sys_clock();
#endif
	//Main loop
	uint32_t output_thread_id = GET_THD_ID % g_num_input_threads;
	if (g_num_nodes == 1)
		return RCOK;
#if CC_ALG == TCM
	// periodically send gc timestamp to other nodes  
	uint64_t last_gc_send_time = get_sys_clock();
#endif

	_start_time = get_sys_clock();
	_last_output_time = _start_time;
	while (!glob_manager->is_sim_done()) {
		bool pop = false;
		uint64_t t = get_sys_clock();
		assert(g_num_worker_threads % g_num_input_threads == 0);
		for (uint32_t i = output_thread_id; i < g_num_worker_threads; i += g_num_input_threads) {
			uint32_t tid = i;
			uint64_t t1 = get_sys_clock();
			if (output_queues[tid]->pop(msg)) {
				pop = true;
				INC_FLOAT_STATS(time_read_queue, get_sys_clock() - t1);
					
				uint64_t t2 = get_sys_clock();
				stats->_stats[GET_THD_ID]->_msg_count[msg->get_type()] ++;
				stats->_stats[GET_THD_ID]->_msg_size[msg->get_type()] += msg->get_packet_len();

				assert(tid % g_num_input_threads == GET_THD_ID % g_num_input_threads);
				
				INC_FLOAT_STATS(time_debug4, get_sys_clock() - t2);
#if ENABLE_MSG_BUFFER
				uint32_t bytes = _transport->sendMsg(msg);
				if (bytes > 0) 
					last_send_time = get_sys_clock();
#else

				INC_FLOAT_STATS(time_debug4, get_sys_clock() - t2);
				_last_output_time = get_sys_clock();
				_transport->sendMsg(msg);
#endif
				INC_FLOAT_STATS(bytes_sent, msg->get_packet_len());
				delete msg;
				INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
			} else  
				INC_FLOAT_STATS(time_read_queue, get_sys_clock() - t1);
		}

#if ENABLE_MSG_BUFFER
		uint64_t t2 = get_sys_clock();
		// TODO. consider checking this per output socket. right now, an output socket can be starved.
		uint32_t max_wait_time = 200 * g_max_num_active_txns; 
		if (get_sys_clock() - last_send_time > max_wait_time) { 
			_transport->sendBufferedMsg();
			INC_FLOAT_STATS(time_send_msg, get_sys_clock() - t2);
			last_send_time = get_sys_clock();
			continue;
		}
#endif
#if CC_ALG == TCM
		// send gc timestamp every 1 ms.
		if (get_sys_clock() - last_gc_send_time > 1000 * 1000) {
			last_gc_send_time = get_sys_clock();
			for (uint32_t i = 0; i < g_num_nodes; i ++) {
				if (i == g_node_id) continue;
				uint64_t ts = glob_manager->get_min_ts();
				Message * msg = new Message(Message::TCM_TS_SYNC_REQ, i, 0, sizeof(uint64_t), (char *)&ts);
				_transport->sendMsg(msg);
			}
		}
#endif
		if (!pop) {
			PAUSE10
			INC_FLOAT_STATS(time_output_idle, get_sys_clock() - t);
		}
		
		// For client node
		if (!sim_done && glob_manager->are_all_worker_threads_done()) {
			sim_done = true;
			terminate();
		}

	}
	INC_FLOAT_STATS(bytes_sent, total_bytes_sent);
	return RCOK;
}

void 
OutputThread::output()
{
}

void 
OutputThread::global_sync()
{
	for (uint32_t i = 0; i < g_num_nodes; i++) {
		if (i == GLOBAL_NODE_ID) continue;
		Message * msg = new Message(Message::TERMINATE, i, 0, 0, NULL);
		uint32_t bytes = _transport->sendMsg(msg);
		if (bytes != msg->get_packet_len()) 
			_transport->sendBufferedMsg();
	}
}

void 
OutputThread::terminate()
{
	// Only executed by clients nodes
	// terminate all client and server nodes
	for (uint32_t i = 0; i < g_num_nodes; i++) {
		if (i == GLOBAL_NODE_ID) continue;
		Message * msg = new Message(Message::TERMINATE, i, 0, 0, NULL);
		uint32_t bytes = _transport->sendMsg(msg);
		if (bytes != msg->get_packet_len())
			_transport->sendBufferedMsg();
	}
	_terminated = true;
}

void 
OutputThread::measure_bw()
{
	// TODO right now, only measure from Node 0 to Node 1
	if (g_num_nodes == 1 || g_node_id != 0) 
		return;
	double total_bytes_sent = 0;
	uint64_t init_time = get_sys_clock();
	char data[90000];
	Message * msg = new Message(Message::REQ, 1, 0, 90000, data);
	while (get_sys_clock() < init_time + 1 * BILLION) {
		int bytes = _transport->sendMsg(msg);
		if (bytes > 0) 
			total_bytes_sent += bytes;
	}
	printf("Total Bytes Sent = %f MB\n", total_bytes_sent / 1000000.0);
	msg = new Message(Message::TERMINATE, 1, 0, 0, NULL);
	while (_transport->sendMsg(msg) <= 0)
		PAUSE
}
