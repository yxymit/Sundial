#include "txn.h"
#include "row.h"
#include "workload.h"
#include "ycsb.h"
#include "server_thread.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "helper.h"
#include "manager.h"
#include "message.h"
#include "query.h"
#include "txn_table.h"
#include "transport.h"
#include "cc_manager.h"
#include "store_procedure.h"
#include "ycsb_store_procedure.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

#include "maat_manager.h"
#include "tcm_manager.h"
#include "tictoc_manager.h"
#include "naive_tictoc_manager.h"
#include "lock_manager.h"
#include "f1_manager.h"
#include "ideal_mvcc_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"


// TODO. cleanup the accesses related malloc code.

TxnManager::TxnManager(QueryBase * query, ServerThread * thread)
	: TxnManager(thread, false)
{
	_store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
}

TxnManager::TxnManager(Message * msg, ServerThread * thread)
	: TxnManager(thread, true)
{
	_store_procedure = GET_WORKLOAD->create_store_procedure(this, NULL);
	_txn_id = msg->get_txn_id();
	_msg = msg;
}

TxnManager::TxnManager(ServerThread * thread, bool sub_txn)
{
	_store_procedure = NULL;
	_txn_state = RUNNING;
	_num_resp_expected = 0;
	_is_sub_txn = sub_txn;	
	waiting_for_remote = false; 
	waiting_for_lock = false; 
	
	pthread_mutex_init( &_txn_lock, NULL );
	
	_server_thread = thread;

	_txn_start_time = get_sys_clock();
	_txn_restart_time = _txn_start_time;
	_lock_wait_time = 0;
	_net_wait_time = 0;
	_msg_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
	_msg_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
	memset(_msg_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
	memset(_msg_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);

	_num_aborts = 0;
	_txn_abort = false;
	_remote_txn_abort = false;
	_cc_manager = CCManager::create(this);
}

TxnManager::TxnManager(TxnManager * txn)
{
	_txn_id = txn->get_txn_id();
	_txn_state = RUNNING;
	_num_resp_expected = 0;
	waiting_for_remote = false; 
	waiting_for_lock = false; 
	pthread_mutex_init( &_txn_lock, NULL );

	_txn_start_time = txn->_txn_start_time;
	_txn_restart_time = get_sys_clock();
	_lock_wait_time = 0;
	_net_wait_time = 0;
	
	_num_aborts = txn->_num_aborts;
	_txn_abort = false;
	_remote_txn_abort = false;
	
	QueryBase * query = GET_WORKLOAD->clone_query(txn->get_store_procedure()->get_query());
	_store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
	_is_sub_txn = false;
	
	_msg_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
	_msg_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
	memset(_msg_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
	memset(_msg_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
	_cc_manager = CCManager::create(this);
}

TxnManager::~TxnManager()
{
	delete _msg_count;
	delete _msg_size;
	if (_store_procedure)
		delete _store_procedure;
#if CC_ALG == TCM
	// if committed, keep the manager.
	if (_txn_state == ABORTED)
		delete _cc_manager;
#else
	delete _cc_manager;
#endif
}

bool
TxnManager::is_all_remote_readonly()
{
	return _store_procedure->get_query()->is_all_remote_readonly();
}

bool
TxnManager::is_txn_ready()
{
	return _cc_manager->is_txn_ready();
}

void 
TxnManager::set_txn_ready(RC rc)
{
	_cc_manager->set_txn_ready(rc);
}

void
TxnManager::update_stats()
{
	_finish_time = get_sys_clock();
	// Stats
	// TODO. collect stats for sub_queries. 
	if (is_sub_txn())
		return;

#if WORKLOAD == TPCC && STATS_ENABLE 
	if (!is_sub_txn()) {
		uint32_t type = ((QueryTPCC *)_store_procedure->get_query())->type; 
		if (_txn_state == COMMITTED) {
			stats->_stats[GET_THD_ID]->_commits_per_txn_type[ type ]++;
			stats->_stats[GET_THD_ID]->_time_per_txn_type[ type ] += 
				_finish_time - _txn_start_time - _lock_wait_time - _net_wait_time;
		} else 
			stats->_stats[GET_THD_ID]->_aborts_per_txn_type[ type ]++;
	}
#endif

	if ( _txn_state == COMMITTED ) {
		INC_INT_STATS(num_commits, 1);
		uint64_t latency = _finish_time - _txn_start_time;
		INC_FLOAT_STATS(txn_latency, latency);
#if CC_ALG == NAIVE_TICTOC
		INC_FLOAT_STATS(execute_phase, _lock_phase_start_time - _txn_restart_time);
		INC_FLOAT_STATS(lock_phase, _prepare_start_time - _lock_phase_start_time);
#else
		INC_FLOAT_STATS(execute_phase, _prepare_start_time - _txn_restart_time);
#endif
		INC_FLOAT_STATS(prepare_phase, _commit_start_time - _prepare_start_time);
		INC_FLOAT_STATS(commit_phase, _finish_time - _commit_start_time);
		INC_FLOAT_STATS(abort, _txn_restart_time - _txn_start_time);

		INC_FLOAT_STATS(wait, _lock_wait_time);
		INC_FLOAT_STATS(network, _net_wait_time);
#if COLLECT_LATENCY
		vector<double> &all = stats->_stats[GET_THD_ID]->all_latency;
		all.push_back(latency);
#endif
		if (!is_sub_txn()) {
			for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i ++) {
				if (i == Message::PREPARED_ABORT)
					M_ASSERT(_msg_count[i] == 0, "txn=%ld\n", get_txn_id());
				stats->_stats[GET_THD_ID]->_msg_committed_count[i] += _msg_count[i];
				stats->_stats[GET_THD_ID]->_msg_committed_size[i] += _msg_size[i];
			}
		}
	} else if ( _txn_state == ABORTED ) {
		INC_INT_STATS(num_aborts, 1);
		if (_remote_txn_abort)
			INC_INT_STATS(num_aborts_remote, 1);
		_num_aborts ++;
	} else 
		assert(false);
}

RC 
TxnManager::start_execute()
{
	_txn_abort = false;
	_remote_txn_abort = false;
	_num_resp_expected = 0;

#if CC_ALG == IDEAL_MVCC
	uint64_t time = get_relative_clock();
	if (g_max_clock_skew > 0 && g_num_nodes > 1) {
		time += g_max_clock_skew * 1000 * g_node_id / (g_num_nodes - 1);
	}
	((MVCCManager*)_cc_manager)->commit_ts = (time * g_num_server_nodes + g_node_id) * g_num_worker_threads + GET_THD_ID; 
#endif
	
	if (_txn_state == ABORTED) {
		_txn_restart_time = get_sys_clock();
		_txn_state = RUNNING;
		waiting_for_lock = false;
		waiting_for_remote = false;
	}
	_store_procedure->init();
	assert(_txn_state == RUNNING);
	return execute();
}

RC 
TxnManager::continue_execute()
{
	if (_txn_state == RUNNING)
		return execute();
	else if (_txn_state == PREPARING) {
		assert(CC_ALG == F_ONE || CC_ALG == TICTOC);
		assert(OCC_LOCK_TYPE == WAIT_DIE);
		// only F1 can wait and reexecute in prepare phase
		return continue_prepare_phase(); 
	} else 
		assert(false);
}

RC
TxnManager::execute(bool restart)
{
	// Stats
	if (waiting_for_lock)
		_lock_wait_time += get_sys_clock() - _lock_wait_start_time;
	waiting_for_lock = false;
	assert (_txn_state == RUNNING);
	// remote request. 
	if (is_sub_txn())
	{
		uint32_t resp_size = 0;
		char * resp_data = NULL;
		char * data = _msg->get_data();
		UnstructuredBuffer buffer(_msg->get_data());
		uint32_t header_size = _cc_manager->process_remote_req_header( &buffer );
		data += header_size;
		RC rc = _store_procedure->process_remote_req(_msg->get_data_size() - header_size, 
													 data, resp_size, resp_data);
		if (rc == RCOK) {
			assert(resp_size > 0);
			send_msg(new Message(Message::RESP_COMMIT, _src_node_id,
								   get_txn_id(), resp_size, resp_data)); 
		} else if (rc == ABORT) {
			send_msg(new Message(Message::RESP_ABORT, _src_node_id, 
								   get_txn_id(), 0, NULL));
			_cc_manager->abort();
		} else if (rc == WAIT) {
			assert(CC_ALG == WAIT_DIE || (CC_ALG == TICTOC && OCC_WAW_LOCK));
			waiting_for_lock = true;
			_lock_wait_start_time = get_sys_clock();
		}
		return rc;
	} else {
		// on the host node.
		_remote_txn_abort = false;
		
		uint64_t tt = get_sys_clock();
		RC rc = _store_procedure->execute();
		INC_FLOAT_STATS(logic, get_sys_clock() - tt);
		
		if (rc == RCOK) {
			if (_num_resp_expected == 0) {
				assert(!_txn_abort);
#if CC_ALG == NAIVE_TICTOC  
				return process_lock_phase();
#else
				return process_2pc_prepare_phase();
#endif
			} 
			assert( !_store_procedure->remote_requests.empty() );
			uint32_t resp_remains = ATOM_SUB_FETCH(_num_resp_expected, 1);
			if (resp_remains == 0) {
				waiting_for_remote = false;
				if (!_txn_abort)
					return continue_execute();
				else 
					return process_2pc_commit_phase(ABORT);
			} else 
				return RCOK;
		} else if (rc == ABORT) {
			_txn_abort = true;
			INC_INT_STATS(num_aborts_execute, 1);
			if (_num_resp_expected == 0 || ATOM_SUB_FETCH(_num_resp_expected, 1) == 0) {
				waiting_for_remote = false;
				return process_2pc_commit_phase(ABORT);
			} else 
				return RCOK;
		} else if (rc == LOCAL_MISS) {
			assert(!is_sub_txn());
			waiting_for_remote = true;
			process_local_miss();
			// TODO assumes a single parallel batch.  
			// Otherwise, different batches may have different remote nodes. 
			return continue_execute();
		} else if (rc == WAIT) {
			assert(CC_ALG == WAIT_DIE || ((CC_ALG == TICTOC || CC_ALG == F_ONE) && OCC_WAW_LOCK));
			waiting_for_lock = true;
			_lock_wait_start_time = get_sys_clock();
			return rc;
		} else 
			M_ASSERT(false, "Unsupported rc (%d)\n", rc);
	} 
}

////////////////////////////////////////////////////////
// For Distributed DBMS
////////////////////////////////////////////////////////

RC
TxnManager::process_msg(Message * msg)
{
	_msg = msg;
	_msg_count[msg->get_type()] ++;
	_msg_size[msg->get_type()] += msg->get_packet_len();
	if (msg->is_response()) {
		assert(!is_sub_txn());
		_net_wait_time += get_sys_clock() - _net_wait_start_time;  
	}

	switch (msg->get_type()) {
	case Message::CLIENT_REQ:
		INC_INT_STATS(num_home_txn, 1);
		return start_execute();
	case Message::REQ: 
		INC_INT_STATS(num_remote_txn, 1);
		return process_remote_req(msg);
	case Message::RESP_COMMIT:
	case Message::RESP_ABORT:
		assert(waiting_for_remote);
		return process_remote_resp(msg);
	// 2PC prepare phase and amend phase.
#if CC_ALG == NAIVE_TICTOC  
	case Message::LOCK_REQ:
		return process_lock_req(msg);
	case Message::LOCK_COMMIT:
	case Message::LOCK_ABORT:
		return process_lock_resp(msg);
#endif
	case Message::PREPARE_REQ:
		return process_2pc_prepare_req(msg);
	case Message::PREPARED_COMMIT:
	case Message::PREPARED_ABORT:
	case Message::COMMITTED:
		assert(_txn_state == PREPARING);
		assert(waiting_for_remote);
		return process_2pc_prepare_resp(msg);
	// 2PC commit phase
	case Message::COMMIT_REQ:
	case Message::ABORT_REQ:
		return process_2pc_commit_req(msg);
	case Message::ACK:
		assert(waiting_for_remote);
		return process_2pc_commit_resp();
	case Message::LOCAL_COPY_RESP:
		assert(ENABLE_LOCAL_CACHING);
		return process_caching_resp(msg);
	default:
		M_ASSERT(false, "Unsupported message type\n");
	}
}

RC 
TxnManager::process_local_miss()
{
	// need to send query to remote node.
	// 1. suspend the txn and put it to txn_table
	assert(!is_sub_txn());
	assert(_num_resp_expected == 0);
	map<uint32_t, UnstructuredBuffer> &remote_requests = _store_procedure->remote_requests;
	_num_resp_expected = remote_requests.size() + 1; 
	if (!remote_requests.empty()) {
		for (map<uint32_t, UnstructuredBuffer>::iterator it = remote_requests.begin(); 
			 it != remote_requests.end(); it ++)
		{
			// notify _txn of remote requests.
			uint32_t node = it->first;
			_cc_manager->add_remote_req_header( &it->second );
			char * data = new char [it->second.size()];
			memcpy(data, it->second.data(), it->second.size());
			send_msg( new Message( Message::REQ, node, get_txn_id(), 
			  	it->second.size(), data ) );
			remote_nodes_involved.insert(node);
		}
	}
	return RCOK;
}

RC
TxnManager::process_remote_req(Message * msg)
{
	_src_node_id = msg->get_src_node_id();
	_store_procedure->init();
	return execute();
}

RC
TxnManager::process_remote_resp(Message * msg) 
{
	uint32_t num_resp_left = ATOM_SUB_FETCH(_num_resp_expected, 1);
	if (msg->get_type() == Message::RESP_COMMIT) {
		// continue the pending transaction
		assert (remote_nodes_involved.find(msg->get_src_node_id()) != remote_nodes_involved.end());
		assert(msg->get_data_size() > 0);
		if (!_txn_abort)
			_cc_manager->process_remote_resp( msg->get_src_node_id(), msg->get_data_size(), msg->get_data() );
		// TODO. right now, the returned data from a remote node is ignored. 
	} else if (msg->get_type() == Message::RESP_ABORT) {
		pthread_mutex_lock(&_txn_lock);
		remote_nodes_involved.erase( msg->get_src_node_id() );
		pthread_mutex_unlock(&_txn_lock);
		_txn_abort = true;
		_remote_txn_abort = true;
	} else 
		M_ASSERT(false, "Unsupported message type");
	if (num_resp_left == 0) {
		waiting_for_remote = false;
		if (!_txn_abort)
			return continue_execute();
		else 
			return process_2pc_commit_phase(ABORT);
	}
	return RCOK;
}

#if CC_ALG == NAIVE_TICTOC  
RC 
TxnManager::process_lock_phase()
{
	_lock_phase_start_time = get_sys_clock();
	// TODO. measure lock phase time.
	RC rc = RCOK;
	_txn_state = PREPARING;
	assert(!waiting_for_lock && !waiting_for_remote);
	
	_num_resp_expected = 0; 
	bool resp_expected = false;
	NaiveTicTocManager * tictoc_manager = (NaiveTicTocManager *) _cc_manager; 
	rc = tictoc_manager->process_lock_phase_coord();
	assert(rc == RCOK || rc == ABORT);
	if (rc == ABORT) {
		INC_INT_STATS(int_debug1, 1); // local abort in lock phase
		return process_2pc_commit_phase(ABORT);
	} else { // rc == RCOK
		// for local caching, some remotes nodes are not registered  
		set<uint32_t> remote_nodes_with_writes;
		_cc_manager->get_remote_nodes_with_writes(&remote_nodes_with_writes);
		for (set<uint32_t>::iterator it = remote_nodes_with_writes.begin(); 
			it != remote_nodes_with_writes.end(); it ++)	
		{
			resp_expected = true;
			waiting_for_remote = true;
			ATOM_ADD_FETCH(_num_resp_expected, 1);
			Message * msg = new Message(Message::LOCK_REQ, *it, get_txn_id(), 0, NULL);
			send_msg(msg);
		} 
	} 
	if (resp_expected) {
		INC_INT_STATS(int_debug5, 1);
		return RCOK;
	} else {
		INC_INT_STATS(int_debug6, 1);
		assert(rc == RCOK);
		return process_2pc_prepare_phase();
	}
}

RC 
TxnManager::process_lock_req(Message * msg)
{
	_lock_phase_start_time = get_sys_clock();
	_txn_state = PREPARING;
	_resp_size = 0;
	_resp_data = NULL; 
	NaiveTicTocManager * tictoc_manager = (NaiveTicTocManager *) _cc_manager; 
	RC rc = tictoc_manager->process_lock_req(msg->get_data_size(), msg->get_data(), _resp_size, _resp_data);
	assert( rc == RCOK || rc == ABORT);

	Message::Type type;
	if (rc == RCOK)
		type = Message::LOCK_COMMIT;
	else // if (rc == ABORT)
		type = Message::LOCK_ABORT;
	Message * resp_msg = new Message(type, msg->get_src_node_id(), get_txn_id(), _resp_size, _resp_data);
	send_msg(resp_msg);
	return rc;
}

RC 
TxnManager::process_lock_resp(Message * msg)
{
	RC rc = (msg->get_type() == Message::LOCK_ABORT)? ABORT : RCOK; 
	// multiple threads may execute the following code concurrently, if multiple responses
	// are received.
	NaiveTicTocManager * tictoc_manager = (NaiveTicTocManager *) _cc_manager; 
	tictoc_manager->process_lock_resp(rc, msg->get_src_node_id(), msg->get_data());
	if (rc == ABORT) { 
		_txn_abort = true;
		_remote_txn_abort = true;
		// TODO. make aborted_remote_nodes lock free.
		pthread_mutex_lock(&_txn_lock);
		aborted_remote_nodes.insert(msg->get_src_node_id());
		pthread_mutex_unlock(&_txn_lock);
		INC_INT_STATS(int_debug2, 1); // remote abort in lock phase
	} 
	COMPILER_BARRIER
	uint32_t num_resp_left = ATOM_SUB_FETCH(_num_resp_expected, 1);
	if (num_resp_left > 0) {
		return RCOK;
	} else {
		waiting_for_remote = false;
		if (_txn_abort)
			return process_2pc_commit_phase(ABORT);
		else {
			assert(rc == RCOK);
			return process_2pc_prepare_phase();
		}
	}
}
#endif

// Two Phase Commit
// TODO consider moving 2pc and ownership to different files. 
RC 
TxnManager::process_2pc_prepare_phase()
{
	_prepare_start_time = get_sys_clock();
	RC rc = RCOK;
#if CC_ALG == MAAT
	((MaaTManager *)_cc_manager)->latch();
#endif
	_txn_state = PREPARING;
#if CC_ALG == MAAT
	((MaaTManager *)_cc_manager)->unlatch();
#endif
	assert(!waiting_for_lock && !waiting_for_remote);
	
	_num_resp_expected = 0; 
	bool resp_expected = false;
	rc = _cc_manager->process_prepare_phase_coord();
	if (rc == WAIT) {
		assert(CC_ALG != TICTOC || OCC_LOCK_TYPE == WAIT_DIE);
		assert(CC_ALG != NAIVE_TICTOC);
		waiting_for_lock = true;
		_num_resp_expected = 1;
	}
	// Logging
#if LOG_ENABLE
	if (rc != WAIT) {
		uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
		char log_record[ log_record_size ];
		log_manager->log(log_record_size, log_record);	
	}
#endif		
	if (rc == ABORT) {
		// local validation fails
		return process_2pc_commit_phase(ABORT);
	} else if (rc == RCOK || rc == WAIT) {
		// for local caching, some remotes nodes are not registered  
		_cc_manager->get_remote_nodes(&remote_nodes_involved);
		for (set<uint32_t>::iterator it = remote_nodes_involved.begin(); 
			it != remote_nodes_involved.end(); 
			it ++)	
		{
			uint32_t data_size = 0;
			char * data = NULL;
			bool send = _cc_manager->need_prepare_req(*it, data_size, data);
			if (send) {
				resp_expected = true;
				waiting_for_remote = true;
				ATOM_ADD_FETCH(_num_resp_expected, 1);
				Message * msg = new Message(Message::PREPARE_REQ, *it, get_txn_id(), data_size, data);
				send_msg(msg);
			} 
		} 
	} 
	if (rc == WAIT) {
		_lock_wait_start_time = get_sys_clock();
		return rc;
	}
	if (resp_expected) {
		return RCOK;
	} else {
		assert(rc == RCOK);
		return process_2pc_commit_phase(COMMIT);
	}
}

RC 
TxnManager::process_2pc_prepare_req(Message * msg) 
{
#if CC_ALG == MAAT
	((MaaTManager *)_cc_manager)->latch();
#endif
	_txn_state = PREPARING;
#if CC_ALG == MAAT
	((MaaTManager *)_cc_manager)->unlatch();
#endif
	_prepare_start_time = get_sys_clock();
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT
	// TODO. right now, assume prepare is always successful.
	// send PREPARED right away
	RC rc = _cc_manager->process_prepare_req(msg->get_data_size(), msg->get_data(), _resp_size, _resp_data);
#if LOG_ENABLE
	// Logging
	uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
	char log_record[ log_record_size ];
	log_manager->log(log_record_size, log_record);	
#endif
	assert(rc == RCOK || rc == COMMIT);
	Message::Type type = (rc == RCOK)? Message::PREPARED_COMMIT : Message::COMMITTED;
	Message * resp_msg = new Message(type, msg->get_src_node_id(), get_txn_id(), 0, NULL);
	send_msg(resp_msg);
	return rc;
#elif CC_ALG == TICTOC || CC_ALG == F_ONE || CC_ALG == MAAT  \
	|| CC_ALG == IDEAL_MVCC || CC_ALG == NAIVE_TICTOC || CC_ALG == TCM
	_resp_size = 0;
	_resp_data = NULL; 
	RC rc = _cc_manager->process_prepare_req(msg->get_data_size(), msg->get_data(), _resp_size, _resp_data);

	// rc can also be WAIT
	if (rc == RCOK || rc == ABORT || rc == COMMIT) {
		Message::Type type;
		if (rc == RCOK)
			type = Message::PREPARED_COMMIT;
		else if (rc == ABORT)
			type = Message::PREPARED_ABORT;
		else 
			type = Message::COMMITTED;
		Message * resp_msg = new Message(type, msg->get_src_node_id(), get_txn_id(), _resp_size, _resp_data);
		send_msg(resp_msg);
	} 
	if (rc == WAIT)
		waiting_for_lock = true;
#if LOG_ENABLE
	else {
		// Logging
		uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
		char log_record[ log_record_size ];
		log_manager->log(log_record_size, log_record);	
	}
#endif
	return rc;
#endif
}

RC 
TxnManager::continue_prepare_phase()
{
	RC rc = RCOK;
	assert (_txn_state == PREPARING);
	assert(waiting_for_lock);
	waiting_for_lock = false;
	if (!is_sub_txn()) {
		uint32_t num_resp_expected = ATOM_SUB_FETCH(_num_resp_expected, 1);
		if (_cc_manager->is_signal_abort()) {
			_txn_abort = true;
			INC_INT_STATS(num_aborts_signal, 1);
		}
		if (num_resp_expected > 0)
			return RCOK;
		else {
			waiting_for_remote = false;
			rc = _txn_abort? ABORT : COMMIT;
#if LOG_ENABLE
			// Logging
			uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
			char log_record[ log_record_size ];
			log_manager->log(log_record_size, log_record);	
#endif
			return process_2pc_commit_phase(rc);
		}
	} else {
		assert(CC_ALG == TICTOC || CC_ALG == F_ONE);	
		Message::Type type;
		if (_cc_manager->is_signal_abort()) {
			rc = ABORT;
			type = Message::PREPARED_ABORT;
			_cc_manager->abort();
		} else if (CC_ALG == TICTOC && _cc_manager->is_read_only()) {
			rc = COMMIT;
			type = Message::COMMITTED;
			_cc_manager->commit();
		} else {
			rc = RCOK;
			type = Message::PREPARED_COMMIT;
		}
		
#if LOG_ENABLE
		// Logging
		uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
		char log_record[ log_record_size ];
		log_manager->log(log_record_size, log_record);	
#endif		
		send_msg(new Message(type, _src_node_id, get_txn_id(), _resp_size, _resp_data));
		return rc;	
	}
}

RC 
TxnManager::process_2pc_prepare_resp(Message * msg)
{
	RC rc = RCOK;
		rc = RCOK;
	if (msg->get_type() == Message::PREPARED_ABORT)
		rc = ABORT;

	// multiple threads may execute the following code concurrently, if multiple responses
	// are received.
	_cc_manager->process_prepare_resp(rc, msg->get_src_node_id(), msg->get_data());
	if (rc == ABORT) { 
		assert(CC_ALG != WAIT_DIE);
		_txn_abort = true;
		_remote_txn_abort = true;
		// TODO. make aborted_remote_nodes lock free.
		pthread_mutex_lock(&_txn_lock);
		aborted_remote_nodes.insert(msg->get_src_node_id());
		pthread_mutex_unlock(&_txn_lock);
		INC_INT_STATS(int_debug4, 1); // remote abort in prepare phase
		// The following code is actually wrong!!! because a response may be received while 
		// another thread is still scanning remote_nodes_involved (process_2pc_prepare_phase()).  
		// 	   	remote_nodes_involved is not protected elsewhere in the code
		//		must use lock free data structure
		//pthread_mutex_lock(&_txn_lock);
		//remote_nodes_involved.erase(msg->get_src_node_id());
		//pthread_mutex_unlock(&_txn_lock);
	} 
	if (msg->get_type() == Message::COMMITTED) {
		pthread_mutex_lock(&_txn_lock);
		readonly_remote_nodes.insert(msg->get_src_node_id());
		pthread_mutex_unlock(&_txn_lock);
	}
	COMPILER_BARRIER
	uint32_t num_resp_left = ATOM_SUB_FETCH(_num_resp_expected, 1);
	if (num_resp_left > 0) {
		return RCOK;
	} else {
		waiting_for_remote = false;
		if (_txn_abort)
			return process_2pc_commit_phase(ABORT);
		else {
			assert(rc == RCOK);
			return process_2pc_commit_phase(COMMIT);
		}
	}
}
	
RC 
TxnManager::process_2pc_commit_phase(RC rc)
{
	_commit_start_time = get_sys_clock();
#if CC_ALG == TCM
	if (rc == COMMIT)
		rc = ((TCMManager *)_cc_manager)->compute_ts_range();
#endif

	assert(!waiting_for_lock && !waiting_for_remote);
	if (rc == COMMIT) {
		_txn_state = COMMITTING;
	} else if (rc == ABORT) {
		_txn_state = ABORTING;
		_store_procedure->txn_abort();
	} else 
		assert(false);

	// TODO. transaction can already return to the user at this moment. should collect stats.
	// TODO. may move cleanup below sending messages, this can reduce the critical path
	Message::Type type = (rc == COMMIT)? Message::COMMIT_REQ : Message::ABORT_REQ;
	_num_resp_expected = 0;
	bool resp_expected = false;
#if CC_ALG == MAAT
	if (rc == COMMIT)
		((MaaTManager *)_cc_manager)->finalize_commit_ts();
#endif
#if LOG_ENABLE
	// Logging
	if (rc == ABORT) {
		uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
		char log_record[ log_record_size ];
		log_manager->log(log_record_size, log_record);	
	} else if (rc == COMMIT) {
		char * log_record = NULL;
		uint32_t log_record_size = _cc_manager->get_log_record(log_record);
		if (log_record_size > 0) {
			assert(log_record);
			log_manager->log(log_record_size, log_record);
			delete log_record;
		}
	}
#endif
	for (set<uint32_t>::iterator it = remote_nodes_involved.begin(); 
		it != remote_nodes_involved.end(); 
		it ++)	
	{
		if (aborted_remote_nodes.find(*it) != aborted_remote_nodes.end())
			continue;
		if (readonly_remote_nodes.find(*it) != readonly_remote_nodes.end())
			continue;
		uint32_t size = 0;
		char * data = NULL;
		bool send = _cc_manager->need_commit_req(rc, *it, size, data);
		if (send) {
			resp_expected = true;
			_num_resp_expected ++;
			Message * msg = new Message(type, *it, get_txn_id(), size, data);
			send_msg(msg);
		}
	}
	_cc_manager->process_commit_phase_coord(rc);
	if (resp_expected) {
		waiting_for_remote = true;
		return RCOK;
	} else {
		remote_nodes_involved.clear();
		aborted_remote_nodes.clear();
		readonly_remote_nodes.clear();
		_txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
		return rc; 
	}
}

RC 
TxnManager::process_2pc_commit_req(Message * msg)
{
	RC rc;
	if (msg->get_type() == Message::COMMIT_REQ) {
		_txn_state = COMMITTED;
		rc = COMMIT;
	} else if (msg->get_type() == Message::ABORT_REQ) {
		_txn_state = ABORTED; 
		rc = ABORT;
	} else 
		assert(false);
	// Logging
#if LOG_ENABLE
	uint32_t log_record_size = sizeof(_txn_id) + sizeof(rc);
	char log_record[ log_record_size ];
	log_manager->log(log_record_size, log_record);	
#endif

	_cc_manager->process_commit_req(rc, msg->get_data_size(), msg->get_data()); 
	send_msg(new Message(Message::ACK, msg->get_src_node_id(), get_txn_id(), 0, NULL));
	return rc; 
}

RC 
TxnManager::process_2pc_commit_resp()
{
	uint32_t resp_left = ATOM_SUB_FETCH(_num_resp_expected, 1);
	if (resp_left > 0) {
		return RCOK;
	} else {
		waiting_for_remote = false;
		remote_nodes_involved.clear();
		aborted_remote_nodes.clear();
		readonly_remote_nodes.clear();
		if (_txn_state == COMMITTING) {
			_txn_state = COMMITTED;
			return COMMIT;
		} else if (_txn_state == ABORTING) {
			_txn_state = ABORTED;
			return ABORT;
		} else 
			assert(false);
	}
}

void
TxnManager::send_msg(Message * msg)
{
	_net_wait_start_time = get_sys_clock();
	_msg_count[msg->get_type()] ++;
	_msg_size[msg->get_type()] += msg->get_packet_len();
	while (!output_queues[GET_THD_ID]->push((uint64_t)msg)) {
		PAUSE10
	}
	INC_FLOAT_STATS(time_write_output_queue, get_sys_clock() - _net_wait_start_time);
}

void 
TxnManager::handle_local_caching()
{
#if CC_ALG == TICTOC
	assert(is_sub_txn());
	char * data = NULL;
	uint32_t size = _cc_manager->handle_local_caching(data);
	// Local caching resp does not target at a particular txn.
	// So the txn_id is 0, which is a wild card txn 
	Message * resp_msg;
	NEW(resp_msg, Message, Message::LOCAL_COPY_RESP, _src_node_id, 0, size, data);
	send_msg(resp_msg);
#endif
}

RC
TxnManager::process_caching_resp(Message * msg)
{
	assert( CC_ALG == TICTOC );
	// inser the tuples into the database.
	_cc_manager->process_caching_resp(msg->get_src_node_id(), msg->get_data_size(), msg->get_data());
	return RCOK;
}

void
TxnManager::print_state()
{
}
