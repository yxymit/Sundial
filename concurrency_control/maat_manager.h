#pragma once 

#include "cc_manager.h"
#include "txn.h"
#include <chrono>
#include <thread>

#if CC_ALG == MAAT
typedef  uint32_t size_type;
typedef uint64_t TxnID;
class MaaTManager : public CCManager
{
public:
	MaaTManager(TxnManager * txn);
	~MaaTManager() {};

	RC 			get_row(row_t * row, access_t type, uint64_t key);
	RC 			get_row(row_t * row, access_t type, char * &data, uint64_t key);
	char * 		get_data(uint64_t key, uint32_t table_id);
	RC 			register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id);

	RC 			index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit = -1);
	RC 			index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1);
	RC			index_insert(INDEX * index, uint64_t key);
	RC			index_delete(INDEX * index, uint64_t key);

	RC 			validate();
	void 		cleanup(RC rc);
	
	// normal execution
	uint32_t 	get_resp_size();
	void 		get_resp_data(char * data);

	void 		get_resp_data(uint32_t &size, char * &data);
	//void get_resp_data(uint32_t &data_size, char * &data); 
	void 		process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

	// prepare phase
	RC 			handle_ts_ranges();
	RC 			process_prepare_phase_coord(); 
	RC 			continue_prepare_phase_coord();
	void 		get_remote_nodes(set<uint32_t> * _remote_nodes);

	bool 		need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data);
	RC 			process_prepare_req( uint32_t size, char * data, uint32_t &resp_size, char * &resp_data );
	void 		process_prepare_resp(RC rc, uint32_t node_id, char * data);
	
	// commit phase
	void 		finalize_commit_ts();
	void 		process_commit_phase_coord(RC rc);
	RC			commit_insdel(); 
	bool 		need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data);
	void 		process_commit_req(RC rc, uint32_t size, char * data); 
	void 		abort();

	// handle WAIT_DIE validation 
	void 		set_ts(uint64_t timestamp) { _timestamp = timestamp; }
	uint64_t 	get_priority() { return _timestamp; } 
	void 		set_txn_ready(RC rc);
	bool 		is_txn_ready();
	bool 		is_signal_abort() { return _signal_abort; }
	
	void 		latch() { pthread_mutex_lock(_latch); }
	void 		unlatch() { pthread_mutex_unlock(_latch); }
    pthread_mutex_t * 	_latch;   	 // to guarantee read/write consistency
    uint64_t 	_refcount;
private:
	struct AccessMaaT : Access {
		bool 		locked;
		uint64_t	wts;
		uint64_t 	rts;
		uint32_t 	data_size;
		char * 		local_data;
		uint32_t	home_node_id;
	};

	struct IndexAccessMaaT : IndexAccess {
		uint64_t wts;
		uint64_t rts;
	};
	static bool 	compare(AccessMaaT * ac1, AccessMaaT * ac2);
	AccessMaaT * 	find_access(uint64_t key, uint32_t table_id, vector<AccessMaaT> * set);
	
	vector<AccessMaaT>			_access_set;
	vector<AccessMaaT>			_remote_set;
	AccessMaaT * 				_last_access;
	vector<IndexAccessMaaT>		_index_access_set;

	// For the coordinator, _min_commit_ts is the final commit time
	// For subordinator, it is the minimal commit ts based on local info
	uint64_t _min_commit_ts;
	uint64_t _max_commit_ts;
	set<TxnManager*>  txnsRacingThisTxn;
	set<TxnManager*>  txnsBeforeThisTxn;
	set<TxnManager*>  txnsAfterThisTxn;

	////////// Only used in the coordinator 
	struct RemoteNodeInfo {
		uint32_t node_id;
		bool 	 readonly;
		uint64_t min_commit_ts;
		uint64_t max_commit_ts;
		uint64_t commit_ts_upper_bound; // NOT in use
	};
	vector<RemoteNodeInfo> _remote_node_info;	
	///////////////////////////////////////
	void split_read_write_set();
	RC lock_write_set();
	RC lock_read_set();
	void unlock_write_set(RC rc);
	void unlock_read_set();
	RC validate_read_set(uint64_t commit_ts);
	void commit(uint64_t commit_ts);

	bool 			_write_copy_ptr;
	bool			_ts_lock;
	static bool 	_pre_abort;
	bool 			_validation_no_wait;
	bool			_atomic_timestamp;
	uint64_t 		_max_wts;
	RC				validate_MaaT();

	// For VALIDATION_LOCK == WAIT_DIE
	// txn start time, serves as the priority of the txn
	uint64_t		_timestamp;
	bool 			_signal_abort;
};

#define MAN(txn) ((MaaTManager*)(txn)->get_cc_manager())

#endif
