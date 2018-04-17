#pragma once 

#include "cc_manager.h"

class LockManager : public CCManager 
{
public:
	LockManager(TxnManager * txn);
	~LockManager() {}	
	
	RC 			get_row(row_t * row, access_t type, uint64_t key);
	RC 			get_row(row_t * row, access_t type, char * &data, uint64_t key);
	char * 		get_data( uint64_t key, uint32_t table_id);
	RC 			register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id);

	RC 			index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit=-1);
	RC 			index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit=-1);
	RC			index_insert(INDEX * index, uint64_t key);
	RC			index_delete(INDEX * index, uint64_t key);

	void 		cleanup(RC rc);

	void 		add_remote_req_header(UnstructuredBuffer * buffer);
	uint32_t 	process_remote_req_header(UnstructuredBuffer * buffer);
	void 		get_resp_data(uint32_t &size, char * &data);
	void 		process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

	void 		set_ts(uint64_t timestamp) { _timestamp = timestamp; }
	uint64_t 	get_ts() { return _timestamp; }
	uint64_t 	get_priority() { return _timestamp; }
	bool 		is_txn_ready();
	void 		set_txn_ready(RC rc);
	
	// Prepare Phase
	RC 			process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data);
	
	// commit phase
	void 		process_commit_phase_coord(RC rc);
	bool 		need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data);
	void 		process_commit_req(RC rc, uint32_t size, char * data);
	RC 			commit_insdel();
	void 		abort();
private:
	struct AccessLock : Access {
		AccessLock() { data = NULL; data_size = 0; }
		char *		data;	// original data.
		uint32_t 	data_size;
	};
	
	AccessLock * find_access(uint64_t key, uint32_t table_id, vector<AccessLock> * set);
	
	vector<AccessLock>		_access_set;
	vector<AccessLock>		_remote_set;
	vector<IndexAccess>		_index_access_set;
	AccessLock * 			_last_access;

	bool 					_lock_ready;
};
