#pragma once 

#include "global.h"
#include "helper.h"

class TxnManager;
class row_t;
class INDEX;
class table_t;
class StoreProcedure;
class UnstructuredBuffer;
class RemoteQuery;
class itemid_t; 

class CCManager 
{
public:
	static CCManager * create(TxnManager * txn);

	CCManager(TxnManager * txn);
	virtual ~CCManager() {};

	virtual void 	init();
	
	// For algorithms other than TicToc, we don't care whether the txn is readonly or not.  
	virtual bool	is_read_only() { return false; }
	virtual RC 		register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, 
										   uint32_t table_id) 
					{ assert(false); }
	virtual RC 		register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, 
										   uint32_t table_id, uint32_t &msg_size, char * &msg_data) 
					{ return register_remote_access(remote_node_id, type, key, table_id); }

	virtual RC 		get_row(row_t * row, access_t type, uint64_t key) { assert(false); }
	virtual RC 		get_row(row_t * row, access_t type, char * &data, uint64_t key) = 0;
	
	virtual char * 	get_data(uint64_t key, uint32_t table_id) { assert(false); }
	virtual char * 	get_data(uint32_t table_id) { assert(false); }
	virtual char * 	get_last_data() { assert(false); }

	RC  			row_insert(table_t * table, row_t * row);
	RC	 			row_delete(row_t * row);
	virtual RC 		index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1) { assert(false); }
protected:
	// reserve permission on the index.
	virtual	RC		index_insert(INDEX * index, uint64_t key) { assert(false); }
	virtual RC		index_delete(INDEX * index, uint64_t key) { assert(false); }
public:
	// rc is either COMMIT or Abort.
	// the following function will cleanup the txn. e.g., release locks, etc.     
	virtual void cleanup(RC rc) { assert(false); }

	StoreProcedure * get_store_procedure();	

	////////// for txn waiting /////////////
	virtual bool 	is_txn_ready() = 0; // { assert(false); }
	virtual void 	set_txn_ready() { assert(false); }
	virtual void 	set_txn_ready(RC rc) { assert(false); }
	virtual bool 	is_signal_abort() { assert(false); }
	virtual uint64_t get_priority() { assert(false); }
	//////////////////////////////////////

	// handle response during normal execution
	virtual RC 		process_remote_req(uint32_t size, char * req_data, uint32_t &resp_size, char * &resp_data) { assert(false); }

	virtual void 	add_remote_req_header(UnstructuredBuffer * buffer) {}
	virtual uint32_t 	process_remote_req_header(UnstructuredBuffer * buffer) { return 0; }
	virtual void 	get_resp_data(uint32_t num_queries, RemoteQuery * queries, uint32_t &size, char * &data) { assert(false); }
	virtual void 	get_resp_data(uint32_t &size, char * &data) { assert(false); }
	virtual void 	process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data) {};

	// prepare phase.
	// return value: whether a prepare message needs to be sent
	virtual void 	get_remote_nodes(set<uint32_t> * _remote_nodes) {};
	void 			get_remote_nodes_with_writes(set<uint32_t> * nodes);
	virtual RC 		process_prepare_phase_coord() { return RCOK; }
	virtual bool 	need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data) 
	{ size = 0; data = NULL; return true; };
	virtual RC 		process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data ) { return RCOK; }
	virtual void 	process_prepare_resp(RC rc, uint32_t node_id, char * data) {};
	
	// amend phase
	virtual RC 		process_amend_phase_coord() { return RCOK; }
	virtual bool 	need_amend_req(uint32_t remote_node_id, uint32_t &size, char * &data) { return false; };
	virtual RC 		process_amend_req(char * data) { return RCOK; }

	// commit phase
	virtual void 	process_commit_phase_coord(RC rc) = 0;
	virtual bool	need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data) { return true; }
	virtual void 	process_commit_req(RC rc, uint32_t size, char * data) = 0;
	virtual void 	abort() = 0;
	virtual void 	commit() { assert(false); }

	// [TICTOC} handle local caching
	virtual uint32_t handle_local_caching(char * &data) { return 0; };
	virtual void 	process_caching_resp(uint32_t node_id, uint32_t size, char * data) { return; }

	virtual uint32_t get_log_record(char *& record) { assert(false); }
protected:
	volatile uint32_t	_num_lock_waits; 
	uint64_t		_timestamp;
	
	struct RemoteNodeInfo {
		uint32_t node_id;
		bool 	 has_write;
	};
	map<uint32_t, RemoteNodeInfo> _remote_node_info;
	void 			add_remote_node_info(uint32_t node_id, bool is_write);


	// TODO. different CC algorithms should have different ways to handle index consistency.
	// For now, just ignore index concurrency control. 
	// Since this is not a problem for YCSB and TPCC.
	virtual RC		commit_insdel(); 
	struct Access {
		access_t 	type;
		uint64_t 	key;
		uint32_t 	table_id;
		row_t * 	row;	// row == NULL for remote accesses
		uint32_t	home_node_id;
	};
	
	struct IndexAccess {
		IndexAccess() {
			index = NULL;
			manager = NULL;
			locked = false;
			rows = NULL;
		};
		uint64_t 	key;
		INDEX * 	index;
		access_t 	type;
		ROW_MAN * 	manager;
		bool		locked;
		set<row_t *> * rows;
	};

	TxnManager * 		_txn;
	struct InsertOp {
		table_t * table;
		row_t * row; 
	};
	vector<InsertOp> _inserts;
	vector<row_t *>	_deletes;
	// remote query processing
	bool			_restart;

	// Stats
	uint64_t 		_time_in_cc_man;
private:
	void 			index_modify(access_t type, INDEX * index, uint64_t key, char * data);
};
