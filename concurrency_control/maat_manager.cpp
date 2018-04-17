/*

Features/Improvements we have added for MaaT:

1. Multi-threading support for MaaT
2. Client is placed inside server, and N clients work at the same time
3. Before sending out validation requests, client performs a pre-abort check.
4. Does not include inequality information inside PREPARE_REQ
5. Does not include UR, UW chain information inside REMOTE_ACCESS_RESP

Bugs we have fixed for MaaT:

1. Added garbage collection (based on reference count, described in the paper)
2. Avoid adding the transaction itself into Before/After/Racing set.
3. Remove wrong statements of updating timestamp with a transaction ID
4. Avoid breaking consistency in multi-threading support.
5. Record both last reader and last writer
6. 

*/
#include <algorithm>
#include "maat_manager.h"
#include "row.h"
#include "row_maat.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"
#include "workload.h"
#include "index_btree.h"
#include "index_hash.h"
#include "table.h"
#include "store_procedure.h"
#include "catalog.h"

#if CC_ALG == MAAT
const unsigned long long TXN_ID_UNKNOWN = (1LL << 62); // quick hack
#define INF_SHRINK_LENTH 100
#define TIMESTAMP_INFINITY ((1LL<<62)-1LL)
// should be 1<<63, however the compiler panics about interger overflow
#define AVG_NUM_CONFLICTS 5 // from MaaT code

bool 
MaaTManager::compare(AccessMaaT * ac1, AccessMaaT * ac2)
{
	return ac1->row->get_primary_key() < ac2->row->get_primary_key();
}

MaaTManager::MaaTManager(TxnManager * txn)
	: CCManager(txn)
{
	// TODO. make these parameters static, initialize them in a static function 
	_validation_no_wait = true;

	_max_wts = 0;
	_write_copy_ptr = false; 
	_atomic_timestamp = false;
    _refcount = 1;	

	_timestamp = glob_manager->get_ts(GET_THD_ID);
	_num_lock_waits = 0;
	_signal_abort = false;
	_min_commit_ts = 0;
	_max_commit_ts = TIMESTAMP_INFINITY;
	_ts_lock = false;
	_latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
	new(_latch) pthread_mutex_t;

	pthread_mutex_init( _latch, NULL );

	if (!_txn->is_sub_txn()) {
		glob_manager->get_max_cts();
		// arbitrary parameter.
		if (_min_commit_ts > 10)
			_min_commit_ts -= 10;  // TODO: what this logic is for?
	}
	
}


void 
MaaTManager::cleanup(RC rc)
{
#if DEBUG_REFCOUNT
	ATOM_COUT(_txn->get_txn_id() << " called cleanup  " << rc << endl);
#endif
	for (vector<AccessMaaT>::iterator it = _access_set.begin();
		it != _access_set.end(); it ++)
	{
		(*it).row->manager->latch();
		(*it).row->manager->_uncommitted_reader.erase(_txn);
		(*it).row->manager->_uncommitted_writer.erase(_txn);
		(*it).row->manager->unlatch();
		delete it->local_data;
	}

	for (set<TxnManager*>::iterator it = txnsBeforeThisTxn.begin(); it 
		!= txnsBeforeThisTxn.end(); it++)
	{
		MaaTManager* maatman = (MaaTManager*)((*it)->get_cc_manager());
		uint64_t rf =  ATOM_SUB_FETCH(maatman->_refcount, 1);
		M_ASSERT(rf >= 0, "rf=%ld", rf);
#if DEBUG_REFCOUNT
		ATOM_COUT("1 DEC REFCOUNT of " << (*it)->get_txn_id() << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#endif
		if (rf == 0)
		{
			txn_table->remove_txn((*it));
			delete (*it); 
		}
	}

	for (set<TxnManager*>::iterator it = txnsAfterThisTxn.begin(); it 
		!= txnsAfterThisTxn.end(); it++)
	{
		MaaTManager* maatman = (MaaTManager*)((*it)->get_cc_manager());
		uint64_t rf =  ATOM_SUB_FETCH(maatman->_refcount, 1);
		M_ASSERT(rf >= 0, "rf=%ld", rf);
#if DEBUG_REFCOUNT
		ATOM_COUT("2 DEC REFCOUNT of " << (*it)->get_txn_id() << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#endif
		if (rf == 0)
		{
			txn_table->remove_txn((*it));
			delete (*it); 
		}
	}
	for (set<TxnManager*>::iterator it = txnsRacingThisTxn.begin(); it 
		!= txnsRacingThisTxn.end(); it++)
	{
		MaaTManager* maatman = (MaaTManager*)((*it)->get_cc_manager());
		uint64_t rf =  ATOM_SUB_FETCH(maatman->_refcount, 1);
		M_ASSERT(rf >= 0, "rf=%ld", rf);
#if DEBUG_REFCOUNT
		ATOM_COUT("3 DEC REFCOUNT of " <<(*it)->get_txn_id() << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#endif
		if (rf == 0)
		{
			txn_table->remove_txn((*it));
			DELETE(TxnManager, *it);
		}
	}

	for (vector<AccessMaaT>::iterator it = _remote_set.begin();
		it != _remote_set.end(); it ++)
	{
		if (it->local_data)
			FREE(it->local_data, it->data_size);
	}
	for (vector<IndexAccessMaaT>::iterator it = _index_access_set.begin(); 
		it != _index_access_set.end(); it ++ ) 
	{
		it->manager->latch();
		it->manager->_uncommitted_reader.erase(_txn);
		it->manager->_uncommitted_writer.erase(_txn);
		it->manager->unlatch();
		if (it->rows)
			delete it->rows;
	}
	if (rc == ABORT)
		for (auto ins : _inserts) 
			delete ins.row;

	txnsAfterThisTxn.clear();
	txnsBeforeThisTxn.clear();
	txnsRacingThisTxn.clear();
	_min_commit_ts = 0;
	_max_commit_ts = TIMESTAMP_INFINITY;

	_access_set.clear();
	_remote_set.clear();
	_index_access_set.clear();
}

RC
MaaTManager::get_row(row_t * row, access_t type, uint64_t key)
// TODO: what is this function for? need to change the interface so we can have transaction ID.
{
	uint64_t tt = get_sys_clock();
	RC rc = RCOK;
	// DEBUG
	for (vector<AccessMaaT>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) 
		assert(it->row != row);

	AccessMaaT access;
	access.home_node_id = g_node_id; 
	access.type = type;
	access.row = row;
	access.key = key; 
	access.data_size = row->get_tuple_size();
	access.table_id = row->get_table()->get_table_id();
	access.local_data = NULL;

	// COMMENT: for both reads and writes, need to make a local copy of the data.
	// Since there is no lock, locked is ignored. 
	access.local_data = (char *) MALLOC(row->get_tuple_size()); // how does get_tuple_size work?

	Row_maat *rowman = row->manager;
	rowman->latch();
	
	if(type == RD) {
		rc = row->manager->read(_txn, access.local_data, access.wts, access.rts, false);
		for (auto txnman : rowman->_uncommitted_writer) {
			if (txnsAfterThisTxn.find(txnman) != txnsAfterThisTxn.end()) continue;
			txnsAfterThisTxn.insert(txnman);			
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
#if DEBUG_REFCOUNT
			uint64_t rf = ATOM_ADD_FETCH(maatman->_refcount, 1);
			ATOM_COUT("4 INC REFCOUNT of " << maatman << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#else
			uint64_t ref = ATOM_ADD(maatman->_refcount, 1);
			assert(ref != 0);
#endif
		}
		rowman->unlatch();

		// This needs to be atomic. Can we do better? 
		// TODO. change to atomic CAS. 
		pthread_mutex_lock(_latch);
		_min_commit_ts = max(_min_commit_ts,
				access.wts);
		pthread_mutex_unlock(_latch);
	} else{
		rc = rowman->write(_txn, access.local_data, access.wts, access.rts, false);
		for (auto txnman : rowman->_uncommitted_reader) {
			if (txnsBeforeThisTxn.find(txnman) != txnsBeforeThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsBeforeThisTxn.insert(txnman);
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
#if DEBUG_REFCOUNT
			uint64_t rf = ATOM_ADD_FETCH(maatman->_refcount, 1);
			ATOM_COUT("5 INC REFCOUNT of " << maatman << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#else
			uint64_t ref = ATOM_ADD(maatman->_refcount, 1);
			assert(ref != 0);
#endif
		}
		for (auto txnman : rowman->_uncommitted_writer) {
			if (txnsAfterThisTxn.find(txnman) != txnsAfterThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsAfterThisTxn.insert(txnman);			
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
			assert(ATOM_ADD(maatman->_refcount, 1) > 0);
		}
		for (auto txnman : rowman->_uncommitted_writer) {
			if (txnsRacingThisTxn.find(txnman) != txnsRacingThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsRacingThisTxn.insert(txnman);
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
#if DEBUG_REFCOUNT
			uint64_t rf = ATOM_ADD_FETCH(maatman->_refcount, 1);
			ATOM_COUT("6 INC REFCOUNT of " << maatman << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#else
			assert(ATOM_ADD(maatman->_refcount, 1) > 0);
#endif
		}
		rowman->unlatch();
		pthread_mutex_lock(_latch);
		_min_commit_ts = max(_min_commit_ts, access.rts + 1);
		pthread_mutex_unlock(_latch);
	}

	_access_set.push_back(access);
	_last_access = &(*_access_set.rbegin());
	INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
	
	return rc; 
}

RC			
MaaTManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
	RC rc = get_row(row, type, key);
	if (rc == RCOK) 
		data = _last_access->local_data;
	return rc;
}

RC
MaaTManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, 
									   uint32_t table_id) 
{
	AccessMaaT ac;
	_remote_set.push_back(ac);
	AccessMaaT * access = &(*_remote_set.rbegin());
	_last_access = access;
	assert(remote_node_id != g_node_id);
	
	access->home_node_id = remote_node_id;
	access->row = NULL;
	access->type = type;
	access->key = key;
	access->table_id = table_id;
	access->local_data = NULL;
	return LOCAL_MISS;
}

RC
MaaTManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
	assert(type != WR); 
	RC rc = RCOK;
	IndexAccessMaaT * access = NULL;
	for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
		access = &_index_access_set[i];
		if (access && access->index == index && access->key == key && access->type == type) 
			return RCOK;
	}

	IndexAccessMaaT ac;
	_index_access_set.push_back(ac);
	access = &(*_index_access_set.rbegin());
	access->key = key;
	access->index = index;
	access->type = type;
	Row_maat * manager = index->index_get_manager(key);
	access->manager = manager;
	
	if(type == RD) {
		rc = manager->read(_txn, NULL, access->wts, access->rts, false);
		assert(rc == RCOK);
		for (auto txnman : manager->_uncommitted_writer) {
			if (txnsAfterThisTxn.find(txnman) != txnsAfterThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsAfterThisTxn.insert(txnman);			
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
			assert(ATOM_ADD(maatman->_refcount, 1) > 0);
		}
		set<row_t *> * rows = index->read(key);
		if (rows) { 
			if (rows->size() > limit) {
				set<row_t *>::iterator it = rows->begin();
				advance(it, 1);
				access->rows = new set<row_t *>( rows->begin(), it );
			} else 
				access->rows = new set<row_t *>( *rows );
		}
		manager->unlatch();
		
		pthread_mutex_lock(_latch);
		_min_commit_ts = max(_min_commit_ts, access->wts);
		pthread_mutex_unlock(_latch);
	} else{
		assert(type == INS || type == DEL);
		rc = manager->write(_txn, NULL, access->wts, access->rts, false); 
		for (auto txnman : manager->_uncommitted_reader) {
			if (txnsBeforeThisTxn.find(txnman) != txnsBeforeThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsBeforeThisTxn.insert(txnman);
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
			assert(ATOM_ADD(maatman->_refcount, 1) > 0);
		}
		for (auto txnman : manager->_uncommitted_writer) {
			if (txnsAfterThisTxn.find(txnman) != txnsAfterThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsAfterThisTxn.insert(txnman);			
			MaaTManager * maatman = (MaaTManager*)(txnman->get_cc_manager());
			assert(ATOM_ADD(maatman->_refcount, 1) > 0);
		}
		for (auto txnman : manager->_uncommitted_writer) {
			if (txnsRacingThisTxn.find(txnman) != txnsRacingThisTxn.end()) continue;
			if (txnman == _txn) continue;
			txnsRacingThisTxn.insert(txnman);
			MaaTManager * man = (MaaTManager *)txnman->get_cc_manager();
			assert(ATOM_ADD(man->_refcount, 1) > 0);
		}
		manager->unlatch();

		pthread_mutex_lock(_latch);
		_min_commit_ts = max(_min_commit_ts, access->rts + 1);
		pthread_mutex_unlock(_latch);
	}
	return RCOK;
}

RC
MaaTManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
	uint64_t tt = get_sys_clock();
	RC rc = index_get_permission(RD, index, key, limit);
	assert(rc == RCOK);
	rows = _index_access_set.rbegin()->rows;
	INC_FLOAT_STATS(index, get_sys_clock() - tt);
	return RCOK;
}

RC
MaaTManager::index_insert(INDEX * index, uint64_t key)
{
	uint64_t tt = get_sys_clock();
	RC rc = index_get_permission(INS, index, key);
	INC_FLOAT_STATS(index, get_sys_clock() - tt);
	return rc;
}

RC
MaaTManager::index_delete(INDEX * index, uint64_t key)
{
	uint64_t tt = get_sys_clock();
	RC rc = index_get_permission(DEL, index, key);
	INC_FLOAT_STATS(index, get_sys_clock() - tt);
	return rc;
}

char * 
MaaTManager::get_data(uint64_t key, uint32_t table_id)
{
	for (vector<AccessMaaT>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) 
		if (it->key == key && it->table_id == table_id)
			return it->local_data;
	
	for (vector<AccessMaaT>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++) 
		if (it->key == key && it->table_id == table_id)
			return it->local_data;

	assert(false);
	return NULL;
}

RC MaaTManager::continue_prepare_phase_coord() {
	// should not be called.
	assert(false);
	return RCOK;
}

void
MaaTManager::get_resp_data(uint32_t &size, char * &data)
{
	// Format:
	//	| n | (key, table_id, wts, rts, tuple_size, data) * n
	UnstructuredBuffer buffer;
	uint32_t num_tuples = _access_set.size(); 
	buffer.put( &num_tuples );
	for (auto access : _access_set) {
		buffer.put( &access.key );
		buffer.put( &access.table_id );
		buffer.put( &access.wts );
		buffer.put( &access.rts );
		buffer.put( &access.data_size );
		buffer.put( access.local_data, access.data_size );
	}
	size = buffer.size();
	data = new char [size];
	memcpy(data, buffer.data(), size);
}

MaaTManager::AccessMaaT *
MaaTManager::find_access(uint64_t key, uint32_t table_id, vector<AccessMaaT> * set)
{
	for (vector<AccessMaaT>::iterator it = set->begin(); it != set->end(); it ++) {
		if (it->key == key && it->table_id == table_id)
			return &(*it);
	}
	return NULL;
}

void 
MaaTManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
	// return data format:
	//		| n | (key, table_id, wts, rts, tuple_size, data) * n 
	// store the remote tuple to local access_set.
	UnstructuredBuffer buffer(resp_data);
	uint32_t num_tuples;
	buffer.get( &num_tuples );
	assert(num_tuples > 0);
	for (uint32_t i = 0; i < num_tuples; i++) {
		uint64_t key;
		uint32_t table_id;
		buffer.get( &key );
		buffer.get( &table_id );
		AccessMaaT * access = find_access(key, table_id, &_remote_set);
		assert(access && node_id == access->home_node_id);
		buffer.get( &access->wts );
		buffer.get( &access->rts );
		buffer.get( &access->data_size );
		
		char * data = NULL;  
		buffer.get( data, access->data_size );
		access->local_data = new char [access->data_size];
		memcpy(access->local_data, data, access->data_size);

		pthread_mutex_lock( _latch );
		if(access->type == RD) 
			_min_commit_ts = max(_min_commit_ts, access->wts);
		else if (access->type == WR) 
			_min_commit_ts = max(_min_commit_ts, access->rts);
		pthread_mutex_unlock( _latch );
	}
}

RC 
MaaTManager::handle_ts_ranges()
{
	for(set<TxnManager*>::iterator iter=txnsAfterThisTxn.begin();
		iter!=txnsAfterThisTxn.end();iter++)
	{
		if(txnsRacingThisTxn.find(*iter) == txnsRacingThisTxn.end()) continue;
		TxnManager* conflictingTxnRecord = *iter;
		MaaTManager *maatman = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());
		
		uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);
		M_ASSERT(rf > 0, "rf = %ld", rf);
#if DEBUG_REFCOUNT
		ATOM_COUT("7 DEC REFCOUNT of " << *iter << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#endif
		txnsRacingThisTxn.erase(*iter);
	}

	for(set<TxnManager*>::iterator iter=txnsBeforeThisTxn.begin();
			iter!=txnsBeforeThisTxn.end();iter++){
		if(txnsRacingThisTxn.find(*iter) == txnsRacingThisTxn.end()) continue;
		TxnManager* conflictingTxnRecord = *iter;
		MaaTManager *maatman = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());
		uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);
		M_ASSERT(rf > 0, "rf=%ld", rf);
#if DEBUG_REFCOUNT
		ATOM_COUT("8 DEC REFCOUNT of " << *iter << " to " << rf << ", BY " << _txn->get_txn_id() << endl);
#endif
		txnsRacingThisTxn.erase(*iter);
	}
	// TODO. ignore write only txns for now.
	assert(txnsRacingThisTxn.empty());
	// We send messages to all servers accessed by the txn, even those with no
	// constraints at all (e.g., servers where the txn performs blind writes
	// with no conflicts).
	
	if(_txn->get_txn_state() == TxnManager::ABORTED || _txn->get_txn_state() == TxnManager::ABORTING){
		assert(false);// return ERROR;  // duplicate
	}else if(_txn->get_txn_state() == TxnManager::COMMITTED || _txn->get_txn_state() == TxnManager::COMMITTING){
		assert(false);  // duplicate
	}


	if (_max_commit_ts >= TIMESTAMP_INFINITY) {  // we need to shrink the infinity right-end to a normal range
		//_max_commit_ts = _min_commit_ts + INF_SHRINK_LENTH;  // INF_SHRINK_LENGTH is an artificial parameter
	}

	// Current bounds are those obtained by not changing the bounds of any other
	// transaction. Feasible bounds are those obtained by utilizing all the room
	// made available by conflicting transactions.
	uint64_t currentBegin, currentEnd, feasibleBegin, feasibleEnd;
	currentBegin = feasibleBegin = _min_commit_ts;
	currentEnd = feasibleEnd = _max_commit_ts;

	// Potential begins and ends of the timestamp range, and how many txns
	// are saved from abortion by not crossing these limits.
	map<uint64_t, unsigned> niceBegins;
	map<uint64_t, unsigned> niceEnds;

	// Txn records of conflicting txns that haven't started their commit
	// phases, thus their timestamp ranges can be adjusted preemptively.
	vector<TxnManager *> adjustableTxnsBeforeThisTxn;
	vector<TxnManager *> adjustableTxnsAfterThisTxn;

	//for(unsigned int i=0;i<nTxnsAfterThisTxn;i++){
	for(set<TxnManager*>::iterator ita = txnsAfterThisTxn.begin(); ita != txnsAfterThisTxn.end(); ++ita) {
		TxnManager* conflictingTxnRecord = *ita;
	        			
		if(conflictingTxnRecord == NULL) assert(false); //continue;
		MaaTManager *maatman = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());

		TxnManager::State conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		if(conflictingTxnStatus == TxnManager::COMMITTING
			|| conflictingTxnStatus == TxnManager::COMMITTED 
			|| conflictingTxnStatus == TxnManager::PREPARING)
		{
			uint64_t conflictingTxnTimeRangeMin = maatman->_min_commit_ts;
			if (conflictingTxnTimeRangeMin == 0)
			{
				INC_INT_STATS(int_aborts_maat_resolve_failure, 1);
				return ABORT;
			}
			currentEnd = min(currentEnd, conflictingTxnTimeRangeMin - 1);
			feasibleEnd = min(feasibleEnd, conflictingTxnTimeRangeMin - 1);
		} else if(conflictingTxnStatus == TxnManager::RUNNING) {
			//|| conflictingTxnStatus == TxnManager::NETWORK_WAIT 
			//|| conflictingTxnStatus == TxnManager::LOCK_WAIT){
			uint64_t conflictingTxnTimeRangeMax =
					((MaaTManager*)(conflictingTxnRecord->get_cc_manager()))->_max_commit_ts; // TODO: notice rangeMax here. To compress it into 1-length txn
			if(conflictingTxnTimeRangeMax != TIMESTAMP_INFINITY)  // if not inifinity
				niceEnds[conflictingTxnTimeRangeMax-1]++; //TODO: notice here.
			adjustableTxnsAfterThisTxn.push_back(conflictingTxnRecord);
		} // TODO: mark
	}

	for(set<TxnManager*>::iterator ita = txnsBeforeThisTxn.begin(); ita != txnsBeforeThisTxn.end(); ++ita) {
		TxnManager* conflictingTxnRecord = *ita;
	        			
		if(conflictingTxnRecord == NULL) assert(false); //continue;
		MaaTManager *maatman = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());

		TxnManager::State conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		if(conflictingTxnStatus == TxnManager::COMMITTING
			|| conflictingTxnStatus == TxnManager::COMMITTED 
			|| conflictingTxnStatus == TxnManager::PREPARING)
		{
			uint64_t conflictingTxnTimeRangeMax = maatman->_max_commit_ts;
			if (conflictingTxnTimeRangeMax == TIMESTAMP_INFINITY)
			{
				INC_INT_STATS(int_aborts_maat_resolve_failure, 1);
				return ABORT;
			}
			currentBegin = max(currentBegin, conflictingTxnTimeRangeMax + 1);
			feasibleBegin = max(feasibleBegin, conflictingTxnTimeRangeMax + 1);
		} else if(conflictingTxnStatus == TxnManager::RUNNING) {
			//|| conflictingTxnStatus == TxnManager::NETWORK_WAIT 
			//|| conflictingTxnStatus == TxnManager::LOCK_WAIT){
			uint64_t conflictingTxnTimeRangeMin =
					maatman->_min_commit_ts; // TODO: notice rangeMax here. To compress it into 1-length txn
			if(conflictingTxnTimeRangeMin != 0)
				niceBegins[conflictingTxnTimeRangeMin+1]++;
			adjustableTxnsBeforeThisTxn.push_back(conflictingTxnRecord);
		} // TODO: mark
	}

	for(set<TxnManager*>::iterator ita = txnsRacingThisTxn.begin(); ita != txnsRacingThisTxn.end(); ++ita) {
		TxnManager* conflictingTxnRecord = *ita;
	        			
		if(conflictingTxnRecord == NULL) assert(false); //continue;
		MaaTManager *maatman = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());

		TxnManager::State conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		if(conflictingTxnStatus == TxnManager::COMMITTING
			|| conflictingTxnStatus == TxnManager::COMMITTED 
			|| conflictingTxnStatus == TxnManager::PREPARING)
		{
			uint64_t conflictingTxnTimeRangeMax = maatman->_max_commit_ts;
			if (conflictingTxnTimeRangeMax == TIMESTAMP_INFINITY)
			{
				INC_INT_STATS(int_aborts_maat_resolve_failure, 1);
				return ABORT;
			}
			currentBegin = max(currentBegin, conflictingTxnTimeRangeMax + 1);
			feasibleBegin = max(feasibleBegin, conflictingTxnTimeRangeMax + 1);
		} else if(conflictingTxnStatus == TxnManager::RUNNING) {
		//|| conflictingTxnStatus == TxnManager::NETWORK_WAIT || conflictingTxnStatus == TxnManager::LOCK_WAIT){
			uint64_t conflictingTxnTimeRangeMax =
					maatman->_max_commit_ts; // TODO: notice rangeMax here. To compress it into 1-length txn
			if(conflictingTxnTimeRangeMax != TIMESTAMP_INFINITY)
				niceEnds[conflictingTxnTimeRangeMax-1]++; // and we place us right beside it
			adjustableTxnsAfterThisTxn.push_back(conflictingTxnRecord);
		} // TODO: mark
	}

	if(feasibleBegin > feasibleEnd){
		INC_INT_STATS(int_aborts_maat_resolve_no_feasibles, 1);
		return ABORT;  // then we need to abort this transaction since no feasible timestamp exists.
	}

	// Now we know for sure that the transaction can commit
	//_txn->set_txn_state(TxnManager::COMMITTING);  // taken care by txn.cpp

	// Compute a nice time range that avoids aborting other running transactions
	// whenever possible
	niceBegins[feasibleBegin]++;
	niceEnds[feasibleEnd]++;
	map<uint64_t, unsigned>::reverse_iterator iterBegin = niceBegins.rbegin();
	map<uint64_t, unsigned>::iterator iterEnd = niceEnds.begin();
	while(iterBegin->first > feasibleEnd)
		iterBegin++;
	while(iterEnd->first < feasibleBegin)
		iterEnd++;
	while(iterBegin->first > iterEnd->first){
		map<uint64_t, unsigned>::reverse_iterator nextBegin=iterBegin;
		map<uint64_t, unsigned>::iterator nextEnd=iterEnd;
		nextBegin++;
		nextEnd++;
		uint64_t diffBegin = iterBegin->first - nextBegin->first;
		uint64_t diffEnd = nextEnd->first - iterEnd->first;
		unsigned countBegin = iterBegin->second;
		unsigned countEnd = iterEnd->second;
		if(countBegin<countEnd || (countBegin==countEnd && diffBegin>diffEnd)){
			iterBegin++;  // greedy alg: count > diff
		} else {
			iterEnd++;
		}
	}
	uint64_t niceFeasibleBegin = iterBegin->first;  // now iterBegin < iterEnd
	uint64_t niceFeasibleEnd = iterEnd->first;

	// Compute the actual commit range
	uint64_t actualBegin, actualEnd;
	if(currentBegin <= currentEnd){  // can't see the difference between current and feasible
		if(currentEnd<niceFeasibleBegin){
			actualBegin = niceFeasibleBegin;
			actualEnd = min(niceFeasibleEnd,
					actualBegin + AVG_NUM_CONFLICTS);
		}else if(currentBegin>niceFeasibleEnd){
			actualEnd = niceFeasibleEnd;
			actualBegin = max(niceFeasibleBegin,
					actualEnd - AVG_NUM_CONFLICTS);
		}else{
			actualBegin = max(currentBegin, niceFeasibleBegin);
			actualEnd = min(currentEnd, niceFeasibleEnd);
		}
	}else{
		if(niceFeasibleEnd - niceFeasibleBegin <= AVG_NUM_CONFLICTS){
			actualBegin = niceFeasibleBegin;
			actualEnd = niceFeasibleEnd;
		}else{
			//Can do better optimizations than this, but this is enough for now
			uint64_t margin = (niceFeasibleEnd - niceFeasibleBegin - AVG_NUM_CONFLICTS) / 2;
			actualBegin = niceFeasibleBegin + margin;
			actualEnd = niceFeasibleEnd - margin;
		}
	}
	// TODO: decrease refcount for adjustables.
	// TODO: figure out what the difference of current / feasible is
	// Adjust the time ranges of conflicting transactions to avoid overlaps
	for(vector<TxnManager*>::iterator iter=adjustableTxnsBeforeThisTxn.begin();
			iter!=adjustableTxnsBeforeThisTxn.end(); iter++)
	{
		TxnManager* conflictingTxnRecord = *iter;
		MaaTManager* target_maat = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());
		TxnManager::State conflictingTxnStatus; 
		do conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		while(conflictingTxnStatus == TxnManager::RUNNING
			&& pthread_mutex_trylock(target_maat->_latch)!=0); 
		conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		if (conflictingTxnStatus != TxnManager::RUNNING) {
			pthread_mutex_unlock(target_maat->_latch);
			INC_INT_STATS(int_aborts_maat_adjust_others_failure, 1);
			return ABORT;
		} else {
			target_maat->_max_commit_ts =
				min(target_maat->_max_commit_ts, actualBegin - 1);
			pthread_mutex_unlock(target_maat->_latch);
		}
	}
	for(vector<TxnManager*>::iterator iter=adjustableTxnsAfterThisTxn.begin();
			iter!=adjustableTxnsAfterThisTxn.end(); iter++)
	{
		TxnManager* conflictingTxnRecord = *iter;
		MaaTManager* target_maat = (MaaTManager*)(conflictingTxnRecord->get_cc_manager());
		
		TxnManager::State conflictingTxnStatus; 
		do conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		while(conflictingTxnStatus == TxnManager::RUNNING
			&& pthread_mutex_trylock(target_maat->_latch)!=0);
		conflictingTxnStatus = conflictingTxnRecord->get_txn_state();
		if (conflictingTxnStatus != TxnManager::RUNNING) {
			pthread_mutex_unlock(target_maat->_latch);
			INC_INT_STATS(int_aborts_maat_adjust_others_failure, 1);
			return ABORT;
		} else {
			target_maat->_min_commit_ts =
				max(target_maat->_min_commit_ts, actualEnd + 1);
			pthread_mutex_unlock(target_maat->_latch);
		}
	}
	// we execute the verfication part on coordinator as well.
	_min_commit_ts = actualBegin;
	_max_commit_ts = actualEnd;

	if(_min_commit_ts > _max_commit_ts)
	{
		INC_INT_STATS(int_aborts_maat_empty_interval_after_adjustment, 1);
		return ABORT;
	}
	// TODO: prepare messages in coord phase.
	return RCOK;

}

RC 
MaaTManager::process_prepare_phase_coord()
{
	// Account for the case when the txn reads a data object then updates it
	// txnsBeforeThisTxn.erase(_txn->get_txn_id());
	// txnsAfterThisTxn.erase(_txn->get_txn_id());
	// txnsRacingThisTxn.erase(_txn->get_txn_id());
	// Erase from txnsRacingThisTxn any txns that are already in
	// txnsAfterThisTxn or txnsBeforeThisTxn
	return handle_ts_ranges();	
}

void
MaaTManager::get_remote_nodes(set<uint32_t> * _remote_nodes)
{
	// empty function to keep the interface unified
	return ;
}

bool
MaaTManager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
	// PREPARE_REQ Format:
	//		| txn_ID | txnsAfterThisTxn | txnsBeforeThisTxn | txnsRacingThisTxn;  // but all the 3 sets should be empty
	return true;
}
	
RC 
MaaTManager::process_prepare_req( uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
	RC rc = handle_ts_ranges();
	if (rc == ABORT) {
		cleanup(ABORT);
		return ABORT;
	}
	
	// prepare the message
	// Format
	// 	 | _min_commit_ts | _max_commit_ts |
	resp_size = 2 * sizeof(uint64_t);
	resp_data = (char *) MALLOC(resp_size); //new char [size];
	memcpy(resp_data, &_min_commit_ts, sizeof(uint64_t));
	memcpy(resp_data + sizeof(uint64_t), &_max_commit_ts, sizeof(uint64_t));
	return RCOK;
}

void 
MaaTManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
	// format
	// | timeRangeMin | timeRangeMax |
	if(data)
	{	
		// if it is not a rage abort
		assert(rc == RCOK);
		//int offset = 0;
		uint64_t timeRangeMin, timeRangeMax;
		UnstructuredBuffer buffer(data);
		buffer.get( &timeRangeMin );
		buffer.get( &timeRangeMax );
		_min_commit_ts = max(timeRangeMin, _min_commit_ts);
		_max_commit_ts = min(timeRangeMax, _max_commit_ts);

		if(_min_commit_ts > _max_commit_ts){
			// TODO this is a hack. But it's ok._
			_txn->_txn_abort = true; // set a abort flag because we cannot modify rc.
		}
	}
}

void
MaaTManager::finalize_commit_ts()
{
	const unsigned int MAX_TIME_RANGE = 5;
	if(_max_commit_ts == TIMESTAMP_INFINITY)  // TODO: infinite mark
		_max_commit_ts = _min_commit_ts + MAX_TIME_RANGE;
	uint64_t commitTimestamp = (_min_commit_ts + _max_commit_ts) / 2;
	_min_commit_ts = _max_commit_ts = commitTimestamp;
}

void 
MaaTManager::process_commit_phase_coord(RC rc)
{

	// we assume that txn.cpp will do this for us.
	///////
	if (rc == COMMIT)
		commit(_min_commit_ts);
	else { 
		abort();
	}
}

void 
MaaTManager::commit(uint64_t commit_ts)
{
	uint64_t timestamp = commit_ts;
	_min_commit_ts = _max_commit_ts = timestamp;
	commit_insdel();
	for(vector<AccessMaaT>::iterator ita = _access_set.begin(); ita != _access_set.end(); ++ita)
	{
		if ((*ita).type == WR) {
			ita->row->manager->write_data(ita->local_data, timestamp);
		} else if ((*ita).type == RD) 
			(*ita).row->manager->extend_rts(timestamp); 
	}
	for (vector<IndexAccessMaaT>::iterator it = _index_access_set.begin(); 
		it != _index_access_set.end(); it ++ ) 
	{
		it->manager->latch();
		if (it->type != RD) {
			M_ASSERT( it->manager->_rts < _min_commit_ts, "[[FAIL]] TXN=%ld, cts=%ld, _rts=%ld\n", 
				_txn->get_txn_id(), _min_commit_ts, it->manager->_rts);
			it->manager->_wts = it->manager->_rts = _min_commit_ts;
		}
		it->manager->unlatch();
	}
	cleanup(COMMIT);
}

RC
MaaTManager::commit_insdel() {
	for (auto ins : _inserts) {
		row_t * row = ins.row;
		row->manager->_wts = _min_commit_ts;
		row->manager->_rts = _min_commit_ts;
		set<INDEX *> indexes;
		ins.table->get_indexes( &indexes );
		for (auto idx : indexes) {
			uint64_t key = row->get_index_key(idx);
			idx->insert(key, row);
		}
	}
	// handle deletes
	for (auto row : _deletes) {
		set<INDEX *> indexes;
		row->get_table()->get_indexes( &indexes );
		for (auto idx : indexes) 
			idx->remove( row );
		for (vector<AccessMaaT>::iterator it = _access_set.begin(); 
			 it != _access_set.end(); it ++) 
		{
			if (it->row == row) {
				_access_set.erase(it);
				break;
			}
		}
		row->manager->delete_row( _min_commit_ts );
	}
	return RCOK;

}


bool 
MaaTManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
	// Still need to send commit req to read only nodes. 
	// Since we need to update the timestamp range for that txn.
	// Format 
	// 	| commit_ts |
	if (rc == ABORT)
		return true;
	// Format
	//   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
	assert(rc == COMMIT);
	UnstructuredBuffer buffer;
	uint32_t num_writes = 0;
	for (vector<AccessMaaT>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++) 
		if ((it)->home_node_id == node_id && (it)->type == WR) {
			buffer.put( &(it)->key );
			buffer.put( &(it)->table_id );
			buffer.put( &(it)->data_size );
			buffer.put( (it)->local_data, (it)->data_size );
			num_writes ++;
		}
	buffer.put_front( &num_writes );
	buffer.put_front( &_min_commit_ts );
	size = buffer.size();
	data = new char [size];
	memcpy(data, buffer.data(), size);
	return true;	
}

void 
MaaTManager::process_commit_req(RC rc, uint32_t size, char * data)
{
	if (rc == COMMIT) { 
		// Format
		//   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
		UnstructuredBuffer buffer(data);
		uint64_t commit_ts;
		buffer.get( &commit_ts );
		_min_commit_ts = _max_commit_ts = commit_ts;
		if (size > sizeof(uint64_t)) {
			uint32_t num_writes; 
			buffer.get( &num_writes );
			for (uint32_t i = 0; i < num_writes; i ++) {
				uint64_t key;
				uint32_t table_id;
				uint32_t size = 0;
				char * tuple_data = NULL;
				buffer.get( &key );
				buffer.get( &table_id );
				buffer.get( &size );
				buffer.get( tuple_data, size );
				AccessMaaT * access = find_access( key, table_id, &_access_set );
				memcpy( access->local_data, tuple_data, size );
			}
		}
		commit( commit_ts );
	} else
		abort();
}

void 
MaaTManager::abort()
{
	cleanup(ABORT);
}

bool 
MaaTManager::is_txn_ready()
{ 
	return _num_lock_waits == 0 || _signal_abort; 
}

void 
MaaTManager::set_txn_ready(RC rc)
{
	// this function can be called by multiple threads concurrently
	if (rc == RCOK)
		ATOM_SUB_FETCH(_num_lock_waits, 1);
	else {
		_signal_abort = true;
	}
}
#endif
