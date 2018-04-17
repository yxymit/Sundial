#include "row.h"
#include "txn.h"
#include "row_tcm.h"
#include "manager.h"
#include "tcm_manager.h"

#if CC_ALG == TCM

Row_TCM::Row_TCM()
{
	_row = NULL;
	pthread_mutex_init(&_latch, NULL);
	_lock_type = LOCK_NONE;
  	_max_num_waits = g_max_num_waits;
	_upgrading_txn = NULL;
}

Row_TCM::Row_TCM(row_t * row)
	: Row_TCM()
{
	_row = row;
}

void
Row_TCM::latch()
{
	pthread_mutex_lock( &_latch );
}

void
Row_TCM::unlatch()
{
	pthread_mutex_unlock( &_latch );
}

void 
Row_TCM::init(row_t * row) 
{
	_row = row;
	pthread_mutex_init(&_latch, NULL);
	_lock_type = LOCK_NONE;
  	_max_num_waits = g_max_num_waits;
}

RC 
Row_TCM::lock_get(LockType type, TxnManager * txn, char * data) 
{
	RC rc = RCOK;
	latch();
	for (set<LockHolder, CompareHolder>::iterator it = _locking_set.begin();
		it != _locking_set.end(); it ++)
	{
		TCMManager * holder = it->manager;
		TCMManager * requester = (TCMManager *)txn->get_cc_manager();
		// Garbage Collection
		if (holder->state == TxnManager::COMMITTED && holder->early < glob_manager->get_gc_ts()) {
			_locking_set.erase(it);
			continue;
		}
	
		bool both_latched = false;
		while (!both_latched) {
			if (requester->try_latch()) {
				if (holder->try_latch()) { both_latched = true; }
				else requester->unlatch(); 
			}
		}
		if (it->type == LOCK_SH && type == LOCK_EX)
			rc = adjust_timestamps_rw(holder, requester);
		else if (it->type == LOCK_EX && type == LOCK_SH)  
			rc = adjust_timestamps_wr(holder, requester);
		else if (it->type == LOCK_EX && type == LOCK_EX)  
			rc = adjust_timestamps_ww(holder, requester);
		requester->unlatch();
		holder->unlatch();
		if (rc == ABORT) {
			goto finish;
		}
	}
	// The request didn't abort, add it to the locking_set
	_locking_set.insert( LockHolder{ (TCMManager *)txn->get_cc_manager(), type } );
	// TODO. Right now, we do not model multiple data versions here 
	// but instead just read the current data. 
	// For YCSB, this is OK.
	memcpy(data, _row->get_data(), _row->get_tuple_size());
finish:
	unlatch();
	return rc;
}


RC
Row_TCM::lock_release(TxnManager * txn, RC rc) 
{
	latch();
	bool released = false;
	for (std::set<LockHolder>::iterator it = _locking_set.begin();
		 it != _locking_set.end(); it ++) 
	{
		if (it->manager == txn->get_cc_manager()) {
			_locking_set.erase( it );
			released = true;
			break;
		}
	}
	assert(released);
	unlatch();
	return RCOK; 
}


RC 
Row_TCM::adjust_timestamps_rw(TCMManager * holder, TCMManager * requester)
{
	RC rc = RCOK;
	if (!requester->end && !holder->end) {
		uint64_t new_time = glob_manager->get_current_time();
		holder->late = new_time;	
		holder->end = true;
		requester->early = new_time;
	} else if (!requester->end && holder->end) {
		if (holder->late > requester->early)
			requester->early = holder->late;		
	} else if (requester->end && !holder->end) {
		if (requester->late > holder->early) {
			requester->early = requester->late - 1;
			holder->late = requester->late - 1;
			holder->end = true; 
		} else {
			rc = ABORT;
		}
	} else {
		if (requester->early > holder->late) 
		{}
		else if (requester->early <= holder->late 
				 && holder->late <= requester->late) 
		{
			requester->early = holder->late;
		} else if (holder->early <= requester->late
				   && requester->late <= holder->late) 
		{
			if (holder->state == TxnManager::RUNNING) {
				uint64_t new_time = requester->late - 1;
				holder->late = new_time;
				requester->early = new_time;
			}
		} else 
			rc = ABORT;
	}
	return rc;
}

RC 
Row_TCM::adjust_timestamps_wr(TCMManager * holder, TCMManager * requester) 
{
	if (!requester->end && !holder->end) {
		uint64_t new_time = glob_manager->get_current_time();
		holder->early = new_time;
		requester->late = new_time;
		requester->end = true;
	} else if (!requester->end && holder->end) {
		if (requester->early > holder->late) {
			if (holder->state == TxnManager::RUNNING)
				holder->try_abort();
		} else {
			uint64_t new_time = holder->late - 1;
			requester->late = new_time;
			requester->end = true;
			holder->early = new_time;
		}
	} else if (requester->end && !holder->end) {
		if (requester->late < holder->early)
			holder->early = requester->late;
	} else {
		if (requester->early < holder->late) {
			if (holder->state == TxnManager::RUNNING)
				holder->try_abort();				
		} else if (requester->early <= holder->late && holder->late <= requester->late) {
			uint64_t new_time = holder->late - 1;
			requester->late = new_time;
			holder->early = new_time;
		} else if (holder->early <= requester->late && requester->late <= holder->late)
			holder->early = requester->late;
	}
	return RCOK;
}

RC 
Row_TCM::adjust_timestamps_ww(TCMManager * holder, TCMManager * requester)
{
	RC rc = RCOK;
	if (!requester->end && !holder->end) {
		uint64_t new_time = glob_manager->get_current_time();
		requester->early = new_time;
		holder->late = new_time;
		holder->end = true;
		rc = ABORT;
	} else if (!requester->end and holder->end) {
		if (requester->early < holder->late)
			requester->early = holder->late;
		if (holder->state == TxnManager::RUNNING) {
			rc = ABORT;
		}
	} else if (requester->end && !holder->end) {
		if (requester->late > holder->early) {
			uint64_t new_time = requester->late - 1;
			holder->late = new_time;
			holder->end = true;
			requester->early = new_time;
			rc = ABORT;
		} else 
			rc = ABORT;
	} else {
		if (requester->early > holder->late)
			rc = ABORT;
		else if (requester->early <= holder->late && holder->late <= requester->late) {
			requester->early = holder->late;
			rc = ABORT;
		} else if (holder->early <= requester->late && requester->late <= holder->late) {
			requester->early = requester->late - 1;
			holder->late = requester->late - 1;
			rc = ABORT;
		} else 
		 	rc = ABORT;
	}
	return rc;
}

#endif
