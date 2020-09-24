#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "manager.h"
#include "lock_manager.h"
#include "f1_manager.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT

#if CC_ALG == WAIT_DIE
bool
Row_lock::Compare::operator() (const LockEntry &en1, const LockEntry &en2) const
{
    // returns true if the first argument goes before the second argument
    // begin(): txn with the smallest ts (oldest)
    // end(): txn with the largest ts (youngest)
    return LOCK_MAN(en1.txn)->get_ts() < LOCK_MAN(en2.txn)->get_ts();
   //return true;
}
#endif

Row_lock::Row_lock()
{
    _row = NULL;
    pthread_mutex_init(&_latch, NULL);
    //_lock_type = LOCK_NONE;
    _max_num_waits = g_max_num_waits;
    _upgrading_txn = NULL;
}

Row_lock::Row_lock(row_t * row)
    : Row_lock()
{
    _row = row;
}

void
Row_lock::latch()
{
    pthread_mutex_lock( &_latch );
}

void
Row_lock::unlatch()
{
    pthread_mutex_unlock( &_latch );
}

void
Row_lock::init(row_t * row)
{
    exit(0);
    _row = row;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
    _max_num_waits = g_max_num_waits;
}
#if CC_ALG == WAIT_DIE
RC Row_lock:: wait_die_check(LockEntry* entry,LockType type,bool need_latch){
    int flag=0;
    if(_locking_set.size()==0){
        return RCOK;
    }
    //check if existing lock has conflicts
    //assert the locking set only contains some sh locks or 1 ex lock
    if(conflict_lock(type, _locking_set.begin()->type)){
     if(_locking_set.begin()->type==LOCK_EX){
         //only one can grab the ex lock
         assert(_locking_set.size()==1);
     }
     //check timestamp   
    for (std::set<LockEntry>::iterator it = _locking_set.begin(); it != _locking_set.end(); it ++){
        LockEntry * eit = NULL;
        eit = (LockEntry *) &(*it);
        //if current txn is younger than any lock holder
        if(LOCK_MAN(entry->txn)->get_ts()>LOCK_MAN(eit->txn)->get_ts()){
            return ABORT;
        }
    }
    
    //has conflict, but no need to abort, so can wait
    pthread_cond_init(&(entry->cv),NULL);
    if(!need_latch){
        latch();
    }
    _waiting_set.insert(*entry);
    printf("txn sleep\n");
    pthread_cond_wait(&(entry->cv),&_latch);
    printf("get out of wait\n");
    if(!need_latch){
        unlatch();
    }
    return RCOK;
    }
    //no conflict
    else{
        //a very special case, a "younger lock already waiting for ex  but another older txn comes in and grabs the shared lock"
        if(type==LOCK_SH){
            //if there is someone waiting and sh has no conflict, then it must be an ex
            if(!_waiting_set.empty()){
            for (std::set<LockEntry>::iterator it = _waiting_set.begin(); it != _waiting_set.end(); it ++){
                LockEntry * eit = NULL;
                eit = (LockEntry *) &(*it);
                //assert the waiting one's type is ex
                assert(eit->type==LOCK_EX);
                if(LOCK_MAN(entry->txn)->get_ts()<LOCK_MAN(eit->txn)->get_ts())
                    return ABORT;
            }
            }
        }
        //no special case, no conflict, can add to the locking set
        return RCOK;
    }
}
#endif
RC
Row_lock::lock_get(LockType type, TxnManager * txn, bool need_latch)
{
   RC rc = RCOK;
    if (need_latch) latch();
    //latch();
#if CC_ALG == NO_WAIT
    LockEntry * entry = NULL;
    for (std::set<LockEntry>::iterator it = _locking_set.begin(); it != _locking_set.end(); it ++)
        if (it->txn == txn)
            entry = (LockEntry *) &(*it);
    if (entry) { // the txn is alreayd a lock owner
        if (entry->type != type) {
            assert(type == LOCK_EX && entry->type == LOCK_SH);
            if (_locking_set.size() == 1)
                entry->type = type;
            else
                rc = ABORT;
        }
    } else {
        if (!_locking_set.empty() && conflict_lock(type, _locking_set.begin()->type))
            rc = ABORT;
        else
            _locking_set.insert( LockEntry {type, txn} );
    }
#elif CC_ALG == WAIT_DIE
    //check if txn already exists
    LockEntry * entry = NULL;
    for (std::set<LockEntry>::iterator it = _locking_set.begin(); it != _locking_set.end(); it ++)
        if (it->txn == txn)
            entry = (LockEntry *) &(*it);
    if(entry){
        if(entry->type!=type){
            assert(type == LOCK_EX && entry->type == LOCK_SH);
            //if current txn is the only txn holding a lock, just upgrade it
            if (_locking_set.size() == 1)
                entry->type = type;
            else{//if there are other txns holding the lock
                rc = wait_die_check(entry,type,need_latch);
                //get out of wait or no wait at all
                if(rc==RCOK){
                    entry->type = type;
                }
            }    
        }
    }
    else{//a new lock entry is needed, not exist
    LockEntry entry1 = LockEntry {type, txn};
    rc = wait_die_check(&entry1, type,need_latch);
    if(rc==RCOK){
        _locking_set.insert(entry1);
    }
    }


    /*LockEntry * entry = NULL;
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++) {
        if (it->txn == txn)
            entry = (Row_lock::LockEntry *)&(*it);
    }
    if (entry != NULL) { // This txn is already holding a lock.
        if (entry->type == type) {} // the txn request the same lock type again, ignore
        else {
            // The transaction must be upgrading a SH-lock to EX-lock
            assert(entry->type == LOCK_SH && type == LOCK_EX);
            for (std::set<LockEntry>::iterator it = _locking_set.begin(); 
                 it != _locking_set.end(); it ++) {
                // If another transaction is already waiting for upgrade, must
                // abort due to conflict.
                if (it->type == LOCK_UPGRADING)
                    rc = ABORT;
            }
            if (rc == RCOK) {
                // If the transaction is the only lock owner, acquire LOCK_EX
                if (_locking_set.size() == 1)
                    entry->type = LOCK_EX;
                else { // otherwise wait for other (SH) owners to release.
                    entry->type = LOCK_UPGRADING;
                    rc = WAIT;
                }
            }
        }
    } else { // This is a new transaction
        if (_locking_set.empty() || (!conflict_lock(type, _locking_set.begin()->type)
                                     && _waiting_set.empty())) {
            // no conflict, directly acquire the lock
            _locking_set.insert( LockEntry {type, txn} );
            //if (_row && txn->get_txn_id() <= 5)
            //    printf("txn %ld locks tuple %ld. type=%d\n",
            //           txn->get_txn_id(), _row->get_primary_key(), _locking_set.begin()->type);
        } else {
            // if the txn conflicts with an older txn, abort
            for (auto entry : _locking_set)
                if (LOCK_MAN(entry.txn)->get_ts() < LOCK_MAN(txn)->get_ts())
                    rc = ABORT;
            for (auto entry : _waiting_set)
                if (LOCK_MAN(entry.txn)->get_ts() < LOCK_MAN(txn)->get_ts()
                    && conflict_lock(type, entry.type))
                    rc = ABORT;
            // Otherwise can wait
            if (rc != ABORT) {
                //if (_row && txn->get_txn_id() <= 5)
                //    printf("--txn %ld waits for tuple %ld\n", txn->get_txn_id(), _row->get_primary_key());
                _waiting_set.insert( LockEntry {type, txn} );
                txn->_start_wait_time = get_sys_clock();
                rc = WAIT;
            } //else {
                //if (_row && txn->get_txn_id() <= 5)
                //    printf("--txn %ld (%ld) fails to lock/wait for tuple %ld\n",
                //           txn->get_txn_id(), LOCK_MAN(txn)->get_ts(), _row->get_primary_key());
            //}
        }
    }*/
#else    
#endif
    if (need_latch) unlatch();
    //unlatch();
   // printf("grab return\n");
   if(rc==ABORT){
       //printf("txn aborted\n");
   }
   else{
    //printf("grab a lock\n");
   }
   
    return rc;
}

RC
Row_lock::lock_release(TxnManager * txn, RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    latch();
  #if CC_ALG == NO_WAIT
    //printf("txn=%ld releases this=%ld\n", txn->get_txn_id(), (uint64_t)this);
    LockEntry entry = {LOCK_NONE, NULL};
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++) {
        if (it->txn == txn) {
            entry = *it;
            assert(entry.txn);
            _locking_set.erase(it);
            break;
        }
    }
    #if CONTROLLED_LOCK_VIOLATION
    // NOTE
    // entry.txn can be NULL. This happens because Row_lock manager locates in
    // each bucket of the hash index. Records with different keys may map to the
    // same bucket and therefore share the manager. They will all call lock_release()
    // during commit. Namely, one txn may call lock_release() multiple times on
    // the same Row_lock manager. For now, we simply ignore calls except the
    // first one.
    if (rc == COMMIT && entry.txn) {
        // DEBUG
        /*if (!entry.txn) {
            cout << "txn_id=" << txn->get_txn_id() << ", _row=" << (int64)_row << endl;
            for (auto en : _locking_set)
                cout << "en.txn_id=" << en.txn->get_txn_id() << ", en.type=" << en.type << endl;
        }
        assert(entry.txn);*/
        _weak_locking_queue.push_back( LockEntry {entry.type, txn} );
        for (auto en : _weak_locking_queue) {
            if (en.txn == entry.txn)
                break;
            if ( conflict_lock(entry.type, en.type) ) {
                // if the current txn has any pending dependency, incr_semaphore
                txn->dependency_semaphore->incr();
                break;
            }
        }
    }
    #endif

  #elif CC_ALG == WAIT_DIE
    LockEntry * entry = NULL;
    // remove from locking set
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++){
        if (it->txn == txn) {
            //entry = *it;
            _locking_set.erase(it);
            break;
        }
        }
    //if waiting set is not empty, we can check if we can wake it up
    if(!_waiting_set.empty()){
        //printf("try to wake up!!!\n");
        //if locking set is empty or no conflict between waiting set's head and locking set's head
        if(_locking_set.empty()||!conflict_lock(_waiting_set.begin()->type, _locking_set.begin()->type)){
            //if waiting set's head is ex
            if(_waiting_set.begin()->type==LOCK_EX){
                std::set<LockEntry>::iterator it = _waiting_set.begin();
                entry = (LockEntry *) &(*it);
                

                //wake 1 ex wait
                printf("wake up an ex\n");
                int a =pthread_cond_signal(&(entry->cv));
                //printf("return is %d\n",a);
                _waiting_set.erase(it);

            }//all we can wake up multiple SH lock threads
            else if(_waiting_set.begin()->type==LOCK_SH){
                for (std::set<LockEntry>::iterator it = _waiting_set.begin();it != _waiting_set.end(); it ++){
                    if(it->type==LOCK_SH){
                        entry = (LockEntry *) &(*it);
                        printf("wake up an sh\n");
                        int o =pthread_cond_signal(&(entry->cv));
                        //printf("return is %d\n",o);
                        _waiting_set.erase(it);
                    }
                    else{
                        break;
                    }

         }
            }
        }
    }
    

    /*LockEntry entry {LOCK_NONE, NULL};
    // remove from locking set
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++)
        if (it->txn == txn) {
            entry = *it;
            _locking_set.erase(it);
            break;
        }

    #if CONTROLLED_LOCK_VIOLATION
    //if (_row && txn->get_txn_id() <= 5)
    //    printf("entry.txn=%lx, type=%d\n", (uint64_t)entry.txn, entry.type);
    if (rc == COMMIT) {
        assert(entry.txn);
        if (!_weak_locking_queue.empty())
            assert(_weak_locking_queue.front().type == LOCK_EX);
        if (!_weak_locking_queue.empty() || entry.type == LOCK_EX) {
            _weak_locking_queue.push_back( LockEntry {entry.type, txn} );
            // mark that the txn has one more unsolved dependency
            LockManager * lock_manager = (LockManager *) txn->get_cc_manager();
            lock_manager->increment_dependency();
            //if (_row)
            //    printf("txn %ld retires to weak queue. tuple=%ld\n", txn->get_txn_id(), _row->get_primary_key());
        }
    }
    #endif
    // remove from waiting set
    for (std::set<LockEntry>::iterator it = _waiting_set.begin();
         it != _waiting_set.end(); it ++)
        if (it->txn == txn) {
            _waiting_set.erase(it);
            break;
        }

    // try to upgrade LOCK_UPGRADING to LOCK_EX
    if (_locking_set.size() == 1 && _locking_set.begin()->type == LOCK_UPGRADING) {
        LockEntry * entry = (LockEntry *) &(*_locking_set.begin());
        entry->type = LOCK_EX;
        entry->txn->set_txn_ready(RCOK);
    }
    // try to move txns from waiting set to locking set
    bool done = false;
    while (!done) {
        std::set<LockEntry>::reverse_iterator rit = _waiting_set.rbegin();
        if (rit != _waiting_set.rend() &&
            (_locking_set.empty()
             || !conflict_lock(rit->type, _locking_set.begin()->type)))
        {
            _locking_set.insert( LockEntry {rit->type, rit->txn} );
            //if (_row && txn->get_txn_id() <= 5)
            //    printf("--txn %ld wakes up txn %ld\n", txn->get_txn_id(), rit->txn->get_txn_id());
            rit->txn->set_txn_ready(RCOK);
            //_waiting_set.erase( rit );
            _waiting_set.erase( --rit.base() );
        } else
            done = true;
    }*/
  #endif
    unlatch();
   // printf("release return\n");
    return RCOK;
}

#if CONTROLLED_LOCK_VIOLATION
// In the current CLV architecture, if a txn calls lock_cleanup(), it must be
// the first txn in the _weak_locking_queue; we simply dequeue it.
// However, the complication comes when some readonly txns depend on a
// dequeueing txn. In this case, we need to check whether all the dependency
// information of the readonly txn has been cleared and commit it if so.
RC
Row_lock::lock_cleanup(TxnManager * txn) //, std::set<TxnManager *> &ready_readonly_txns)
{
    latch();

    //assert(!_weak_locking_queue.empty());
    // Find the transaction in the _weak_locking_queue and remove it.
    auto it = _weak_locking_queue.begin();
    for (; it != _weak_locking_queue.end(); it ++) {
        if (it->txn == txn)
            break;
    }
    // txn does not exist in the _weak_locking_queue when two index nodes map to
    // the same hash bucket. Only one entry is inserted into _weak_locking_queue.
    // Cleanup for the second index node will miss.
    if ( it != _weak_locking_queue.end() ) {
        LockType type = it->type;
        if (type == LOCK_EX) assert( it == _weak_locking_queue.begin() );
        if (type == LOCK_SH)
            for (auto it2 = _weak_locking_queue.begin(); it2 != it; it2 ++)
                assert( it2->type == LOCK_SH);
        _weak_locking_queue.erase( it );

        // notify dependent transactions
        // If the new leading lock is LOCK_EX, wake it up.
        // Else if the removed txn has LOCK_EX, wake up leading txns with LOCK_SH
        if (_weak_locking_queue.front().type == LOCK_EX)
            _weak_locking_queue.front().txn->dependency_semaphore->decr();
        else if (type == LOCK_EX) {
            for (auto entry : _weak_locking_queue) {
                if (entry.type == LOCK_SH)
                    entry.txn->dependency_semaphore->decr();
                else
                    break;
            }
        }
    }

    unlatch();
    return RCOK;
}
#endif

bool Row_lock::conflict_lock(LockType l1, LockType l2)
{
    if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else if (l1 == LOCK_UPGRADING && l2 == LOCK_UPGRADING)
        return true;
    else
        return false;
}

#endif
