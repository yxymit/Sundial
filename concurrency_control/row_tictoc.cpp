#include "row_tictoc.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "tictoc_manager.h"
#include "manager.h"
#include "stdlib.h"
#include "table.h"

#if CC_ALG==TICTOC

#if MULTI_VERSION
int Row_tictoc::_history_num = 30;
#endif

#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
bool
Row_tictoc::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}
#endif

Row_tictoc::Row_tictoc(row_t * row)
{
    _row = row;
    _latch = new pthread_mutex_t;
    _blatch = false;
    pthread_mutex_init( _latch, NULL );
    _wts = 0;
    _rts = 0;
    _ts_lock = false;
    _num_remote_reads = 0;
  #if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
      _max_num_waits = g_max_num_waits;
    _lock_owner = NULL;
  #endif
#if TICTOC_MV
    _hist_wts = 0;
#endif
    _deleted = false;
    _delete_timestamp = 0;
}

RC
Row_tictoc::read(TxnManager * txn, char * data,
                 uint64_t &wts, uint64_t &rts, bool latch, bool remote)
{
    if (latch)
        this->latch();
    wts = _wts;
    rts = _rts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());

    if (txn->is_sub_txn())
        _num_remote_reads ++;
#if ENABLE_LOCAL_CACHING
    if (_row && remote) {
    #if RO_LEASE
        uint64_t max_rts = _row->get_table()->get_max_rts();
        if (max_rts > _rts)
            rts = _rts = max_rts;
    #endif
    }
#endif
    if (latch)
        unlatch();
    return RCOK;
}

RC
Row_tictoc::write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch)
{
    RC rc = RCOK;
    assert(OCC_WAW_LOCK);
#if OCC_LOCK_TYPE == NO_WAIT
    if (_ts_lock)
        return ABORT;
#endif
    if (latch)
        pthread_mutex_lock( _latch );
      if (!_ts_lock) {
        _ts_lock = true;
        _lock_owner = txn;
    } else if (_lock_owner != txn) {
#if OCC_LOCK_TYPE == NO_WAIT
        rc = ABORT;
#else
          assert(OCC_LOCK_TYPE == WAIT_DIE);
        M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(),
                    "txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
        // txn has higher priority, should wait.
        assert (MAN(txn)->get_priority() != MAN(_lock_owner)->get_priority());
        if (_waiting_set.size() < _max_num_waits
            && MAN(txn)->get_priority() < MAN(_lock_owner)->get_priority())
        {
            _waiting_set.insert(txn);
            rc = WAIT;
            txn->_start_wait_time = get_sys_clock();
        } else
            rc = ABORT;
#endif
    }
    if (rc == RCOK) {
        wts = _wts;
        rts = _rts;
    }
    if (latch)
        pthread_mutex_unlock( _latch );
    return rc;
}

RC
Row_tictoc::update(char * data, uint64_t wts, uint64_t rts)
{
    if (wts > _wts) {
        pthread_mutex_lock( _latch );
        _wts = wts;
        _rts = rts;
        _row->set_data(data);
        pthread_mutex_unlock( _latch );
    }
    return RCOK;
}

void
Row_tictoc::update_rts(uint64_t rts)
{
    pthread_mutex_lock( _latch );
    if (rts > _rts)
        _rts = rts;
    pthread_mutex_unlock( _latch );
}

void
Row_tictoc::latch()
{
    pthread_mutex_lock( _latch );
}

void
Row_tictoc::unlatch()
{
    pthread_mutex_unlock( _latch );
}

void
Row_tictoc::write_data(char * data, ts_t wts)
{
#if ATOMIC_WORD
    // TODO. asserts can be removed for performance.
    assert(_ex_lock);
    assert(wts > _wts);

    _wts = wts | LOCK_BIT; // the tuple is being modified.
    COMPILER_BARRIER
    _rts = wts;
    _row->copy(data);
    COMPILER_BARRIER
    _wts = wts;                // wts/rts and data are consistent.
#else
    latch();
  #if TRACK_LAST
    #if MULTI_VERSION
    --_last_ptr;
    if(_last_ptr<0) _last_ptr += _history_num;
    _lastrts_array[_last_ptr] = _rts;
    _lastwts_array[_last_ptr] = _wts;
    #else
    _lastwts = _wts;
    _lastrts = _rts;
    #endif
  #endif
    if (_deleted)
        assert(wts < _delete_timestamp);

    _wts = wts;
    _rts = wts;
    _row->copy(data);
    unlatch();

  #if UPDATE_TABLE_TS
    _row->get_table()->update_max_wts(_wts);
  #endif
#endif
}

void
Row_tictoc::update_ts(uint64_t cts)
{
    latch();
    assert(cts > _rts);
    _wts = cts;
    _rts = cts;
    unlatch();
}

bool
Row_tictoc::try_renew(ts_t rts)
{
    assert(false);
    if (rts < _wts) {
        if (rts + 1 < _wts)
            INC_INT_STATS(int_possibMVCC, 1);  // multiple version in between.
        return false;
    }
    bool success = false;
    pthread_mutex_lock( _latch );
    if (rts > _rts) {
        _rts = rts;
        success = true;
    }
    pthread_mutex_unlock( _latch );
    return success;
}

bool
Row_tictoc::try_renew(ts_t wts, ts_t rts, ts_t &new_rts)
{
#if ATOMIC_WORD
    uint64_t v = _ts_word;
    uint64_t lock_mask = (WRITE_PERMISSION_LOCK)? WRITE_BIT : LOCK_BIT;
    if ((v & WTS_MASK) == wts && ((v & RTS_MASK) >> WTS_LEN) >= rts - wts)
        return true;
    if (v & lock_mask)
        return false;
  #if TICTOC_MV
      COMPILER_BARRIER
      uint64_t hist_wts = _hist_wts;
    if (wts != (v & WTS_MASK)) {
        if (wts == hist_wts && rts < (v & WTS_MASK)) {
            return true;
        } else {
            return false;
        }
    }
  #else
    if (wts != (v & WTS_MASK))
        return false;
  #endif

    ts_t delta_rts = rts - wts;
    if (delta_rts < ((v & RTS_MASK) >> WTS_LEN)) // the rts has already been extended.
        return true;
    bool rebase = false;
    if (delta_rts >= (1 << RTS_LEN)) {
        rebase = true;
        uint64_t delta = (delta_rts & ~((1 << RTS_LEN) - 1));
        delta_rts &= ((1 << RTS_LEN) - 1);
        wts += delta;
    }
    uint64_t v2 = 0;
    v2 |= wts;
    v2 |= (delta_rts << WTS_LEN);
    while (true) {
        uint64_t pre_v = __sync_val_compare_and_swap(&_ts_word, v, v2);
        if (pre_v == v)
            return true;
        v = pre_v;
        if (rebase || (v & lock_mask) || (wts != (v & WTS_MASK)))
            return false;
        else if (rts < ((v & RTS_MASK) >> WTS_LEN))
            return true;
    }
    assert(false);
    return false;
#else
    if (wts != _wts) {
        if(_wts < rts)
        {
            INC_INT_STATS(int_inevitable, 1);
        }

        if(_wts == rts + 1) {
            INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
        }
        else {
            INC_INT_STATS(int_possibMVCC, 1);
#if TRACK_LAST
#if MULTI_VERSION
#else
            uint64_t & lrts = _lastrts;
            uint64_t & lwts = _lastwts;
            if(lwts == wts && lrts >= rts) {
                INC_INT_STATS(int_saved_by_hist, 1);  // count the number
                return true; // someone else extended rts for us and we don't need to abort;
            }
#endif
#endif
            INC_INT_STATS(int_aborts_rs1, 1);  // The latest version is right behind our version.
        }
        return false;
    }
    latch();
    if (_deleted) {
        bool success = (wts == _wts && rts < _delete_timestamp);
        unlatch();
        return success;
    }
    assert(!_deleted);
    if (_ts_lock) {
        INC_INT_STATS(int_aborts_rs3, 1);  // Locked by others
        // TODO. even if _ts_lock == true (meaning someone may write to the tuple soon)
        // should check the lower bound of upcoming wts. Maybe we can still renew the current rts without
        // hurting the upcoming write.
        pthread_mutex_unlock( _latch );
        return false;
    }

    if (wts != _wts) {
        if(_wts < rts)
        {
            INC_INT_STATS(int_inevitable, 1);
        }

        if(_wts == rts + 1) {
            INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
        }
        else {
            INC_INT_STATS(int_possibMVCC, 1);
#if TRACK_LAST
#if MULTI_VERSION
            int current_ptr = 0;
            while(current_ptr < _history_num && _lastwts_array[(_last_ptr + current_ptr) % _history_num] > wts)
                ++ current_ptr;
            if(current_ptr < _history_num && _lastrts_array[(_last_ptr + current_ptr) % _history_num] >= rts) // means found an lwts==wts
            {
                INC_INT_STATS(int_saved_by_hist, 1);  // count the number
                return true; // someone else extended rts for us and we don't need to abort;
            }
#else
            uint64_t & lrts = _lastrts;
            uint64_t & lwts = _lastwts;
            if(lwts == wts && lrts >= rts) {
                INC_INT_STATS(int_saved_by_hist, 1);  // count the number
                return true; // someone else extended rts for us and we don't need to abort;
            }
#endif
#endif
            INC_INT_STATS(int_aborts_rs1, 1);  // The latest version is right behind our version.
        }
        pthread_mutex_unlock( _latch );
        return false;
    }
      new_rts = _rts;
    if (rts > _rts) {
        _rts = rts;
        new_rts = rts;
    }
#if ENABLE_LOCAL_CACHING
    if (_row) {
    #if RO_LEASE
        uint64_t max_rts = _row->get_table()->get_max_rts();
        if (max_rts > _rts)
            new_rts = _rts = max_rts;
    #endif
    }
#endif

    pthread_mutex_unlock( _latch );
    return true;
#endif
}

bool
Row_tictoc::renew(ts_t wts, ts_t rts, ts_t &new_rts) // Renew without lock checking
{
#if LOCK_ALL_BEFORE_COMMIT
#if ATOMIC_WORD
    uint64_t v = _ts_word;
    uint64_t lock_mask = (WRITE_PERMISSION_LOCK)? WRITE_BIT : LOCK_BIT;
    if ((v & WTS_MASK) == wts && ((v & RTS_MASK) >> WTS_LEN) >= rts - wts)
        return true;
    if (v & lock_mask)
        return false;
  #if TICTOC_MV
      COMPILER_BARRIER
      uint64_t hist_wts = _hist_wts;
    if (wts != (v & WTS_MASK)) {
        if (wts == hist_wts && rts < (v & WTS_MASK)) {
            return true;
        } else {
            return false;
        }
    }
  #else
    if (wts != (v & WTS_MASK))
        return false;
  #endif

    ts_t delta_rts = rts - wts;
    if (delta_rts < ((v & RTS_MASK) >> WTS_LEN)) // the rts has already been extended.
        return true;
    bool rebase = false;
    if (delta_rts >= (1 << RTS_LEN)) {
        rebase = true;
        uint64_t delta = (delta_rts & ~((1 << RTS_LEN) - 1));
        delta_rts &= ((1 << RTS_LEN) - 1);
        wts += delta;
    }
    uint64_t v2 = 0;
    v2 |= wts;
    v2 |= (delta_rts << WTS_LEN);
    while (true) {
        uint64_t pre_v = __sync_val_compare_and_swap(&_ts_word, v, v2);
        if (pre_v == v)
            return true;
        v = pre_v;
        if (rebase || (v & lock_mask) || (wts != (v & WTS_MASK)))
            return false;
        else if (rts < ((v & RTS_MASK) >> WTS_LEN))
            return true;
    }
    assert(false);
    return false;
#else
#if TICTOC_MV
    if (wts < _hist_wts)
        return false;
#else
    if (wts != _wts){
        if (_wts > rts + 1)
            INC_INT_STATS(int_possibMVCC, 1);
        return false;
    }
#endif
    pthread_mutex_lock( _latch );

    if (wts != _wts) {
#if TICTOC_MV
        if (wts == _hist_wts && rts < _wts) {
            pthread_mutex_unlock( _latch );
            return true;
        }
#endif
        if (_wts > rts + 1)
            INC_INT_STATS(int_possibMVCC, 1);
        pthread_mutex_unlock( _latch );
        return false;
    }
    new_rts = _rts;
    if (rts > _rts) {
        _rts = rts;
        new_rts = rts;
    }
#if ENABLE_LOCAL_CACHING
    if (_row) {
    #if RO_LEASE
        uint64_t max_rts = _row->get_table()->get_max_rts();
        if (max_rts > _rts)
            new_rts = _rts = max_rts;
    #endif
    }
#endif

    pthread_mutex_unlock( _latch );
    return true;
#endif
#else
    assert(false); // Should not be called if LOCK_ALL_BEFORE_COMMIT is false.
#endif

}

ts_t
Row_tictoc::get_wts()
{
#if ATOMIC_WORD
    return _ts_word & WTS_MASK;
#else
    return _wts;
#endif
}

ts_t
Row_tictoc::get_rts()
{
#if ATOMIC_WORD
    uint64_t v = _ts_word;
    return ((v & RTS_MASK) >> WTS_LEN) + (v & WTS_MASK);
#else
    return _rts;
#endif
}

void
Row_tictoc::get_ts(uint64_t &wts, uint64_t &rts)
{
    pthread_mutex_lock( _latch );
    wts = _wts;
    rts = _rts;
    pthread_mutex_unlock( _latch );
}

void
Row_tictoc::set_ts(uint64_t wts, uint64_t rts)
{
    pthread_mutex_lock( _latch );
    _wts = wts;
    _rts = rts;
    pthread_mutex_unlock( _latch );
}

void
Row_tictoc::lock()
{
#if ATOMIC_WORD
    uint64_t lock_mask = (WRITE_PERMISSION_LOCK)? WRITE_BIT : LOCK_BIT;
    uint64_t v = _ts_word;
    while ((v & lock_mask) || !__sync_bool_compare_and_swap(&_ts_word, v, v | lock_mask)) {
        PAUSE
        v = _ts_word;
    }
#else
    pthread_mutex_lock( _latch );
    assert( __sync_bool_compare_and_swap(&_ts_lock, false, true) );
    pthread_mutex_unlock( _latch );
#endif
}

bool
Row_tictoc::try_lock()
{
    assert(false);
    return false;
}

RC
Row_tictoc::try_lock(TxnManager * txn)
{
#if LOCK_ALL_DEBUG
    printf("Trying to lock read set for txn_id %ld\n", txn_id);
#endif
    RC rc = RCOK;
    pthread_mutex_lock( _latch );
    assert(!OCC_WAW_LOCK);
#if OCC_LOCK_TYPE == NO_WAIT
      if (!_ts_lock) {
        _ts_lock = true;
        rc = RCOK;
    } else
        rc = ABORT;
#elif OCC_LOCK_TYPE == WAIT_DIE
      if (!_ts_lock) {
        _ts_lock = true;
        assert(_lock_owner == NULL);
        _lock_owner = txn;
        rc = RCOK;
    } else {
        if (txn->get_txn_id() == _lock_owner->get_txn_id())
            cout << "Error  " << txn->get_txn_id() << "  " << _lock_owner->get_txn_id() << endl;
        M_ASSERT(txn->get_txn_id() != _lock_owner->get_txn_id(),
                    "txn=%ld, _lock_owner=%ld. ID=%ld\n", (uint64_t)txn, (uint64_t)_lock_owner, txn->get_txn_id());
        // txn has higher priority, should wait.
        if (_waiting_set.size() >= _max_num_waits)
            rc = ABORT;
        else if (MAN(txn)->get_priority() < MAN(_lock_owner)->get_priority()) {
            _waiting_set.insert(txn);
            rc = WAIT;
        } else
            rc = ABORT;
    }
#endif
    pthread_mutex_unlock( _latch );
    return rc;
}

void
Row_tictoc::release(TxnManager * txn, RC rc)
{
    pthread_mutex_lock( _latch );
#if !OCC_WAW_LOCK
  #if OCC_LOCK_TYPE == NO_WAIT
    _ts_lock = false;
  #elif OCC_LOCK_TYPE == WAIT_DIE
    if (rc == RCOK)
        assert(_ts_lock && txn == _lock_owner);
    if (txn == _lock_owner) {
        if (_waiting_set.size() > 0) {
            // TODO. should measure how often each case happens
            if (rc == ABORT) {
                TxnManager * next = *_waiting_set.rbegin();
                set<TxnManager *>::iterator last = _waiting_set.end();
                last --;
                _waiting_set.erase( last );
                _lock_owner = next;
                next->set_txn_ready(RCOK);
            } else { // rc == COMMIT
                for (std::set<TxnManager *>::iterator it = _waiting_set.begin();
                     it != _waiting_set.end(); it ++) {
                    (*it)->set_txn_ready(ABORT);
                }
                _waiting_set.clear();
                _ts_lock = false;
                _lock_owner = NULL;
            }
        } else {
            _ts_lock = false;
            _lock_owner = NULL;
        }
    } else {
        assert(rc == ABORT);
        // the txn may not be in _waiting_set.
          _waiting_set.erase(txn);
    }
  #endif
#else // OCC_WAW_LOCK
    assert(OCC_WAW_LOCK);
    if (txn != _lock_owner) {
        _waiting_set.erase( txn );
        pthread_mutex_unlock( _latch );
        return;
    }

    assert(_ts_lock);
  #if OCC_LOCK_TYPE == NO_WAIT
    _ts_lock = false;
  #elif OCC_LOCK_TYPE == WAIT_DIE
    if (_waiting_set.size() > 0) {
        set<TxnManager *>::iterator last = _waiting_set.end();
        last --;

        TxnManager * next = *last;
        _waiting_set.erase( last );
        _lock_owner = next;
        next->_lock_acquire_time = get_sys_clock();
        if (rc == ABORT) {
            next->_lock_acquire_time_abort = get_sys_clock();
            next->_lock_acquire_time_commit = 0;
        } else {
            next->_lock_acquire_time_commit = get_sys_clock();
            next->_lock_acquire_time_abort = 0;
        }
        assert(MAN(next)->get_priority() < MAN(txn)->get_priority());
        COMPILER_BARRIER
        next->set_txn_ready(RCOK);
    } else {
        _ts_lock = false;
        _lock_owner = NULL;
    }
  #endif
#endif
    pthread_mutex_unlock( _latch );
}

void
Row_tictoc::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}


#if MULTI_VERSION
void Row_tictoc::get_last_array(ts_t* & last_rts_array, ts_t* & last_wts_array, int & last_ptr)
{
    last_rts_array = _lastrts_array;
    last_wts_array = _lastwts_array;
    last_ptr = _last_ptr;
}
#else
void Row_tictoc::get_last_ts(ts_t & last_rts, ts_t & last_wts)
{
    last_rts = _lastrts;
    last_wts = _lastwts;
}
ts_t Row_tictoc::get_last_rts()
{
    return _lastrts;
}

ts_t Row_tictoc::get_last_wts()
{
    return _lastwts;
}
#endif


#endif
