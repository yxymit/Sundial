#include "row_ideal_mvcc.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "ideal_mvcc_manager.h"
#include "manager.h"
#include "stdlib.h"

#if CC_ALG==IDEAL_MVCC
#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
bool
Row_MVCC::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
        return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}
#endif
Row_MVCC::Row_MVCC()
{
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
    _deleted = false;
    _delete_timestamp = 0;
}

RC
Row_MVCC::read(TxnManager * txn, char * data,
                 uint64_t &wts, uint64_t &rts, uint64_t &cts, bool latch)
{
    if (latch)
        this->latch();
    if (cts > _pending_cts && _ts_lock)
    {
        if (latch)
            unlatch();
        INC_INT_STATS(int_aborts_rs1, 1);  // locked and cts > _rts
        return ABORT;
    }
    wts = _wts;
    if (cts > _rts) // _ts_lock must be false
    {
        _rts = cts;  // extend & atomic
    }
    rts = _rts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());

    if (latch)
        unlatch();
    if (txn->is_sub_txn())
        _num_remote_reads ++;
    return RCOK;
}

RC
Row_MVCC::write(TxnManager * txn, uint64_t &wts, uint64_t &rts, uint64_t &cts, bool latch)
{
    RC rc = RCOK;
    assert(OCC_WAW_LOCK);
    if (latch)
        pthread_mutex_lock( _latch );
    if (cts <= _rts)
    {
        rc = ABORT;
        INC_INT_STATS(int_aborts_ws1, 1);  // cts <= _rts
    }
    else {
        if (!_ts_lock) {
            _ts_lock = true;
            _lock_owner = txn;
        } else if (_lock_owner != txn) {
            INC_INT_STATS(int_aborts_ws2, 1);  // locked by others
            rc = ABORT;
        }
    }

    if (rc == RCOK) {
        _pending_cts = cts;
        wts = _wts;
        rts = _rts;
    }
    if (latch)
        pthread_mutex_unlock( _latch );
    return rc;
}

RC
Row_MVCC::update(char * data, uint64_t wts, uint64_t rts)
{ return RCOK; }

void
Row_MVCC::latch()
{
    pthread_mutex_lock( _latch );
}

void
Row_MVCC::unlatch()
{
    pthread_mutex_unlock( _latch );
}

void
Row_MVCC::write_data(char * data, ts_t wts)
{
    latch();

    if (_deleted)
        assert(wts < _delete_timestamp);
    assert(wts == _pending_cts);
    _wts = wts;
    _rts = wts;
    if (data)  // in case data == NULL
        _row->copy(data);
    unlatch();
}

void
Row_MVCC::update_ts(uint64_t cts)
{
    assert(false);
    latch();
    assert(cts > _rts);
    _wts = cts;
    _rts = cts;
    unlatch();
}

bool
Row_MVCC::try_renew(ts_t rts)
{
    assert(false);
    if (rts < _wts){
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
Row_MVCC::try_renew(ts_t wts, ts_t rts, ts_t &new_rts)
{
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
    pthread_mutex_unlock( _latch );
    return true;
}

ts_t
Row_MVCC::get_wts()
{
    return _wts;
}

ts_t
Row_MVCC::get_rts()
{
    return _rts;
}

void
Row_MVCC::get_ts(uint64_t &wts, uint64_t &rts)
{
    pthread_mutex_lock( _latch );
    wts = _wts;
    rts = _rts;
    pthread_mutex_unlock( _latch );
}

void
Row_MVCC::set_ts(uint64_t wts, uint64_t rts)
{
    assert(false);
    pthread_mutex_lock( _latch );
    _wts = wts;
    _rts = rts;
    pthread_mutex_unlock( _latch );
}

void
Row_MVCC::lock()
{
    pthread_mutex_lock( _latch );
    assert( __sync_bool_compare_and_swap(&_ts_lock, false, true) );
    pthread_mutex_unlock( _latch );
}

bool
Row_MVCC::try_lock()
{
    assert(false);
    return false;
}

RC
Row_MVCC::try_lock(TxnManager * txn)
{
#if LOCK_ALL_DEBUG
    printf("Trying to lock read set for txn_id %ld\n", txn_id);
#endif
    RC rc = RCOK;
    pthread_mutex_lock( _latch );
    // handle pre abort
    assert(!OCC_WAW_LOCK);
      if (!_ts_lock) {
        _ts_lock = true;
        rc = RCOK;
    } else
        rc = ABORT;
    pthread_mutex_unlock( _latch );
    return rc;
}

void
Row_MVCC::release(TxnManager * txn, RC rc)
{
    pthread_mutex_lock( _latch );
#if !OCC_WAW_LOCK
    _ts_lock = false;
#else // OCC_WAW_LOCK
    assert(OCC_WAW_LOCK);
    if (txn != _lock_owner) {
        _waiting_set.erase( txn );
        pthread_mutex_unlock( _latch );
        return;
    }
    assert(_ts_lock);
    _ts_lock = false;
#endif
    pthread_mutex_unlock( _latch );
}

void
Row_MVCC::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}

#endif
