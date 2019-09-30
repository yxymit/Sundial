#include "row_naive_tictoc.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "naive_tictoc_manager.h"
#include "manager.h"
#include "stdlib.h"
#include "table.h"

#if CC_ALG==NAIVE_TICTOC

#if MULTI_VERSION
int Row_naive_tictoc::_history_num = 30;
#endif

#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
bool
Row_naive_tictoc::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}
#endif

Row_naive_tictoc::Row_naive_tictoc(row_t * row)
{
    _row = row;
    _latch = new pthread_mutex_t;
    _blatch = false;
    pthread_mutex_init( _latch, NULL );
    _wts = 0;
    _rts = 0;
    _ts_lock = false;
    _num_remote_reads = 0;
    _deleted = false;
    _delete_timestamp = 0;
}

RC
Row_naive_tictoc::read(TxnManager * txn, char * data,
                 uint64_t &wts, uint64_t &rts, bool latch, bool remote)
{
    if (latch)
        this->latch();
    wts = _wts;
    rts = _rts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());
    if (latch)
        unlatch();
    return RCOK;
}

RC
Row_naive_tictoc::write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch)
{
    RC rc = RCOK;
    assert(OCC_WAW_LOCK);
    if (_ts_lock)
        return ABORT;

    if (latch)
        pthread_mutex_lock( _latch );

    if (!_ts_lock) {
        _ts_lock = true;
        _lock_owner = txn;
    } else if (_lock_owner != txn) {
        rc = ABORT;
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
Row_naive_tictoc::update(char * data, uint64_t wts, uint64_t rts)
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
Row_naive_tictoc::update_rts(uint64_t rts)
{
    pthread_mutex_lock( _latch );
    if (rts > _rts)
        _rts = rts;
    pthread_mutex_unlock( _latch );
}

void
Row_naive_tictoc::latch()
{
    pthread_mutex_lock( _latch );
}

void
Row_naive_tictoc::unlatch()
{
    pthread_mutex_unlock( _latch );
}

void
Row_naive_tictoc::write_data(char * data, ts_t wts)
{
    latch();
    if (_deleted)
        assert(wts < _delete_timestamp);

    _wts = wts;
    _rts = wts;
    _row->copy(data);
    unlatch();
}

void
Row_naive_tictoc::update_ts(uint64_t cts)
{
    latch();
    assert(cts > _rts);
    _wts = cts;
    _rts = cts;
    unlatch();
}

bool
Row_naive_tictoc::try_renew(ts_t rts)
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
Row_naive_tictoc::try_renew(ts_t wts, ts_t rts, ts_t &new_rts)
{
    if (wts != _wts) {
        if(_wts < rts) {
            INC_INT_STATS(int_inevitable, 1);
        }
        if(_wts == rts + 1) {
            INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
        } else {
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
            INC_INT_STATS(int_inevitable, 1);

        if(_wts == rts + 1) {
            INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
        } else {
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
Row_naive_tictoc::get_wts()
{
    return _wts;
}

ts_t
Row_naive_tictoc::get_rts()
{
    return _rts;
}

void
Row_naive_tictoc::get_ts(uint64_t &wts, uint64_t &rts)
{
    pthread_mutex_lock( _latch );
    wts = _wts;
    rts = _rts;
    pthread_mutex_unlock( _latch );
}

void
Row_naive_tictoc::set_ts(uint64_t wts, uint64_t rts)
{
    pthread_mutex_lock( _latch );
    _wts = wts;
    _rts = rts;
    pthread_mutex_unlock( _latch );
}


RC
Row_naive_tictoc::try_lock(TxnManager * txn)
{
    RC rc = RCOK;
    pthread_mutex_lock( _latch );

    if (!_ts_lock) {
        _ts_lock = true;
        rc = RCOK;
    } else
        rc = ABORT;
    pthread_mutex_unlock( _latch );
    return rc;
}

void
Row_naive_tictoc::release(TxnManager * txn, RC rc)
{
    pthread_mutex_lock( _latch );
    _ts_lock = false;
    pthread_mutex_unlock( _latch );
}

void
Row_naive_tictoc::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}


#endif
