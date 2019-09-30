#include "row_maat.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>
#include "maat_manager.h"
#include "manager.h"
#include "stdlib.h"

#if CC_ALG==MAAT

#if VALIDATION_LOCK == WAIT_DIE
bool Row_maat::CompareWait::operator() (TxnManager * en1, TxnManager * en2) const
{
    return MAN(en1)->get_priority() < MAN(en2)->get_priority();
}
#endif

Row_maat::Row_maat()
{
    _latch = new pthread_mutex_t;
    pthread_mutex_init( _latch, NULL );
    _wts = 0;
    _rts = 0;
    _ts_lock = false;
    _num_remote_reads = 0;
  #if VALIDATION_LOCK == WAIT_DIE
      _max_num_waits = g_max_num_waits;
    _lock_owner = NULL;
  #endif
      _deleted = false;
    _delete_timestamp = 0;
}

RC
Row_maat::read(TxnManager * txn, char * data, uint64_t &wts, uint64_t &rts, bool latch)
{
    if (latch)
        pthread_mutex_lock( _latch );
    wts = _wts;
    rts = _rts;
    _uncommitted_reader.insert(txn);
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());
    if (latch)
        pthread_mutex_unlock( _latch );
    if (txn->is_sub_txn())
        _num_remote_reads ++;
    return RCOK;
}

RC
Row_maat::write(TxnManager * txn, char * data, uint64_t &wts, uint64_t &rts, bool latch)
{
    RC rc = RCOK;
    uint64_t tt = get_sys_clock();
    if (latch)
        pthread_mutex_lock( _latch );
      wts = _wts;
    rts = _rts;
    _uncommitted_reader.insert(txn);
    _uncommitted_writer.insert(txn);
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());
    if (latch)
        pthread_mutex_unlock( _latch );
    INC_FLOAT_STATS(time_debug1, get_sys_clock() - tt);
    return rc;
}

void
Row_maat::extend_rts(uint64_t rts)
{
    latch();
    _rts = max(_rts, rts);
    unlatch();
}

void
Row_maat::write_data(char * data, uint64_t commit_ts)
{
    pthread_mutex_lock( _latch );
    assert(!_deleted);
    if (_deleted)
        assert( _wts < _delete_timestamp );
    _row->copy(data);
    M_ASSERT(commit_ts > _rts, "row=%ld commit=%ld, _wts=%ld, _rts=%ld",
        _row->get_primary_key(), commit_ts, _wts, _rts);
    _wts = _rts = commit_ts;
    pthread_mutex_unlock( _latch );
}

ts_t
Row_maat::get_wts()
{
#if ATOMIC_WORD
    return _ts_word & WTS_MASK;
#else
    return _wts;
#endif
}

ts_t
Row_maat::get_rts()
{
#if ATOMIC_WORD
    uint64_t v = _ts_word;
    return ((v & RTS_MASK) >> WTS_LEN) + (v & WTS_MASK);
#else
    return _rts;
#endif

}

void
Row_maat::delete_row(uint64_t del_ts)
{
    latch();
    _deleted = true;
    _delete_timestamp = del_ts;
    unlatch();
}

#endif
