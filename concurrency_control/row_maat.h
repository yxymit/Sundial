#pragma once

#include "global.h"

#if CC_ALG == MAAT

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else

#define LOCK_BIT (1UL << 63)

#endif

class TxnManager;
class CCManager;
//class MaatManager;
class row_t;

class Row_maat {
public:
    Row_maat();
    Row_maat(row_t * row) : Row_maat()
    { _row = row; }

    RC                     read(TxnManager * txn, char * data,
                             uint64_t &wts, uint64_t &rts, bool latch = true);
    RC                    write(TxnManager * txn, char * data, uint64_t &wts, uint64_t &rts, bool latch = true);

    void                latch() { pthread_mutex_lock(_latch); };
    void                unlatch() { pthread_mutex_unlock(_latch); };

    void                write_data(char * data, uint64_t commit_ts);
    void                 extend_rts(uint64_t rts);
    void                 delete_row(uint64_t del_ts);
    void                get_last_ts(ts_t & last_rts, ts_t & last_wts);

    ts_t                 get_wts();
    ts_t                 get_rts();

#if VALIDATION_LOCK == WAIT_DIE

      TxnManager *        _lock_owner;
    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };
    std::set<TxnManager *, CompareWait> _waiting_set;
    uint32_t _max_num_waits;
 #endif

    enum State {
        SHARED,
        EXCLUSIVE
    };
    State                 _state;
    uint32_t            _owner; // host node ID

    bool                 _deleted;
    uint64_t             _delete_timestamp;

    row_t *             _row;
    uint64_t            _wts; // last write timestamp
    uint64_t            _rts; // end lease timestamp
    set<TxnManager *>        _uncommitted_writer;
    set<TxnManager *>        _uncommitted_reader;
    uint64_t            _lastrts; // last rts
    uint64_t            _lastwts;
    // TODO _wr_latch can be removed using the atomic word trick.
    pthread_mutex_t *     _latch;        // to guarantee read/write consistency
    bool                _ts_lock;     // wts/rts cannot be changed if _ts_lock is true.
    // for locality predictor
    uint32_t             _num_remote_reads; // should cache a local copy if this number is too large.
};

#endif
