#pragma once

#include "global.h"

#if CC_ALG == TICTOC

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
class TicTocManager;
class row_t;

class Row_tictoc {
public:
#if MULTI_VERSION
    static int          _history_num;
#endif
    Row_tictoc() : Row_tictoc(NULL) {};
    Row_tictoc(row_t * row);

    RC                     read(TxnManager * txn, char * data,
                             uint64_t &wts, uint64_t &rts, bool latch = true, bool remote = false);
    RC                    write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch = true);
    RC                     update(char * data, uint64_t wts, uint64_t rts);
    void                 update_rts(uint64_t rts);

    void                 latch();
    void                 unlatch();

    void                 lock();
    bool                  try_lock();
    RC                  try_lock(TxnManager * txn);
    void                 release(TxnManager * txn, RC rc);

#if SPECULATE
    RC                    write_speculate(row_t * data, ts_t version, bool spec_read);
#endif
    void                write_data(char * data, ts_t wts);
    void                 update_ts(uint64_t cts);
    void                write_ptr(row_t * data, ts_t wts, char *& data_to_free);
    bool                 try_renew(ts_t rts);
    bool                 try_renew(ts_t wts, ts_t rts, ts_t &new_rts);
    bool                 renew(ts_t wts, ts_t rts, ts_t &new_rts);

#if MULTI_VERSION
    void                get_last_array(ts_t* & last_rts_array, ts_t* & last_wts_array, int & _last_ptr);
#else
    void                get_last_ts(ts_t & last_rts, ts_t & last_wts);

    ts_t                get_last_rts();
    ts_t                get_last_wts();
#endif
    uint64_t             get_wts();
    uint64_t             get_rts();
    void                 get_ts(uint64_t &wts, uint64_t &rts);
    void                set_ts(uint64_t wts, uint64_t rts);

  #if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
    TxnManager *        _lock_owner;
    #define MAN(txn) ((TicTocManager *) (txn)->get_cc_manager())
    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };
    std::set<TxnManager *, CompareWait> _waiting_set;
    uint32_t _max_num_waits;
  #endif

#if ENABLE_LOCAL_CACHING
    bool                 is_read_intensive() { return _write_intensity == 0; }
#endif
    bool                 is_deleted() { return _deleted; }
    void                 delete_row(uint64_t del_ts);
    enum State {
        SHARED,
        EXCLUSIVE
    };
    State                 _state;
    uint32_t            _owner; // host node ID

    row_t *             _row;
#if ATOMIC_WORD
    //volatile uint64_t    _ts_word;
    // the first bit in _wts is the write_latch bit, indicating that the tuple is being written to.
    volatile uint64_t    _wts; // last write timestamp.
    volatile uint64_t    _rts; // end lease timestamp
    volatile bool         _write_latch;
    // when locked, only the owner can change wts/rts.
    // however, the tuple can still be read by other txns.
    volatile bool        _ex_lock;
#else
    uint64_t            _wts; // last write timestamp
    uint64_t            _rts; // end lease timestamp
  #if MULTI_VERSION
    int                 _last_ptr;
    uint64_t *            _lastrts_array;
    uint64_t *            _lastwts_array;
  #else
    uint64_t            _lastrts; // last rts
    uint64_t            _lastwts;
  #endif
    pthread_mutex_t *     _latch;        // to guarantee read/write consistency
    bool                 _blatch;        // to guarantee read/write consistency
    bool                _ts_lock;     // wts/rts cannot be changed if _ts_lock is true.
#endif
    bool                _deleted;
    uint64_t            _delete_timestamp;
#if ENABLE_LOCAL_CACHING
    uint32_t             _write_intensity;
    uint32_t             _curr_lease;
#endif


#if TICTOC_MV
    volatile ts_t         _hist_wts;
#endif
    // for locality predictor
    uint32_t             _num_remote_reads; // should cache a local copy if this number is too large.
};
// __attribute__ ((aligned(64)));

#endif
