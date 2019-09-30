#pragma once

#include <set>

class TxnManager;
class CCManager;
class LockManager;
class row_t;

class Row_lock {
public:
    enum LockType {
        LOCK_EX,
        LOCK_SH,
        LOCK_UPGRADING,
        LOCK_NONE
    };
    Row_lock();
    Row_lock(row_t * row);
    virtual     ~Row_lock() {}
    virtual void init(row_t * row);
    RC             lock_get(LockType type, TxnManager * txn, bool need_latch = true);
    RC             lock_release(TxnManager * txn, RC rc);
    bool         is_owner(TxnManager * txn);

    void         latch();
    void         unlatch();

    uint32_t     _max_num_waits;
    LockType     _lock_type;
protected:
    struct WaitEntry {
        LockType type;
        TxnManager * txn;
    };
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT
    #define LOCK_MAN(txn) ((LockManager *) (txn)->get_cc_manager())
#elif CC_ALG == F_ONE
    #define LOCK_MAN(txn) ((F1Manager *) (txn)->get_cc_manager())
#endif
    // only store timestamp which uniquely identifies a txn.
    // for NO_WAIT, store the txn_id
#if CC_ALG == WAIT_DIE || CC_ALG == F_ONE
    struct CompareLock {
        bool operator() (TxnManager * txn1, TxnManager * txn2) const;
    };
    std::set<TxnManager *, CompareLock >        _locking_set;

    struct CompareWait {
        bool operator() (const WaitEntry &en1, const WaitEntry &en2) const;
    };
    std::set<WaitEntry, CompareWait>             _waiting_set;
#else
    std::set<TxnManager *>                        _locking_set;
#endif
    TxnManager *     _upgrading_txn;


    pthread_mutex_t      _latch;

    row_t *        _row;
    bool         conflict_lock(LockType l1, LockType l2);
};
//__attribute__ ((aligned(64)));
