#pragma once

#include <set>
#include <queue>
#include "global.h"

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
    virtual         ~Row_lock() {}
    virtual void    init(row_t * row);
    RC              lock_get(LockType type, TxnManager * txn, bool need_latch = true);
    RC              lock_release(TxnManager * txn, RC rc);
#if CONTROLLED_LOCK_VIOLATION
    RC              lock_cleanup(TxnManager * txn); //, std::set<TxnManager *> &ready_readonly_txns);
#endif

    void            latch();
    void            unlatch();

    uint32_t        _max_num_waits;
    LockType        _lock_type;
protected:
    struct LockEntry {
        LockType type;
        TxnManager * txn;
    };
    #define LOCK_MAN(txn) ((LockManager *) (txn)->get_cc_manager())
    // only store timestamp which uniquely identifies a txn.
    // for NO_WAIT, store the txn_id
#if CC_ALG == WAIT_DIE
    struct Compare {
        bool operator() (const LockEntry &en1, const LockEntry &en2) const;
    };
    std::set<LockEntry, Compare>        _locking_set;
    std::set<LockEntry, Compare>        _waiting_set;
#else // CC_ALG == NO_WAIT
    struct Compare {
        bool operator() (const LockEntry &en1, const LockEntry &en2) const { return &en1 < &en2; }
    };
    std::set<LockEntry, Compare>        _locking_set;
#endif
#if CONTROLLED_LOCK_VIOLATION
    // locks in weak_locking_set can be violated.
    std::vector<LockEntry>              _weak_locking_queue;
#endif

    TxnManager *    _upgrading_txn;
    pthread_mutex_t _latch;
    row_t *         _row;
    bool            conflict_lock(LockType l1, LockType l2);
};
