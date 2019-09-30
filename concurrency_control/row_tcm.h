#pragma once

#include <set>

#if CC_ALG == TCM

class TxnManager;
class CCManager;
class LockManager;
class row_t;

class Row_TCM {
public:
    enum LockType {
        LOCK_EX,
        LOCK_SH,
        LOCK_UPGRADING,
        LOCK_NONE
    };
    Row_TCM();
    Row_TCM(row_t * row);
    virtual     ~Row_TCM() {}
    virtual void init(row_t * row);
    RC             lock_get(LockType type, TxnManager * txn, char * data);
    RC             lock_release(TxnManager * txn, RC rc);

    void         latch();
    void         unlatch();

    uint32_t     _max_num_waits;
    LockType     _lock_type;
protected:
    struct WaitEntry {
        LockType type;
        TxnManager * txn;
    };
    struct LockHolder {
        TCMManager * manager;
        LockType type;
    };
    struct CompareHolder {
        bool operator() (const LockHolder &en1, const LockHolder &en2) const {
            return en1.manager < en2.manager;
        };
    };
    std::set<LockHolder, CompareHolder>                        _locking_set;

    TxnManager *     _upgrading_txn;
    pthread_mutex_t      _latch;
    row_t *        _row;
private:
    RC adjust_timestamps_rw(TCMManager * holder, TCMManager * requester);
    RC adjust_timestamps_wr(TCMManager * holder, TCMManager * requester);
    RC adjust_timestamps_ww(TCMManager * holder, TCMManager * requester);

} __attribute__ ((aligned(64)));

#endif
