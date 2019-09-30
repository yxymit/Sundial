#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "manager.h"
#include "lock_manager.h"
#include "f1_manager.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == F_ONE

#if CC_ALG == WAIT_DIE || CC_ALG == F_ONE
bool
Row_lock::CompareWait::operator() (const WaitEntry &en1, const WaitEntry &en2) const
{
    return LOCK_MAN(en1.txn)->get_ts() < LOCK_MAN(en2.txn)->get_ts();
}

bool
Row_lock::CompareLock::operator() (TxnManager * txn1, TxnManager * txn2) const
{
    return LOCK_MAN(txn1)->get_ts() < LOCK_MAN(txn2)->get_ts();
}
#endif

Row_lock::Row_lock()
{
    _row = NULL;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
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
    _row = row;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
      _max_num_waits = g_max_num_waits;
}

RC
Row_lock::lock_get(LockType type, TxnManager * txn, bool need_latch)
{
    RC rc = RCOK;
#if CC_ALG == NO_WAIT
    if (conflict_lock(_lock_type, type)) {
        if (type == _lock_type) {
            INC_INT_STATS(num_aborts_ws, 1);
        } else {
            INC_INT_STATS(num_aborts_rs, 1);
        }
        return ABORT;
    }
#endif
    if (need_latch)
        pthread_mutex_lock( &_latch );
    if (_locking_set.find(txn) != _locking_set.end()) {
        // upgrade request.
        if (_lock_type != type) {
            if (_lock_type == LOCK_SH) {
                assert(type == LOCK_EX);
                _upgrading_txn = txn;
                if (_locking_set.size() == 1) {
                    _lock_type = LOCK_EX;
                    rc = RCOK;
                } else {
                    _lock_type = LOCK_UPGRADING;
                    rc = WAIT;
                }
            } else {
                assert(_lock_type == LOCK_UPGRADING);
                rc = ABORT;
            }
        } // else just ignore.
        if (need_latch)
            pthread_mutex_unlock( &_latch );
        return rc;
    }
    bool conflict = conflict_lock(_lock_type, type);
#if CC_ALG == NO_WAIT
    if (conflict) {
        if (type == _lock_type) {
            assert(type == LOCK_EX);
            INC_INT_STATS(num_aborts_ws, 1);
        } else {
            INC_INT_STATS(num_aborts_rs, 1);
        }
        rc = ABORT;
    }
#elif CC_ALG == WAIT_DIE || CC_ALG == F_ONE
    // check conflict between incoming txn and waiting txns.
    if (!conflict)
        if (!_waiting_set.empty() && LOCK_MAN(_waiting_set.rbegin()->txn)->get_ts() > LOCK_MAN(txn)->get_ts())
            conflict = true;
    if (conflict) {
        assert(!_locking_set.empty());
        if (_waiting_set.size() > _max_num_waits) {
            rc = ABORT;
            INC_INT_STATS(int_debug1, 1);
        }
        else if (LOCK_MAN(txn)->get_ts() > LOCK_MAN(*_locking_set.begin())->get_ts() ||
            (!_waiting_set.empty() && LOCK_MAN(txn)->get_ts() > LOCK_MAN(_waiting_set.begin()->txn)->get_ts())) {
            rc = ABORT;
            INC_INT_STATS(int_debug2, 1);
        } else {
            rc = WAIT;
            INC_INT_STATS(int_debug3, 1);
        }
    }
    if (rc == WAIT) {
        WaitEntry entry = {type, txn};
        assert(!_locking_set.empty());
        for (auto entry : _waiting_set)
            assert(entry.txn != txn);
        _waiting_set.insert(entry);
        txn->_start_wait_time = get_sys_clock();
    }
    // ABORT Stats
    if (rc == ABORT) {
        if (type == _lock_type && type == LOCK_EX) {
            INC_INT_STATS(num_aborts_ws, 1);
        } else {
            INC_INT_STATS(num_aborts_rs, 1);
        }
    }
#endif
    if (rc == RCOK) {
        _lock_type = type;
        uint32_t size = _locking_set.size();
        _locking_set.insert(txn);
        M_ASSERT(_locking_set.size() == size + 1, "locking_set.size=%ld, size=%d\n", _locking_set.size(), size);
    }

    if (need_latch)
        pthread_mutex_unlock( &_latch );
    return rc;
}


RC
Row_lock::lock_release(TxnManager * txn, RC rc)
{
    pthread_mutex_lock( &_latch );

    uint32_t released = _locking_set.erase(txn);
#if CC_ALG == F_ONE
    if (released != 1) {
        // remove the txn from the _waiting_set
        for (std::set<WaitEntry>::iterator it = _waiting_set.begin();
            it != _waiting_set.end(); it ++)
        {
            if (it->txn == txn) {
                _waiting_set.erase(it);
                break;
            }
        }
        pthread_mutex_unlock( &_latch );
        return RCOK;
    }
#endif
    if (released == 0) {
        pthread_mutex_unlock( &_latch );
        return RCOK;
    }

#if CC_ALG == F_ONE || CC_ALG == WAIT_DIE
    assert(LOCK_MAN(txn)->get_ts() > 0);
    bool done = (released == 1)? false : true;
  #if CC_ALG == F_ONE
    if (!done && rc == COMMIT && _lock_type == LOCK_EX) {
        for ( set<WaitEntry>::iterator it = _waiting_set.begin();
                it != _waiting_set.end(); it ++)
        {
            it->txn->set_txn_ready(ABORT);
            // ABORT Stats
            if (it->type == LOCK_EX) {
                INC_INT_STATS(num_aborts_ws, 1);
            } else {
                INC_INT_STATS(num_aborts_rs, 1);
            }
        }
        _waiting_set.clear();
        done = true;
    }
  #endif
    // handle upgrade
    if (_lock_type == LOCK_UPGRADING) {
        assert(_locking_set.size() >= 1);
        if (_locking_set.size() == 1) {
            TxnManager * t = *_locking_set.begin();
            assert(t == _upgrading_txn);
            _lock_type = LOCK_EX;
            t->set_txn_ready(RCOK);
        }
        pthread_mutex_unlock( &_latch );
        return RCOK;
    }
    if (_locking_set.empty())
        _lock_type = LOCK_NONE;
    while (!done) {
        std::set<WaitEntry>::reverse_iterator rit = _waiting_set.rbegin();
        if (rit != _waiting_set.rend() && !conflict_lock(rit->type, _lock_type))
        {
            _lock_type = rit->type;
            _locking_set.insert(rit->txn);

            // for F_ONE, if the current txn commits a write, all waiting txns should abort.
            rit->txn->set_txn_ready(RCOK);
            _waiting_set.erase( --rit.base() );
        } else
            done = true;
    }
    if (_locking_set.empty())
        assert(_waiting_set.empty());
#elif CC_ALG == NO_WAIT
    if (_locking_set.empty())
        _lock_type = LOCK_NONE;
#else
    assert(false);
#endif
    pthread_mutex_unlock( &_latch );
    return RCOK;
}

bool Row_lock::conflict_lock(LockType l1, LockType l2)
{
    if (l1 == LOCK_UPGRADING || l2 == LOCK_UPGRADING)
        return true;
    if (l1 == LOCK_NONE || l2 == LOCK_NONE)
        return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else
        return false;
}

bool
Row_lock::is_owner(TxnManager * txn)
{
    return _lock_type == LOCK_EX
        && _locking_set.size() == 1
        && (*_locking_set.begin()) == txn;
}

#endif
