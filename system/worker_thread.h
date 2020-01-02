#pragma once

#include "global.h"
#include <queue>
#include <stack>
#include "thread.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class SundialRPCClient;

class WorkerThread : public BaseThread {
public:
    WorkerThread(uint64_t thd_id);
    RC                      run();

    TxnManager *            get_native_txn() { return _native_txn; }
private:
    //void                  handle_req_finish(TxnManager * &txn_man);
    TxnManager *            _native_txn;

public:
    void                    wakeup();
    void                    add_to_pool();
    void                    wait();
private:
    bool                    _is_ready;
    pthread_cond_t *        _cond;
    pthread_mutex_t *       _mutex;
};
