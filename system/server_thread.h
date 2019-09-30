#pragma once

#include "global.h"
#include <queue>
#include <stack>
#include "thread.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class Message;

class ServerThread : public Thread {
public:
    ServerThread(uint64_t thd_id);
    RC run();

    void signal();

    TxnManager * get_native_txn() { return _native_txn; }
    pthread_mutex_t cond_mutex;
    pthread_cond_t     cond;
private:
    void handle_req_finish(RC rc, TxnManager * &txn_man);

    TxnManager *     _native_txn;
    uint64_t         _ready_time;
    // wait_buffer
    set<TxnManager *> _wait_buffer;
    Message *        _msg;
    // So only malloc at the beginning

    uint64_t     _client_node_id;
    uint64_t     _num_active_txns;
    uint64_t     _max_num_active_txns;
    // For timestamp allocation
    uint64_t     _curr_ts;
    bool         already_printed_debug;
};
//__attribute__ ((aligned(64)));
