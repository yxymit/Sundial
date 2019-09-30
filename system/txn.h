#pragma once

#include "global.h"
#include "helper.h"

class workload;
//class WorkerThread;
class row_t;
class table_t;
class QueryBase;
class SubQuery;
class Message;
class StoreProcedure;
class CCManager;

class TxnManager
{
public:
    enum State {
        RUNNING,
        PREPARING,
        COMMITTING,
        ABORTING,
        COMMITTED,
        ABORTED
    };
    bool waiting_for_remote;
    bool waiting_for_lock;

    TxnManager(QueryBase * query, ServerThread * thread);
    TxnManager(Message * msg, ServerThread * thread);
    TxnManager(ServerThread * thread, bool sub_txn);
    TxnManager(TxnManager * txn);

    virtual ~TxnManager();

    void             set_txn_id(uint64_t txn_id) { _txn_id = txn_id; }
    uint64_t         get_txn_id() { return _txn_id; }

    // wake up a waiting txn
    void              set_txn_ready(RC rc);
    bool             is_txn_ready();

    // debug time
    uint64_t        _start_wait_time;
    uint64_t         _lock_acquire_time;
    uint64_t         _lock_acquire_time_commit;
    uint64_t         _lock_acquire_time_abort;

    CCManager *     get_cc_manager() { return _cc_manager; }
    StoreProcedure *     get_store_procedure() { return _store_procedure; };
    bool            is_all_remote_readonly();

    /////////////////////////////////////
    // [Distributed DBMS]
    /////////////////////////////////////
    void send_msg(Message * msg);
    RC process_msg(Message * msg);
    // execute may start from beginning, or it may resume a waiting txn.
    RC execute(bool restart = false);

    RC start_execute();
    RC continue_execute();

    void set_sub_txn(bool is_sub_txn)     { _is_sub_txn = is_sub_txn; }
    bool is_sub_txn()                     { return _is_sub_txn; }
    void set_msg(Message * msg)         { _msg = msg; }

    State             get_txn_state() { return _txn_state; }
    void             set_txn_state(State state) { _txn_state = state; }

    // Stats
    void             update_stats();
    uint32_t         get_num_aborts() { return _num_aborts; }
    uint64_t         _txn_start_time;
    uint32_t         _num_aborts;

    // Debug
    void             print_state();

    // remote nodes involved in this txn
    set<uint32_t>     remote_nodes_involved;
    set<uint32_t>   aborted_remote_nodes;
    set<uint32_t>    readonly_remote_nodes;
    bool            _txn_abort;
private:
    // TODO. for now, a txn is mapped to a single thread.
    ServerThread *    _server_thread;

    // Store procedure (YCSB, TPCC, etc.)
    StoreProcedure * _store_procedure;
    RC execute_store_procedure();

    // Concurrency control manager (2PL, TicToc, etc.)
    CCManager * _cc_manager;

    // For remote request, only keep msg.
    Message *         _msg;

    State             _txn_state;
    uint32_t         _num_resp_expected;
    pthread_mutex_t _txn_lock;

    RC finish(RC rc) { assert(false); }

    // For OCC without acquiring write locks during execution,
    // a thrid lock_write_set phase is required before 2PC.
    // TODO should give a differet
    bool            _remote_txn_abort;

    // read phse
    RC process_local_miss();
    RC process_remote_req(Message * msg);
    RC process_renew_req(Message * msg);
    RC process_remote_resp(Message * msg);

    // Commit phase for single-partition transactions.
    RC process_commit_phase_singlepart(RC rc);

#if CC_ALG == NAIVE_TICTOC
    // For naive TicToc or naive Silo, we need three phase commit.
    // The following lock phase is only for these two algorithms
    RC process_lock_phase();
    RC process_lock_req(Message * msg);
    RC process_lock_resp(Message * msg);
#endif

    // prepare phase
    RC process_2pc_prepare_phase();
    RC process_2pc_prepare_req(Message * msg);
    RC process_2pc_prepare_resp(Message * msg);
    // for F1 and TicToc.
    // a txn in F1/TicToc may need to wait during the prepare phase.
    RC continue_prepare_phase();

    // commit phase
    // rc can be either COMMIT or Abort.
    RC process_2pc_commit_phase(RC rc);
    RC process_2pc_commit_req(Message * msg);
    RC process_2pc_commit_resp();


    bool             _is_sub_txn;
    uint32_t         _src_node_id; // for sub_query, the src_node is stored.

    // stats
    uint64_t        _txn_restart_time;     // after aborts
    // only lock_phase exists only in NAIVE_TICTOC
    uint64_t         _lock_phase_start_time;
    uint64_t         _prepare_start_time;
    uint64_t         _cc_time;
    uint64_t *         _msg_count;
    uint64_t *         _msg_size;

    uint64_t         _commit_start_time;
    uint64_t         _finish_time;

    uint64_t         _lock_wait_time;
    uint64_t         _lock_wait_start_time;


    uint64_t        _net_wait_start_time;
    uint64_t         _net_wait_time;

    // txn_id format. Each txn has a unique ID
    // | per thread monotonically increasing ID   |  thread ID   |   Node ID |
    uint64_t         _txn_id;

    // for continued prepare.
    uint32_t _resp_size;
    char * _resp_data;
    // locality handler for TicToc
    void handle_local_caching();
    RC process_caching_resp(Message * msg);
};
