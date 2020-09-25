#pragma once

#include "global.h"
#include "helper.h"

//#include "rpc_server.h"
//#include "rpc_client.h"
#include "semaphore_sync.h"
#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
//#include "worker_thread.h"
class workload;
//class WorkerThread;
class row_t;
class table_t;
class QueryBase;
class SubQuery;
class Message;
class StoreProcedure;
class CCManager;
using grpc::CompletionQueue;
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
    TxnManager() : TxnManager(NULL, NULL) {};
    TxnManager(QueryBase * query, WorkerThread * thread);
    virtual ~TxnManager();

    // start to run transactions
    RC start();

    // rerun previously aborted transactions
    RC restart();

    void              set_txn_id(uint64_t txn_id) { _txn_id = txn_id; }
    uint64_t          get_txn_id()          { return _txn_id; }
    bool              is_read_only()        { return _is_read_only; }
    void              set_read_only(bool readonly) { _is_read_only = readonly; }
    bool              is_single_partition() { return _is_single_partition; }

    CCManager *       get_cc_manager()      { return _cc_manager; }
    StoreProcedure *  get_store_procedure() { return _store_procedure; };
    State             get_txn_state()       { return _txn_state; }
    void              set_txn_state(State state) { _txn_state = state; }

    // Synchronization
    // ===============
    SemaphoreSync *   log_semaphore;
    SemaphoreSync *   dependency_semaphore;
    SemaphoreSync *   rpc_semaphore;

    // Distributed transactions
    // ========================
public:
    RC send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                uint64_t table_id, access_t access_type);
    RC process_remote_request(const SundialRequest* request, SundialResponse* response);

    void set_sub_txn(bool is_sub_txn)     { _is_sub_txn = is_sub_txn; }
    bool is_sub_txn()                     { return _is_sub_txn; }
private:
    RC process_2pc_phase1(CompletionQueue* cq);
    RC process_2pc_phase2(RC rc, CompletionQueue* cq);

public:
    // Stats
    // =====
    void              update_stats();

    // Debug
    void              print_state()       { };

private:
    // TODO. for now, a txn is mapped to a single thread.
    WorkerThread *    _worker_thread;
    StoreProcedure *  _store_procedure;
    CCManager *       _cc_manager;

    State             _txn_state;
    bool              _is_single_partition;
    bool              _is_read_only;
    bool              _is_remote_abort;
    // txn_id format.
    // | per thread monotonically increasing ID   |  thread ID   |   Node ID |
    uint64_t          _txn_id;


    // Single-part transactions
    // ========================
    RC process_commit_phase_singlepart(RC rc);


    bool              _is_sub_txn;
    struct RemoteNodeInfo {
        State state;
        // At any point in time, a remote node has at most 1 request and 1
        // response.
        SundialRequest request;
        SundialResponse response;
    };
    std::map<uint32_t, RemoteNodeInfo *> _remote_nodes_involved;

    // stats
    // =====
    uint64_t          _txn_start_time;
    uint64_t          _txn_restart_time;
    uint64_t          _prepare_start_time;
    uint64_t          _commit_start_time;
    uint64_t          _log_ready_time;
    uint64_t          _precommit_finish_time;
    uint64_t          _finish_time;

    //uint64_t          _lock_wait_start_time;
    uint64_t          _lock_wait_time;

    //uint64_t          _net_wait_start_time;
    uint64_t          _net_wait_time;
};
