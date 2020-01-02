#pragma once

#include "helper.h"
#include "global.h"
#include <stack>

class row_t;
class TxnManager;
class workload;
class Transport;
class WorkerThread;

// Global Manager shared by all the threads.
// For distributed version, Manager is shared by all threads on a single node.
class Manager {
public:
    Manager();

    // Global timestamp allocation
    uint64_t                get_ts(uint64_t thread_id);
    void                    calibrate_cpu_frequency();

    // For MVCC. To calculate the min active ts in the system
    void                    add_ts(uint64_t ts);
    uint64_t                get_min_ts(uint64_t tid = 0);

    // For DL_DETECT.
    void                    set_txn_man(TxnManager * txn);
    TxnManager *            get_txn_man(int thd_id) { return _all_txns[thd_id]; };

    // per-thread random number generator
    void                    init_rand(uint64_t thd_id) {  srand48_r(thd_id, &_buffer); }
    uint64_t                rand_uint64();
    uint64_t                rand_uint64(uint64_t max);
    uint64_t                rand_uint64(uint64_t min, uint64_t max);
    double                  rand_double();

    // thread id
    void                    set_thd_id(uint64_t thread_id) { _thread_id = thread_id; }
    uint64_t                get_thd_id() { return _thread_id; }

    // TICTOC, max_cts
    void                    set_max_cts(uint64_t cts) { _max_cts = cts; }
    uint64_t                get_max_cts() { return _max_cts; }

    // workload
    void                    set_workload(workload * wl)    { _workload = wl; }
    inline workload *       get_workload()    { return _workload; }

    // For client worker thd id conversion
    uint32_t                txnid_to_node(uint64_t txn_id);
    uint32_t                txnid_to_worker_thread(uint64_t txn_id);

    // global synchronization
    bool                    is_sim_done()
    { return are_all_worker_threads_done() && are_all_remote_nodes_done(); }

    uint32_t                worker_thread_done();
    bool                    are_all_worker_threads_done()
    { return _num_finished_worker_threads == g_num_worker_threads; }

    bool                    are_all_remote_nodes_done()
    { return _num_sync_received == (g_num_nodes - 1) * 2; }

    void                    receive_sync_request();
    uint32_t                num_sync_requests_received() { return _num_sync_received; }

    // Thread pool
    bool                    add_to_thread_pool(WorkerThread * worker);
    void                    wakeup_next_thread();
    /*
    void                    register_worker_thread(uint64_t thread_id, WorkerThread * thread)
    { _worker_threads[thread_id] = thread; }

    WorkerThread *          get_worker_thread(uint64_t thread_id) {
        return _worker_threads[thread_id];
    }
    uint64_t        next_wakeup_thread();
    */
private:
    pthread_mutex_t         ts_mutex;
    uint64_t *              timestamp;
    uint64_t                hash(row_t * row);
    uint64_t volatile * volatile * volatile all_ts;
    TxnManager **           _all_txns;

    uint32_t                _num_finished_worker_threads;
    volatile uint32_t       _num_sync_received;

    // per-thread random number
    static __thread drand48_data _buffer;

    // thread id
    static __thread uint64_t _thread_id;

    // For TICTOC timestamp
    static __thread uint64_t _max_cts; // max commit timestamp seen by the thread so far.

    // workload
    workload *              _workload;

    // for MVCC
    volatile uint64_t       _last_min_ts_time;
    uint64_t                _min_ts;

    pthread_mutex_t *       _worker_pool_mutex;
    uint64_t                _unused_quota;
    std::stack<WorkerThread *> _ready_workers;
    //WorkerThread ** _worker_threads;
    //uint64_t        _wakeup_thread;
};
