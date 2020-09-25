#include <sched.h>
#include <iomanip>
#include "global.h"
#include "manager.h"
#include "worker_thread.h"
#include "txn.h"
#include "store_procedure.h"
#include "workload.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "txn_table.h"
#include "cc_manager.h"

WorkerThread::WorkerThread(uint64_t thd_id)
    : BaseThread(thd_id, WORKER_THREAD)
{
    _mutex = new pthread_mutex_t;
    _cond = new pthread_cond_t;
    pthread_mutex_init(_mutex, NULL);
    pthread_cond_init (_cond, NULL);

    _native_txn = NULL;
    _is_ready = true;
}

// Each thread executes at most one active transaction.
// If the transaction has aborted or is waiting for a lock, it will sleep, waiting for a signal.

// For local miss, suspend the txn in txn_table
// For txn abort, add the txn to abort_buffer
// For txn waiting, add the txn to wait_buffer

RC WorkerThread::run() {
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    assert( glob_manager->get_thd_id() == get_thd_id() );
    //glob_manager->register_worker_thread( get_thd_id(), this );
#if ENABLE_ADMISSION_CONTROL
    //_num_complete_txns = 0;
    if (get_thd_id() >= g_max_num_active_txns)
        add_to_pool();
#endif
    pthread_barrier_wait( &global_barrier );


    RC rc = RCOK;
    assert (rc == RCOK);

    uint64_t init_time = get_sys_clock();
    // calculate which client thread this worker thread corresponds to.
    uint64_t max_txn_id = 0;

    uint64_t last_stats_cp_time = init_time;
    __attribute__((unused)) uint64_t last_idle_time = get_sys_clock();

    // Main loop
    while (get_sys_clock() - init_time < g_run_time * BILLION || _native_txn) {
        if (GET_THD_ID == 0 && get_sys_clock() - last_stats_cp_time > STATS_CP_INTERVAL * 1000 * 1000) {
            glob_stats->checkpoint();
            last_stats_cp_time += STATS_CP_INTERVAL * 1000000;
        }
        if (_native_txn) {
            // restart a previously aborted transaction
            rc = _native_txn->restart();
        } else {
  #if ENABLE_ADMISSION_CONTROL
            uint64_t tt = get_sys_clock();
            INC_FLOAT_STATS( time_process_txn, tt - last_idle_time );
            last_idle_time = tt;

            wait();

            tt = get_sys_clock();
            INC_FLOAT_STATS( time_idle, tt - last_idle_time );
            last_idle_time = tt;
            if (!_is_ready)
                continue;
  #endif
            // start a new transaction
            QueryBase * query = GET_WORKLOAD->gen_query();
            // txn_id format:
            //     | unique number | worker_thread_id | node_id |
            uint64_t txn_id = max_txn_id ++;
            txn_id = txn_id * g_num_worker_threads + _thd_id;
            txn_id = txn_id * g_num_nodes + g_node_id;

            _native_txn = new TxnManager(query, this);
            _native_txn->set_txn_id( txn_id );
            txn_table->add_txn( _native_txn );

            rc = _native_txn->start();
        }
        if (_native_txn->get_txn_state() == TxnManager::COMMITTED
            || (_native_txn->get_store_procedure()->is_self_abort()
                && _native_txn->get_txn_state() == TxnManager::ABORTED)) {
            // TODO the following line should be moved inside TxnManager.
            // INC_INT_STATS(num_aborts_terminate, 1);
            //if (_native_txn->get_txn_state() == TxnManager::COMMITTED)
            //    _num_complete_txns ++;
            if (_native_txn->get_txn_state() == TxnManager::COMMITTED)
                add_to_pool();
            txn_table->remove_txn(_native_txn);
            delete _native_txn;
            _native_txn = NULL;
        } else { // should restart
            assert( _native_txn->get_txn_state() == TxnManager::ABORTED);
            uint64_t sleep_time = g_abort_penalty * glob_manager->rand_double(); // in nanoseconds
            usleep(sleep_time / 1000);
           // printf("after sleep\n");
        }
    }
    glob_manager->worker_thread_done();
    INC_FLOAT_STATS(run_time, get_sys_clock() - init_time);
    //if (get_thd_id() == 0) {
    //    uint32_t size = txn_table->get_size();
    //    cout << "number of active transactions: " << size << endl;
    //}
    //delete system_txn_man;
    return RCOK;
}

void
WorkerThread::wakeup() {
    pthread_mutex_lock(_mutex);
    assert( _is_ready == false );
    _is_ready = true;
    pthread_mutex_unlock(_mutex);
    pthread_cond_signal(_cond);
}


void
WorkerThread::add_to_pool() {
    assert(_is_ready);
    _is_ready = false;
    if ( glob_manager->add_to_thread_pool( this ) )
        _is_ready = true;
}


void
WorkerThread::wait() {
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if (tp.tv_nsec > 900*1000*1000) {
        tp.tv_sec ++;
        tp.tv_nsec -= 900*1000*1000;
    } else
        tp.tv_nsec += 100*1000*1000;

    pthread_mutex_lock(_mutex);
    if (!_is_ready)
        pthread_cond_timedwait(_cond, _mutex, &tp);
    pthread_mutex_unlock(_mutex);
}
