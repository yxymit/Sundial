#include <sched.h>
#include <iomanip>
#include "global.h"
#include "manager.h"
#include "server_thread.h"
#include "txn.h"
#include "store_procedure.h"
#include "workload.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "message.h"
#include "txn_table.h"
#include "transport.h"
#include "cc_manager.h"
#if CC_ALG == MAAT
#include "maat_manager.h"
#endif

#define DEBUG_INFO_DELAY_SEC 3

ServerThread::ServerThread(uint64_t thd_id)
    : Thread(thd_id, WORKER_THREAD)
{
    pthread_mutex_init(&cond_mutex, NULL);
    pthread_cond_init (&cond, NULL);
    _msg = NULL;
    _native_txn = NULL;
    already_printed_debug = false;
}

// Each thread executes at most one active transaction.
// If the transaction has aborted or is waiting for a lock, it will sleep, waiting for a signal.

// For local miss, suspend the txn in txn_table
// For txn abort, add the txn to abort_buffer
// For txn waiting, add the txn to wait_buffer

// when a worker thread is free, check the job queues in the following priority
// 1. wait_buffer
// 2. remote message
// 3. abort_buffer

// Worker thread
RC ServerThread::run() {
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    assert( glob_manager->get_thd_id() == get_thd_id() );

    pthread_barrier_wait( &global_barrier );

    RC rc = RCOK;
    assert (rc == RCOK);

    uint64_t init_time = get_sys_clock();
    _num_active_txns = 0;
    // calculate which client thread this server thread corresponds to.
    uint64_t max_txn_id = 0;

    bool sim_done = false;
    TxnManager * system_txn_man = new TxnManager(this, false);
    uint64_t last_stats_cp_time = init_time;
    Message * msg = NULL;
    TxnManager * txn_man = NULL;
    //////////////////////////
    // Main loop
    //////////////////////////
    while (true) {
        // try to get job from waiting buffer
        // TODO. waiting buffer can be optimized.
        txn_man = NULL;
        uint64_t t0 = get_sys_clock();
        for (auto txn : _wait_buffer)
        {
            if (txn->is_txn_ready()) {
                _wait_buffer.erase(txn);
                txn_man = txn;
                break;
            }
        }
        INC_FLOAT_STATS(time_wait_buffer, get_sys_clock() - t0);
        uint64_t t2 = get_sys_clock();
        if (txn_man) {
            rc = txn_man->continue_execute();
            handle_req_finish(rc, txn_man);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t2);
            continue;
        }
        uint64_t t1 = get_sys_clock();

        // checkpoint the stats every 100 ms.
        if (GET_THD_ID == 0 && get_sys_clock() - last_stats_cp_time > STATS_CP_INTERVAL * 1000 * 1000) {
            stats->checkpoint();
            last_stats_cp_time += STATS_CP_INTERVAL * 1000000;
        }

        // For Distributed DBMS
        if (_msg || input_queues[get_thd_id()]->pop(msg)) {
            if (_msg) {
                msg = _msg;
                _msg = NULL;
            }
            INC_FLOAT_STATS(time_read_input_queue, get_sys_clock() - t1);
            uint64_t t2 = get_sys_clock();
            // make sure the correct txn_id is received.
            M_ASSERT(msg && (msg->get_txn_id() / g_num_server_nodes) % g_num_server_threads == get_thd_id(),
                    "msg=%ld\n", (uint64_t)msg);
            if (msg->get_type() != Message::CLIENT_REQ) {
                txn_man = txn_table->get_txn(msg->get_txn_id());
            }
            if (txn_man == NULL) {
                if (msg->get_type() == Message::PREPARE_REQ
                    || msg->get_type() == Message::COMMIT_REQ
                    || msg->get_type() == Message::ABORT_REQ)
                {
                    assert((CC_ALG == TICTOC && ENABLE_LOCAL_CACHING) || CC_ALG == MAAT);  // maat allows short-cut abort
                    system_txn_man->get_cc_manager()->init();
                    system_txn_man->set_txn_id( msg->get_txn_id() );
                    system_txn_man->process_msg(msg);
                    DELETE(Message, msg);
                    continue;
                }
                M_ASSERT(msg->get_type() == Message::REQ || msg->get_type() == Message::CLIENT_REQ,
                        "msg->type = %s\n", msg->get_name().c_str());
                txn_man = new TxnManager(msg, this);
                txn_table->add_txn( txn_man );
            }
            RC rc = txn_man->process_msg(msg);
            handle_req_finish(rc, txn_man);

            if (rc == RCOK || rc == ABORT || rc == COMMIT)
                DELETE(Message, msg);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t2);
            continue;
        } else
            INC_FLOAT_STATS(time_read_input_queue, get_sys_clock() - t1);
        // TODO. should balance the priority between abort queue and input queue.
        uint64_t t3 = get_sys_clock();
        // re-execute aborted txn
        if (_native_txn && _native_txn->get_txn_state() == TxnManager::ABORTED && get_sys_clock() > _ready_time) {
            TxnManager * restart_txn = new TxnManager( _native_txn );
#if CC_ALG == MAAT
            MaaTManager *maatman = (MaaTManager*)(_native_txn->get_cc_manager());
            uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);

            // TODO. this is a hack now
            // for MAAT, need to change txn id.
            // since otherwise there will more multiple txns in the txn_table with the
            // same txn id.
            uint64_t txn_id = max_txn_id ++;
            txn_id = txn_id * g_num_server_threads + _thd_id;
            txn_id = txn_id * g_num_server_nodes + g_node_id;
            restart_txn->set_txn_id( txn_id );
            if (rf == 0)
#endif
            {
                txn_table->remove_txn(_native_txn);
                delete _native_txn;
            }

            _native_txn = restart_txn;
            txn_table->add_txn( _native_txn );

            rc = _native_txn->start_execute();
            handle_req_finish(rc, _native_txn);
            if (rc == COMMIT)
                assert(_native_txn == NULL);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t3);
            continue;
        }
        if (get_sys_clock() - init_time > (g_warmup_time + g_run_time) * BILLION) {
            if (!sim_done && !_native_txn) {
                sim_done = true;
                glob_manager->worker_thread_done();
                glob_manager->set_gc_ts( (uint64_t)-1 );
            }
            if (glob_manager->is_sim_done())
            {
                if (GET_THD_ID==0) stats->checkpoint();
                break;
            }
            continue;
        }
        // Generate a new transaction
        if (!_native_txn) {
            INC_INT_STATS(num_home_txn, 1);
            QueryBase * query = GET_WORKLOAD->gen_query();
            // txn_id format:
            //     | unique number | server_thread_id | server_node_id |
            uint64_t txn_id = max_txn_id ++;
            txn_id = txn_id * g_num_server_threads + _thd_id;
            txn_id = txn_id * g_num_server_nodes + g_node_id;

            _native_txn = (TxnManager *) MALLOC(sizeof(TxnManager));
            new(_native_txn) TxnManager(query, this);
            _native_txn->set_txn_id( txn_id );
            txn_table->add_txn( _native_txn );

            rc = _native_txn->start_execute();
            handle_req_finish(rc, _native_txn);
            if (rc == COMMIT)
                assert(_native_txn == NULL);
            INC_FLOAT_STATS(time_process_txn, get_sys_clock() - t3);
            continue;
        }
        assert(!_msg);
        assert(_native_txn);
        if (_native_txn->get_txn_state() == TxnManager::ABORTED) {
#if ENABLE_LOCAL_CACHING
            if (!_native_txn->is_all_remote_readonly())
#endif
            {
                int64_t wait_time = _ready_time - get_sys_clock();
                if (wait_time > 100 * 1000)
                    PAUSE100
            }
            INC_FLOAT_STATS(time_abort_queue, get_sys_clock() - t3);
            continue;
        }

        bool done = false;
        for (auto txn_man : _wait_buffer)
            if (txn_man->is_txn_ready())
                done = true;

        if (done || input_queues[get_thd_id()]->pop(_msg)) {
            continue;
        }
        PAUSE100
        INC_FLOAT_STATS(time_idle, get_sys_clock() - t3);
    }
    INC_FLOAT_STATS(run_time, get_sys_clock() - init_time);
    if (get_thd_id() == 0) {
        uint32_t size = txn_table->get_size();
        M_ASSERT(size == 0, "size = %d", size);
    }
    delete system_txn_man;
    return FINISH;
}

// RCOK: txn active, do nothing.
// COMMIT: txn commits
// ABORT: txn aborts
// WAIT: txn waiting for lock
// LOCAL_MISS: txn needs data from a remote node.
void
ServerThread::handle_req_finish(RC rc, TxnManager * &txn_man)
{
#if DEBUG_CC
    printf("[txn=%ld] rc=%d\n", txn_man->get_txn_id(), rc);
#endif
    if (rc == LOCAL_MISS)
        return;
    else if (rc == WAIT) {
        INC_INT_STATS(num_waits, 1);
        _wait_buffer.insert(txn_man);
        return;
    }
    else if (rc == RCOK) {
#if CC_ALG == TICTOC
        // For a readonly remote sub-txn in TICTOC, the client might be
        // able to commit the txn locally without contacting the remote node again.
        // Therefore, we do not keep any local information for this sub-txn.
        // If the client does need to contact a remote node again (e.g., lease renewal)
        // all the involved rts will be sent again.
        if (txn_man->is_sub_txn()
            && txn_man->get_cc_manager()->is_read_only()
            && txn_man->get_txn_state() == TxnManager::PREPARING
            )
        {
            txn_table->remove_txn( txn_man );
            delete txn_man;
            txn_man = NULL;
        }
        return;
#endif
    } else {
        assert(rc == COMMIT || rc == ABORT);
        txn_man->print_state();
        txn_man->update_stats();
        if (txn_man->is_sub_txn()) {
#if CC_ALG == MAAT
            MaaTManager *maatman = (MaaTManager*)(txn_man->get_cc_manager());
            uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);
            if (rf == 0) {
                txn_table->remove_txn(txn_man);
                DELETE(TxnManager, txn_man);
            }
#else
            txn_table->remove_txn( txn_man );
            delete txn_man;
#endif
            txn_man = NULL;
        } else {
            if (rc == ABORT) {
                if ((WORKLOAD == TPCC && txn_man->get_store_procedure()->is_self_abort())
                    || (MAX_NUM_ABORTS > 0 && txn_man->get_num_aborts() >= MAX_NUM_ABORTS))
                {
#if CC_ALG == MAAT
                    MaaTManager *maatman = (MaaTManager*)(txn_man->get_cc_manager());
                    uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);
                    if (rf == 0)
#endif
                    {
                        txn_table->remove_txn(txn_man);
                        DELETE(TxnManager, txn_man);
                    }

                    assert(txn_man == _native_txn);
                    _native_txn = NULL;
                    txn_man = NULL;
                    INC_INT_STATS(num_aborts_terminate, 1);
                    return;
                }
                INC_INT_STATS(num_aborts_restart, 1);
                _ready_time = get_sys_clock() + g_abort_penalty * glob_manager->rand_double();
            } else {
#if CC_ALG == MAAT
                MaaTManager *maatman = (MaaTManager*)(txn_man->get_cc_manager());
                uint64_t rf = ATOM_SUB_FETCH(maatman->_refcount, 1);
#if DEBUG_REFCOUNT
                ATOM_COUT("S2 DEC REFCOUNT of " << txn_man->get_txn_id() << " to " << rf << ", BY ServerThread" << endl);
#endif
                if (rf == 0)
#endif
                {
                    txn_table->remove_txn(txn_man);
                    DELETE(TxnManager, txn_man);
                }
                if (txn_man == _native_txn)
                    _native_txn = NULL;
                txn_man = NULL;
                _num_active_txns --;
                // so we will always decrease the number of active txns in
                // server_thread.cpp as committed (but still in the table) txn
                // should not be seen as an active txn.
                //delete txn_man;
            }
        }
    }
}

void
ServerThread::signal()
{
    pthread_mutex_lock( &cond_mutex );
    pthread_cond_signal( &cond );
    pthread_mutex_unlock( &cond_mutex );
}
