#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc.h"
#include "worker_thread.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"

#if LOG_ENABLE
#include "logging_thread.h"
#include "log.h"
#endif

#include "stubby/rpc_server.h"
#include "stubby/rpc_client.h"

void * start_thread(void *);
void * start_rpc_server(void *);

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{
    g_node_id = ;
    parser(argc, argv);

    g_total_num_threads = g_num_worker_threads;

    glob_manager = new Manager;
    txn_table = new TxnTable();
    glob_manager->calibrate_cpu_frequency();

#if DISTRIBUTED
    rpc_client = new SundialRPCClient;
    rpc_server = new SundialRPCServerImpl;

    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);
#endif
#if LOG_ENABLE
    g_total_num_threads ++;
    log_manager = new LogManager();
#endif

    glob_stats = new Stats;

    printf("mem_allocator initialized!\n");
    workload * m_wl;
    switch (WORKLOAD) {
        case YCSB :
            m_wl = new WorkloadYCSB;
            QueryYCSB::calculateDenom();
            break;
        case TPCC :
            m_wl = new WorkloadTPCC;
            break;
        default:
            assert(false);
    }

    glob_manager->set_workload(m_wl);
    m_wl->init();
    printf("workload initialized!\n");
    warmup_finish = true;
    pthread_barrier_init( &global_barrier, NULL, g_total_num_threads);
    pthread_mutex_init( &global_lock, NULL);

    // Thread numbering:
    //    worker_threads | input_thread | output_thread | logging_thread
    uint32_t next_thread_id = 0;
    WorkerThread ** worker_threads = new WorkerThread * [g_num_worker_threads];
    pthread_t ** pthreads_worker = new pthread_t * [g_num_worker_threads];
    for (uint32_t i = 0; i < g_num_worker_threads; i++) {
        worker_threads[i] = new WorkerThread(next_thread_id ++);
        pthreads_worker[i] = new pthread_t;
    }
#if DISTRIBUTED
    cout << "Synchronization starts" << endl;
    // Notify other nodes that the current node has finished initialization
    for (int i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id) continue;
        SundialRequest request;
        SundialResponse response;
        request.set_request_type( SundialRequest::SYS_REQ );
        rpc_client->sendRequest(i, request, response);
    }
    // Can start only if all other nodes have also finished initialization
    while (glob_manager->num_sync_requests_received() < g_num_nodes - 1)
        usleep(1);
    cout << "Synchronization done" << endl;
#endif
    for (uint64_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_create(pthreads_worker[i], NULL, start_thread, (void *)worker_threads[i]);

#if LOG_ENABLE
    LoggingThread * logging_thread = new LoggingThread(next_thread_id ++);
    pthread_t * pthreads_logging = new pthread_t;
    pthread_create(pthreads_logging, NULL, start_thread, (void *)logging_thread);
#endif
    assert(next_thread_id == g_total_num_threads);

    uint64_t starttime = get_server_clock();
    start_thread((void *)(worker_threads[g_num_worker_threads - 1]));

    for (uint32_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_join(*pthreads_worker[i], NULL);
#if DISTRIBUTED
    assert( glob_manager->are_all_worker_threads_done() );
    SundialRequest request;
    SundialResponse response;
    request.set_request_type( SundialRequest::SYS_REQ );
    // Notify other nodes the completion of the current node.
    for (int i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id) continue;
        rpc_client->sendRequest(i, request, response);
    }
    while (glob_manager->num_sync_requests_received() < (g_num_nodes - 1) * 2)
        usleep(1);
#endif
#if LOG_ENABLE
    pthread_join(*pthreads_logging, NULL);
#endif
    assert( txn_table->get_size() == 0 );
    uint64_t endtime = get_server_clock();
    cout << "Complete. Total RunTime = " << 1.0 * (endtime - starttime) / BILLION << endl;
    if (STATS_ENABLE)
        glob_stats->print();

    for (uint32_t i = 0; i < g_num_worker_threads; i ++) {
        delete pthreads_worker[i];
        delete worker_threads[i];
    }
    delete [] pthreads_worker;
    delete [] worker_threads;
#if LOG_ENABLE
    delete pthreads_logging;
    delete logging_thread;
    delete log_manager;
#endif
    return 0;
}

void * start_thread(void * thread) {
    ((BaseThread *)thread)->run();
    return NULL;
}

void * start_rpc_server(void * input) {
    rpc_server->run();
    return NULL;
}
