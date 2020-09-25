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

/*#include "rpc_server.h"
#include "rpc_client.h"*/
#include "sundial_grpc.grpc.pb.h"
#include "sundial_grpc.pb.h"
#include "grpc_async_server.h"
#include "grpc_sync_server.h"
#include "grpc_async_client.h"
#include "grpc_sync_client.h"

void * start_thread(void *);
void * start_sync_rpc_server(void *);
void * start_async_rpc_server(void *);

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{
    parser(argc, argv);
    cout<< "node id is "<< g_node_id<<"\n";
    //string sync_port(":5100");
    //string async_port(":5101");
    //printf("async port address is %s\n",async_port.c_str());
    //string p1(":8170");
    //string p2(":6150");
    //sync_port=":8170";
    //async_port=":6150";
    g_total_num_threads = g_num_worker_threads;

    glob_manager = new Manager;
    txn_table = new TxnTable();
    glob_manager->calibrate_cpu_frequency();

#if DISTRIBUTED
    //get all server addresses here
    //std::shared_ptr<grpc::Channel>channels_sync[g_num_nodes];
    //std::shared_ptr<grpc::Channel>channels_async[g_num_nodes];
    string channels[8];
    std::istringstream in(ifconfig_string);
    string line;
    uint32_t num_nodes = 0;
    while ( num_nodes < g_num_nodes && getline(in, line) ) {
        if (line[0] == '#')
            continue;
        else {
            string url1 = line;
            string url2 = line;
            if (num_nodes != g_node_id){
                /*
                url1.append(sync_port);
                url2.append(async_port);
                //cout<<url1<<"\n";
                channels_sync[num_nodes]=grpc::CreateChannel(
                url1, grpc::InsecureChannelCredentials());
                channels_async[num_nodes]=grpc::CreateChannel(
                url2, grpc::InsecureChannelCredentials());*/
                channels[num_nodes]=line;
            }
            num_nodes ++;
        }
    }
    //printf("access address accomplished\n");
    
    grpc_async_server=new SundialAsyncServiceImp;
    grpc_sync_server = new SundialServiceImp;
    //printf("create sync server accomplished\n");
    //grpc_async_server->run();
    //grpc_sync_server->run();

    pthread_t * pthread_rpc = new pthread_t;
    pthread_t * pthread_rpc1 = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_sync_rpc_server, NULL);
    pthread_create(pthread_rpc1, NULL, start_async_rpc_server, NULL);
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
    printf("start building clients\n");
    grpc_async_client= new Sundial_Async_Client(channels);
    grpc_sync_client= new Sundial_Sync_Client(channels);
    #endif
#if DISTRIBUTED
    cout << "Synchronization starts" << endl;
    // Notify other nodes that the current node has finished initialization
    for (int i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id) continue;
        //printf("syncronize with other nodes\n");
        SundialRequest request;
        SundialResponse response;
        request.set_request_type( SundialRequest::SYS_REQ );
        //rpc_client->sendRequest(i, request, response);
        while(response.response_type()!=SundialResponse::SYS_RESP){
            usleep(1000000);
        grpc_sync_client->contactRemote(i, request, &response);
        }
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
    printf("worker thread joined\n");    
#if DISTRIBUTED
    assert( glob_manager->are_all_worker_threads_done() );
    SundialRequest request;
    SundialResponse response;
    request.set_request_type( SundialRequest::SYS_REQ );
    // Notify other nodes the completion of the current node.
    for (int i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id) continue;
        //rpc_client->sendRequest(i, request, response);
        printf("upon completion\n");
        grpc_sync_client->contactRemote(i,request,&response);
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
    printf("reach right before stats enable\n");
    if (STATS_ENABLE){
         printf("reach right before print stats\n");
        glob_stats->print();
    }

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

void * start_sync_rpc_server(void * input) {
    printf("running a sync server\n");
    //rpc_server->run();
    grpc_sync_server->run();
    //printf("running sync server done\n");    
    return NULL;
}

void * start_async_rpc_server(void * input) {
    printf("running a async server\n");
    grpc_async_server->run();
    //printf("running async server done\n");   
    return NULL;
}