#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc.h"
#include "server_thread.h"
#include "manager.h"
#include "query.h"
#include "transport.h"
#include "txn_table.h"
#include "input_thread.h"
#include "output_thread.h"
#include "caching.h"
#include "log.h"

void * start_thread(void *);

InputThread ** input_threads;
OutputThread ** output_threads;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	M_ASSERT(INDEX_STRUCT != IDX_BTREE, "btree is not supported yet\n");
	transport = new Transport * [g_num_input_threads];
	for (uint32_t i = 0; i < g_num_input_threads; i ++)
		transport[i] = new Transport(i);

	// g_num_worker_threads is the # of server threads running on each node
	g_num_worker_threads = g_num_server_threads;
	
	for (uint32_t i = 0; i < g_num_input_threads; i ++)
		transport[i]->test_connect();	

	g_total_num_threads = g_num_worker_threads + g_num_input_threads + g_num_output_threads;

	input_queues = new InOutQueue * [g_num_worker_threads];
	output_queues = new InOutQueue * [g_num_worker_threads];
	for (uint32_t i = 0; i < g_num_worker_threads; i++) {
		input_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
		new (input_queues[i]) InOutQueue;
		output_queues[i] = (InOutQueue *) _mm_malloc(sizeof(InOutQueue), 64);
		new (output_queues[i]) InOutQueue;
	}
  #if CC_ALG == TICTOC && ENABLE_LOCAL_CACHING 
	local_cache_man = new CacheManager;
  #endif

	stats = (Stats *) _mm_malloc(sizeof(Stats), 64);
	new(stats) Stats();

	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	txn_table = new TxnTable();
	log_manager = new LogManager();
	
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
	
	Thread ** worker_threads;
	server_threads = new ServerThread * [g_num_worker_threads];
	for (uint32_t i = 0; i < g_num_worker_threads; i++)
		server_threads[i] = new ServerThread(i);
	worker_threads = (Thread **) server_threads;
	input_threads = new InputThread * [g_num_input_threads];
	output_threads = new OutputThread * [g_num_output_threads];
	for (uint64_t i = 0; i < g_num_input_threads; i++)
		input_threads[i] = new InputThread(i + g_num_worker_threads);  // thread sequence number
	for (uint64_t i = 0; i < g_num_output_threads; i++)
		output_threads[i] = new OutputThread(i + g_num_input_threads + g_num_worker_threads);  // same here
	pthread_barrier_init( &global_barrier, NULL, g_num_worker_threads + g_num_input_threads + g_num_output_threads);
	pthread_mutex_init( &global_lock, NULL);

	warmup_finish = true;
	
	pthread_t pthreads[g_num_worker_threads + g_num_input_threads + g_num_output_threads]; 
	// spawn and run txns
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;

	int64_t starttime = get_server_clock();
	for (uint64_t i = 0; i < g_num_worker_threads - 1; i++) 
		pthread_create(&pthreads[i], NULL, start_thread, (void *)worker_threads[i]);
	for (uint64_t i = 0; i < g_num_input_threads; i++)
		pthread_create(&pthreads[g_num_worker_threads + i], NULL, start_thread, (void *)input_threads[i]);
	for (uint64_t i = 0; i < g_num_output_threads; i++)
		pthread_create(&pthreads[g_num_worker_threads + g_num_input_threads + i], NULL, start_thread, (void *)output_threads[i]);
	
	start_thread((void *)(worker_threads[g_num_worker_threads - 1]));
	
	for (uint32_t i = 0; i < g_num_worker_threads - 1; i++) 
		pthread_join(pthreads[i], NULL);
	for (uint64_t i = 0; i < g_num_input_threads + g_num_output_threads; i++)
		pthread_join(pthreads[g_num_worker_threads + i], NULL);
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t end_t = tp->tv_sec * 1000000000 + tp->tv_nsec;

	int64_t endtime = get_server_clock();
	int64_t runtime = end_t - start_t;
	if (abs(1.0 * runtime / (endtime - starttime) - 1) > 0.01)
		M_ASSERT(false, "the CPU_FREQ is inaccurate! correct value should be %f\n", 
			1.0 * runtime / (endtime - starttime) * CPU_FREQ);
	printf("PASS! SimTime = %ld\n", endtime - starttime);
  
#if CC_ALG == TICTOC && ENABLE_LOCAL_CACHING 
	delete local_cache_man;
#endif
	if (STATS_ENABLE)
		stats->print();
	return 0;
}

void * start_thread(void * thread) {
	Thread * thd = (Thread *) thread;
	thd->run();
	return NULL;
}
