#include "stats.h"
#include "manager.h"
#include "query.h"
#include "transport.h"
#include "txn_table.h"
#include "log.h"

Stats * stats;
Manager * glob_manager;
LogManager * log_manager;

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t global_barrier;
pthread_mutex_t global_lock;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

#if ENABLE_LOCAL_CACHING
CacheManager * local_cache_man;
#endif

////////////////////////////
// Global Parameter
////////////////////////////
uint64_t g_abort_penalty     = ABORT_PENALTY;
uint32_t g_ts_alloc         = TS_ALLOC;
bool g_key_order             = KEY_ORDER;
bool g_ts_batch_alloc         = TS_BATCH_ALLOC;
uint32_t g_ts_batch_num     = TS_BATCH_NUM;
uint32_t g_max_num_active_txns = MAX_NUM_ACTIVE_TXNS;
double g_warmup_time         = WARMUP_TIME;
double g_run_time             = RUN_TIME;
bool g_prt_lat_distr         = PRT_LAT_DISTR;
uint64_t g_max_clock_skew    = MAX_CLOCK_SKEW;

////////////////////////////
// YCSB
////////////////////////////
uint32_t g_cc_alg             = CC_ALG;
double g_perc_remote         = PERC_REMOTE;
double g_read_perc             = READ_PERC;
double g_zipf_theta         = ZIPF_THETA;
uint64_t g_synth_table_size = SYNTH_TABLE_SIZE;
uint32_t g_req_per_query     = REQ_PER_QUERY;
uint32_t g_init_parallelism = INIT_PARALLELISM;
double g_readonly_perc         = PERC_READONLY_DATA;

////////////////////////////
// TPCC
////////////////////////////
uint32_t g_num_wh             = NUM_WH;
uint32_t g_payment_remote_perc = PAYMENT_REMOTE_PERC;
uint32_t g_new_order_remote_perc = NEW_ORDER_REMOTE_PERC;
double g_perc_payment         = PERC_PAYMENT;
double g_perc_new_order     = PERC_NEWORDER;
double g_perc_order_status     = PERC_ORDERSTATUS;
double g_perc_delivery         = PERC_DELIVERY;

////////////////////////////
// TATP
////////////////////////////
uint64_t g_tatp_population     = TATP_POPULATION;


#if TPCC_SMALL
uint32_t g_max_items = 10000;
uint32_t g_cust_per_dist = 2000;
#else
uint32_t g_max_items = 100000;
uint32_t g_cust_per_dist = 3000;
#endif


char * output_file = NULL;
char ifconfig_file[80] = "ifconfig.txt";

// TICTOC
uint32_t g_max_num_waits = MAX_NUM_WAITS;
uint64_t g_local_cache_size = LOCAL_CACHE_SIZE;
double g_read_intensity_thresh = READ_INTENSITY_THRESH;

//////////////////////////////////////////////////
// Distributed DBMS
//////////////////////////////////////////////////

uint32_t g_num_worker_threads = 0;
uint32_t g_num_server_threads = NUM_SERVER_THREADS;
uint32_t g_total_num_threads = 0;

uint32_t g_num_nodes = 0;
uint32_t g_num_client_nodes = 0;
uint32_t g_num_server_nodes = 0;

uint32_t g_node_id;

bool g_is_client_node = false;

uint32_t g_num_input_threads = NUM_INPUT_THREADS;
uint32_t g_num_output_threads = NUM_OUTPUT_THREADS;

Transport ** transport;
InOutQueue ** input_queues;
InOutQueue ** output_queues;
ServerThread ** server_threads;
// TODO. tune this table size
uint32_t g_txn_table_size = NUM_SERVER_THREADS * 10;
TxnTable * txn_table;

FreeQueue * free_queue_txn_man;
uint32_t g_dummy_size = 0;
