#include "global.h"
#include "helper.h"
#include "time.h"

//int get_thdid_from_txnid(uint64_t txnid) {
//    return txnid % g_num_worker_threads;
//}

//uint64_t get_part_id(void * addr) {
//    return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt;
//}

//uint64_t key_to_part(uint64_t key) {
//    return 0;
//}
//
//uint64_t txn_id_to_node_id(uint64_t txn_id)
//{
//    return txn_id % g_num_nodes;
//}
//
//uint64_t txn_id_to_thread_id(uint64_t txn_id)
//{
//    return (txn_id / g_num_server_nodes) % g_num_server_threads;
//}
