#include <sched.h>
#include "global.h"
#include "helper.h"
#include "workload.h"
#include "server_thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "query.h"

#include "ycsb.h"
#include "ycsb_query.h"
#include "ycsb_store_procedure.h"

#if WORKLOAD == YCSB

int WorkloadYCSB::next_tid;

RC WorkloadYCSB::init() {
	workload::init();
	next_tid = 0;
	char * cpath = getenv("GRAPHITE_HOME");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/YCSB_schema.txt";
	else { 
		path = string(cpath);
		path += "/tests/apps/dbms/YCSB_schema.txt";
	}
	init_schema( path );
	init_table_parallel();
	return RCOK;
}

RC WorkloadYCSB::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables[0]; 	
	the_index = indexes[0];
	return RCOK;
}
	
int 
WorkloadYCSB::key_to_part(uint64_t key) {
	return 0;
}
	
uint32_t 
WorkloadYCSB::key_to_node(uint64_t key, uint32_t table_id)
{
	return key % g_num_nodes;
}

// init table in parallel
void WorkloadYCSB::init_table_parallel() {
	enable_thread_mem_pool = true;
	pthread_t p_thds[g_init_parallelism - 1];
	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
}

void * WorkloadYCSB::init_table_slice() {
	uint32_t tid = ATOM_FETCH_ADD(next_tid, 1);
	RC rc;
	assert(tid < g_init_parallelism);
	while ((uint32_t)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((uint32_t)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);

	uint64_t start = tid * g_synth_table_size / g_init_parallelism; 
	uint64_t end = (tid + 1) * g_synth_table_size / g_init_parallelism; 
	for (uint64_t key = start; key < end; key ++)
	{
		row_t * new_row = NULL;
		int part_id = key_to_part(key);
		rc = the_table->get_new_row(new_row, part_id); 
		assert(rc == RCOK);
		// LSBs of a key indicate the node ID 
		uint64_t primary_key = key * g_num_server_nodes + g_node_id;
		new_row->set_value(0, &primary_key);
		Catalog * schema = the_table->get_schema();
		
		for (uint32_t fid = 1; fid < schema->get_field_cnt(); fid ++) {
			char value[6] = "hello";
			new_row->set_value(fid, value);
		}
		uint64_t idx_key = primary_key;
		
		rc = the_index->insert(idx_key, new_row);

		assert(idx_key == new_row->get_primary_key());
		assert(rc == RCOK);
	}
	return NULL;
}

StoreProcedure * 
WorkloadYCSB::create_store_procedure(TxnManager * txn, QueryBase * query)
{
	return new YCSBStoreProcedure(txn, query);
}

QueryBase * 
WorkloadYCSB::gen_query()
{
	QueryBase * query = (QueryYCSB *) MALLOC(sizeof(QueryYCSB));
	new(query) QueryYCSB();
	return query;
}

QueryBase * 
WorkloadYCSB::clone_query(QueryBase * query) 
{
	QueryYCSB * q = (QueryYCSB *) query;
	QueryYCSB * new_q = new QueryYCSB(q->get_requests(), q->get_request_count());
	return new_q;	
}

QueryBase * 
WorkloadYCSB::deserialize_subquery(char * data)
{
	QueryYCSB * query = (QueryYCSB *) MALLOC(sizeof(QueryYCSB));
	new(query) QueryYCSB(data);
	return query;
}

void
WorkloadYCSB::table_to_indexes(uint32_t table_id, set<INDEX *> * indexes)
{
	assert(table_id == 0);
	indexes->insert(the_index);
}

uint64_t 
WorkloadYCSB::get_primary_key(row_t * row)
{
	uint64_t key;
	row->get_value(0, &key);
	return key;
}

#endif
