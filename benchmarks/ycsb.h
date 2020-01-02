#pragma once

#include "workload.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class QueryYCSB;
class SubQueryYCSB;
class StoreProcedure;

class WorkloadYCSB : public workload {
public :
    RC init();
    RC init_schema(string schema_file);
    RC          init_schema(std::istream &in);
    StoreProcedure * create_store_procedure(TxnManager * txn, QueryBase * query);
    QueryBase * gen_query();
    QueryBase * clone_query(QueryBase * query);
    QueryBase * deserialize_subquery(char * data);

    uint64_t    get_primary_key(row_t * row);
    uint64_t    get_index_key(row_t * row, uint32_t index_id)
    { return get_primary_key(row); }
    INDEX *     get_index() { return the_index; }
    INDEX *     get_index(uint32_t index_id) { return the_index; }
    table_t *   get_table(uint32_t table_id) { return the_table; };
    void        table_to_indexes(uint32_t table_id, set<INDEX *> * indexes);

    int         key_to_part(uint64_t key);
    uint32_t    key_to_node(uint64_t key, uint32_t table_id = 0);
    INDEX *     the_index;
    table_t *   the_table;
private:
    void init_table_parallel();
    void * init_table_slice();
    static void * threadInitTable(void * This) {
        ((WorkloadYCSB *)This)->init_table_slice();
        return NULL;
    }
    pthread_mutex_t insert_lock;
    //  For parallel initialization
    static int next_tid;
};

extern std::string YCSB_schema_string;
