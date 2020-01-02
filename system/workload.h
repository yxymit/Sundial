#pragma once

#include "global.h"

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class TxnManager;
class WorkerThread;
class IndexBase;
class Timestamp;
class Mvcc;
class QueryBase;
class StoreProcedure;

// this is the base class for all workload
class workload
{
public:
    virtual ~workload() {}
    // tables indexed by table name
    vector<table_t *> tables;
    vector<INDEX *> indexes;

    // initialize the tables and indexes.
    virtual RC              init();
    virtual RC              init_schema(std::istream &in);

    virtual StoreProcedure * create_store_procedure(TxnManager * txn, QueryBase * query) { assert(false) ; return NULL; }

    virtual QueryBase *     gen_query() = 0;
    virtual QueryBase *     clone_query(QueryBase * query) = 0;
    virtual QueryBase *     deserialize_subquery(char * data) = 0;

    virtual uint64_t        get_primary_key(row_t * row) = 0;
    virtual uint64_t        get_index_key(row_t * row, uint32_t index_id) = 0;
    virtual INDEX *         get_index() { return NULL; }
    virtual INDEX *         get_index(uint32_t index_id) { return NULL; }
    virtual table_t *       get_table(uint32_t table_id) { assert(false); return NULL; }
    virtual uint32_t        index_to_table(uint32_t index_id) { return 0; }
    virtual void            table_to_indexes(uint32_t table_id, set<INDEX *> * indexes) { assert(false); }
    virtual Isolation       get_isolation_type(uint32_t txn_type) { return SR; }

    virtual uint32_t        key_to_node(uint64_t key, uint32_t table_id = 0) = 0;
    bool sim_done;
protected:
    void index_insert(INDEX * index, uint64_t key, row_t * row);
};

