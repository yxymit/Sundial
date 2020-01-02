#pragma once
#include "global.h"

class QueryBase;
class CCManager;
class TxnManager;
class row_t;

class StoreProcedure {
public:
    StoreProcedure(TxnManager * txn_man, QueryBase * query)
        : _query(query)
        , _txn(txn_man)
        //, _is_single_partition(true)
    {
        init();
    }
    virtual ~StoreProcedure();

    void init();

    QueryBase * get_query() { return _query; }
    void set_query(QueryBase * query);

    virtual RC execute() = 0;

    //RC process_remote_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data);

    virtual void txn_abort();
    // for a sub transaction
    bool is_self_abort() { return _self_abort; }

    access_t _local_miss_type;
    uint64_t _local_miss_key;
    //map<uint32_t, UnstructuredBuffer> remote_requests;
    //bool is_single_partition() { return _is_single_partition; }
protected:
#define LOAD_VALUE(type, var, schema, data, col) \
    type var = *(type *)row_t::get_value(schema, col, data);
#define STORE_VALUE(var, schema, data, col) \
    row_t::set_value(schema, col, data, (char *)&var);

#define GET_DATA(key, index, type) {{ \
    set<row_t *> * rows = NULL; \
    rc = get_cc_manager()->index_read(index, key, rows, 1); \
    assert(rc == RCOK || rc == ABORT); \
    if (rc != RCOK) return rc; \
    assert(!rows->empty()); \
    _curr_row = *rows->begin(); \
    rc = get_cc_manager()->get_row(_curr_row, type, _curr_data, key);\
    assert(rc == RCOK || rc == ABORT); \
    if (rc != RCOK) return rc; }}


#define REMOTE_ACCESS(node_id, key, type, table, index) {\
    if (node_id != g_node_id) { \
        uint32_t cc_specific_msg_size = 0; \
        char * cc_specific_msg_data = NULL; \
        RC rc = get_cc_manager()->register_remote_access(node_id, type, key, table, \
                        cc_specific_msg_size, cc_specific_msg_data); \
        if (rc == LOCAL_MISS) { \
            if (remote_requests.find(node_id) == remote_requests.end()) \
                remote_requests[node_id] = UnstructuredBuffer(); \
            uint32_t iid = index; \
            uint32_t access_type = type; \
            uint64_t k = key; \
            remote_requests[node_id].put( &k ); \
            remote_requests[node_id].put( &iid ); \
            remote_requests[node_id].put( &access_type ); \
            if (cc_specific_msg_size > 0) { \
                remote_requests[node_id].put( cc_specific_msg_data, cc_specific_msg_size ); \
                delete cc_specific_msg_data; \
        }}}}



    CCManager *       get_cc_manager();
    QueryBase *       _query;
    TxnManager *      _txn;

    bool              _self_abort;

    //bool              _is_single_partition;
    // [For distributed DBMS]
    uint32_t          _phase;
    bool              _waiting_for_index;
    bool              _waiting_for_data;
    row_t *           _curr_row;
    char *            _curr_data;
    uint32_t          _curr_query_id;
    uint64_t          _curr_offset;
};
