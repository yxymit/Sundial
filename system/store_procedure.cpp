#include "store_procedure.h"
#include "cc_manager.h"
#include "manager.h"
#include "txn.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "workload.h"
#include "index_base.h"
#include "tictoc_manager.h"

StoreProcedure::~StoreProcedure()
{
    delete _query;
}

void
StoreProcedure::init() {
    _self_abort = false;
    _phase = 0;
    _curr_row = NULL;
    _curr_query_id = 0;
    _curr_offset = 0;
    //remote_requests.clear();
}

void
StoreProcedure::set_query(QueryBase * query)
{
#if CC_ALG != MAAT
    assert(_query);  // _query might be NULL in MaaT.
#endif
    delete _query;
    _query = query;
}

CCManager *
StoreProcedure::get_cc_manager()
{
    return _txn->get_cc_manager();
}

void
StoreProcedure::txn_abort()
{
    _phase = 0;
    _curr_row = NULL;
    _curr_query_id = 0;
    _curr_offset = 0;
    //remote_requests.clear();
}

/*
RC
StoreProcedure::process_remote_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data)
{
    RC rc = RCOK;
    UnstructuredBuffer buffer(data + _curr_offset);
    while (_curr_offset != size) {
        //    | key | index_id | type | [optional] cc_specific_data |
        uint64_t key = -1;
        uint32_t index_id = -1;
        access_t type;
        uint32_t rec_size = sizeof(key) + sizeof(index_id) + sizeof(type);
        buffer.get( &key );
        buffer.get( &index_id );
        buffer.get( &type );

        INDEX * index = GET_WORKLOAD->get_index(index_id);
        set<row_t *> * rows = NULL;
        // TODO. all the matching rows should be returned.
        rc = get_cc_manager()->index_read(index, key, rows, 1);
        if (rc != RCOK) return rc;
        _curr_row = *rows->begin();
#if CC_ALG == TICTOC && ENABLE_LOCAL_CACHING
        uint64_t wts = -1;
        if (type == RD) {
            buffer.get( &wts );
            rec_size += sizeof(wts);
        }
        rc = ((TicTocManager *)get_cc_manager())->get_row(_curr_row, type, key, wts);
#else
        rc = get_cc_manager()->get_row(_curr_row, type, key);
#endif
        if (rc != RCOK) return rc;
        _curr_offset += rec_size;
    }
    get_cc_manager()->get_resp_data(resp_size, resp_data);
    assert(resp_size > 0);
    return RCOK;
}*/
