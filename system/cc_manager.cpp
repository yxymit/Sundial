#include "cc_manager.h"
#include "store_procedure.h"
#include "lock_manager.h"
#include "f1_manager.h"
#include "tictoc_manager.h"
#include "index_btree.h"
#include "index_hash.h"
#include "manager.h"
#include "txn.h"
#include "row.h"
#include "table.h"
#include "workload.h"

CCManager *
CCManager::create(TxnManager * txn)
{
    return new CC_MAN(txn);
}

CCManager::CCManager(TxnManager * txn)
    : _txn(txn)
{
    _time_in_cc_man = 0;
    init();
}

void
CCManager::init()
{
    _restart = false;
    _deletes.clear();
    _inserts.clear();
    //_remote_node_info.clear();
}

StoreProcedure *
CCManager::get_store_procedure()
{
    return _txn->get_store_procedure();
}

RC
CCManager::row_insert(table_t * table, row_t * row)
{
    RC rc = RCOK;
    _num_lock_waits = 0;
    InsertOp insert = {table, row};
    _inserts.push_back(insert);
    set<INDEX *> indexes;
    table->get_indexes( &indexes );
    for (auto idx : indexes) {
        uint64_t idx_key = GET_WORKLOAD->get_index_key( row, idx->get_index_id() );
        RC rc1 = index_insert(idx, idx_key);
        if (rc1 == ABORT)
            return ABORT;
        if (rc1 == WAIT)
            rc = WAIT;
    }
    return rc;
}

RC
CCManager::row_delete(row_t * row)
{
    RC rc = RCOK;
    _num_lock_waits = 0;
    _deletes.push_back(row);
    set<INDEX *> indexes;
    row->get_table()->get_indexes( &indexes );
    for (auto idx : indexes) {
        RC rc1 = index_delete( idx, GET_WORKLOAD->get_index_key( row, idx->get_index_id()) );
        if (rc1 == ABORT)
            return ABORT;
        if (rc1 == WAIT)
            rc = WAIT;
    }
    return rc;
}

RC
CCManager::commit_insdel()
{
    assert(false);
    return RCOK;
}
