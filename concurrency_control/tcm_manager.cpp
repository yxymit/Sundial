#include "tcm_manager.h"
#include "manager.h"
#include "txn.h"
#include "row_tcm.h"
#include "row_lock.h"
#include "row.h"
#include "index_base.h"
#include "table.h"
#include "index_hash.h"
#include "packetize.h"
#include "query.h"
#include "workload.h"
#include "store_procedure.h"

// The changes we made to the original TCM scheme
// 1. MV is not fully supported. This is OK since we only run current time transactions.
//       We only mount a write at the end of a transaction, so a reading transaction can still
//    read a previous version. This essentially provides the gain of a MV scheme.

#if CC_ALG == TCM

TCMManager::TCMManager(TxnManager * txn)
    : CCManager(txn)
{
    _num_lock_waits = 0;
#if WORKLOAD == TPCC
    _access_set.reserve(128);
#endif
    _latch = new pthread_mutex_t;
    // to guarantee read/write consistency
    pthread_mutex_init(    _latch, NULL );
    state = TxnManager::RUNNING;
    early = glob_manager->get_current_time();
    end = false;
    late = (uint64_t)-1;
}

RC
TCMManager::get_row(row_t * row, access_t type, uint64_t key)
{
    RC rc = RCOK;
    _num_lock_waits = 0;
    //_lock_ready = false;
    Isolation isolation = SR;
    if (!_txn->is_sub_txn())
        isolation = _txn->get_store_procedure()->get_query()->get_isolation_level();
    AccessLock * access = NULL;

    for (vector<AccessLock>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) {
        if (it->row == row) {
            access = &(*it);
            break;
        }
    }
    if (!access) {
        Row_TCM::LockType lock_type = (type == RD)? Row_TCM::LOCK_SH : Row_TCM::LOCK_EX;
        char data[row->get_tuple_size()];
        if (isolation != NO_ACID)
            rc = row->manager->lock_get(lock_type, _txn, data);
        if (rc == ABORT) {
            return rc;
        }
        AccessLock ac;
        _access_set.push_back(ac);
        access = &(*_access_set.rbegin());
        _last_access = access;
        access->key = key;
        access->home_node_id = g_node_id;
        access->table_id = row->get_table()->get_table_id();
        access->type = type;
        access->row = row;
        access->data = new char [access->row->get_tuple_size()];
        memcpy(access->data, data, row->get_tuple_size());
        access->data_size = row->get_tuple_size();
        // blocking is not yet supported.
        assert(rc == RCOK);
    }
    return rc;
}

RC
TCMManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    RC rc = get_row(row, type, key);
    if (rc == RCOK)
        data = _last_access->data; //row->get_data();
    return rc;
}

char *
TCMManager::get_data( uint64_t key, uint32_t table_id)
{
    for (vector<AccessLock>::iterator it = _access_set.begin(); it != _access_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->data;

    for (vector<AccessLock>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->data;

    assert(false);
    return NULL;
}

RC
TCMManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id)
{
    AccessLock ac;
    _remote_set.push_back(ac);
    AccessLock * access = &(*_remote_set.rbegin());
    assert(remote_node_id != g_node_id);

    access->home_node_id = remote_node_id;
    access->row = NULL;
    access->type = type;
    access->key = key;
    access->table_id = table_id;
    access->data = NULL;
    return LOCAL_MISS;
}

RC
TCMManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    IndexAccess access;
    access.key = key;
    access.index = index;
    access.type = type;
    // TODO for now, only support index read
    assert(type == RD);
    access.rows = index->read(key);
    _index_access_set.push_back(access);
    return RCOK;
}

RC
TCMManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(RD, index, key, limit);
    if (rc == RCOK)
        rows = _index_access_set.rbegin()->rows;
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}

RC
TCMManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
    return rc;
}

RC
TCMManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug3, get_sys_clock() - tt);
    return rc;
}

void
TCMManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
}

uint32_t
TCMManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    return 0;
}

void
TCMManager::cleanup(RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    state = (rc == COMMIT)? TxnManager::COMMITTED : TxnManager::ABORTED;
    timestamp = early;
    late = early;
    Isolation isolation = SR;
    if (!_txn->is_sub_txn())
        isolation = _txn->get_store_procedure()->get_query()->get_isolation_level();

    for (uint32_t i = 0; i < _access_set.size(); i ++)
    {
        AccessLock * access = &_access_set[i];
        //access_t type = access->type;
        // NOTE: for commit, do not release locks
        if (isolation != NO_ACID && rc == ABORT)
            access->row->manager->lock_release(_txn, rc);

        assert(access->data);
        delete access->data;
    }
    for (vector<AccessLock>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->data)
            delete it->data;
    }
    if (rc == ABORT)
        for (auto ins : _inserts)
            delete ins.row;
    _num_lock_waits = 0;
    _access_set.clear();
    _remote_set.clear();
    _inserts.clear();
    _deletes.clear();
    _index_access_set.clear();
}

bool
TCMManager::is_txn_ready()
{
//    bool ready = _lock_ready;
    return _num_lock_waits == 0;
}

void
TCMManager::set_txn_ready(RC rc)
{
    assert(rc == RCOK);
    ATOM_SUB_FETCH(_num_lock_waits, 1);
}

void
TCMManager::get_resp_data(uint32_t &size, char * &data)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, tuple_size, data) * n
    UnstructuredBuffer buffer;
    uint32_t num_tuples = _access_set.size();
    buffer.put( &num_tuples );
    vector<AccessLock>::iterator it;
    for (it = _access_set.begin(); it != _access_set.end(); it ++) {
        buffer.put( &it->key );
        buffer.put( &it->table_id );
        buffer.put( &it->data_size );
        buffer.put( it->row->get_data(), it->data_size );
    }
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
}

TCMManager::AccessLock *
TCMManager::find_access(uint64_t key, uint32_t table_id, vector<AccessLock> * set)
{
    for (vector<AccessLock>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}


void
TCMManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //        | n | (key, table_id, tuple_size, data) * n
    // store the remote tuple to local access_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    buffer.get( &num_tuples );
    assert(num_tuples > 0);
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key;
        uint32_t table_id;
        buffer.get( &key );
        buffer.get( &table_id );
        AccessLock * access = find_access(key, table_id, &_remote_set);
        assert(access && node_id == access->home_node_id);
        buffer.get( &access->data_size );
        char * data = NULL;
        buffer.get( data, access->data_size );
        access->data = new char [access->data_size];
        memcpy(access->data, data, access->data_size);
    }
}

RC
TCMManager::process_prepare_phase_coord()
{
    latch();
    if (end && early > late) {
        state = TxnManager::ABORTED;
        unlatch();
        return ABORT;
    } else if (state == TxnManager::RUNNING)
        state = TxnManager::PREPARING;
    else {
        assert(state == TxnManager::ABORTED);
        unlatch();
        return ABORT;
    }
    unlatch();
    return RCOK;
}

RC
TCMManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data)
{
    latch();
    assert(size == 0 && data == NULL);
    if (end && early > late)  {
        state = TxnManager::ABORTED;
        cleanup(ABORT);
        unlatch();
        return ABORT;
    }
    state = TxnManager::PREPARING;
    resp_size = sizeof(uint64_t) * 2;
    resp_data = (char *) malloc(sizeof(uint64_t) * 2);
    memcpy(resp_data, (void *)&early, sizeof(early));
    if (!end) assert(late == (uint64_t)-1);
    memcpy(resp_data + sizeof(early), (void *)&late, sizeof(late));

    unlatch();
    return RCOK;
}

void
TCMManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    if (rc == RCOK) {
        uint64_t remote_early, remote_late;
        memcpy(&remote_early, data, sizeof(uint64_t));
        memcpy(&remote_late, data + sizeof(uint64_t), sizeof(uint64_t));
        if (early < remote_early)
            early = remote_early;
        if (remote_late < late)
            late = remote_late;
    }
}

RC
TCMManager::compute_ts_range()
{
    latch();
    if (early > late) {
        state = TxnManager::ABORTED;
        unlatch();
        return ABORT;
    } else {
        state = TxnManager::COMMITTED;
        timestamp = early;
        late = early;
        unlatch();
        return COMMIT;
    }
}

void
TCMManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT)
        commit_insdel();
    cleanup(rc);
    return;
}

bool
TCMManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    if (rc == ABORT)
        return true;
    // Format
    //   | timestamp | num_writes | (key, table_id, size, data) * num_writes
    UnstructuredBuffer buffer;
    buffer.put( (uint64_t*)&timestamp );
    uint32_t num_writes = 0;
    for (vector<AccessLock>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;
    buffer.put( &num_writes );
    if (num_writes) {
        for (vector<AccessLock>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
            if ((it)->home_node_id == node_id && (it)->type == WR) {
                buffer.put( &(it)->key );
                buffer.put( &(it)->table_id );
                buffer.put( &(it)->data_size );
                buffer.put( (it)->data, (it)->data_size );
            }
    }
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
    return true;
}

void
TCMManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    if (size > 0) {
        assert(rc == COMMIT);
        // Format
        //   | timestamp | num_writes | (key, table_id, size, data) * num_writes
        UnstructuredBuffer buffer(data);
        uint32_t num_writes;
        buffer.get( (uint64_t*)&timestamp );
        buffer.get( &num_writes );
        for (uint32_t i = 0; i < num_writes; i ++) {
            uint64_t key;
            uint32_t table_id;
            uint32_t size = 0;
            char * tuple_data = NULL;
            buffer.get( &key );
            buffer.get( &table_id );
            buffer.get( &size );
            buffer.get( tuple_data, size );
            AccessLock * access = find_access( key, table_id, &_access_set );
            access->row->copy(tuple_data);
        }
    }
    cleanup(rc);
    latch();
    if (rc == COMMIT)
        state = TxnManager::COMMITTED;
    else if (rc == ABORT)
        state = TxnManager::ABORTED;
    unlatch();
}

void
TCMManager::abort()
{
    cleanup(ABORT);
}

RC
TCMManager::commit_insdel()
{
    // TODO. Ignoring index consistency.
    // handle inserts
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        set<INDEX *> indexes;
        ins.table->get_indexes( &indexes );
        for (auto idx : indexes) {
            uint64_t key = row->get_index_key(idx);
            idx->insert(key, row);
        }
    }
    // handle deletes
    for (auto row : _deletes) {
        set<INDEX *> indexes;
        row->get_table()->get_indexes( &indexes );
        for (auto idx : indexes)
            idx->remove( row );
        for (vector<AccessLock>::iterator it = _access_set.begin();
             it != _access_set.end(); it ++)
        {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
    }
    return RCOK;
}

void
TCMManager::try_abort()
{
    assert(state == TxnManager::RUNNING);
    state = TxnManager::ABORTED;
}

#endif
