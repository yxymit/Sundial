#include "lock_manager.h"
#include "manager.h"
#include "txn.h"
#include "row_lock.h"
#include "row.h"
#include "index_base.h"
#include "table.h"
#include "index_hash.h"
#include "packetize.h"
#include "query.h"
#include "workload.h"
#include "store_procedure.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT

LockManager::LockManager(TxnManager * txn)
    : CCManager(txn)
{
    _num_lock_waits = 0;
#if CC_ALG == WAIT_DIE
    assert(g_ts_alloc == TS_CLOCK);
    _timestamp = glob_manager->get_ts(GET_THD_ID);
#endif
#if WORKLOAD == TPCC
    _access_set.reserve(128);
#endif
}

RC
LockManager::get_row(row_t * row, access_t type, uint64_t key)
{
    RC rc = RCOK;
    _num_lock_waits = 0;
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
        Row_lock::LockType lock_type = (type == RD)? Row_lock::LOCK_SH : Row_lock::LOCK_EX;
        if (isolation != NO_ACID)
            rc = row->manager->lock_get(lock_type, _txn);
        if (rc == ABORT) return rc;
        AccessLock ac;
        _access_set.push_back(ac);
        access = &(*_access_set.rbegin());
        _last_access = access;
        access->key = key;
        access->home_node_id = g_node_id;
        access->table_id = row->get_table()->get_table_id();
        access->type = type;
        access->row = row;
        access->data = NULL;
        access->data_size = row->get_tuple_size();
        if (rc == WAIT) {
            ATOM_ADD_FETCH(_num_lock_waits, 1);
        }
        if (rc != RCOK)
            return rc;
    }
    if (type == WR) {
        assert(access->data == NULL);
        access->data = new char [access->row->get_tuple_size()];
        memcpy(access->data, access->row->get_data(), access->row->get_tuple_size());
    }
    return rc;
}

RC
LockManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    RC rc = get_row(row, type, key);
    if (rc == RCOK)
        data = _last_access->row->get_data();
    return rc;
}

char *
LockManager::get_data( uint64_t key, uint32_t table_id)
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
LockManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id)
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
LockManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    RC rc = RCOK;
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccess * ac = &_index_access_set[i];
        if (ac->index == index && ac->key == key)  {
            if (ac->type == type) {
                ac->rows = index->read(key);
                return RCOK;
            } else {
                if (type != RD) {
                    ac->type = type;
                    rc = ac->manager->lock_get(Row_lock::LOCK_EX, _txn);
                    if (rc == WAIT)
                        ATOM_ADD_FETCH(_num_lock_waits, 1);
                    return rc;
                }
            }
        }
    }
    Row_lock * manager = index->index_get_manager(key);
    if (type == RD || type == WR)
        rc = manager->lock_get(Row_lock::LOCK_SH, _txn, false);
    else // if (type == INS || type == DEL)
        rc = manager->lock_get(Row_lock::LOCK_EX, _txn, false);
    manager->unlatch();
    if (rc == ABORT)
        return ABORT;

    IndexAccess access;
    access.key = key;
    access.index = index;
    access.type = type;
    access.manager = manager;

    if (rc == RCOK && type == RD)
        access.rows = index->read(key);
    _index_access_set.push_back(access);

    if (rc == WAIT) {
        ATOM_ADD_FETCH(_num_lock_waits, 1);
    }
    return rc;
}

RC
LockManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    RC rc = RCOK;
    uint64_t tt = get_sys_clock();
    rc = index_get_permission(RD, index, key, limit);
    if (rc == RCOK)
        rows = _index_access_set.rbegin()->rows;
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}

RC
LockManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
    return rc;
}

RC
LockManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug3, get_sys_clock() - tt);
    return rc;
}

void
LockManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
#if CC_ALG == WAIT_DIE
    buffer->put_front( &_timestamp );
#endif
}

uint32_t
LockManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
#if CC_ALG == WAIT_DIE
    buffer->get( &_timestamp );
    return sizeof(_timestamp);
#else
    return 0;
#endif
}

void
LockManager::cleanup(RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    Isolation isolation = SR;
    if (!_txn->is_sub_txn())
        isolation = _txn->get_store_procedure()->get_query()->get_isolation_level();
    for (uint32_t i = 0; i < _access_set.size(); i ++) {
        AccessLock * access = &_access_set[i];
        access_t type = access->type;
        if (type == WR && rc == ABORT)
            access->row->copy(access->data);
        if (isolation != NO_ACID)
            access->row->manager->lock_release(_txn, rc);

        if (type == WR) {
            assert(access->data);
            delete access->data;
        } else
            assert(access->data == NULL);
    }
    for (vector<AccessLock>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->data)
            delete it->data;
    }
    for (auto access : _index_access_set) {
        access.manager->lock_release(_txn, rc);
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
LockManager::is_txn_ready()
{
    return _num_lock_waits == 0;
}

void
LockManager::set_txn_ready(RC rc)
{
    assert(rc == RCOK);
    ATOM_SUB_FETCH(_num_lock_waits, 1);
}

void
LockManager::get_resp_data(uint32_t &size, char * &data)
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

LockManager::AccessLock *
LockManager::find_access(uint64_t key, uint32_t table_id, vector<AccessLock> * set)
{
    for (vector<AccessLock>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}


void
LockManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
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
LockManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data)
{
    assert(size == 0 && data == NULL);
    for (auto access : _access_set)    {
        if (access.type == WR)
            return RCOK;
    }
    // this sub_txn is read only.
    cleanup(COMMIT);
    return COMMIT;
}

void
LockManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT)
        commit_insdel();
    cleanup(rc);
    return;
}

bool
LockManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    if (rc == ABORT)
        return true;
    // Format
    //   | num_writes | (key, table_id, size, data) * num_writes
    UnstructuredBuffer buffer;
    uint32_t num_writes = 0;
    for (vector<AccessLock>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;
    if (num_writes) {
        buffer.put( &num_writes );
        for (vector<AccessLock>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
            if ((it)->home_node_id == node_id && (it)->type == WR) {
                buffer.put( &(it)->key );
                buffer.put( &(it)->table_id );
                buffer.put( &(it)->data_size );
                buffer.put( (it)->data, (it)->data_size );
            }
        size = buffer.size();
        data = new char [size];
        memcpy(data, buffer.data(), size);
        return true;
    } else
        // read only txn, no need to commit.
        return false;
}

void
LockManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    if (size > 0) {
        assert(rc == COMMIT);
        // Format
        //   | num_writes | (key, table_id, size, data) * num_writes
        UnstructuredBuffer buffer(data);
        uint32_t num_writes;
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
}

void
LockManager::abort()
{
    cleanup(ABORT);
}

RC
LockManager::commit_insdel()
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

#endif
