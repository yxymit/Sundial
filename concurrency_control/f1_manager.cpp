#include "f1_manager.h"
#include "row.h"
#include "row_f1.h"
#include "txn.h"
#include "manager.h"
#include "table.h"
#include "query.h"
#include "workload.h"
#include "index_base.h"
#include "index_hash.h"
#include "store_procedure.h"

#if CC_ALG == F_ONE

bool F1Manager::_pre_abort = PRE_ABORT;

F1Manager::F1Manager(TxnManager * txn)
    : CCManager(txn)
{
    _num_lock_waits = 0;
    _signal_abort = false;
    _timestamp = glob_manager->get_ts(GET_THD_ID);
}

bool
F1Manager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

void
F1Manager::set_txn_ready(RC rc)
{
    if (rc == RCOK)
        ATOM_SUB_FETCH(_num_lock_waits, 1);
    else if (rc == ABORT) {
        _signal_abort = true;
    }
}

void
F1Manager::cleanup(RC rc)
{
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessF1 * access = &_index_access_set[i];
        if (access->locked) {
            access->manager->lock_release(_txn, rc);
            access->locked = false;
        }
        if (access->rows)
            delete access->rows;
    }
    for (uint32_t i = 0; i < _access_set.size(); i ++)
    {
        if (!_access_set[i].locked)
            continue;
        AccessF1 * access = &_access_set[i];
        if (access->locked)
            access->row->manager->lock_release(_txn, rc);
        delete access->local_data;
    }
    for (vector<AccessF1>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->local_data)
            delete it->local_data;
    }
    if (rc == ABORT)
        for (auto ins : _inserts)
            delete ins.row;

    _access_set.clear();
    _remote_set.clear();
    _index_access_set.clear();
    _inserts.clear();
    _deletes.clear();

}

RC
F1Manager::get_row(row_t * row, access_t type, uint64_t key)
{
    RC rc = RCOK;
    AccessF1 * access = NULL;
    char local_data[row->get_tuple_size()];

    assert (_txn->get_txn_state() == TxnManager::RUNNING);
    for (vector<AccessF1>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) {
        if (it->row == row) {
            access = &(*it);
            break;
        }
    }
    if (!access) {
        AccessF1 ac;
        _access_set.push_back(ac);
        access = &(*_access_set.rbegin());

        _last_access = access;
        assert(type == RD || type == WR);

        access->home_node_id = g_node_id;
        access->row = row;
        access->type = type;
        access->key = key;
        access->table_id = row->get_table()->get_table_id();
        access->data_size = row->get_tuple_size();
        access->local_data = NULL;

        rc = row->manager->read(_txn, local_data, access->wts);
        assert(rc == RCOK);
    }

    access->local_data = new char [access->data_size];
    memcpy(access->local_data, local_data, access->data_size);
    return rc;
}

RC
F1Manager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    RC rc = get_row(row, type, key);
    if (rc == RCOK)
        data = _last_access->local_data;
    return rc;
}

RC
F1Manager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    assert(type != WR);
    RC rc = RCOK;
    IndexAccessF1 * access = NULL;
    for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
        access = &_index_access_set[i];
        if (access->index == index && access->key == key)  {
            if (type != RD)
                access->type = type;
            return RCOK;
        }
    }

    IndexAccessF1 ac;
    _index_access_set.push_back(ac);
    access = &(*_index_access_set.rbegin());
    access->key = key;
    access->index = index;
    access->type = type;
    access->rows = NULL;

    Row_f1 * manager = index->index_get_manager(key);
    access->manager = manager;
    rc = manager->read(_txn, NULL, access->wts, false);
    assert(rc == RCOK);
    if (type == RD) {
        set<row_t *> * rows = index->read(key);
        if (rows) {
            if (rows->size() > limit) {
                set<row_t *>::iterator it = rows->begin();
                advance(it, limit);
                access->rows = new set<row_t *>( rows->begin(), it );
                assert(access->rows->size() == limit);
            } else
                access->rows = new set<row_t *>( *rows );
        }
    }
    manager->unlatch();
    return rc;
}


RC
F1Manager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(RD, index, key, limit);
    assert(rc == RCOK);
    rows = _index_access_set.rbegin()->rows;
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return RCOK;

}

RC
F1Manager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}

RC
F1Manager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    return rc;
}



char *
F1Manager::get_data(uint64_t key, uint32_t table_id)
{
    for (vector<AccessF1>::iterator it = _access_set.begin(); it != _access_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    for (vector<AccessF1>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    assert(false);
    return NULL;
}

RC
F1Manager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id)
{
    AccessF1 ac;
    _remote_set.push_back(ac);
    AccessF1 * access = &(*_remote_set.rbegin());
    _last_access = access;
    assert(remote_node_id != g_node_id);

    access->home_node_id = remote_node_id;
    access->row = NULL;
    access->type = type;
    access->key = key;
    access->table_id = table_id;
    access->local_data = NULL;
    return LOCAL_MISS;
}

void
F1Manager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &_timestamp );
}

uint32_t
F1Manager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &_timestamp );
    return sizeof(_timestamp);
}

void
F1Manager::get_resp_data(uint32_t &size, char * &data)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, wts, tuple_size, data) * n
    UnstructuredBuffer buffer;
    uint32_t num_tuples = _access_set.size();
    buffer.put( &num_tuples );
    vector<AccessF1>::iterator it;
    for (it = _access_set.begin(); it != _access_set.end(); it ++) {
        buffer.put( &it->key );
        buffer.put( &it->table_id );
        buffer.put( &it->wts );
        buffer.put( &it->data_size );
        buffer.put( it->local_data, it->data_size );
    }
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
}

void
F1Manager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //        | n | (key, table_id, wts, tuple_size, data) * n
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
        AccessF1 * access = find_access(key, table_id, &_remote_set);
        assert(node_id == access->home_node_id);
        buffer.get( &access->wts );
        buffer.get( &access->data_size );
        char * data = NULL;
        buffer.get( data, access->data_size );
        access->local_data = new char [access->data_size];
        memcpy(access->local_data, data, access->data_size);
    }
}

RC
F1Manager::validate_local_txn()
{
    RC rc = RCOK;
    _num_lock_waits = 0;
    // lock all tuples, check version number.
    for (uint32_t i = 0; i < _access_set.size(); i ++)
    {
        Row_lock::LockType lock_type = (_access_set[i].type == RD)? Row_lock::LOCK_SH : Row_lock::LOCK_EX;
        rc = _access_set[i].row->manager->lock_get(lock_type, _txn);
        if (rc == RCOK || rc == WAIT) // after the txn wakes up, the lock will have been acquired.
            _access_set[i].locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);

        // validate version number
        if (_access_set[i].row->manager->get_wts() != _access_set[i].wts) {
            if (_access_set[i].type == RD) {
                INC_INT_STATS(num_aborts_rs, 1);
            } else {
                INC_INT_STATS(num_aborts_ws, 1);
            }
            rc = ABORT;
        }
        if (rc == ABORT)
            return ABORT;
    }
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessF1 &access = _index_access_set[i];
        Row_lock::LockType lock_type = (access.type == RD)?
            Row_lock::LOCK_SH : Row_lock::LOCK_EX;

        rc = access.manager->lock_get(lock_type, _txn);
        if (rc == RCOK || rc == WAIT) // after the txn wakes up, the lock will have been acquired.
            access.locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);

        // validate version number
        if (access.manager->get_wts() != access.wts) {
            if (access.type == RD) {
                INC_INT_STATS(num_aborts_rs, 1);
            } else {
                INC_INT_STATS(num_aborts_ws, 1);
            }
            rc = ABORT;
        }
        if (rc == ABORT)
            return ABORT;
    }
    if (rc == ABORT)
        return ABORT;
    else if (_num_lock_waits > 0)
        return WAIT;
    // ABORT || WAIT || RCOK
    return rc;
}

RC
F1Manager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    Isolation isolation = _txn->get_store_procedure()->get_query()->get_isolation_level();
    if (_pre_abort && isolation == SR) {
        for (uint32_t i = 0; i < _access_set.size(); i ++)
            if (_access_set[i].row->manager->get_wts() != _access_set[i].wts) {
                if (_access_set[i].type == RD) {
                    INC_INT_STATS(num_aborts_rs, 1);
                } else {
                    INC_INT_STATS(num_aborts_ws, 1);
                }
                return ABORT;
            }
        for (uint32_t i = 0; i < _index_access_set.size(); i ++)
            if (_index_access_set[i].manager->get_wts() != _index_access_set[i].wts) {
                if (_index_access_set[i].type == RD) {
                    INC_INT_STATS(num_aborts_rs, 1);
                } else {
                    INC_INT_STATS(num_aborts_ws, 1);
                }
                return ABORT;
            }
    }
    if (isolation == SR)
        rc = validate_local_txn();
    return rc;
}

bool
F1Manager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
    // Format
    //    | timestamp |
    size = sizeof(uint64_t);
    data = new char [size];
    *(uint64_t *)data = _timestamp; // for WAIT_DIE locking.
    return true;
}

RC
F1Manager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
    // if the txn waits/restarts in prepare phase, data will be NULL.
    // but the first time this function is called, data will contain the timestamp.
    assert(!_access_set.empty());
    if (data)
        _timestamp = *(uint64_t *)data;
    RC rc = validate_local_txn();
    if (rc == ABORT)
        cleanup(rc);
    return rc;
}

void
F1Manager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        commit_insdel();
        for (auto &access : _access_set) {
            if (access.type == WR)
                access.row->manager->write_data(access.local_data, access.wts + 1);
        }
    }
    cleanup(rc);
}

RC
F1Manager::commit_insdel() {
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
    }
    for (auto access : _index_access_set)
        if (access.type != RD) {
            access.manager->update_ts();
        }
    return RCOK;
}

bool
F1Manager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    if (rc == ABORT)
        return true;
    // Format
    //   | num_writes | (key, table_id, size, data) * num_writes
    UnstructuredBuffer buffer;
    uint32_t num_writes = 0;
    for (vector<AccessF1>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;
    if (num_writes) {
        buffer.put( &num_writes );
        for (vector<AccessF1>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
            if ((it)->home_node_id == node_id && (it)->type == WR) {
                buffer.put( &(it)->key );
                buffer.put( &(it)->table_id );
                buffer.put( &(it)->data_size );
                buffer.put( (it)->local_data, (it)->data_size );
            }
        size = buffer.size();
        data = new char [size];
        memcpy(data, buffer.data(), size);
    }
    return true;
}

void
F1Manager::process_commit_req(RC rc, uint32_t size, char * data)
{
    assert(!_access_set.empty());
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
            AccessF1 * access = find_access( key, table_id, &_access_set );
            access->row->manager->write_data(tuple_data, access->wts + 1);
        }
    }
    cleanup(rc);
}

F1Manager::AccessF1 *
F1Manager::find_access(uint64_t key, uint32_t table_id, vector<AccessF1> * set)
{
    for (vector<AccessF1>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}

void
F1Manager::abort()
{
    cleanup(ABORT);
}
#endif
