#include <algorithm>
#include "ideal_mvcc_manager.h"
#include "row.h"
#include "row_ideal_mvcc.h"
#include "txn.h"
#include "manager.h"
#include "workload.h"
#include "index_btree.h"
#include "index_hash.h"
#include "table.h"
#include "store_procedure.h"
#include "catalog.h"
#include "packetize.h"
#include "message.h"
#include "packetize.h"
#include "caching.h"
#include "query.h"
#if CC_ALG == IDEAL_MVCC

bool MVCCManager::_pre_abort = PRE_ABORT;


MVCCManager::MVCCManager(TxnManager * txn)
    : CCManager(txn)
{
    assert(WORKLOAD == YCSB);
    _is_read_only = true;
    _validation_no_wait = true;

    _max_wts = 0;
    _write_copy_ptr = false;
    _atomic_timestamp = false;

    _timestamp = glob_manager->get_ts(GET_THD_ID);
    _num_lock_waits = 0;
    _signal_abort = false;
    commit_ts = -1;

    LEASE = 10;

    if (!_txn->is_sub_txn()) {
        glob_manager->get_max_cts();
    }
}

void
MVCCManager::cleanup(RC rc)
{
    split_read_write_set();
    unlock_write_set(rc);
    for (vector<AccessMVCC>::iterator it = _access_set.begin();
        it != _access_set.end(); it ++)
    {
        if (it->local_data)
            delete it->local_data;
        it->local_data = NULL;
    }
    for (vector<AccessMVCC>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->local_data)
            delete it->local_data;
    }

    for (vector<IndexAccessMVCC>::iterator it = _index_access_set.begin();
        it != _index_access_set.end(); it ++ )
    {
        if (it->rows)
            delete it->rows;
    }
    if (rc == ABORT)
        for (auto ins : _inserts)
            delete ins.row;
    _access_set.clear();
    _remote_set.clear();
    _index_access_set.clear();

    _read_set.clear();
    _write_set.clear();

    _inserts.clear();
    _deletes.clear();
}


RC
MVCCManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key,
                                       uint32_t table_id, uint32_t &msg_size, char * &msg_data)
{
    AccessMVCC ac;
    _remote_set.push_back(ac);
    AccessMVCC * access = &(*_remote_set.rbegin());
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

RC
MVCCManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id)
{
    assert(!ENABLE_LOCAL_CACHING);
    uint32_t size = 0;
    char * data = NULL;
    return register_remote_access(remote_node_id, type, key, table_id, size, data);
}


RC
MVCCManager::get_row(row_t * row, access_t type, uint64_t key)
{
    return get_row(row, type, key, -1);
}

RC
MVCCManager::get_row(row_t * row, access_t type, uint64_t key, uint64_t wts)
{
    RC rc = RCOK;
    char local_data[row->get_tuple_size()];
    assert (_txn->get_txn_state() == TxnManager::RUNNING);

    AccessMVCC * access = NULL;
    for (vector<AccessMVCC>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) {
        if (it->row == row) {
            access = &(*it);
            break;
        }
    }

    if (!access) {
        AccessMVCC ac;
        _access_set.push_back(ac);
        access = &(*_access_set.rbegin());
        _last_access = access;

        access->home_node_id = g_node_id;
        access->row = row;
        access->type = type;
        assert(type == RD || type == WR);
        access->key = key;
        access->table_id = row->get_table()->get_table_id();
        access->data_size = row->get_tuple_size();
        access->local_data = NULL;

        assert(access->row->table);
        if (type == RD || !OCC_WAW_LOCK) {
            rc = row->manager->read(_txn, local_data, access->wts, access->rts, commit_ts);
            if (rc == ABORT) {
                return rc;
            }
        } else {
            assert(type == WR && OCC_WAW_LOCK);
            rc = row->manager->write(_txn, access->wts, access->rts, commit_ts);
            assert(rc != WAIT);
            if (rc == ABORT) {
                return rc;
            }
        }
    } else
        assert(type == WR && OCC_WAW_LOCK);

    // TODO. If the locally cached copy is too old. Also treat it as a local miss.
    // However, the miss request contains the wts of the cached tuple. So the response
    // can be a renewal.
    if (type == WR)
        _is_read_only = false;
    access->local_data = new char [access->data_size];
    if (!OCC_WAW_LOCK || type == RD) {
        memcpy(access->local_data, local_data, access->data_size);
        access->locked = false;
    } else {
        memcpy(access->local_data, row->get_data(), access->data_size);
        access->locked = true;
    }
    assert(rc == RCOK);
    return rc;

}

RC
MVCCManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    assert(type != WR);
    RC rc = RCOK;
    IndexAccessMVCC * access = NULL;
    for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
        access = &_index_access_set[i];
        if (access->index == index && access->key == key)  {
            access = &_index_access_set[i];
            break;
        }
        access = NULL;
    }
    if (access)    {
        if (access->type == type) {
            // the correct access permission is already acquired.
            return RCOK;
        }
        else if (type == RD && access->rows->empty()) {
        } else if (access->type == RD && type != RD) {
            access->type = type;
#if OCC_WAW_LOCK
            // should get write permission.
            uint64_t wts, rts;
            rc = access->manager->write(_txn, wts, rts, commit_ts);
            if (rc == ABORT) return ABORT;
            access->locked = true;
            if (wts != access->wts || rc == WAIT) return ABORT;
            return rc;
#else
            return RCOK;
#endif
        }
        assert(false);
    }
    // access the index
    IndexAccessMVCC ac;
    _index_access_set.push_back(ac);
    access = &(*_index_access_set.rbegin());
    access->key = key;
    access->index = index;
    access->type = type;
    access->rows = NULL;

    Row_MVCC * manager = index->index_get_manager(key);
    access->manager = manager;
    if (type != RD && OCC_WAW_LOCK) {
        rc = manager->write(_txn, access->wts, access->rts, commit_ts, false);
        if (rc == RCOK || rc == WAIT)
            access->locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);
    } else {
        rc = manager->read(_txn, NULL, access->wts, access->rts, commit_ts, false);
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
    }
    manager->unlatch();
    return rc;
}

RC
MVCCManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(RD, index, key, limit);
    assert(rc == RCOK);
    rows = _index_access_set.rbegin()->rows;
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug1, get_sys_clock() - tt);
    return RCOK;
}

RC
MVCCManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
    return rc;
}

RC
MVCCManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug3, get_sys_clock() - tt);
    return rc;
}

char *
MVCCManager::get_data(uint64_t key, uint32_t table_id)
{
    for (vector<AccessMVCC>::iterator it = _access_set.begin(); it != _access_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    for (vector<AccessMVCC>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    assert(false);
    return NULL;
}

RC
MVCCManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = get_row(row, type, key);
    if (rc == RCOK)
        data = _last_access->local_data;
    INC_FLOAT_STATS(row, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug4, get_sys_clock() - tt);
    return rc;
}

void
MVCCManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &commit_ts ); // transfer commit_ts
}

uint32_t
MVCCManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &commit_ts);
    return /*sizeof(_timestamp) + */ sizeof(commit_ts);
}

void
MVCCManager::compute_commit_ts()
{ // pass
}

void
MVCCManager::get_resp_data(uint32_t &size, char * &data)
{
    // construct the return message.
    // Format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    UnstructuredBuffer buffer;
    uint32_t num_tuples = _access_set.size();
    buffer.put( &num_tuples );
    for (auto access : _access_set) {
        buffer.put( &access.key );
        buffer.put( &access.table_id );
        buffer.put( &access.wts );
        buffer.put( &access.rts );
        buffer.put( &access.data_size );
        buffer.put( access.local_data, access.data_size );
    }
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
}

MVCCManager::AccessMVCC *
MVCCManager::find_access(uint64_t key, uint32_t table_id, vector<AccessMVCC> * set)
{
    for (vector<AccessMVCC>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}

void
MVCCManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //        | n | (key, table_id, wts, rts, tuple_size, data) * n
    // store the remote tuple to local access_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    buffer.get( &num_tuples );
    assert(num_tuples > 0);
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key;
        uint64_t wts;
        uint32_t table_id;
        buffer.get( &key );
        buffer.get( &table_id );
        AccessMVCC * access = find_access(key, table_id, &_remote_set);
        assert(access && node_id == access->home_node_id);
        buffer.get( &wts );
        buffer.get( &access->rts );
        buffer.get( &access->data_size );
        if ( access->data_size == 0 ) {
            assert( ENABLE_LOCAL_CACHING);
            assert( access->wts == wts );
            access->wts = wts;
        } else {
            char * data = NULL;
            buffer.get( data, access->data_size );
            access->local_data = new char [access->data_size];
            memcpy(access->local_data, data, access->data_size);
            access->wts = wts;
        }
    }
}

// Lock the tuples in the write set.
// if fails, should release all the locks.
RC
MVCCManager::lock_write_set()  // will not be called
{
    assert(!OCC_WAW_LOCK);
    RC rc = RCOK;
    _num_lock_waits = 0;
    vector<AccessMVCC *>::iterator it;
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessMVCC &access = _index_access_set[i];
        if (access.type == RD || access.type == WR) continue;
        rc = access.manager->try_lock(_txn);
        if (rc == ABORT) return ABORT;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);
        if (rc == WAIT || rc == RCOK) {
            access.locked = true;
            access.rts = access.manager->get_rts();
            if (access.wts != access.manager->get_wts()) {
                return ABORT;
            }
        }
    }

    for (it = _write_set.begin(); it != _write_set.end(); it ++)
    {
        assert((*it)->home_node_id == g_node_id);
        rc = (*it)->row->manager->try_lock(_txn);
        if (rc == WAIT || rc == RCOK)
            (*it)->locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);
        (*it)->rts = (*it)->row->manager->get_rts();
        if ((*it)->wts != (*it)->row->manager->get_wts()) {
            rc = ABORT;
        }

        if (rc == ABORT)
            return ABORT;
    }
    if (rc == ABORT)
        return ABORT;
    else if (_num_lock_waits > 0)
        return WAIT;
    return rc;
}

RC
MVCCManager::lock_read_set()
{
    return RCOK;
}

void
MVCCManager::unlock_write_set(RC rc)
{
    for (vector<IndexAccessMVCC>::iterator it = _index_access_set.begin();
        it != _index_access_set.end(); it ++ )
    {
        if (it->locked) {
            it->manager->release(_txn, rc);
            it->locked = false;
        }
    }
    vector<AccessMVCC *>::iterator it;
    for (it = _write_set.begin(); it != _write_set.end(); it ++) {
        if ((*it)->locked) {
            (*it)->row->manager->release(_txn, rc);
            (*it)->locked = false;
        }
    }
}

void
MVCCManager::unlock_read_set()
{
    vector<AccessMVCC *>::iterator it;
    for (it = _read_set.begin(); it != _read_set.end(); it ++)
        if ((*it)->locked) {
            (*it)->row->manager->release(_txn, ABORT);
            (*it)->locked = false;
        }
}

RC
MVCCManager::validate_read_set(uint64_t commit_ts)
{
    return RCOK;
}

RC
MVCCManager::validate_write_set(uint64_t commit_ts)
{
    return RCOK;
}

void
MVCCManager::split_read_write_set()
{
    _read_set.clear();
    _write_set.clear();
    for (vector<AccessMVCC>::iterator it = _access_set.begin();
         it != _access_set.end(); it ++)
    {
        assert(it->row);
        if (it->type == RD)
            _read_set.push_back(&(*it));
        else if ((it)->type == WR)
            _write_set.push_back(&(*it));
        else
            assert(false);
    }
}

RC
MVCCManager::handle_pre_abort()
{
    return RCOK;  // we don't need pre-aborts
}

RC
MVCCManager::process_prepare_phase_coord()
{
    return RCOK;
}

void
MVCCManager::get_remote_nodes(set<uint32_t> * remote_nodes)
{
}

bool
MVCCManager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
    return true;
}

RC
MVCCManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
    if (is_read_only()) {
        cleanup(COMMIT);
        return COMMIT;
    } else
        return RCOK;
}

void
MVCCManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    assert(data == NULL);
}

void
MVCCManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        commit_insdel();
        split_read_write_set();
        for (vector<AccessMVCC *>::iterator it = _write_set.begin();
            it != _write_set.end(); it ++)
        {
            (*it)->row->manager->write_data((*it)->local_data, commit_ts);

        }
        if (!_txn->is_sub_txn() && commit_ts > glob_manager->get_max_cts())
            glob_manager->set_max_cts(commit_ts);
        // handle inserts and deletes
        cleanup(COMMIT);
    } else {
        abort();
    }
}

RC
MVCCManager::commit_insdel() {
    return RCOK;
}

bool
MVCCManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    uint32_t num_writes = 0;
    for (vector<AccessMVCC>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;

    if (rc == ABORT)
        return true;
    // COMMIT and the remote node is not readonly.
    // Format
    //   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
    assert(rc == COMMIT);
    UnstructuredBuffer buffer;
    buffer.put( &commit_ts );  // seems we don't need to include commit_ts here since the remote server already knows it
    buffer.put( &num_writes );
    for (vector<AccessMVCC>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR) {
            buffer.put( &(it)->key );
            buffer.put( &(it)->table_id );
            buffer.put( &(it)->data_size );
            buffer.put( (it)->local_data, (it)->data_size );
        }
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
    return true;
}

void
MVCCManager::process_commit_req(RC rc, uint32_t size, char * data)
{
    if (rc == COMMIT) {
        // Format
        //   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
        UnstructuredBuffer buffer(data);
        uint64_t commit_ts;
        buffer.get( &commit_ts );
        if (size > sizeof(uint64_t)) {
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
                AccessMVCC * access = find_access( key, table_id, &_access_set );
                access->row->manager->write_data(tuple_data, commit_ts);
            }
        }
        cleanup(COMMIT);
    } else
        abort();
}

void
MVCCManager::abort()
{
    cleanup(ABORT);
}

void
MVCCManager::commit()
{
    cleanup(COMMIT);
}

bool
MVCCManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

void
MVCCManager::set_txn_ready(RC rc)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        ATOM_SUB_FETCH(_num_lock_waits, 1);
    else {
        _signal_abort = true;
    }
}
#endif
