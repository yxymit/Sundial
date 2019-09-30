#include <algorithm>
#include "naive_tictoc_manager.h"
#include "row_naive_tictoc.h"
#include "row.h"
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

#if CC_ALG == NAIVE_TICTOC

bool
NaiveTicTocManager::compare(AccessTicToc * ac1, AccessTicToc * ac2)
{
    return ac1->row->get_primary_key() < ac2->row->get_primary_key();
}

NaiveTicTocManager::NaiveTicTocManager(TxnManager * txn)
    : CCManager(txn)
{
    _is_read_only = true;
    _validation_no_wait = true;

    _max_wts = 0;
    _write_copy_ptr = false;
    _atomic_timestamp = false;

    _timestamp = glob_manager->get_ts(GET_THD_ID);
    _num_lock_waits = 0;
    _signal_abort = false;
    _min_commit_ts = 0;

    LEASE = 10;

}

void
NaiveTicTocManager::init()
{
    CCManager::init();
    _min_commit_ts = 0;
}

void
NaiveTicTocManager::cleanup(RC rc)
{
    split_read_write_set();
    unlock_write_set(rc);
#if LOCK_ALL_BEFORE_COMMIT
    unlock_read_set();
#endif
    for (vector<AccessTicToc>::iterator it = _access_set.begin();
        it != _access_set.end(); it ++)
    {
        if (it->local_data)
            delete it->local_data;
        it->local_data = NULL;
    }
    for (vector<AccessTicToc>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->local_data)
            delete it->local_data;
    }

    for (vector<IndexAccessTicToc>::iterator it = _index_access_set.begin();
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
NaiveTicTocManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key,
                                       uint32_t table_id, uint32_t &msg_size, char * &msg_data)
{
    AccessTicToc ac;
    _remote_set.push_back(ac);
    AccessTicToc * access = &(*_remote_set.rbegin());
    _last_access = access;
    assert(remote_node_id != g_node_id);

    access->home_node_id = remote_node_id;
    access->row = NULL;
    access->type = type;
    access->key = key;
    access->table_id = table_id;
    access->local_data = NULL;
    add_remote_node_info(remote_node_id, type == WR);
    return LOCAL_MISS;
}

RC
NaiveTicTocManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id)
{
    assert(false);
    return RCOK;
}


RC
NaiveTicTocManager::get_row(row_t * row, access_t type, uint64_t key)
{
    return get_row(row, type, key, -1);
}

RC
NaiveTicTocManager::get_row(row_t * row, access_t type, uint64_t key, uint64_t wts)
{
    RC rc = RCOK;
    assert (_txn->get_txn_state() == TxnManager::RUNNING);

    AccessTicToc * access = NULL;
    AccessTicToc ac;
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
    access->local_data = new char [access->data_size];
    rc = row->manager->read(_txn, access->local_data, access->wts, access->rts, true, wts != (uint64_t)-1);

    if (type == WR) {
        _is_read_only = false;
        _min_commit_ts = max(_min_commit_ts, access->rts + 1);
    } else
        _min_commit_ts = max(_min_commit_ts, access->wts);
    assert(rc == RCOK);
    return rc;
}

RC
NaiveTicTocManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    // XXX. Naive TicToc only supports YCSB, which only needs index lookups.
    assert(type == RD);
    RC rc = RCOK;
    IndexAccessTicToc * access = NULL;
    for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
        access = &_index_access_set[i];
        if (access->index == index && access->key == key)
            assert(false);
    }
    access = NULL;
    // access the index
    IndexAccessTicToc ac;
    _index_access_set.push_back(ac);
    access = &(*_index_access_set.rbegin());
    access->key = key;
    access->index = index;
    access->type = type;
    access->rows = NULL;

    // manager will be latched in the following function.
    Row_naive_tictoc * manager = index->index_get_manager(key);
    access->manager = manager;
    rc = manager->read(_txn, NULL, access->wts, access->rts, false);
    assert(rc == RCOK);
    _min_commit_ts = max(_min_commit_ts, access->wts);
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

    manager->unlatch();
    return rc;
}

RC
NaiveTicTocManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
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
NaiveTicTocManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
    return rc;
}

RC
NaiveTicTocManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug3, get_sys_clock() - tt);
    return rc;
}

char *
NaiveTicTocManager::get_data(uint64_t key, uint32_t table_id)
{
    for (vector<AccessTicToc>::iterator it = _access_set.begin(); it != _access_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    for (vector<AccessTicToc>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if (it->key == key && it->table_id == table_id)
            return it->local_data;

    assert(false);
    return NULL;
}

RC
NaiveTicTocManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
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
NaiveTicTocManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &_timestamp );
}

uint32_t
NaiveTicTocManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &_timestamp );
    return sizeof(_timestamp);
}

void
NaiveTicTocManager::compute_commit_ts()
{
    for (auto access : _access_set) {
        if (access.type == RD) {
            assert(_min_commit_ts >= access.wts);
            _min_commit_ts = max(access.wts, _min_commit_ts);
        }
        else if (access.type == WR) {
            assert(_min_commit_ts >= access.rts + 1);
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
        }
    }
    for (auto access : _remote_set)
    {
        if (access.type == RD) {
            assert(_min_commit_ts >= access.wts);
            _min_commit_ts = max(access.wts, _min_commit_ts);
        } else if (access.type == WR) {
            assert(_min_commit_ts >= access.rts + 1);
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
        }
    }
    for (auto access : _index_access_set) {
        if (access.type == RD || access.type == WR)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == INS || access.type == DEL)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
}

void
NaiveTicTocManager::get_resp_data(uint32_t &size, char * &data)
{
    // construct the return message.
    // if !ENABLE_LOCAL_CACHING
    // Format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    // if ENABLE_LOCAL_CACHING
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n |
    //  | m | (table_id , read_intensive) * m
    UnstructuredBuffer buffer;
    uint32_t num_tuples = 0;
    for (uint32_t i = 0; i < _access_set.size(); i++) {
        AccessTicToc& access = _access_set[i];
        if (access.responded) continue;
        buffer.put( &access.key );
        buffer.put( &access.table_id );
        buffer.put( &access.wts );
        buffer.put( &access.rts );
#if ENABLE_LOCAL_CACHING
        if (access.cached) {
            uint32_t size = 0;
            buffer.put( &size );
        } else {
            buffer.put( &access.data_size );
            buffer.put( access.local_data, access.data_size );
        }
#else
        buffer.put( &access.data_size );
        buffer.put( access.local_data, access.data_size );
#endif
        access.responded = true;
        num_tuples ++;
    }
#if ENABLE_LOCAL_CACHING && CACHING_POLICY == READ_INTENSIVE
    uint32_t m = 1;
    uint32_t table_id = 0;
    buffer.put( &m );
    if (m > 0) {
        buffer.put( &table_id );
        bool read_intensive = GET_WORKLOAD->get_table(table_id)->is_read_intensive();
        buffer.put( &read_intensive );
    }
#endif
    assert(num_tuples > 0);
    buffer.put_front( &num_tuples );
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
}

NaiveTicTocManager::AccessTicToc *
NaiveTicTocManager::find_access(uint64_t key, uint32_t table_id, vector<AccessTicToc> * set)
{
    for (vector<AccessTicToc>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}

void
NaiveTicTocManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    // store the remote tuple to local access_set.
    UnstructuredBuffer buffer(resp_data);
    uint32_t num_tuples;
    buffer.get( &num_tuples );
    assert(num_tuples > 0);
    uint64_t max_commit_ts = (uint64_t)-1;
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key;
        uint64_t wts;
        uint32_t table_id;
        buffer.get( &key );
        buffer.get( &table_id );
        AccessTicToc * access = find_access(key, table_id, &_remote_set);
        assert(access && node_id == access->home_node_id);
        buffer.get( &wts );
        buffer.get( &access->rts );
        if (access->type == RD) {
            _min_commit_ts = max(_min_commit_ts, wts);
            max_commit_ts = min(max_commit_ts, access->rts);
        }
        else
            _min_commit_ts = max(_min_commit_ts, access->rts + 1);

        buffer.get( &access->data_size );
        assert ( access->data_size > 0 );
        char * data = NULL;
        buffer.get( data, access->data_size );
        assert(access->data_size != 0 && access->data_size < 65536);
        access->local_data = new char [access->data_size];
        memcpy(access->local_data, data, access->data_size);
        access->wts = wts;
    }
    add_tictoc_remote_node_info(node_id, max_commit_ts);
}

RC
NaiveTicTocManager::process_lock_phase_coord()
{
    RC rc = RCOK;
    split_read_write_set();

    Isolation isolation = SR;
    rc = lock_write_set();
    if (rc == ABORT)
        INC_INT_STATS(num_aborts_ws, 1);  // local abort
    if (rc == RCOK) {
        // if any rts has been updated, update _min_commit_ts.
        //compute_commit_ts();
        // at this point, _min_commit_ts is the final min_commit_ts for the prepare phase. .
        if (isolation == SR) {
            rc = validate_read_set(_min_commit_ts);
            if (rc == ABORT)
                INC_INT_STATS(num_aborts_rs, 1);  // local abort
        }
    }
    if (rc == ABORT)
        unlock_write_set(rc);
    return rc;
}

RC
NaiveTicTocManager::process_lock_req(uint32_t size, char * data,
                                     uint32_t &resp_size, char * &resp_data )
{
    RC rc = RCOK;
    split_read_write_set();

    Isolation isolation = SR;
    rc = lock_write_set();
    if (rc == ABORT)
        INC_INT_STATS(num_aborts_ws, 1);  // local abort
    if (rc == RCOK) {
        // if any rts has been updated, update _min_commit_ts.
        //compute_commit_ts();
        // at this point, _min_commit_ts is the final min_commit_ts for the prepare phase. .
        if (isolation == SR) {
            rc = validate_read_set(_min_commit_ts);
            if (rc == ABORT)
                INC_INT_STATS(num_aborts_rs, 1);  // local abort
        }
    }
    if (rc == ABORT)
        unlock_write_set(rc);
    else { // rc == RCOK
        // Format
        //         | min_commit_ts | max_commit_ts |
        UnstructuredBuffer buffer;
        buffer.put( &_min_commit_ts );
        buffer.put( &_max_commit_ts );

        resp_size = buffer.size();
        resp_data = new char [resp_size];
        memcpy(resp_data, buffer.data(), resp_size);
    }
    return rc;
}

void
NaiveTicTocManager::process_lock_resp(RC rc, uint32_t node_id, char * data)
{
    if (rc == RCOK) {
        UnstructuredBuffer buffer(data);
        uint64_t min_ts, max_ts;
        buffer.get( &min_ts );
        buffer.get( &max_ts );

        _min_commit_ts = max(_min_commit_ts, min_ts);

        add_tictoc_remote_node_info(node_id, max_ts);
    }
}

// Lock the tuples in the write set.
// if fails, should release all the locks.
RC
NaiveTicTocManager::lock_write_set()
{
    RC rc = RCOK;
    _num_lock_waits = 0;
    vector<AccessTicToc *>::iterator it;
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessTicToc &access = _index_access_set[i];
        assert(access.type == RD);
    }

    for (it = _write_set.begin(); it != _write_set.end(); it ++)
    {
        assert((*it)->home_node_id == g_node_id);
        rc = (*it)->row->manager->try_lock(_txn);
        assert(rc == RCOK || rc == ABORT);
        if (rc == RCOK) {
            (*it)->locked = true;
            (*it)->rts = (*it)->row->manager->get_rts();
            _min_commit_ts = max( (*it)->rts + 1, _min_commit_ts);

            if ((*it)->wts != (*it)->row->manager->get_wts())
                return ABORT;
        } else {  // (rc == ABORT)
            return ABORT;
        }
    }
    assert(rc == RCOK);
    return rc;
}

RC
NaiveTicTocManager::lock_read_set()
{
#if LOCK_ALL_DEBUG
    printf("Trying to lock all the read set..\n");
#endif
    vector<AccessTicToc *>::iterator it;
    for (it = _read_set.begin(); it != _read_set.end(); it ++)
    {
        if ((*it)->home_node_id == g_node_id) {
            // local data
                RC rc = (*it)->row->manager->try_lock(_txn);
                if (rc == RCOK || rc == WAIT) {
                    (*it)->locked = true;
                    if ((*it)->wts != (*it)->row->manager->get_wts()){
                        if ((*it)->row->manager->get_wts() > (*it)->rts + 1)
                            INC_INT_STATS(int_possibMVCC, 1);
                        break;
                    }
                }
                else
                    break;
        } else // does not support exclusive caching yet.
            assert(false);
    }
    if (it != _read_set.end()) {
        unlock_read_set();
        return ABORT;
    }
    return RCOK;
}

void
NaiveTicTocManager::unlock_write_set(RC rc)
{
    for (vector<IndexAccessTicToc>::iterator it = _index_access_set.begin();
        it != _index_access_set.end(); it ++ )
    {
        assert(!it->locked);
    }
    vector<AccessTicToc *>::iterator it;
    for (it = _write_set.begin(); it != _write_set.end(); it ++) {
        if ((*it)->locked) {
            (*it)->row->manager->release(_txn, rc);
            (*it)->locked = false;
        }
    }
}

void
NaiveTicTocManager::unlock_read_set()
{
    vector<AccessTicToc *>::iterator it;
    for (it = _read_set.begin(); it != _read_set.end(); it ++)
        if ((*it)->locked) {
            (*it)->row->manager->release(_txn, ABORT);
            (*it)->locked = false;
        }
}

RC
NaiveTicTocManager::validate_read_set(uint64_t commit_ts)
{
    _max_commit_ts = (uint64_t)-1;
    for (auto access : _index_access_set) {
        if (access.type == INS || access.type == DEL)
            continue;
        if (access.rts >= commit_ts) {
            INC_INT_STATS(num_no_need_to_renewal, 1);
            continue;
        }
        INC_INT_STATS(num_renewals, 1);
        if (!access.manager->try_renew(access.wts, commit_ts, access.rts))
            return ABORT;
        _max_commit_ts = min(_max_commit_ts, access.rts);
    }
    // validate the read set.
    for (vector<AccessTicToc *>::iterator it = _read_set.begin();
        it != _read_set.end(); it ++)
    {
        if ((*it)->rts >= commit_ts) {
            INC_INT_STATS(num_no_need_to_renewal, 1);
            continue;
        }
        INC_INT_STATS(num_renewals, 1);
        if (!(*it)->row->manager->try_renew((*it)->wts, commit_ts, (*it)->rts))
            return ABORT;
        _max_commit_ts = min(_max_commit_ts, (*it)->rts);
    }
    if (_max_commit_ts == (uint64_t)-1)
        _max_commit_ts = _min_commit_ts;
    return RCOK;
}

void
NaiveTicTocManager::add_tictoc_remote_node_info(uint32_t node_id, uint64_t max_commit_ts)
{
    if (_tictoc_remote_node_info.find(node_id) == _tictoc_remote_node_info.end()) {
        TicTocRemoteNodeInfo info;
        info.node_id = node_id;
        _tictoc_remote_node_info.insert(
            pair<uint32_t, TicTocRemoteNodeInfo>(node_id, info));
    }
    _tictoc_remote_node_info[node_id].max_commit_ts = max_commit_ts;
}

void
NaiveTicTocManager::split_read_write_set()
{
    _read_set.clear();
    _write_set.clear();
    for (vector<AccessTicToc>::iterator it = _access_set.begin();
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
NaiveTicTocManager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    Isolation isolation = SR;
    if (isolation == SR)
        rc = validate_read_set(_min_commit_ts);
    if (rc == ABORT)
        unlock_write_set(rc);
    return rc;
}

void
NaiveTicTocManager::get_remote_nodes(set<uint32_t> * remote_nodes)
{
}

bool
NaiveTicTocManager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
    if (_tictoc_remote_node_info[remote_node_id].max_commit_ts >= _min_commit_ts)
        return false;
    // Format:
    //  | commit_ts |
    UnstructuredBuffer buffer;
    buffer.put( &_min_commit_ts );

    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
    return true;
}

RC
NaiveTicTocManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
    RC rc = RCOK;
    // Request Format:
    //     | commit_ts |
    UnstructuredBuffer req_buffer(data);
    uint64_t commit_ts;
    req_buffer.get( &commit_ts );
    M_ASSERT(_min_commit_ts <= commit_ts, "txn=%ld. _min_commit_ts=%ld, commit_ts=%ld",
            _txn->get_txn_id(), _min_commit_ts, commit_ts);
    _min_commit_ts = commit_ts;

    rc = validate_read_set(_min_commit_ts);
    assert(resp_data == NULL);
    if (rc == ABORT) {
        cleanup(rc);
        return rc;
    } else {
        if (is_read_only()) {
            cleanup(COMMIT);
            return COMMIT;
        } else
            return rc;
    }
}

void
NaiveTicTocManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
    assert(data == NULL);
}

void
NaiveTicTocManager::process_commit_phase_coord(RC rc)
{
    if (rc == COMMIT) {
        commit_insdel();
        split_read_write_set();
        for (vector<AccessTicToc *>::iterator it = _write_set.begin();
            it != _write_set.end(); it ++)
        {
            M_ASSERT(_min_commit_ts > (*it)->rts, "commit_ts=%ld, (*it)->rts=%ld\n", _min_commit_ts, (*it)->rts);
            (*it)->row->manager->write_data((*it)->local_data, _min_commit_ts);
        }
#if ENABLE_LOCAL_CACHING
        for (vector<AccessTicToc>::iterator it = _remote_set.begin();
            it != _remote_set.end(); it++)
        {
            // for remote write, also update the local cache.
            if (it->type == WR)
                local_cache_man->update_data(it->key, _min_commit_ts,it->local_data);
        }
#endif
        if (!_txn->is_sub_txn() && _min_commit_ts > glob_manager->get_max_cts())
            glob_manager->set_max_cts(_min_commit_ts);
        // handle inserts and deletes
        cleanup(COMMIT);
    } else {
        cleanup(ABORT);
    }
}

RC
NaiveTicTocManager::commit_insdel() {
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        row->manager->set_ts(_min_commit_ts, _min_commit_ts);
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
        for (vector<AccessTicToc>::iterator it = _access_set.begin();
             it != _access_set.end(); it ++)
        {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
        row->manager->delete_row( _min_commit_ts );
        // TODO. not deleting the row here. because another txn might be accessing it.
        // A better solution is to handle deletion in a special way. For example, access the
        // index again to see if the tuple still exists.
        //delete row;
    }
    for (auto access : _index_access_set)
        if (access.type != RD) {
            assert(_min_commit_ts > access.manager->_rts);
            M_ASSERT(access.manager->_lock_owner == _txn, "lock_owner=%#lx, _txn=%#lx",
                (uint64_t)access.manager->_lock_owner, (uint64_t)_txn);
            access.manager->update_ts(_min_commit_ts);
        }

    return RCOK;

}

bool
NaiveTicTocManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    if (rc == ABORT)
        return true;
    uint32_t num_writes = 0;
    for (vector<AccessTicToc>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;
    }

    // COMMIT and the remote node is not readonly.
    // Format
    //   | commit_ts | num_writes | (key, table_id, size, data) * num_writes
    assert(rc == COMMIT);
    UnstructuredBuffer buffer;
    buffer.put( &_min_commit_ts );
    buffer.put( &num_writes );
    for (vector<AccessTicToc>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
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
NaiveTicTocManager::process_commit_req(RC rc, uint32_t size, char * data)
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
                AccessTicToc * access = find_access( key, table_id, &_access_set );
                access->row->manager->write_data(tuple_data, commit_ts);
            }
        }
        cleanup(COMMIT);
    } else
        cleanup(ABORT);
}

void
NaiveTicTocManager::abort()
{
    cleanup(ABORT);
}

void
NaiveTicTocManager::commit()
{
    cleanup(COMMIT);
}

bool
NaiveTicTocManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

void
NaiveTicTocManager::set_txn_ready(RC rc)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        ATOM_SUB_FETCH(_num_lock_waits, 1);
    else {
        _signal_abort = true;
    }
}
#endif
