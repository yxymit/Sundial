#include <algorithm>
#include "tictoc_manager.h"
#include "row.h"
#include "row_tictoc.h"
#include "txn.h"
#include "manager.h"
#include "workload.h"
#include "index_btree.h"
#include "index_hash.h"
#include "table.h"
#include "store_procedure.h"
#include "catalog.h"
#include "caching.h"
#include "query.h"
#if CC_ALG == TICTOC

bool TicTocManager::_pre_abort = PRE_ABORT;

bool
TicTocManager::compare(AccessTicToc * ac1, AccessTicToc * ac2)
{
    return ac1->row->get_primary_key() < ac2->row->get_primary_key();
}

TicTocManager::TicTocManager(TxnManager * txn)
    : CCManager(txn)
{
    _is_read_only = true;
    // TODO. make these parameters static, initialize them in a static function
    _validation_no_wait = true;

    _max_wts = 0;
    _write_copy_ptr = false;
    _atomic_timestamp = false;

    _timestamp = glob_manager->get_ts(GET_THD_ID);
    _num_lock_waits = 0;
    _signal_abort = false;
    _min_commit_ts = 0;

    LEASE = 10;

    if (!_txn->is_sub_txn()) {
        _min_commit_ts = glob_manager->get_max_cts();
        // arbitrary parameter.
        if (_min_commit_ts > 10)
            _min_commit_ts -= 10;
    }
}

void
TicTocManager::init()
{
    CCManager::init();
    _min_commit_ts = 0;
}

void
TicTocManager::cleanup(RC rc)
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
TicTocManager::register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key,
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
#if ENABLE_LOCAL_CACHING
    bool cached = false;
    // with local caching, a read might hit in the cache.

    if (type == RD) {
        access->data_size = GET_WORKLOAD->get_table(table_id)->get_schema()->get_tuple_size();
        // If access.row == NULL, the data is mapped to a remote node.
        // (although it might be cached locally)
        char data[access->data_size];
        bool read_cached_value;
        cached = local_cache_man->lookup(key, data, access->wts, access->rts, read_cached_value, _min_commit_ts);
        // When caching is enabled, remote access read request uses 'wts' as an extra field.
        if (cached) {
            access->cached = true;
            access->local_data = new char [access->data_size];
            memcpy(access->local_data, data, access->data_size);
            if (WORKLOAD == TPCC)
                assert(read_cached_value);
            if (read_cached_value) {
                INC_INT_STATS(num_cache_reads, 1);
    #if REUSE_IF_NO_REMOTE
                msg_size = sizeof(uint64_t);
                msg_data = new char [msg_size];
                memcpy(msg_data, &access->wts, msg_size);
                return SPECULATE;
    #else
                return RCOK;
    #endif
            } else {
                INC_INT_STATS(num_cache_bypass, 1);
                msg_size = sizeof(uint64_t);
                msg_data = new char [msg_size];
                memcpy(msg_data, &access->wts, msg_size);
    #if REUSE_IF_NO_REMOTE
                return SPECULATE;
    #else
                return LOCAL_MISS;
    #endif
            }
        } else {
              uint64_t wts = -1;
            msg_size = sizeof(uint64_t);
            msg_data = new char [msg_size];
            memcpy(msg_data, &wts, msg_size);
            return LOCAL_MISS;
        }
    }
#endif
    return LOCAL_MISS;
}

RC
TicTocManager::get_row(row_t * row, access_t type, uint64_t key)
{
    return get_row(row, type, key, -1);
}

RC
TicTocManager::get_row(row_t * row, access_t type, uint64_t key, uint64_t wts)
{
    RC rc = RCOK;
    char local_data[row->get_tuple_size()];
    assert (_txn->get_txn_state() == TxnManager::RUNNING);

    AccessTicToc * access = NULL;
    for (vector<AccessTicToc>::iterator it = _access_set.begin(); it != _access_set.end(); it ++) {
        if (it->row == row) {
            assert(type == WR && OCC_WAW_LOCK); // not always true in general
            access = &(*it);
            access->row->manager->get_ts(access->wts, access->rts);
            break;
        }
    }

    if (!access) {
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
        if (type == RD || !OCC_WAW_LOCK) {
            rc = row->manager->read(_txn, local_data, access->wts, access->rts, true, wts != (uint64_t)-1);
            _min_commit_ts = max(_min_commit_ts, access->wts);
#if ENABLE_LOCAL_CACHING
            if (access->wts == wts) {
                access->cached = true;
            }
#endif
        } else {
            assert(type == WR && OCC_WAW_LOCK);
            rc = row->manager->write(_txn, access->wts, access->rts);
            if (rc == WAIT)
                ATOM_ADD_FETCH(_num_lock_waits, 1);
            if (rc == ABORT || rc == WAIT)
                return rc;
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
        _min_commit_ts = max(_min_commit_ts, access->rts + 1);
        access->locked = true;
    }
    assert(rc == RCOK);
    return rc;

}

RC
TicTocManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    assert(type != WR);
    RC rc = RCOK;
    IndexAccessTicToc * access = NULL;
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
            // TODO
            assert(false);
        } else if (access->type == RD && type != RD) {
            access->type = type;
#if OCC_WAW_LOCK
            // should get write permission.
            uint64_t wts, rts;
            rc = access->manager->write(_txn, wts, rts);
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
    IndexAccessTicToc ac;
    _index_access_set.push_back(ac);
    access = &(*_index_access_set.rbegin());
    access->key = key;
    access->index = index;
    access->type = type;
    access->rows = NULL;

    Row_tictoc * manager = index->index_get_manager(key);
    access->manager = manager;
    if (type != RD && OCC_WAW_LOCK) {
        rc = manager->write(_txn, access->wts, access->rts, false);
        if (rc == RCOK || rc == WAIT)
            access->locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(_num_lock_waits, 1);
    } else {
        rc = manager->read(_txn, NULL, access->wts, access->rts, false);
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
TicTocManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
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
TicTocManager::index_insert(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(INS, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug2, get_sys_clock() - tt);
    return rc;
}

RC
TicTocManager::index_delete(INDEX * index, uint64_t key)
{
    uint64_t tt = get_sys_clock();
    RC rc = index_get_permission(DEL, index, key);
    INC_FLOAT_STATS(index, get_sys_clock() - tt);
    INC_FLOAT_STATS(time_debug3, get_sys_clock() - tt);
    return rc;
}

char *
TicTocManager::get_data(uint64_t key, uint32_t table_id)
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
TicTocManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
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
TicTocManager::add_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->put_front( &_timestamp );
}

uint32_t
TicTocManager::process_remote_req_header(UnstructuredBuffer * buffer)
{
    buffer->get( &_timestamp );
    return sizeof(_timestamp);
}

void
TicTocManager::compute_commit_ts()
{
    for (auto access : _access_set) {
        if (access.type == RD)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == WR)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
    for (auto access : _remote_set)
    {
        if (access.type == RD)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == WR)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
    for (auto access : _index_access_set) {
        if (access.type == RD || access.type == WR)
            _min_commit_ts = max(access.wts, _min_commit_ts);
        else if (access.type == INS || access.type == DEL)
            _min_commit_ts = max(access.rts + 1, _min_commit_ts);
    }
}

void
TicTocManager::get_resp_data(uint32_t &size, char * &data)
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

uint32_t
TicTocManager::get_log_record(char *& record)
{
    uint32_t size = 0;
    for (auto access : _access_set) {
        if (access.type == WR) {
            //    access_t     type;
            // uint64_t     key;
            // uint32_t     table_id;
            size += sizeof(access_t) + sizeof(uint32_t) + sizeof(uint64_t) + access.data_size;
        }
    }
    if (size > 0) {
        uint32_t offset = 0;
        record = new char[size];
        for (auto access : _access_set) {
            if (access.type == WR) {
                //    access_t     type;
                // uint64_t     key;
                // uint32_t     table_id;
                memcpy(record + offset, &access.type, sizeof(access_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.key, sizeof(uint64_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.table_id, sizeof(uint32_t));
                offset += sizeof(access_t);
                memcpy(record + offset, &access.local_data, access.data_size);
            }
        }
    }
    return size;
}


TicTocManager::AccessTicToc *
TicTocManager::find_access(uint64_t key, uint32_t table_id, vector<AccessTicToc> * set)
{
    for (vector<AccessTicToc>::iterator it = set->begin(); it != set->end(); it ++) {
        if (it->key == key && it->table_id == table_id)
            return &(*it);
    }
    return NULL;
}

void
TicTocManager::process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data)
{
    // return data format:
    // if !ENABLE_LOCAL_CACHING
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n
    // else // if ENABLE_LOCAL_CACHING
    //    | n | (key, table_id, wts, rts, tuple_size, data) * n |
    //  | m | (table_id , read_intensive) * m

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
        AccessTicToc * access = find_access(key, table_id, &_remote_set);
        assert(access && node_id == access->home_node_id);
        buffer.get( &wts );
        buffer.get( &access->rts );
        if (access->type == RD)
            _min_commit_ts = max(_min_commit_ts, wts);
        else
            _min_commit_ts = max(_min_commit_ts, access->rts + 1);
        buffer.get( &access->data_size );
        if ( access->data_size == 0 ) {
            assert( ENABLE_LOCAL_CACHING);
#if ENABLE_LOCAL_CACHING
            local_cache_man->vote_local();
#endif
            assert( access->wts == wts );
            //access->wts = wts;
        } else {
            char * data = NULL;
            buffer.get( data, access->data_size );
            assert(access->data_size != 0 && access->data_size < 65536);
            access->local_data = new char [access->data_size];
            memcpy(access->local_data, data, access->data_size);
#if ENABLE_LOCAL_CACHING
            // TODO. should update the local rts if it is extended.
            if (access->cached) {
                assert( access->wts != wts );
                local_cache_man->vote_remote();
            }
            if (access->type == RD) {
                local_cache_man->update_or_insert(access->key, access->local_data, wts, access->rts);
                if (WORKLOAD == TPCC)
                    assert(wts == 0);
            }
#endif
            access->wts = wts;
        }
    }
#if ENABLE_LOCAL_CACHING && CACHING_POLICY == READ_INTENSIVE
    uint32_t m;
    buffer.get( &m );
    if (m > 0) {
        uint32_t table_id;
        bool read_intensive;
        buffer.get( &table_id );
        buffer.get( &read_intensive );
        local_cache_man->update_table_read_intensity(table_id, read_intensive);
    }
#endif
}

// Lock the tuples in the write set.
// if fails, should release all the locks.
RC
TicTocManager::lock_write_set()
{
    assert(!OCC_WAW_LOCK);
    RC rc = RCOK;
    _num_lock_waits = 0;
    vector<AccessTicToc *>::iterator it;
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccessTicToc &access = _index_access_set[i];
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
TicTocManager::lock_read_set()
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
TicTocManager::unlock_write_set(RC rc)
{
    for (vector<IndexAccessTicToc>::iterator it = _index_access_set.begin();
        it != _index_access_set.end(); it ++ )
    {
        if (it->locked) {
            it->manager->release(_txn, rc);
            it->locked = false;
        }
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
TicTocManager::unlock_read_set()
{
    vector<AccessTicToc *>::iterator it;
    for (it = _read_set.begin(); it != _read_set.end(); it ++)
        if ((*it)->locked) {
            (*it)->row->manager->release(_txn, ABORT);
            (*it)->locked = false;
        }
}

RC
TicTocManager::validate_read_set(uint64_t commit_ts)
{
    for (auto access : _index_access_set) {
        if (access.type == INS || access.type == DEL)
            continue;
        if (access.rts >= commit_ts) {
            INC_INT_STATS(num_no_need_to_renewal, 1);
            continue;
        }
        INC_INT_STATS(num_renewals, 1);
#if LOCK_ALL_BEFORE_COMMIT
        if (!access.manager->renew(access.wts, commit_ts, access.rts))  // without lock check
#else
        if (!access.manager->try_renew(access.wts, commit_ts, access.rts))
#endif
        {
            return ABORT;
        }
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
#if LOCK_ALL_BEFORE_COMMIT
        if (!(*it)->row->manager->renew((*it)->wts, commit_ts, (*it)->rts))  // without lock check
#else
        if (!(*it)->row->manager->try_renew((*it)->wts, commit_ts, (*it)->rts))
#endif
        {
            return ABORT;
        }
    }
    return RCOK;
}

RC
TicTocManager::validate_write_set(uint64_t commit_ts)
{
    for (auto access : _write_set) {
        if (_min_commit_ts <= access->rts) {
            INC_INT_STATS(int_urgentwrite, 1);  // write too urgent
            return ABORT;
        }
    }
    for (auto access : _index_access_set) {
        if ((access.type == INS || access.type == DEL) && _min_commit_ts <= access.rts) {
            INC_INT_STATS(int_urgentwrite, 1);  // write too urgent
            return ABORT;
        }
    }
    return RCOK;
}



void
TicTocManager::split_read_write_set()
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
TicTocManager::handle_pre_abort()
{
    RC rc = RCOK;
    for (auto access : _index_access_set) {
        uint64_t current_wts = access.manager->get_wts();
        if ((access.type == WR && access.wts != current_wts) // written by another txn
            || (access.type == RD && access.rts < _min_commit_ts && access.wts != current_wts))
        {
            if (access.type == RD && (current_wts == access.rts + 1 || current_wts < _min_commit_ts)) // exactly one version behind
                INC_INT_STATS(int_inevitable, 1); // this is inevitable, cannot renew a rts written by another one
            rc = ABORT;
            break;
        }
    }
    for (vector<AccessTicToc *>::iterator it = _write_set.begin(); it != _write_set.end(); it ++) {
        if ((*it)->wts != (*it)->row->manager->get_wts()) {
            rc = ABORT;
            INC_INT_STATS(int_aborts_ws1, 1);  // Value changed
            INC_INT_STATS(num_aborts_ws, 1);
            break;
        }
    }
    if (rc == RCOK) {
        for (vector<AccessTicToc *>::iterator it = _read_set.begin(); it != _read_set.end(); it ++) {
            uint64_t new_wts = (*it)->row->manager->get_wts();
            if ((*it)->wts != new_wts && (*it)->rts < _min_commit_ts) {
                if(new_wts < _min_commit_ts)
                       INC_INT_STATS(int_inevitable, 1);  // this abort is inevitable.

                if(new_wts == (*it)->rts + 1) {
                    INC_INT_STATS(int_inevitable, 1);  // For this case it is also inevitable for MVCC
                    INC_INT_STATS(int_aborts_rs2, 1);  // The latest version is right behind our version.
                } else if(new_wts > (*it)->rts + 1){
                    INC_INT_STATS(int_possibMVCC, 1);  // possibly ok for MVCC
#if TRACK_LAST
#if MULTI_VERSION
                    uint64_t * lrts_a, * lwts_a;
                    int lastptr;
                    (*it)->row->manager->get_last_array(lrts_a, lwts_a, lastptr);
                    int current_ptr = 0;
                    while(current_ptr < Row_tictoc::_history_num && lwts_a[(lastptr + current_ptr) % Row_tictoc::_history_num] > (*it)->wts)
                        ++ current_ptr;
                    if(current_ptr < Row_tictoc::_history_num && lrts_a[(lastptr + current_ptr) % Row_tictoc::_history_num] >= _min_commit_ts) // means found an lwts==wts
                    {
                        INC_INT_STATS(int_saved_by_hist, 1);  // count the number
                        continue; // someone else extended rts for us and we don't need to abort;
                    }
#else
                    uint64_t lwts, lrts;
                    (*it)->row->manager->get_last_ts(lrts, lwts);
                    if(lwts == (*it)->wts && lrts >= _min_commit_ts) {
                        INC_INT_STATS(int_saved_by_hist, 1);  // count the number
                        continue;  // someone else extended rts for us and we don't need to abort;
                    }
#endif
#endif
                    INC_INT_STATS(int_aborts_rs1, 1);  // We don't know what happened between the latest version and the version we have
                }
                INC_INT_STATS(num_aborts_rs, 1);
                rc = ABORT;
                break;
            }
        }
    }
    if (rc == ABORT)
        INC_INT_STATS(num_pre_aborts, 1);
    return rc;
}

RC
TicTocManager::process_prepare_phase_coord()
{
    RC rc = RCOK;
    // split _access_set into read_set and write_set
    split_read_write_set();
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

#if LOCK_ALL_BEFORE_COMMIT
    rc = lock_read_set();
    if (rc!= RCOK)
        return rc;
#endif
#if OCC_WAW_LOCK
    // TODO. we need this for TPCC.
    // In the current TPCC implementation, if row_insert() returns WAIT, the function will
    // not be called again. Therefore, after the lock is acquired, the manager does not have
    // the latest wts and rts.
    for (uint32_t i = 0; i < _index_access_set.size(); i ++) {
        IndexAccessTicToc * access = &_index_access_set[i];
        if (access->type != RD) {
            assert(access->locked);
            assert(access->manager->_lock_owner == _txn);
            access->manager->get_ts(access->wts, access->rts);
        }
    }
#endif

    // [Compute the commit timestamp]
    compute_commit_ts();
    Isolation isolation = _txn->get_store_procedure()->get_query()->get_isolation_level();
#if OCC_WAW_LOCK
    if (isolation == SR)
        rc = validate_read_set(_min_commit_ts);
#else
    if (rc == RCOK) {
        rc = lock_write_set();
        if (rc == ABORT)
            INC_INT_STATS(num_aborts_ws, 1);  // local abort
    }
    if (rc != ABORT) {
        // if any rts has been updated, update _min_commit_ts.
        compute_commit_ts();
        // at this point, _min_commit_ts is the final min_commit_ts for the prepare phase. .
        if (isolation == SR) {
            RC rc2 = validate_read_set(_min_commit_ts);
            if (rc2 == ABORT) {
                rc = rc2;
                INC_INT_STATS(num_aborts_rs, 1);  // local abort
            }
        }
    }
#endif
    if (rc == ABORT) {
        unlock_write_set(rc);
#if LOCK_ALL_BEFORE_COMMIT
        unlock_read_set();
#endif
    }
    return rc;
}

void
TicTocManager::get_remote_nodes(set<uint32_t> * remote_nodes)
{
#if ENABLE_LOCAL_CACHING
    for (vector<AccessTicToc>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->type == RD && it->rts < _min_commit_ts)
            if (remote_nodes->find(it->home_node_id) == remote_nodes->end())
                remote_nodes->insert(it->home_node_id);
        if (it->cached) {
            assert(it->type == RD);
            if (it->rts >= _min_commit_ts) {
                local_cache_man->vote_local();
                INC_INT_STATS(num_local_hits, 1);
            } else {
                INC_INT_STATS(num_renew, 1);
            }
        }
    }
#endif
    //printf("[txn=%ld] remote_nodes.size() = %ld\n", _txn->get_txn_id(), remote_nodes->size());
    // TODO If a remote noed is readonly and no tuples have expired, no need to prepare it.
    // In theory, no need to commit it as well if it does not maintain any local states for the txn.
    // Since we do maintain states on a remote node, we should still send commit req.
}

bool
TicTocManager::need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data)
{
    uint32_t num_renewals = 0;
    bool readonly = true;
    for (vector<AccessTicToc>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->home_node_id == remote_node_id) {
            if (it->type == RD && it->rts < _min_commit_ts)
                num_renewals ++;
            else if (it->type == WR)
                readonly = false;
        }
    }
    // Format:
    //  | commit_ts |
    // or
    //     | commit_ts | n | (key, table_id, wts) * n
    // Need to send information for reads in the following two cases.
    //      1. If the remote node is readonly, it already forgot the sub-txn.
    //        2. For a local cache hit, the remote node does not know the access
    UnstructuredBuffer buffer;
    buffer.put( &_min_commit_ts );

    // the # of renewals that the remode node does not know.
    num_renewals = 0;
    for (vector<AccessTicToc>::iterator it = _remote_set.begin();
        it != _remote_set.end(); it ++)
    {
        if (it->home_node_id == remote_node_id && it->type == RD
            && it->rts < _min_commit_ts
            && (readonly
#if ENABLE_LOCAL_CACHING && (CACHING_POLICY == ALWAYS_READ || CACHING_POLICY == READ_INTENSIVE)
            || it->cached
#endif
            ))
        {
            buffer.put( &it->key );
            buffer.put( &it->table_id );
            buffer.put( &it->wts );
            if (WORKLOAD == TPCC)
                assert(it->wts == 0);
            num_renewals ++;
        }
    }
    if (num_renewals > 0)
        buffer.put_at( &num_renewals, sizeof(_min_commit_ts));
    size = buffer.size();
    data = new char [size];
    memcpy(data, buffer.data(), size);
    return true;
}

RC
TicTocManager::process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data )
{
    RC rc = RCOK;
    // Request Format:
    //     | commit_ts | ...
    UnstructuredBuffer req_buffer(data);
    uint64_t commit_ts;
    req_buffer.get( &commit_ts );
    M_ASSERT(_min_commit_ts <= commit_ts, "txn=%ld. _min_commit_ts=%ld, commit_ts=%ld",
            _txn->get_txn_id(), _min_commit_ts, commit_ts);
    _min_commit_ts = commit_ts;

    if (size > sizeof(commit_ts)) {
        // Request Format:
        //     | commit_ts | n | (key, table_id, wts) * n
        // If this remote node is readonly, or if the client needs to renew local hits,
        // the remote node is not aware of rows. So we need to process them separately.
        uint32_t num_local_reads;
        req_buffer.get( &num_local_reads );

        // TODO. if version_changed, then rts is never used.
        // Response Format.
        // if version_changed == false
        //    | n | (version_changed, key, table_id, rts) * n | table_lease_renew
        // else
        //    | n | (version_changed, key, table_id, wts, rts, tuple_size, data) * n | table_lease_renew
        // table_lease_renew is optional, it may contain the following fields.
        //  | table_id | max_wts | max_rts |
        uint32_t num_resp = 0;
#if ENABLE_LOCAL_CACHING
        UnstructuredBuffer buffer;
        bool version_changed;
#endif
        assert( num_local_reads < 16 );
        for (uint32_t i = 0; i < num_local_reads; i ++) {
            uint64_t key;
            uint32_t table_id;
            uint64_t wts;
            req_buffer.get( &key );
            req_buffer.get( &table_id );
            req_buffer.get( &wts );
            bool has_local_access = false;
            for (auto access : _access_set)
                if (access.key == key)
                    has_local_access = true;
            if (has_local_access)
                continue;

            uint64_t rts = 0;

            set<INDEX *> indexes;
            GET_WORKLOAD->table_to_indexes(table_id, &indexes);
            // TODO. this may not be true in general.
            assert(indexes.size() == 1);
            INDEX * index = *indexes.begin();
            set<row_t *> * rows = NULL;
            rc = index_read(index, key, rows);
            // TODO. should handle cases for wait or abort
            assert(rc == RCOK);
            row_t * row = *rows->begin();
            bool renew = row->manager->try_renew(wts, commit_ts, rts);
            if (!renew)
                rc = ABORT;
#if ENABLE_LOCAL_CACHING
            version_changed = !renew;
            if (WORKLOAD == TPCC)
                M_ASSERT(!version_changed, "table_id=%d\n", table_id);
            buffer.put( &version_changed );
            buffer.put( &key );
            buffer.put( &table_id );
            if (version_changed) {
                uint32_t size = row->get_tuple_size();
                char data[size];
                uint64_t wts, rts;
                row->manager->read(_txn, data, wts, rts);

                buffer.put( &wts );
                buffer.put( &rts );
                buffer.put( &size );
                buffer.put( data, size );
            } else
                buffer.put( &rts );
#endif
            num_resp ++;
        }

        //  | table_id | is_read_intensive | max_wts | max_rts |
// TODO this is a hack right now. Should check to see if the table is readonly.
#if ENABLE_LOCAL_CACHING
  #if WORKLOAD == TPCC && RO_LEASE
        uint32_t table_id = 7;
        uint64_t tab_wts = GET_WORKLOAD->get_table(7)->get_max_wts();
        uint64_t tab_rts = GET_WORKLOAD->get_table(7)->get_max_rts();
        assert(tab_wts == 0);
        buffer.put( &table_id );
        buffer.put( &tab_wts );
        buffer.put( &tab_rts );
  #endif
        buffer.put_front( &num_resp );

        resp_size = buffer.size();
        resp_data = new char [resp_size];
        memcpy(resp_data, buffer.data(), resp_size);
#endif
        if (rc == ABORT) {
            cleanup(rc);
            return rc;
        }
    }
    split_read_write_set();
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

#if LOCK_ALL_BEFORE_COMMIT
    RC rc2 = lock_read_set(); // lock the read sets in case that someone write to the tuples which affects our proposed commit timestamp.
    if (rc2 != RCOK)
    {
        cleanup(rc2);
        return rc2;
    }
#endif

#if OCC_WAW_LOCK
    rc = validate_read_set(_min_commit_ts);
#else
    // [Handle pre aborts]
    if (_pre_abort)
        rc = handle_pre_abort();
    if (rc == RCOK) {
        rc = lock_write_set();  // and refresh rts
        if (rc == ABORT) {
            INC_INT_STATS(int_aborts_ws2, 1); // lock failure
            INC_INT_STATS(num_aborts_ws, 1);
        }
    }
    if (rc != ABORT) {
        if (validate_write_set(_min_commit_ts) == ABORT)
            rc = ABORT;
    }
    if (rc != ABORT) {
        if (validate_read_set(_min_commit_ts) == ABORT) {
            rc = ABORT;
            INC_INT_STATS(num_aborts_rs, 1);
        }
    }
#endif

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
TicTocManager::process_prepare_resp(RC rc, uint32_t node_id, char * data)
{
#if ENABLE_LOCAL_CACHING
    // CAUTION. this function may be executed by multiple threads in parallel with different node_id
    // So the logic must be thread safe

    if (!data)
        return;
    // return message format
    // if version_changed == false
    //    | n | (version_changed, key, table_id, rts) * n |
    // else
    //    | n | (version_changed, key, table_id, wts, rts, tuple_size, data) * n |
    UnstructuredBuffer buffer(data);
    uint32_t n;
    buffer.get( &n );
    for (uint32_t i = 0; i < n; i ++) {
        bool version_changed;
        uint64_t key;
        uint32_t table_id;
        uint64_t rts;

        buffer.get( &version_changed );
        buffer.get( &key );
        buffer.get( &table_id );

        AccessTicToc * access = find_access(key, table_id, &_remote_set);

        M_ASSERT(access, "i=%d v=%d key=%ld, table=%d\n", i, version_changed, key, table_id);
        assert(version_changed == 0 || version_changed == 1);
        if (version_changed) {
            local_cache_man->vote_remote();
            uint64_t wts;
            uint32_t tuple_size;
            char * data;
            buffer.get( &wts );
            buffer.get( &rts );
            buffer.get( &tuple_size );
            buffer.get( data, tuple_size );
            assert(WORKLOAD == YCSB);
            assert(GET_WORKLOAD->key_to_node(key) != g_node_id);
            if (access->cached)
                INC_INT_STATS(num_renew_failure, 1);

            if (access->type == RD) {
                local_cache_man->update_or_insert(access->key, data, wts, rts);
            }
        } else {
            local_cache_man->vote_local();
            buffer.get( &rts );
            local_cache_man->update(key, rts);
            if (access->cached)
                INC_INT_STATS(num_renew_success, 1);
        }
    }
  #if WORKLOAD == TPCC && RO_LEASE
    uint32_t table_id;
    uint64_t tab_wts = 0, tab_rts = 0;
    buffer.get( &table_id );
    buffer.get( &tab_wts );
    buffer.get( &tab_rts );
    local_cache_man->update_table_lease( table_id, tab_wts, tab_rts );
  #endif
#else
    assert(data == NULL);
#endif
}

void
TicTocManager::process_commit_phase_coord(RC rc)
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
        abort();
    }
}

RC
TicTocManager::commit_insdel() {
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
TicTocManager::need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data)
{
    uint32_t num_writes = 0;
    for (vector<AccessTicToc>::iterator it = _remote_set.begin(); it != _remote_set.end(); it ++)
        if ((it)->home_node_id == node_id && (it)->type == WR)
            num_writes ++;

    if (rc == ABORT)
        return true;
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
TicTocManager::process_commit_req(RC rc, uint32_t size, char * data)
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
        abort();
}

void
TicTocManager::abort()
{
    cleanup(ABORT);
}

void
TicTocManager::commit()
{
    cleanup(COMMIT);
}

bool
TicTocManager::is_txn_ready()
{
    return _num_lock_waits == 0 || _signal_abort;
}

void
TicTocManager::set_txn_ready(RC rc)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        ATOM_SUB_FETCH(_num_lock_waits, 1);
    else {
        _signal_abort = true;
    }
}

#endif
