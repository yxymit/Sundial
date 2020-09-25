#include "lock_manager.h"
#include "manager.h"
#include "txn.h"
#include "row_lock.h"
#include "row.h"
#include "index_base.h"
#include "table.h"
#include "index_hash.h"
#include "query.h"
#include "workload.h"
#include "store_procedure.h"
#include "txn_table.h"
#include "packetize.h"

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
    assert(type == RD || type == WR);
    _num_lock_waits = 0;

    Row_lock::LockType lock_type = (type == RD)? Row_lock::LOCK_SH : Row_lock::LOCK_EX;
    rc = row->manager->lock_get(lock_type, _txn);
    if (rc == RCOK) {
        if (type == WR) _txn->set_read_only(false);
        // For now, assume get_row() will not be called if the record is already
        // locked.
        for (vector<AccessLock>::iterator it = _access_set.begin(); it != _access_set.end(); it ++)
            if (it->row == row)
                assert(false);
        AccessLock ac;
        _access_set.push_back( ac );
        AccessLock * access = &(*_access_set.rbegin());
        access->key = key;
        access->home_node_id = g_node_id;
        access->table_id = row->get_table()->get_table_id();
        access->type = type;
        access->row = row;
        access->data = new char [row->get_tuple_size()];
        access->data_size = row->get_tuple_size();
        memcpy(access->data, access->row->get_data(), access->row->get_tuple_size());
    } else if (rc == WAIT)
        ATOM_ADD_FETCH(_num_lock_waits, 1);
    return rc;
}

RC
LockManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    RC rc = get_row(row, type, key);
    if (rc == RCOK) {
        data = _access_set.rbegin()->data;
        assert(data);
    }
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
LockManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    RC rc = RCOK;
    assert(type == RD || type == INS || type == DEL);
    if (type == INS || type == DEL) _txn->set_read_only(false);

    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        IndexAccess * ac = &_index_access_set[i];
        if ( ac->index == index && ac->key == key )  {
            if (ac->type == type) {
                ac->rows = index->read(key);
                return RCOK;
            } else {
                assert( (ac->type == RD)
                       && (type == INS || type == DEL) );
                ac->type = type;
                rc = ac->manager->lock_get(Row_lock::LOCK_EX, _txn);
                return rc;
            }
        }
    }

    Row_lock * manager = index->index_get_manager(key);
    if (type == RD)
        rc = manager->lock_get(Row_lock::LOCK_SH, _txn, false);
    else // if (type == INS || type == DEL)
        rc = manager->lock_get(Row_lock::LOCK_EX, _txn, false);
    manager->unlatch();
    if (rc == ABORT) return ABORT;
    // NOTE
    // records with different keys on the same index may share the same manager.
    // This is because manager locates in each bucket of the hash index.
    // Records with different keys may map to the same bucket and therefore share the manager.

    IndexAccess access;
    access.key = key;
    access.index = index;
    access.type = type;
    access.manager = manager;

    if (rc == RCOK && type == RD)
        access.rows = index->read(key);
    _index_access_set.push_back(access);

    return rc;
}

RC
LockManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    RC rc = RCOK;
    rc = index_get_permission(RD, index, key, limit);
    if (rc == RCOK) {
        assert(_index_access_set.rbegin()->key == key);
        rows = _index_access_set.rbegin()->rows;
    }
    return rc;
}

RC
LockManager::index_insert(INDEX * index, uint64_t key)
{
    return index_get_permission(INS, index, key);
}

RC
LockManager::index_delete(INDEX * index, uint64_t key)
{
    return index_get_permission(DEL, index, key);
}

uint32_t
LockManager::get_log_record(char *& record)
{
    // TODO inserted rows should also be in the log record.
    // Log Record Format
    //   | num_tuples | (table_id, key, size, data) * num_tuples
    UnstructuredBuffer buffer;
    for (auto access : _access_set) {
        access_t type = access.type;
        if (type == WR) {
          buffer.put( &access.table_id );
          buffer.put( &access.key );
          buffer.put( &access.data_size );
          buffer.put( access.row->get_data(), access.data_size );
        }
    }
    uint32_t size = buffer.size();
    record = new char [size];
    memcpy(record, buffer.data(), size);
    return size;
}


#if CONTROLLED_LOCK_VIOLATION
RC
LockManager::process_precommit_phase_coord()
{
    // the transaction tries to commit.
    // the actual index and data changes should happen at precommit time.
    commit_insdel();
    for (auto access : _access_set)
        if (access.type == WR)
            access.row->copy(access.data);

    // DEBUG
    /*for (int i = 0; i < _index_access_set.size(); i ++)
        for (int j = i + 1; j < _index_access_set.size(); j ++) {
            if (_index_access_set[i].manager == _index_access_set[j].manager) {
                cout << "size=" << _index_access_set.size() << endl;
                cout << "[" << i << "]: "
                     << "type=" << _index_access_set[i].type << ", key=" << _index_access_set[i].key
                     << ", index=" << _index_access_set[i].index->get_index_id() << endl;
                cout << "[" << j << "]: "
                     << "type=" << _index_access_set[j].type << ", key=" << _index_access_set[j].key
                     << ", index=" << _index_access_set[j].index->get_index_id() << endl;
            }
            assert(_index_access_set[i].manager != _index_access_set[j].manager);
        }*/

    for (auto access : _index_access_set)
        access.manager->lock_release(_txn, COMMIT);
    for (auto access : _access_set)
        access.row->manager->lock_release(_txn, COMMIT);

    return RCOK;
}
#endif

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

void
LockManager::cleanup(RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    if (rc == ABORT) {
        //printf("abort clean up\n");
        for (auto access : _access_set)
            access.row->manager->lock_release(_txn, rc);
        for (auto access : _index_access_set)
            access.manager->lock_release(_txn, rc);
    } else { // rc == COMMIT
    #if !CONTROLLED_LOCK_VIOLATION
        commit_insdel();
    #endif
        for (auto access : _access_set) {
    #if CONTROLLED_LOCK_VIOLATION
            access.row->manager->lock_cleanup(_txn);
    #else
            if (access.type == WR)
                access.row->copy(access.data);
            access.row->manager->lock_release(_txn, rc);
    #endif
        }
        for (auto access : _index_access_set) {
    #if CONTROLLED_LOCK_VIOLATION
            access.manager->lock_cleanup(_txn);
    #else
            access.manager->lock_release(_txn, rc);
    #endif
        }
    }
    for (auto access : _access_set) {
        assert(access.data);
        delete [] access.data;
    }
    for (auto access : _remote_set) {
        assert(access.data);
        delete [] access.data;
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

// Distributed transactions
// ========================
void
LockManager::process_remote_read_response(uint32_t node_id, access_t type, SundialResponse &response)
{
    assert(response.response_type() == SundialResponse::RESP_OK);
    for (int i = 0; i < response.tuple_data_size(); i ++) {
        AccessLock ac;
        _remote_set.push_back(ac);
        AccessLock * access = &(*_remote_set.rbegin());
        assert(node_id != g_node_id);

        access->home_node_id = node_id;
        access->row = NULL;
        access->key = response.tuple_data(i).key();
        access->table_id = response.tuple_data(i).table_id();
        access->type = type;
        access->data_size = response.tuple_data(i).size();
        access->data = new char [access->data_size];
        memcpy(access->data, response.tuple_data(i).data().c_str(), access->data_size);
    }
}

void
LockManager::build_prepare_req(uint32_t node_id, SundialRequest &request)
{
    for (auto access : _remote_set) {
        if (access.home_node_id == node_id && access.type == WR) {
            SundialRequest::TupleData * tuple = request.add_tuple_data();
            uint64_t tuple_size = access.data_size;
            tuple->set_key(access.key);
            tuple->set_table_id( access.table_id );
            tuple->set_size( tuple_size );
            tuple->set_data( access.data, tuple_size );
        }
    }
}

#endif
