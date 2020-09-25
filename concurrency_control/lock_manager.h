#pragma once

#include "cc_manager.h"
//#include "rpc_client.h"

class LockManager : public CCManager
{
public:
    LockManager(TxnManager * txn);
    ~LockManager() {}

    //bool          is_read_only() { return _is_read_only; }
    RC            get_row(row_t * row, access_t type, uint64_t key);
    RC            get_row(row_t * row, access_t type, char * &data, uint64_t key);
    char *        get_data( uint64_t key, uint32_t table_id);

    RC            index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit=-1);
    RC            index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit=-1);
    RC            index_insert(INDEX * index, uint64_t key);
    RC            index_delete(INDEX * index, uint64_t key);

    void          process_remote_read_response(uint32_t node_id, access_t type, SundialResponse &response);
    void          build_prepare_req(uint32_t node_id, SundialRequest &request);

    RC            commit_insdel();
    void          cleanup(RC rc);
    // Logging
    // Get the log record for a single partition transaction.
    // Return value: size of the log record.
    uint32_t      get_log_record(char *& record);

#if CONTROLLED_LOCK_VIOLATION
    // This function is called right after the transaction is appended to the log buffer.
    RC            process_precommit_phase_coord();

    //void          increment_dependency()  { ATOM_ADD(_num_pending_tuples, 1); }
    // returns true if the dependency is clear
    //bool          decrement_dependency() {
    //    return ATOM_SUB(_num_pending_tuples, 1) == 1;
    //}
#endif

private:
    class AccessLock : public Access {
      public:
        ~AccessLock() {}
        AccessLock() { data = NULL; data_size = 0; }
        char *        data;    // original data.
        uint32_t      data_size;
    };

    AccessLock * find_access(uint64_t key, uint32_t table_id, vector<AccessLock> * set);

    vector<AccessLock>        _access_set;
    vector<AccessLock>        _remote_set;

    vector<IndexAccess>       _index_access_set;
};
