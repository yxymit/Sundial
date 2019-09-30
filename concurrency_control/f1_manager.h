#pragma once

#include "cc_manager.h"

#if CC_ALG == F_ONE

class F1Manager : public CCManager
{
public:
    F1Manager(TxnManager * txn);
    ~F1Manager() {};

    RC             get_row(row_t * row, access_t type, uint64_t key);
    RC             get_row(row_t * row, access_t type, char * &data, uint64_t key);
    char *         get_data( uint64_t key, uint32_t table_id);
    RC             register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key, uint32_t table_id);

    uint64_t     get_ts() { return _timestamp; }
    bool         is_txn_ready();
    void         set_txn_ready(RC rc);
    bool         is_signal_abort() { return _signal_abort; }

    RC             index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit=-1);
    RC             index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit=-1);
    RC            index_insert(INDEX * index, uint64_t key);
    RC            index_delete(INDEX * index, uint64_t key);

    void         cleanup(RC rc);

    // normal execution
    void         add_remote_req_header(UnstructuredBuffer * buffer);
    uint32_t     process_remote_req_header(UnstructuredBuffer * buffer);
    //void         get_resp_data(uint32_t num_queries, RemoteQuery * queries, uint32_t &size, char * &data);
    void         get_resp_data(uint32_t &size, char * &data);
    void         process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

    // prepare phase
    RC             process_prepare_phase_coord();
    bool         need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data);
    RC             process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data );

    // commit phase
    void         process_commit_phase_coord(RC rc);
    RC            commit_insdel();
    bool         need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data);
    void         process_commit_req(RC rc, uint32_t size, char * data);
    void         abort();
private:
    struct IndexAccessF1 : IndexAccess {
        uint64_t wts;
    };

    struct AccessF1 : Access {
        AccessF1() { locked = false; }
        bool         locked;
        uint64_t    wts;
        uint32_t     data_size;
        char *         local_data;
    };

    vector<AccessF1>    _access_set;
    vector<AccessF1>    _remote_set;
    vector<IndexAccessF1> _index_access_set;
    AccessF1 *             _last_access;

    AccessF1 *         find_access(uint64_t key, uint32_t table_id, vector<AccessF1> * set);
    RC                 validate_local_txn();

    static bool     _pre_abort;

    bool             _signal_abort;
};

#endif
