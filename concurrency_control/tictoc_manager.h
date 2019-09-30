#pragma once

#include "cc_manager.h"
#include <chrono>
#include <thread>

#if CC_ALG == TICTOC

class TicTocManager : public CCManager
{
public:
    TicTocManager(TxnManager * txn);
    ~TicTocManager() {};

    void         init();

    bool        is_read_only() { return _is_read_only; }

    RC             get_row(row_t * row, access_t type, uint64_t key);
    RC             get_row(row_t * row, access_t type, uint64_t key, uint64_t wts);
    RC             get_row(row_t * row, access_t type, char * &data, uint64_t key);
    char *         get_data(uint64_t key, uint32_t table_id);
    RC             register_remote_access(uint32_t remote_node_id, access_t type, uint64_t key,
                                       uint32_t table_id, uint32_t &msg_size, char * &msg_data);

    RC             index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit = -1);
    RC             index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit = -1);
    RC            index_insert(INDEX * index, uint64_t key);
    RC            index_delete(INDEX * index, uint64_t key);


    void         cleanup(RC rc);

    // normal execution
    void         add_remote_req_header(UnstructuredBuffer * buffer);
    uint32_t     process_remote_req_header(UnstructuredBuffer * buffer);
    void         get_resp_data(uint32_t &size, char * &data);
    void         process_remote_resp(uint32_t node_id, uint32_t size, char * resp_data);

    // prepare phase
    RC             validate();
    RC             process_prepare_phase_coord();
    void         get_remote_nodes(set<uint32_t> * remote_nodes);

    bool         need_prepare_req(uint32_t remote_node_id, uint32_t &size, char * &data);
    RC             process_prepare_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data );
    void         process_prepare_resp(RC rc, uint32_t node_id, char * data);

    // commit phase
    void         process_commit_phase_coord(RC rc);
    RC            commit_insdel();
    bool         need_commit_req(RC rc, uint32_t node_id, uint32_t &size, char * &data);
    void         process_commit_req(RC rc, uint32_t size, char * data);
    void         abort();
    void         commit();

    // handle WAIT_DIE validation
    void         set_ts(uint64_t timestamp) { _timestamp = timestamp; }
    uint64_t     get_priority() { return _timestamp; }
    void         set_txn_ready(RC rc);
    bool         is_txn_ready();
    bool         is_signal_abort() { return _signal_abort; }

    uint64_t     LEASE;
    uint32_t     get_log_record(char *& record);
private:
    bool         _is_read_only;

    struct IndexAccessTicToc : IndexAccess {
        uint64_t wts;
        uint64_t rts;
    };

    struct AccessTicToc : Access {
        AccessTicToc() {
            locked = false;
            row = NULL;
            local_data = NULL;
            responded = false;
#if ENABLE_LOCAL_CACHING
            // this indicates whether the tuple is cached at the client side.
            cached = false;
#endif
        };
        bool         locked;
        uint64_t    wts;
        uint64_t     rts;
        uint32_t     data_size;
        char *         local_data;
        // only for sub txns
        bool        responded;
#if ENABLE_LOCAL_CACHING
        // the locally cached version is read. The remote node does not know about this read.
        bool         cached;
#endif
    };

    static bool compare(AccessTicToc * ac1, AccessTicToc * ac2);

    vector<IndexAccessTicToc>        _index_access_set;
    vector<AccessTicToc>            _access_set;
    vector<AccessTicToc>            _remote_set;

    vector<AccessTicToc *>            _read_set;
    vector<AccessTicToc *>            _write_set;
    AccessTicToc *                     _last_access;

    // For the coordinator, _min_commit_ts is the final commit time
    // For subordinator, it is the minimal commit ts based on local info
    uint64_t _min_commit_ts;
    uint64_t _max_commit_ts;
    ////////// Only used in the coordinator
    struct RemoteNodeInfo {
        uint32_t node_id;
        bool      readonly;
        uint64_t min_commit_ts;
        uint64_t max_commit_ts;
        uint64_t commit_ts_upper_bound; // NOT in use
    };
    vector<RemoteNodeInfo> _remote_node_info;
    ///////////////////////////////////////
    AccessTicToc * find_access(uint64_t key, uint32_t table_id, vector<AccessTicToc> * set);

    void     split_read_write_set();
    RC         lock_write_set();
    RC         lock_read_set();
    void     unlock_write_set(RC rc);
    void     unlock_read_set();
    void     compute_commit_ts();
    RC         validate_read_set(uint64_t commit_ts);
    RC         validate_write_set(uint64_t commit_ts);

    RC handle_pre_abort();

    bool             _write_copy_ptr;
    static bool     _pre_abort;
    bool             _validation_no_wait;
    bool            _atomic_timestamp;
    uint64_t         _max_wts;
    RC                validate_tictoc();

    // For OCC_LOCK_TYPE == WAIT_DIE
    // txn start time, serves as the priority of the txn
    bool             _signal_abort;
};

#endif
