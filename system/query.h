#pragma once

#include "global.h"
#include "helper.h"

class workload;
class Message;

// For distributed DBMS
// A query can be either a full query or a sub query.
// A sub query is sent to be executed on a remote node.

class RemoteQuery {
public:
    uint64_t key;
    uint32_t index_id;
    access_t type;
};

class QueryBase {
public:
    QueryBase() { _isolation_level = SR; }
    virtual ~QueryBase() {}

    virtual void gen_requests() { assert(false); }
    virtual uint32_t serialize(char * &raw_data) { assert(false); return 0; }

    Isolation     get_isolation_level() { return _isolation_level; }
    virtual bool        is_all_remote_readonly() { return false; }
#if CC_ALG == WAIT_DIE || CC_ALG == F_ONE || (CC_ALG == TICTOC && OCC_LOCK_TYPE == WAIT_DIE)
    uint64_t     get_ts() { return _txn_ts; }
    void         set_ts(uint64_t txn_ts) { _txn_ts = txn_ts; }
protected:
    // timestamp of the transaction. Only used for remote requests.
    uint64_t     _txn_ts;
#endif
    Isolation     _isolation_level;
};


