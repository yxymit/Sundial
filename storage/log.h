#pragma once

#include <queue>

#include "global.h"
#include "helper.h"
#include "txn.h"

class TxnManager;

struct LogBufferEntry {
    TxnManager * txn;
    bool filled;
    int64_t lsn;
    string record_string;
};

struct LogMetadata {
    uint32_t num_records;
    int64_t lsn;
};


class LogManager {
public:
    LogManager();
    ~LogManager();
    // Methods accessed by worker threads
    void                log(TxnManager * txn, uint32_t size, char * record);

    // Methods accessed by logging threads
    RC                  run();
private:
    bool                check_response();

    LogBufferEntry *    _log_buffer;
    uint32_t            _buffer_size;
    uint32_t            _num_shards;
    int64_t               _curr_shard_id;

    int64_t             _lsn;         // next lsn to allocate
    int64_t             _request_lsn;
    int64_t             _commit_lsn;  // next lsn to commit

    std::queue<LogMetadata> _log_metadata;

    uint64_t             _max_logging_interval;
    uint64_t             _max_records_per_log;
    uint32_t             _curr_records;
    uint64_t             _last_logging_time;
};
