#include "log.h"
#include "manager.h"
#include "txn_table.h"

#if LOG_ENABLE

//#define BOOST_NO_EXCEPTIONS
/*void boost::throw_exception(std::exception const & e){
    //do nothing
}*/

LogManager::LogManager()
{
    _buffer_size = 8192 * 2; //g_num_worker_threads * 2;
    _num_shards = 500;
    _curr_shard_id = 0;
    _max_logging_interval = 1000000; // in nanoseconds
    _max_records_per_log = 100;

    // TODO. Shuffle the entries in _log_metadata to avoid false sharing.
    _log_buffer = new LogBufferEntry [_buffer_size];
    for (int i = 0; i < _buffer_size; i ++) {
        _log_buffer[i].txn = NULL;
        _log_buffer[i].filled = false;
    }

    _lsn = 0;
    _commit_lsn = 0;
    _request_lsn = 0;

    _curr_records = 0;
    _last_logging_time = get_sys_clock();

}

LogManager::~LogManager()
{
}


void
LogManager::log(TxnManager * txn, uint32_t size, char * record)
{
    // If the buffer size is too small, make it bigger or limit the number of
    // threads.
    //assert (_commit_lsn + _buffer_size - _lsn > g_max_num_active_txns * 2);

    int64_t  curr_lsn = ATOM_FETCH_ADD(_lsn, 1);
    assert(curr_lsn < _commit_lsn + _buffer_size);

    // Write to _log_buffer.
    int32_t buffer_index = curr_lsn % _buffer_size;
    LogBufferEntry &entry = _log_buffer[buffer_index];
    entry.txn = txn;
    entry.lsn = curr_lsn;
    entry.record_string = string(record);
    entry.record_string.resize(size);
    entry.record_string += std::to_string(curr_lsn);

    COMPILER_BARRIER

    entry.filled = true;
    INC_FLOAT_STATS(log_size, entry.record_string.length());
    INC_INT_STATS(log_num, 1);
}

RC
LogManager::run()
{
    cout << "[log] start loop" << endl;
    // run() is executed by the logging thread. It has two jobs:
    //    1. receive responses from logging service, and commit transactions.
    //    2. write records in the log_buffer to disk

    //std::string record_string;
    while (!glob_manager->is_sim_done()) {
        if (check_response())
            continue;
        assert(_curr_records < _max_records_per_log);
        while (_curr_records < _max_records_per_log) {
            LogBufferEntry &entry = _log_buffer[(_request_lsn + _curr_records) % _buffer_size];
            if (_request_lsn < _lsn && entry.filled) {
                uint64_t t1 = get_sys_clock();
                // Log
                _curr_records ++;
                INC_FLOAT_STATS(logging_send_time, get_sys_clock() - t1);
            } else
                break;
        }
        uint64_t curr_time = get_sys_clock();
        if (_curr_records == _max_records_per_log
            || (curr_time - _last_logging_time >= _max_logging_interval
                && _curr_records > 0)) {
            uint64_t t1 = get_sys_clock();
            _last_logging_time = curr_time;
            _request_lsn += _curr_records;
           // _log_metadata.push(metadata);
            _curr_records = 0;
            INC_FLOAT_STATS(logging_send_time, get_sys_clock() - t1);
        }
    }
    return RCOK;
}

bool
LogManager::check_response() {
    if (!_log_metadata.empty()) {
        printf("meta data not empty\n");
        LogMetadata &metadata = _log_metadata.front();
            uint64_t t1 = get_sys_clock();
            // commit this transaction
            for (int i = 0; i < metadata.num_records; i++) {
                printf("free 1 txn\n");
                LogBufferEntry &entry = _log_buffer[(metadata.lsn + i) % _buffer_size];
                entry.txn->log_semaphore->decr();
                entry.txn = NULL;
                entry.filled = false;
            }
            _commit_lsn += metadata.num_records;
            _log_metadata.pop();
            INC_FLOAT_STATS(logging_commit_time, get_sys_clock() - t1);
            return true;
        }
    
    return false;
}

#endif
