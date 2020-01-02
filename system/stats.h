#pragma once
#include "global.h"
#include "helper.h"

using std::vector;


enum StatsFloat {
    // Worker Thread
    STAT_run_time,
    STAT_txn_latency,

    STAT_time_process_txn,
    //STAT_time_commit_process,
    STAT_time_idle,

    //STAT_time_read_input_queue,
    //STAT_time_process_txn,
    //STAT_time_wait_for_lock,
    //STAT_time_wait_for_restart,
    //STAT_time_process_remote_req,
    //STAT_time_idle,

    //STAT_time_wait_buffer,
    //STAT_time_abort_queue,
    STAT_time_write_output_queue,

    // Logging
    STAT_log_size,

    // Output Thread
    STAT_bytes_sent,
    STAT_dummy_bytes_sent,
    STAT_time_send_msg,
    STAT_time_read_queue, // read output_queue
    STAT_time_output_idle,

    // Input Thread
    STAT_bytes_received,
    STAT_time_recv_msg,
    STAT_time_write_queue, // write input_queue
    STAT_time_input_idle,

    // Logging thread
    STAT_logging_send_time,
    STAT_logging_commit_time,

    // txn lifetime breakdown
    STAT_single_part_execute_phase,
    STAT_single_part_precommit_phase,
    STAT_single_part_log_latency,
    STAT_single_part_commit_phase,
    STAT_single_part_abort,

    STAT_multi_part_execute_phase,
    STAT_multi_part_precommit_phase,
    STAT_multi_part_prepare_phase,
    STAT_multi_part_commit_phase,
    STAT_multi_part_abort,

    STAT_log_ready_time,
    STAT_dependency_ready_time,

    STAT_row,
    STAT_index,
    STAT_logic,
    STAT_wait,
    STAT_network,
    STAT_cache,

    // debug stats
    STAT_time_debug1,
    STAT_time_debug2,
    STAT_time_debug3,
    STAT_time_debug4,
    STAT_time_debug5,
    STAT_time_debug6,
    STAT_time_debug7,

    NUM_FLOAT_STATS
};

enum StatsInt {
    STAT_num_commits,
    STAT_num_aborts,
    STAT_num_precommits,
    STAT_num_waits,

    STAT_num_single_part_txn,
    STAT_num_multi_part_txn,

    STAT_num_aborts_restart,
    STAT_num_aborts_terminate,
    STAT_num_aborts_local,
    STAT_num_aborts_remote,

    // Logging
    STAT_log_num,

    STAT_num_renewals,
    STAT_num_no_need_to_renewal,

    // For local caching
    STAT_num_cache_bypass,
    STAT_num_cache_reads,
    STAT_num_cache_hits,
    STAT_num_cache_misses,
    STAT_num_cache_remove,
    STAT_num_cache_inserts,
    STAT_num_cache_updates,
    STAT_num_cache_evictions,

    STAT_num_local_hits,
    STAT_num_renew,
    STAT_num_renew_success,
    STAT_num_renew_failure,

    // for READ_INTENSIVE
    STAT_num_ro_read,
    STAT_num_ro_check,
    STAT_num_rw_read,
    STAT_num_rw_check,

    STAT_int_debug1,
    STAT_int_debug2,
    STAT_int_debug3,
    STAT_int_debug4,
    STAT_int_debug5,
    STAT_int_debug6,

    STAT_int_urgentwrite,
    STAT_int_aborts_rs1,
    STAT_int_aborts_rs2,
    STAT_int_aborts_rs3,
    STAT_int_aborts_ws1,
    STAT_int_aborts_ws2,

    STAT_int_saved_by_hist,

    NUM_INT_STATS
};

class Stats_thd {
public:
    Stats_thd();
    void copy_from(Stats_thd * stats_thd);

    void init(uint64_t thd_id);
    void clear();

    double * _float_stats;
    uint64_t * _int_stats;

#if COLLECT_LATENCY
    vector<double> all_latency;
#endif
    uint64_t * _req_msg_count;
    uint64_t * _req_msg_size;
    uint64_t * _resp_msg_count;
    uint64_t * _resp_msg_size;
    //uint64_t * _msg_committed_count;
    //uint64_t * _msg_committed_size;
#if WORKLOAD == TPCC
    uint64_t _commits_per_txn_type[5];
    uint64_t _aborts_per_txn_type[5];
    uint64_t _time_per_txn_type[5];
#endif
};

class Stats {
public:
    Stats();
    // PER THREAD statistics
    Stats_thd ** _stats;

    // GLOBAL statistics
    double dl_detect_time;
    double dl_wait_time;
    uint64_t cycle_detect;
    uint64_t deadlock;

    // output thread
    uint64_t bytes_sent;
    uint64_t bytes_recv;

    double last_cp_bytes_sent(double &dummy_bytes);
    void init();
    void init(uint64_t thread_id);
    void clear(uint64_t tid);
    void print();
    void print_lat_distr();

    void checkpoint();
    void copy_from(Stats * stats);

    void output(std::ostream * os);

    std::string statsFloatName[NUM_FLOAT_STATS] = {
        // worker thread
        "run_time",
        "average_latency",

        //"time_read_input_queue",
        "time_process_txn",
        //"time_commit_process",
        "time_idle",
        //"time_wait_for_lock",
        //"time_wait_for_restart",
        //"time_process_remote_req",
        //"time_idle",

        //"time_waiting_for_job",
        //"time_wait_buffer",
        //"time_abort_queue",
        "time_write_output_queue",

        // Logging
        "log_size",

        // output thread
        "bytes_sent",
        "dummy_bytes_sent",
        "time_send_msg",
        "time_read_queue",
        "time_output_idle",

        // Input Thread
        "bytes_received",
        "time_recv_msg",
        "time_write_queue",
        "time_input_idle",

        // Logging thread
        "logging_send_time",
        "logging_commit_time",

        // txn lifetime breakdown
        "single_part_execute_phase",
        "single_part_precommit_phase",
        "single_part_log_latency",
        "single_part_commit_phase",
        "single_part_abort",

        "multi_part_execute_phase",
        "multi_part_precommit_phase",
        "multi_part_prepare_phase",
        "multi_part_commit_phase",
        "multi_part_abort",

        "log_ready_time",
        "dependency_ready_time",

        "CC (row)",
        "CC (index)",
        "logic",
        "wait",
        "network",
        "cache",

        // debug
        "time_debug1",
        "time_debug2",
        "time_debug3",
        "time_debug4",
        "time_debug5",
        "time_debug6",
        "time_debug7",
    };

    std::string statsIntName[NUM_INT_STATS] = {
        "num_commits",
        "num_aborts",
        "num_precommits",
        "num_waits",

        "num_single_part_txn",
        "num_multi_part_txn",

        "num_aborts_restart",
        "num_aborts_terminate",
        "num_aborts_local",
        "num_aborts_remote",

        // Logging
        "log_num",

        "num_renewals",
        "num_no_need_to_renewal",

        // For local caching
        "num_cache_bypass",
        "num_cache_reads",

        "num_cache_hits",
        "num_cache_misses",
        "num_cache_remove",
        "num_cache_inserts",
        "num_cache_updates",
        "num_cache_evictions",

        "num_local_hits",
        "num_renew",
        "num_renew_success",
        "num_renew_failure",

        // for READ_INTENSIVE
        "num_ro_read",
        "num_ro_check",
        "num_rw_read",
        "num_rw_check",

        "int_debug1",
        "int_debug2",
        "int_debug3",
        "int_debug4",
        "int_debug5",
        "int_debug6",

        "int_urgentwrite",

        "int_aborts_rs1",
        "int_aborts_rs2",
        "int_aborts_rs3",
        "int_aborts_ws1",
        "int_aborts_ws2",

        "int_saved_by_hist"

    };
private:
    vector<double> _aggregate_latency;
    vector<Stats *> _checkpoints;
    uint32_t        _num_cp;
};
