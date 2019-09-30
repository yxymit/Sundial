#include "global.h"
#include "helper.h"
#include "stats.h"
#include "message.h"
#include "tpcc_helper.h"

Stats_thd::Stats_thd()
{
    _float_stats = (double *) _mm_malloc(sizeof(double) * NUM_FLOAT_STATS, 64);
    _int_stats = (uint64_t *) _mm_malloc(sizeof(uint64_t) * NUM_INT_STATS, 64);

    _msg_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_committed_count = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);
    _msg_committed_size = (uint64_t *) _mm_malloc(sizeof(uint64_t) * Message::NUM_MSG_TYPES, 64);

    clear();
}

void Stats_thd::init(uint64_t thd_id) {
    clear();
}

void Stats_thd::clear() {
    for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
        _float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_INT_STATS; i++)
        _int_stats[i] = 0;

    memset(_msg_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_committed_count, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memset(_msg_committed_size, 0, sizeof(uint64_t) * Message::NUM_MSG_TYPES);

#if WORKLOAD == TPCC
    memset(_commits_per_txn_type, 0, sizeof(uint64_t) * 5);
    memset(_aborts_per_txn_type, 0, sizeof(uint64_t) * 5);
    memset(_time_per_txn_type, 0, sizeof(uint64_t) * 5);
#endif
}

void
Stats_thd::copy_from(Stats_thd * stats_thd)
{
    memcpy(_float_stats, stats_thd->_float_stats, sizeof(double) * NUM_FLOAT_STATS);
    memcpy(_int_stats, stats_thd->_int_stats, sizeof(double) * NUM_INT_STATS);
    memcpy(_msg_count, stats_thd->_msg_count, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_size, stats_thd->_msg_size, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_committed_count, stats_thd->_msg_committed_count, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
    memcpy(_msg_committed_size, stats_thd->_msg_committed_size, sizeof(uint64_t) * Message::NUM_MSG_TYPES);
}

////////////////////////////////////////////////
// class Stats
////////////////////////////////////////////////
Stats::Stats()
{
    _num_cp = 0;
    _stats = new Stats_thd * [g_total_num_threads];
    for (uint32_t i = 0; i < g_total_num_threads; i++) {
        _stats[i] = (Stats_thd *) _mm_malloc(sizeof(Stats_thd), 64);
        new(_stats[i]) Stats_thd();
    }
}

void Stats::init() {
    if (!STATS_ENABLE)
        return;
    _stats = (Stats_thd**) _mm_malloc(sizeof(Stats_thd*) * g_total_num_threads, 64);
}

void Stats::init(uint64_t thread_id) {
    if (!STATS_ENABLE)
        return;
    _stats[thread_id] = (Stats_thd *)
        _mm_malloc(sizeof(Stats_thd), 64);

    _stats[thread_id]->init(thread_id);
}

void Stats::clear(uint64_t tid) {
    if (STATS_ENABLE) {
        _stats[tid]->clear();

        dl_detect_time = 0;
        dl_wait_time = 0;
        cycle_detect = 0;
        deadlock = 0;
    }
}

void Stats::output(std::ostream * os)
{
    std::ostream &out = *os;

    if (g_warmup_time > 0) {
        // subtract the stats in the warmup period
        uint32_t cp = int(1000 * g_warmup_time / STATS_CP_INTERVAL) - 1;
        Stats * base = _checkpoints[cp];
        for (uint32_t i = 0; i < g_total_num_threads; i++) {
            for    (uint32_t n = 0; n < NUM_FLOAT_STATS; n++)
                _stats[i]->_float_stats[n] -= base->_stats[i]->_float_stats[n];
            if (i < g_num_worker_threads)
                _stats[i]->_float_stats[STAT_run_time] = g_run_time * BILLION;
            for    (uint32_t n = 0; n < NUM_INT_STATS; n++)
                _stats[i]->_int_stats[n] -= base->_stats[i]->_int_stats[n];

            for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
                _stats[i]->_msg_count[n] -= base->_stats[i]->_msg_count[n];
                _stats[i]->_msg_size[n] -= base->_stats[i]->_msg_size[n];
                _stats[i]->_msg_committed_count[n] -= base->_stats[i]->_msg_committed_count[n];
                _stats[i]->_msg_committed_size[n] -= base->_stats[i]->_msg_committed_size[n];
            }
        }
    }

    uint64_t total_num_commits = 0;
    double total_run_time = 0;
    for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
        total_num_commits += _stats[tid]->_int_stats[STAT_num_commits];
        total_run_time += _stats[tid]->_float_stats[STAT_run_time];
    }

    assert(total_num_commits > 0);
    out << "=Worker Thread=" << endl;
    out << "    " << setw(30) << left << "Throughput:"
        << BILLION * total_num_commits / total_run_time * g_num_server_threads << endl;
    // print floating point stats
    for    (uint32_t i = 0; i < NUM_FLOAT_STATS; i++) {
        if (i == STAT_txn_latency)
            continue;
        double total = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid ++)
            total += _stats[tid]->_float_stats[i];
        string suffix = "";
        if (i >= STAT_execute_phase && i <= STAT_network) {
            total = total / total_num_commits * 1000000; // in us.
            suffix = " (in us) ";
        }
        out << "    " << setw(30) << left << statsFloatName[i] + suffix + ':' << total / BILLION;
        out << " (";
        for (uint32_t tid = 0; tid < g_total_num_threads; tid ++)
            out << _stats[tid]->_float_stats[i] / BILLION << ',';
        out << ')' << endl;
    }

    out << endl;

#if COLLECT_LATENCY
    double avg_latency = 0;
    for (uint32_t tid = 0; tid < g_total_num_threads; tid ++)
        avg_latency += _stats[tid]->_float_stats[STAT_txn_latency];
    avg_latency /= total_num_commits;

    out << "    " << setw(30) << left << "average_latency:" << avg_latency / BILLION << endl;
    // print latency distribution
    out << "    " << setw(30) << left << "90%_latency:"
        << _aggregate_latency[(uint64_t)(total_num_commits * 0.90)] / BILLION << endl;
    out << "    " << setw(30) << left << "95%_latency:"
        << _aggregate_latency[(uint64_t)(total_num_commits * 0.95)] / BILLION << endl;
    out << "    " << setw(30) << left << "99%_latency:"
        << _aggregate_latency[(uint64_t)(total_num_commits * 0.99)] / BILLION << endl;
    out << "    " << setw(30) << left << "max_latency:"
        << _aggregate_latency[total_num_commits - 1] / BILLION << endl;

    out << endl;
#endif
    // print integer stats
    for    (uint32_t i = 0; i < NUM_INT_STATS; i++) {
        double total = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
            total += _stats[tid]->_int_stats[i];
        }
        out << "    " << setw(30) << left << statsIntName[i] + ':'<< total;
        out << " (";
        for (uint32_t tid = 0; tid < g_total_num_threads; tid ++)
            out << _stats[tid]->_int_stats[i] << ',';
        out << ')' << endl;

    }
#if WORKLOAD == TPCC
    out << "TPCC Per Txn Type Commits/Aborts" << endl;
    out << "    " << setw(18) << left << "Txn Name"
        << setw(12) << left << "Commits"
        << setw(12) << left << "Aborts"
        << setw(12) << left << "Time" << endl;
    for (uint32_t i = 0; i < 5; i ++) {
        uint64_t commits = 0, aborts = 0;
        double time = 0;
        for (uint32_t tid = 0; tid < g_num_worker_threads; tid ++) {
            commits += _stats[tid]->_commits_per_txn_type[i];
            aborts += _stats[tid]->_aborts_per_txn_type[i];
            time += 1.0 * _stats[tid]->_time_per_txn_type[i] / BILLION;
        }
        out << "    " << setw(18) << left << TPCCHelper::get_txn_name(i)
            << setw(12) << left << commits
            << setw(12) << left << aborts
            << setw(12) << left << time << endl;
    }
#endif

    // print stats for input thread
    out << "=Input/Output Thread=" << endl;
    out << "    " << setw(18) << left
        << "Message Types"
        << setw(12) << left << "#recv"
        << setw(12) << left << "bytesRecv"
        << setw(12) << left << "#sent"
        << setw(12) << left << "bytesSent"
        << setw(12) << left << "#committed"
        << setw(12) << left << "bytesCommitted" << endl;

    for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++) {
        string msg_name = Message::get_name( (Message::Type)i );
        out << "    " << setw(18) << left << msg_name
             << setw(12) << left << _stats[g_total_num_threads - 2]->_msg_count[i]
             << setw(12) << left << _stats[g_total_num_threads - 2]->_msg_size[i]
             << setw(12) << left << _stats[g_total_num_threads - 1]->_msg_count[i]
             << setw(12) << left << _stats[g_total_num_threads - 1]->_msg_size[i];
        uint64_t committed_count = 0;
        uint64_t committed_size = 0;
        for (uint32_t j = 0; j < g_num_worker_threads; j ++) {
            committed_count += _stats[j]->_msg_committed_count[i];
            committed_size += _stats[j]->_msg_committed_size[i];
        }
        out << setw(12) << left << committed_count
            << setw(12) << left << committed_size
            << endl;
    }

    // print the checkpoints
    if (_checkpoints.size() > 1) {
        out << "\n=Check Points=\n" << endl;
        out << "Metrics:\tthr,";
        for    (uint32_t i = 0; i < NUM_INT_STATS; i++)
            out << statsIntName[i] << ',';
        for    (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
            out << statsFloatName[i] << ',';
        for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++)
            out << Message::get_name( (Message::Type)i ) << ',';
        out << endl;
    }

    for (uint32_t i = 1; i < _checkpoints.size(); i ++)
    {
        uint64_t num_commits = 0;
        for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
            num_commits += _checkpoints[i]->_stats[tid]->_int_stats[STAT_num_commits];
            num_commits -= _checkpoints[i - 1]->_stats[tid]->_int_stats[STAT_num_commits];
        }
        double thr = 1.0 * num_commits / STATS_CP_INTERVAL * 1000;
        out << "CP" << i << ':';
        out << "\t" << thr << ',';
        for    (uint32_t n = 0; n < NUM_INT_STATS; n++) {
            uint64_t value = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
                value += _checkpoints[i]->_stats[tid]->_int_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_int_stats[n];
            }
            out << value << ',';
        }
        for    (uint32_t n = 0; n < NUM_FLOAT_STATS; n++) {
            double value = 0;
            for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
                value += _checkpoints[i]->_stats[tid]->_float_stats[n];
                value -= _checkpoints[i - 1]->_stats[tid]->_float_stats[n];
            }
            out << value / BILLION << ',';
        }
        for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
            uint64_t value = 0;
            // for input thread
            value += _checkpoints[i]->_stats[g_total_num_threads - 2]->_msg_count[n];
            value -= _checkpoints[i - 1]->_stats[g_total_num_threads - 2]->_msg_count[n];
            out << value << ',';
        }
        out << endl;
    }
}

void Stats::print()
{
    ofstream file;
    bool write_to_file = false;
    if (output_file != NULL) {
        write_to_file = true;
        file.open (output_file);
    }
    // compute the latency distribution
#if COLLECT_LATENCY
    for (uint32_t tid = 0; tid < g_total_num_threads; tid ++) {
        M_ASSERT(_stats[tid]->all_latency.size() == _stats[tid]->_int_stats[STAT_num_commits],
                 "%ld vs. %ld\n",
                 _stats[tid]->all_latency.size(), _stats[tid]->_int_stats[STAT_num_commits]);
        // TODO. should exclude txns during the warmup
        _aggregate_latency.insert(_aggregate_latency.end(),
                                 _stats[tid]->all_latency.begin(),
                                 _stats[tid]->all_latency.end());
    }
    std::sort(_aggregate_latency.begin(), _aggregate_latency.end());
#endif
    output(&cout);
    if (write_to_file) {
        std::ofstream fout (output_file);
        g_warmup_time = 0;
        output(&fout);
        fout.close();
    }

    return;
}

void Stats::print_lat_distr() {
}

void
Stats::checkpoint()
{
    Stats * stats = new Stats();
    stats->copy_from(this);
    if (_checkpoints.size() > 0)
        for (uint32_t i = 0; i < NUM_INT_STATS; i++)
            assert(stats->_stats[0]->_int_stats[i] >= _checkpoints.back()->_stats[0]->_int_stats[i]);
    _checkpoints.push_back(stats);
    COMPILER_BARRIER
    _num_cp ++;
}

void
Stats::copy_from(Stats * stats)
{
    // TODO. this checkpoint may be slightly inconsistent. But it should be fine.
    for (uint32_t i = 0; i < g_total_num_threads; i ++)
        _stats[i]->copy_from(stats->_stats[i]);
}

double
Stats::last_cp_bytes_sent(double &dummy_bytes)
{
    uint32_t num_cp = _num_cp;
    if (num_cp > 0) {
        if (num_cp == 1) {
            Stats * cp = _checkpoints[num_cp - 1];
            dummy_bytes = cp->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent];
            return cp->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent];
        } else {
            Stats * cp1 = _checkpoints[num_cp - 1];
            Stats * cp2 = _checkpoints[num_cp - 2];
            dummy_bytes = cp1->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent]
                        - cp2->_stats[g_total_num_threads - 1]->_float_stats[STAT_dummy_bytes_sent];
            return cp1->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent]
                 - cp2->_stats[g_total_num_threads - 1]->_float_stats[STAT_bytes_sent];
        }
    } else {
        dummy_bytes = 0;
        return 0;
    }
}
