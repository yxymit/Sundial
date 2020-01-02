#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
#include "worker_thread.h"

__thread drand48_data Manager::_buffer;
__thread uint64_t Manager::_thread_id;
__thread uint64_t Manager::_max_cts = 1;

Manager::Manager() {
    timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
    *timestamp = 1;
    _last_min_ts_time = 0;
    _min_ts = 0;
    // For MVCC garbage collection
    all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_num_worker_threads, 64);
    for (uint32_t i = 0; i < g_num_worker_threads; i++)
        all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), 64);

    for (uint32_t i = 0; i < g_num_worker_threads; i++)
        *all_ts[i] = UINT64_MAX;

    _num_finished_worker_threads = 0;
    _num_sync_received = 0;

    _worker_pool_mutex = new pthread_mutex_t;
    pthread_mutex_init(_worker_pool_mutex, NULL);
    _unused_quota = 0;
    //_worker_threads = new WorkerThread * [g_num_worker_threads];
    //_wakeup_thread = g_max_num_active_txns;
}

uint64_t
Manager::get_ts(uint64_t thread_id) {
    if (g_ts_batch_alloc)
        assert(g_ts_alloc == TS_CAS);
    uint64_t time = 0;
    switch(g_ts_alloc) {
    case TS_MUTEX :
        pthread_mutex_lock( &ts_mutex );
        time = ++(*timestamp);
        pthread_mutex_unlock( &ts_mutex );
        break;
    case TS_CAS :
        if (g_ts_batch_alloc)
            time = ATOM_FETCH_ADD((*timestamp), g_ts_batch_num);
        else
            time = ATOM_FETCH_ADD((*timestamp), 1);
        break;
    case TS_HW :
        assert(false);
        break;
    case TS_CLOCK :
        time = (get_sys_clock() * g_num_worker_threads + thread_id) * g_num_nodes + g_node_id;
        break;
    default :
        assert(false);
    }
    return time;
}

void
Manager::calibrate_cpu_frequency()
{
    // measure CPU Freqency
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
    int64_t starttime = get_server_clock();

    sleep(1);

    int64_t endtime = get_server_clock();
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t end_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
    int64_t runtime = end_t - start_t;

    g_cpu_freq = 1.0 * (endtime - starttime) * g_cpu_freq / runtime;
    cout << "the CPU freqency is " << g_cpu_freq << endl;
}

ts_t
Manager::get_min_ts(uint64_t tid) {
    uint64_t now = get_sys_clock();
    uint64_t last_time = _last_min_ts_time;
    if (tid == 0 && now - last_time > MIN_TS_INTVL)
    {
        ts_t min = UINT64_MAX;
        for (uint32_t i = 0; i < g_num_worker_threads; i++)
            if (*all_ts[i] < min)
                min = *all_ts[i];
        if (min > _min_ts)
            _min_ts = min;
    }
    return _min_ts;
}

void
Manager::add_ts(ts_t ts) {
    assert( ts >= *all_ts[_thread_id] ||
        *all_ts[_thread_id] == UINT64_MAX);
    *all_ts[_thread_id] = ts;
}

void Manager::set_txn_man(TxnManager * txn) {
    assert(false);
}

uint64_t
Manager::rand_uint64()
{
    int64_t rint64 = 0;
    lrand48_r(&_buffer, &rint64);
    return rint64;
}

uint64_t
Manager::rand_uint64(uint64_t max)
{
    return rand_uint64() % max;
}

uint64_t
Manager::rand_uint64(uint64_t min, uint64_t max)
{
    return min + rand_uint64(max - min + 1);
}

double
Manager::rand_double()
{
    double r = 0;
    drand48_r(&_buffer, &r);
    return r;
}

uint32_t
Manager::worker_thread_done()
{
    return ATOM_ADD_FETCH(_num_finished_worker_threads, 1);
}

void
Manager::receive_sync_request()
{
    ATOM_ADD_FETCH(_num_sync_received, 1);
}

uint32_t
Manager::txnid_to_node(uint64_t txn_id)
{
    return txn_id % g_num_nodes;
}

uint32_t
Manager::txnid_to_worker_thread(uint64_t txn_id)
{
    return txn_id / g_num_nodes % g_num_worker_threads;
}

//uint64_t
//Manager::next_wakeup_thread()
//{
//    return ATOM_FETCH_ADD(_wakeup_thread, 1);
//}

// TODO. Right now the thread pool is guarded by a single mutex. This may become
// a bottleneck as the throughput increases.
bool
Manager::add_to_thread_pool(WorkerThread * worker)
{
    bool is_worker_ready = false;
    pthread_mutex_lock( _worker_pool_mutex );
    if (_unused_quota > 0) {
        _unused_quota --;
        is_worker_ready = true;
    } else {
        _ready_workers.push(worker);
    }
    pthread_mutex_unlock( _worker_pool_mutex );
    return is_worker_ready;
}

void
Manager::wakeup_next_thread()
{
    WorkerThread * worker = NULL;
    pthread_mutex_lock( _worker_pool_mutex );
    if ( _ready_workers.empty() )
        _unused_quota ++;
    else {
        worker = _ready_workers.top();
        _ready_workers.pop();
    }
    pthread_mutex_unlock( _worker_pool_mutex );
    if (worker) {
        worker->wakeup();
    }
}

/*uint64_t
Manager::get_current_time()
{
    uint64_t ts = get_sys_clock() * g_num_nodes + g_node_id;
    _early_per_thread[GET_THD_ID] = ts;
    uint64_t min = (uint64_t)-1;
    for (uint64_t i = 0; i < g_num_worker_threads; i++)
        if (_early_per_thread[i] < min)
            min = _early_per_thread[i];
    uint64_t old_min = _min_ts;
    bool success = false;
    while ( min > old_min ) {
        success = ATOM_CAS( _min_ts, old_min, min );
        if (!success) old_min = _min_ts;
    }
    if (g_num_nodes == 1)
        _global_gc_min_ts = _min_ts;
    return ts;
}

void
Manager::set_gc_ts(uint64_t ts)
{
    _early_per_thread[GET_THD_ID] = ts;
}

void
Manager::update_global_gc_ts(uint32_t node_id, uint64_t ts)
{
    _gc_ts_per_node[g_node_id] = _min_ts;
    _gc_ts_per_node[node_id] = ts;
    uint64_t min = (uint64_t)-1;
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (_gc_ts_per_node[i] < min)
            min = _gc_ts_per_node[i];
    }
    assert(_global_gc_min_ts <= min);
    _global_gc_min_ts = min;
}
*/
