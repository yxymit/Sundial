#include "caching.h"
#include "row.h"
#include "row_tictoc.h"
#include "manager.h"
#include "workload.h"

#if CC_ALG == TICTOC

uint64_t CacheManager::max_size_per_bank;

CacheManager::CacheManager()
{
    _max_cache_size = g_local_cache_size * 1024; // in Bytes
    _table_size = _max_cache_size / 1024;
    _num_banks = 64;
    max_size_per_bank = _max_cache_size / _num_banks;
    assert(_max_cache_size % _num_banks == 0);
    if (_table_size > 0) {
        _table = new Bucket[_table_size];
        _banks = new Bank[_num_banks];
    }
    _last_output_time = get_sys_clock();
    pthread_mutex_init( &_latch, NULL );

    _num_vote_local = 0;
    _num_vote_remote = 0;
    _read_cached_value = true;
}

CacheManager::~CacheManager()
{
    if (_table_size == 0)
        return;
    uint64_t occupancy = 0;
    for (uint32_t i = 0; i < _num_banks; i++)
        occupancy += _banks[i].cur_size;
    printf("%f%% (%ld/%ld)\n", 100.0 * occupancy / _max_cache_size, occupancy, _max_cache_size);
}

uint64_t
CacheManager::key_to_bucket(uint64_t key)
{
    // the lower bits of a key is always the node id.
    return (key ^ (key / _table_size)) % _table_size;
};

uint64_t CacheManager::key_to_bank(uint64_t key)
{
    return (key / g_num_nodes) % _num_banks;
}

bool
CacheManager::lookup(uint64_t key, char * data, uint64_t &wts, uint64_t &rts,
                     bool &read_cached_value, uint64_t commit_ts)
{
    if (_table_size == 0)
        return false;
    uint64_t tt = get_sys_clock();
    uint64_t bucket_id = key_to_bucket(key);

    uint32_t bank_id = key_to_bank(key);
    Bank * bank = &_banks[bank_id];

    bool readonly = false;
    bool cached = _table[bucket_id].read_data(key, data, wts, rts, readonly, bank);

    if (cached) {
#if CACHING_POLICY == ALWAYS_READ
        read_cached_value = true;
#elif CACHING_POLICY == ALWAYS_CHECK
        read_cached_value = false;
#elif CACHING_POLICY == READ_INTENSIVE
        read_cached_value = _read_cached_value; //_table_read_intensive;
    #if REUSE_FRESH_DATA
        if (commit_ts <= rts)
            read_cached_value = true;
    #endif
#endif
        INC_INT_STATS(num_cache_hits, 1);
    } else
        INC_INT_STATS(num_cache_misses, 1);
#if WORKLOAD == TPCC
    uint64_t tab_wts, tab_rts;
    bool tab_lease = get_table_lease(7, tab_wts, tab_rts);
    if (tab_lease && rts >= tab_wts && rts < tab_rts) {
        rts = tab_rts;
    }
#endif

    INC_FLOAT_STATS(cache, get_sys_clock() - tt);
    return cached;
}

void
CacheManager::update(uint64_t key, uint64_t rts)
{
    if (_table_size == 0)
        return;
    uint64_t tt = get_sys_clock();
    uint64_t bucket_id = key_to_bucket(key);
    uint32_t bank_id = key_to_bank(key);
    Bank * bank = &_banks[bank_id];
    bank->latch();
    Node * node = _table[bucket_id].update(key, rts);
    if (node) {
        if (rts > node->row->manager->_rts) {
            bank->pq.erase(node);
            node->row->manager->_rts = rts;
            node->last_access_time = get_sys_clock();
            bank->pq.insert(node);
        }
    }
    bank->unlatch();
    INC_FLOAT_STATS(cache, get_sys_clock() - tt);
}


void
CacheManager::update_data(uint64_t key, uint64_t cts, char * data)
{
    if (_table_size == 0)
        return;
    uint64_t tt = get_sys_clock();
    uint64_t bucket_id = key_to_bucket(key);
    // this is to update locally cached data.
    // do not update LRU since we only want to cache read intensive data.
    _table[bucket_id].update_data(key, cts, data);
    INC_FLOAT_STATS(cache, get_sys_clock() - tt);
}

void
CacheManager::update_or_insert(uint64_t key, char * data, uint64_t wts, uint64_t rts)
{
    if (_table_size == 0)
        return;
    uint64_t tt = get_sys_clock();
    uint64_t bucket_id = key_to_bucket(key);
    uint32_t bank_id = key_to_bank(key);
    Bank * bank = &_banks[bank_id];
    bank->latch();

    _table[bucket_id].update_or_insert(key, data, wts, rts, false, bank);
    // Cache overflow
    if (bank->cur_size > CacheManager::max_size_per_bank) {
        Node * node = bank->remove_front();
        _table[key_to_bucket(node->key)].remove_data(node->key);
        INC_INT_STATS(num_cache_evictions, 1)
        delete node;
    }
    bank->unlatch();
    INC_FLOAT_STATS(cache, get_sys_clock() - tt);
}

void
CacheManager::remove(uint64_t key)
{
    if (_table_size == 0)
        return;
    uint64_t tt = get_sys_clock();
    uint64_t bucket_id = key_to_bucket(key);
    uint32_t bank_id = key_to_bank(key);
    Bank * bank = &_banks[bank_id];
    bank->latch();
    Node * node = _table[bucket_id].remove_data(key);
    if (node) {
        INC_INT_STATS(num_cache_remove, 1);
        bank->remove(node);
        delete node;
    }
    bank->unlatch();
    INC_FLOAT_STATS(cache, get_sys_clock() - tt);
}



void
CacheManager::update_table_lease(uint32_t table_id, uint64_t wts, uint64_t rts)
{
    pthread_mutex_lock( &_latch );
    _table_lease[table_id] = make_pair(wts, rts);
    pthread_mutex_unlock( &_latch );
}

bool
CacheManager::get_table_lease(uint32_t table_id, uint64_t &wts, uint64_t &rts)
{
    if (_table_lease.find(table_id) == _table_lease.end())
        return false;
    wts = _table_lease[table_id].first;
    rts = _table_lease[table_id].second;
    return true;
}

///////////////////////////////////////
// Bucket
///////////////////////////////////////
CacheManager::Bucket::Bucket()
{
    _first_node = NULL;
    _latch = false;
}

bool
CacheManager::Bucket::read_data(uint64_t key, char * data, uint64_t &wts, uint64_t &rts,
                                bool &readonly, Bank * bank) //read_cached_value)
{
    if (!_first_node)
        return false;
    get_latch();
    Node * node = _first_node;
    while (node && node->key != key)
        node = node->next;
    if (node) {
        memcpy(data, node->row->get_data(), node->row->get_tuple_size());
        wts = node->row->manager->get_wts();
        rts = node->row->manager->get_rts();
    }
    release_latch();
    return node != NULL;
}

CacheManager::Node *
CacheManager::Bucket::update(uint64_t key, uint64_t rts)
{
    get_latch();
    Node * node = _first_node;
    while (node && node->key != key)
        node = node->next;
    release_latch();
    return node;
}

void
CacheManager::Bucket::update_data(uint64_t key, uint64_t cts, char * data)
{
    get_latch();
    Node * node = _first_node;
    while (node && node->key != key)
        node = node->next;
    if (node)
        node->row->manager->update(data, cts, cts);
    release_latch();
}

CacheManager::Node *
CacheManager::Bucket::update_or_insert(uint64_t key, char * data, uint64_t wts, uint64_t rts,
                                       bool readonly,
                                       Bank * bank)
{
    get_latch();
    Node * node = _first_node;
    while (node && node->key != key)
        node = node->next;
    if (node) {
        // update
        INC_INT_STATS(num_cache_updates, 1);
        node->row->manager->update(data, wts, rts);
        bank->pq.erase(node);
        node->last_access_time = get_sys_clock();
        bank->pq.insert(node);
        release_latch();
        return node;
    } else {
        // insert
        // TODO. this assumes a single table in the system (YCSB).
        INC_INT_STATS(num_cache_inserts, 1);
        table_t * table = glob_manager->get_workload()->get_table(0);
        // create a new row.
        row_t * row = new row_t(table);
        row->manager->set_ts(wts, rts);
        row->copy(data);

        // create a new node.
        node = new Node(key);
        node->row = row;
        node->next = _first_node;
        _first_node = node;
        release_latch();
        bank->insert(node);
        return node;
    }
}

CacheManager::Node *
CacheManager::Bucket::remove_data(uint64_t key)
{
    get_latch();
    Node * prev = NULL;
    Node * node = _first_node;
    while (node && node->key != key) {
        prev = node;
        node = node->next;
    }
    if (node) {
        if (!prev)
            _first_node = _first_node->next;
        else
            prev->next = node->next;
    }
    release_latch();
    return node;
}

void
CacheManager::Bucket::get_latch() {
    while (!ATOM_CAS(_latch, false, true))
        PAUSE
}

void
CacheManager::Bucket::release_latch() {
    _latch = false;
}

/////////////////////////////////////
// Node
/////////////////////////////////////
CacheManager::Node::~Node()
{
    delete row;
    row = NULL;
}

/////////////////////////////////////
// Bank
/////////////////////////////////////
CacheManager::Bank::Bank()
{
    cur_size = 0;
    pthread_mutex_init( &_latch, NULL );
}

bool
CacheManager::Bank::Compare::operator() (const Node * left, const Node * right) const
{
    return left->last_access_time < right->last_access_time;
}

void
CacheManager::Bank::latch()
{
    pthread_mutex_lock( &_latch );
}

void
CacheManager::Bank::unlatch()
{
    pthread_mutex_unlock( &_latch );
}

CacheManager::Node *
CacheManager::Bank::insert(Node * node)
{
    node->last_access_time = get_sys_clock();
    auto ret = pq.insert(node);
    assert(ret.second);
    cur_size += node->row->get_tuple_size();
    return NULL;
}

void
CacheManager::Bank::remove(Node * node)
{
    int n = pq.erase(node);
    M_ASSERT(n > 0, "n=%d\n", n);
    cur_size -= node->row->get_tuple_size();
}

CacheManager::Node *
CacheManager::Bank::remove_front()
{
    set<Node *>::iterator it = pq.begin();
    Node * node = *it;
    cur_size -= node->row->get_tuple_size();
    pq.erase( it );
    return node;
}

void
CacheManager::vote_local()
{
    ATOM_ADD(_num_vote_local, 1);
    update_voting_states();
}


void
CacheManager::vote_remote()
{
    ATOM_ADD(_num_vote_remote, 1);
    update_voting_states();
}

void
CacheManager::update_voting_states()
{
    if (_num_vote_local + _num_vote_remote > 100000) {
        // NOTE. the following updates are not atomic.
        // But having some error is OK since the this is just an approximate.
        _num_vote_local /= 2;
        _num_vote_remote /= 2;
        _read_cached_value = (1.0 * _num_vote_local / (_num_vote_remote + _num_vote_local) > g_read_intensity_thresh);
    }
}

#endif
