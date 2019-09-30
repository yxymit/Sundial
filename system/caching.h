#include "global.h"
#include "helper.h"
#include <queue>

#if CC_ALG == TICTOC

// The cache manager maintains the locally cached data in Sundial.
// The structure is partitioned into multiple banks.
// Each bank independently manages information like LRU, occupancy, etc.
class row_t;

class CacheManager {
public:
    // manager cached tuples. Manage replacement.
    CacheManager();
    ~CacheManager();
    // TODO. implement cache replacement
    bool lookup(uint64_t key, char * data, uint64_t &wts, uint64_t &rts,
                bool &read_cached_value, uint64_t commit_ts);
    void update(uint64_t key, uint64_t rts);
    void update_data(uint64_t key, uint64_t cts, char * data);
    void update_or_insert(uint64_t key, char * data, uint64_t wts, uint64_t rts);//, bool readonly);
    void remove(uint64_t key);

    void update_table_read_intensity(uint32_t table_id, bool read_intensive)
    { _table_read_intensive = read_intensive; }
    bool get_table_read_intensity(uint32_t table_id)
    { return _table_read_intensive; };

    void update_table_lease(uint32_t table_id, uint64_t wts, uint64_t rts);
    bool get_table_lease(uint32_t table_id, uint64_t &wts, uint64_t &rts);

    static uint64_t max_size_per_bank;

    void vote_local();
    void vote_remote();
private:
    class Node;
    class Bank {
    public:
        Bank();
        void latch();
        void unlatch();
        // return value: exivted node.
        // TODO. assuem only one node is evicted.
        Node * insert(Node * node);
        void remove(Node * node);
        Node * remove_front();

        class Compare
        {
        public:
            bool operator() (const Node * left, const Node * right) const;
        };
        pthread_mutex_t _latch;
        set<Node *, Compare> pq;
        uint64_t cur_size;
    };

    class Node {
    public:
        Node(uint64_t key) {
            this->key = key;
            next = NULL;
            row = NULL;
        }
        ~Node();
        // the tuple is predicted to be readonly
        bool         readonly; //read_cached_value;
        uint64_t    key;
        Node *         next;
        row_t *     row;
        uint64_t    last_access_time;
    };

    class Bucket {
    public:
        Bucket();
        void init();
        Node * update(uint64_t key, uint64_t rts);
        void update_data(uint64_t key, uint64_t cts, char * data);
        bool read_data(uint64_t key, char * data, uint64_t &wts, uint64_t &rts,
                                bool &readonly, Bank * bank);
        // return value: size of inserted/removed data.
        Node * update_or_insert(uint64_t key, char * data, uint64_t wts, uint64_t rts,
                                bool readonly, Bank * bank);
        Node * remove_data(uint64_t key);
    private:
        void get_latch();
        void release_latch();

        Node * find_node(uint64_t key);

        Node *         _first_node;
        uint64_t     _node_cnt;
        bool         _latch;
    };


    // key to bucket.
    uint64_t     key_to_bucket(uint64_t key);
    uint64_t     key_to_bank(uint64_t key);
    Bucket *     _table;

    uint64_t    _max_cache_size;
    uint64_t     _table_size;
    uint64_t     _num_banks;
    Bank *         _banks;

    pthread_mutex_t _latch;
    // _table_lease is deprecated. Should remove.
    map<uint32_t, pair<uint64_t, uint64_t>> _table_lease;

    map<uint32_t, uint64_t> _table_rts;

    bool        _read_cached_value;
    uint32_t     _num_vote_local;
    uint32_t    _num_vote_remote;
    void         update_voting_states();
    // HACK
    // TODO should have this for each table, but since there is only one table in YCSB, this is OK.
    bool         _table_read_intensive;
    // debug.
    uint64_t     _last_output_time;
};

#endif
