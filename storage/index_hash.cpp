#include "global.h"
#include "index_hash.h"
#include "table.h"
#include "row.h"
#include "row_lock.h"
#include "row_tictoc.h"
#include "row_f1.h"
#include "manager.h"

IndexHash::IndexHash(bool is_key_index)
    : IndexBase (is_key_index)
{
}

RC
IndexHash::init(table_t * table, uint64_t bucket_cnt)
{
    this->table = table;
    _bucket_cnt = bucket_cnt;
    _buckets = new Bucket[_bucket_cnt];
    for (uint32_t n = 0; n < _bucket_cnt; n ++)
        _buckets[n].init();
    return RCOK;
}

ROW_MAN *
IndexHash::index_get_manager(uint64_t key)
{
    Bucket * cur_bkt = &_buckets[ hash(key) ];
    return cur_bkt->get_manager(key);
}

set<row_t *> *
IndexHash::read(uint64_t key)
{
    Bucket * cur_bkt = &_buckets[ hash(key) ];
    return cur_bkt->read(key);
}

RC IndexHash::insert(uint64_t key, row_t * row) {
    uint64_t bkt_idx = hash(key);
    Bucket * cur_bkt = &_buckets[bkt_idx];

    bool success = cur_bkt->insert(key, row);
    return success? RCOK : ABORT;
}

RC
IndexHash::remove(row_t * row)
{
    uint64_t index_key = row->get_index_key(this);
    uint64_t bkt_idx = hash(index_key);
    Bucket * cur_bkt = &_buckets[bkt_idx];

//    cur_bkt->latch();
    cur_bkt->remove(index_key, row);
//    cur_bkt->unlatch();
    return RCOK;
}

////////////////////////////////////////
// Bucket
////////////////////////////////////////

IndexHash::Bucket::Bucket()
{
}

IndexHash::Bucket::~Bucket()
{
    delete _manager;
}

void
IndexHash::Bucket::init()
{
    _node_cnt = 0;
    _first_node = NULL;
    _latch = false;
    _manager = new ROW_MAN();
}

void
IndexHash::Bucket::latch() {

    _manager->latch();
}

void
IndexHash::Bucket::unlatch() {
    _manager->unlatch();
}

ROW_MAN *
IndexHash::Bucket::get_manager(uint64_t key)
{
    _manager->latch();
    return _manager;
}

set<row_t *> *
IndexHash::Bucket::read(uint64_t key)
{
    Node * node = find_node(key);
    return node? &(node->rows) : NULL;
}

IndexHash::Node *
IndexHash::Bucket::find_node(uint64_t key)
{
    Node * cur_node = _first_node;
    while (cur_node != NULL) {
        if (cur_node->key == key)
            return cur_node;
        cur_node = cur_node->next;
    }
    return NULL;
}

bool
IndexHash::Bucket::insert(uint64_t key, row_t * row)
{
    latch();
    assert(row->get_table());
    Node * node = find_node(key);
    if (node == NULL) {
        node = new Node(key);
        node->rows.insert(row);
        node->next = _first_node;
        _first_node = node;
    } else {
        // TODO. should diferentiate between unique vs. nonunique indexes.
        assert(WORKLOAD != YCSB);
        node->rows.insert(row);
    }
    unlatch();
    return true;
}

void
IndexHash::Bucket::remove(uint64_t index_key, row_t * row)
{
    latch();
    Node * cur_node = _first_node;
    Node * prev_node = NULL;
    while (cur_node != NULL && cur_node->key != index_key) {
        prev_node = cur_node;
        cur_node = cur_node->next;
    }
    assert(cur_node);
    // if the node is found, delete it.
    cur_node->remove(row);
    if (cur_node->rows.empty()) {
        if (!prev_node)
            _first_node = _first_node->next;
        else
            prev_node->next = cur_node->next;
        delete cur_node;
    }
    unlatch();
}


uint64_t
IndexHash::hash(uint64_t key)
{
    return (key ^ (key / _bucket_cnt)) % _bucket_cnt;
}

////////////////////////////////////////
// Node
////////////////////////////////////////
IndexHash::Node::Node(uint64_t key)
{
    this->key = key;
    next = NULL;
    rows.clear();
}

IndexHash::Node::~Node()
{}

void
IndexHash::Node::remove(row_t * row)
{
    M_ASSERT(rows.find(row) != rows.end(), "rows.size() = %ld. thd=%ld\n", rows.size(), GET_THD_ID);
    rows.erase(row);
}
