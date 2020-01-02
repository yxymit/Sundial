#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

//TODO make proper variables private
// each BucketNode contains items sharing the same key

// BucketHeader does concurrency control of Hash

// TODO Hash index does not support partition yet.
class IndexHash  : public IndexBase
{
public:
    IndexHash() {};
    IndexHash(bool is_key_index);
    RC init(table_t * table, uint64_t bucket_cnt);
    // Index lookups, inserts and deletes
    // This function latches the bucket, and return the manager.
    // return value: if the key exists or not.
    ROW_MAN * index_get_manager(uint64_t key);
    set<row_t *> * read(uint64_t key);
    RC insert(uint64_t key, row_t * row);
    // Right now, can only delete one row at a time.
    RC remove(row_t * row);

private:
    class Node {
    public:
        Node(uint64_t key);
        ~Node();

        void remove(row_t * row);
        uint64_t key;
        Node * next;
        set<row_t *> rows;
    };

    class Bucket {
    public:
        Bucket();
        ~Bucket();
        void init();
        ROW_MAN *       get_manager(uint64_t key);
        set<row_t *> *     read(uint64_t key);

        bool insert(uint64_t key, row_t * row);
        void remove(uint64_t index_key, row_t * row);
        void latch();
        void unlatch();
    private:
        ROW_MAN *     _manager;

        Node *         find_node(uint64_t key);

        Node *         _first_node;
        uint64_t     _node_cnt;
        bool         _latch;
    };

    // TODO implement more complex hash function
    uint64_t hash(uint64_t key);
    std::hash<uint64_t> key_hash;

    Bucket *             _buckets;
    uint64_t             _bucket_cnt;
};
