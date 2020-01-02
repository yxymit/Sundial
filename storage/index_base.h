#pragma once

#include "global.h"

class table_t;
class row_t;

class itemid_t {
public:
    itemid_t() { next = NULL; }
    itemid_t(row_t * row) {
        this->row = row;
        next = NULL;
    };
    ~itemid_t();

    row_t * row;
    itemid_t * next;
    bool valid;
    void init();
    bool operator==(const itemid_t &other) const;
    bool operator!=(const itemid_t &other) const;
    void operator=(const itemid_t &other);
};

class IndexBase {
public:
    IndexBase() {};
    IndexBase(bool is_key_index) {};
    virtual ~IndexBase() {}

    ROW_MAN *           find_node(uint64_t key) { assert(false); return NULL; }
    uint32_t            get_index_id() { return _index_id; }
    void                set_index_id(uint32_t index_id) { _index_id = index_id; }

    // the index in on "table". The key is the merged key of "fields"
    table_t *           table;

    bool                is_key_index() { return _is_key_index; }
private:
    uint32_t            _index_id;
    bool                _is_key_index;
};
