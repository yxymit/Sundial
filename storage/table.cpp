#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "workload.h"
#include "manager.h"

#if ENABLE_LOCAL_CACHING
__thread uint32_t table_t::_write_intensity = 0;
__thread uint32_t table_t::_num_writes = 0;
__thread uint32_t table_t::_num_reads = 0;
#endif

void table_t::init(Catalog * schema) {
    this->table_name = schema->table_name;
    this->schema = schema;
#if ENABLE_LOCAL_CACHING
    _curr_lease = 0;
#endif
    _max_wts = 0;
    _max_rts = 0;
}

void
table_t::update_max_wts(uint64_t wts)
{
    bool done;
    do {
        done = true;
        uint64_t max_wts = _max_wts;
        if (wts > max_wts)
            done = ATOM_CAS(_max_wts, max_wts, wts);
    } while (!done);
}

void
table_t::update_max_rts(uint64_t rts)
{
    bool done;
    do {
        done = true;
        uint64_t max_rts = _max_wts;
        if (rts > max_rts)
            done = ATOM_CAS(_max_rts, max_rts, rts);
    } while (!done);
}

RC table_t::get_new_row(row_t *& row) {
    return get_new_row(row, 0);
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id) {
    RC rc = RCOK;
    cur_tab_size ++;
    assert(this);
    row = new row_t(this);
    return rc;
}

void
table_t::get_indexes(set<INDEX *> * indexes)
{
    GET_WORKLOAD->table_to_indexes(_table_id, indexes);
}

const char *
table_t::get_table_name()
{
    return schema->table_name;
}

