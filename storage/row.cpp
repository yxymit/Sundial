#include <mm_malloc.h>
#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_f1.h"
#include "row_tictoc.h"
#include "manager.h"
#include "workload.h"
#include "index_hash.h"

row_t::row_t(table_t * table)
{
    _part_id = 0;
    this->table = table;
    assert(table);
    data = new char [get_tuple_size()];
    init_manager(this);
}

row_t::~row_t()
{
    if (data)
        delete [] data;
    if (manager)
        delete manager;
}

RC
row_t::init(table_t * host_table, uint64_t part_id) {
    _part_id = part_id;
    assert(host_table);
    this->table = host_table;
    Catalog * schema = host_table->get_schema();
    int tuple_size = schema->get_tuple_size();
    data = new char [tuple_size];
    return RCOK;
}
void
row_t::init(int size)
{
    data = new char [size];
}

RC
row_t::switch_schema(table_t * host_table) {
    assert(false);
    this->table = host_table;
    return RCOK;
}

void row_t::init_manager(row_t * row) {
    manager = new ROW_MAN(this);
}

table_t * row_t::get_table() {
    return table;
}

Catalog * row_t::get_schema() {
    return get_table()->get_schema();
}

const uint64_t row_t::get_table_id() {
    return get_table()->get_table_id();
}

const char * row_t::get_table_name() {
    return get_table()->get_table_name();
}

uint64_t row_t::get_tuple_size() {
    return get_schema()->get_tuple_size();
}

uint64_t row_t::get_field_cnt() {
    return get_schema()->field_cnt;
}

void row_t::set_value(int id, void * ptr) {
    int datasize = get_schema()->get_field_size(id);
    int pos = get_schema()->get_field_index(id);
    memcpy( &data[pos], ptr, datasize);
}

void
row_t::get_value(int col_id, void * value)
{
    int pos = get_schema()->get_field_index(col_id);
    int size = get_schema()->get_field_size(col_id);
    memcpy(value, &data[pos], size);
}

char *
row_t::get_value(int col_id)
{
    int pos = get_schema()->get_field_index(col_id);
    return &data[pos];
}

char *
row_t::get_value(Catalog * schema, uint32_t col_id, char * data)
{
    return &data[ schema->get_field_index(col_id) ];
}
void
row_t::set_value(Catalog * schema, uint32_t col_id, char * data, char * value)
{
    memcpy( &data[ schema->get_field_index(col_id) ],
            value,
            schema->get_field_size(col_id)
          );
}

char * row_t::get_data() { return data; }

void row_t::set_data(char * data, uint64_t size) {
    memcpy(this->data, data, size);
}

void row_t::set_data(char * data) {
    set_data(data, get_tuple_size());
}

// copy from the src to this
void row_t::copy(row_t * src) {
    set_data(src->get_data(), src->get_tuple_size());
}

void row_t::copy(char * data) {
    set_data(data, get_tuple_size());
}

void row_t::free_row() {
    free(data);
}

uint64_t
row_t::get_primary_key()
{
    return GET_WORKLOAD->get_primary_key(this);
}

uint64_t
row_t::get_index_key(INDEX * index)
{
    return GET_WORKLOAD->get_index_key(this, index->get_index_id());
}
