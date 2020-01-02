#pragma once

#include <cassert>
#include "global.h"

class table_t;
class Catalog;
class TxnManager;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_f1;
class Row_maat;
class LocalityManager;
class INDEX;

class row_t
{
public:
    row_t() {};
    row_t(table_t * table);
    ~row_t();
    RC              init(table_t * host_table, uint64_t part_id);
    void            init(int size);
    RC              switch_schema(table_t * host_table);
    // not every row has a manager
    void            init_manager(row_t * row);

    table_t *       get_table();
    Catalog *       get_schema();
    const uint64_t  get_table_id();
    const char *    get_table_name();
    uint64_t        get_field_cnt();
    uint64_t        get_tuple_size();

    void            copy(row_t * src);
    void            copy(char * data);

    uint64_t        get_primary_key();
    uint64_t        get_index_key(INDEX * index);
    uint64_t        get_part_id() { return _part_id; };


    static char *   get_value(Catalog * schema, uint32_t col_id, char * data);
    static void     set_value(Catalog * schema, uint32_t col_id, char * data, char * value);

    void            set_value(int id, void * ptr);
    void            get_value(int col_id, void * value);
    char *          get_value(int col_id);

    void            set_data(char * data, uint64_t size);
    void            set_data(char * data);
    char *          get_data();

    void            free_row();

    ROW_MAN *       manager;
    char *          data;
    table_t *       table;

private:
    // primary key should be calculated from the data stored in the row.
    uint64_t        _part_id;
#if CC_ALG == TICTOC
    LocalityManager * locality_manager;
#endif

};
