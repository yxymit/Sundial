#pragma once

#include "workload.h"
#include "txn.h"

class table_t;
class INDEX;
class tpcc_query;
class TxnManagerTPCC;

class WorkloadTPCC : public workload {
public:
    RC          init();
    RC          init_table();
    RC          init_schema(std::istream &in);
    TxnManager * get_txn_man();
    QueryBase * gen_query();
    QueryBase * clone_query(QueryBase * query);
    StoreProcedure * create_store_procedure(TxnManager * txn, QueryBase * query);
    QueryBase * deserialize_subquery(char * data);

    uint32_t    key_to_node(uint64_t key, uint32_t table_id = 0);
    uint32_t    index_to_table(uint32_t index_id);
    void        table_to_indexes(uint32_t table_id, set<INDEX *> * indexes);

    uint64_t    get_primary_key(row_t * row);

    uint64_t    get_index_key(row_t * row, uint32_t index_id);
    table_t *   get_table(uint32_t table_id) { return tables[table_id]; }
    INDEX *     get_index(uint32_t index_id) { return indexes[index_id]; }
    //uint64_t         get_index_key(uint32_t index_id, row_t * row);

    table_t *   t_warehouse;
    table_t *   t_district;
    table_t *   t_customer;
    table_t *   t_history;
    table_t *   t_neworder;
    table_t *   t_order;
    table_t *   t_orderline;
    table_t *   t_item;
    table_t *   t_stock;

    INDEX *     i_item;
    INDEX *     i_warehouse;
    INDEX *     i_district;
    INDEX *     i_customer_id;
    INDEX *     i_customer_last;
    INDEX *     i_stock;
    INDEX *     i_order; // key = (w_id, d_id, o_id)
    INDEX *     i_order_cust;
    INDEX *     i_orderline; // key = (w_id, d_id, o_id)
    INDEX *     i_neworder;

    bool **     delivering;
    uint32_t    next_tid;
private:
    uint64_t num_wh;
    void init_tab_item();
    void init_tab_wh(uint64_t wid);
    void init_tab_dist(uint64_t w_id);
    void init_tab_stock(uint64_t w_id);
    void init_tab_cust(uint64_t d_id, uint64_t w_id);
    void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
    void init_tab_order(uint64_t d_id, uint64_t w_id);

    void init_permutation(uint64_t * perm_c_id, uint64_t wid);

/*    static void * threadInitItem(void * This);
    static void * threadInitWh(void * This);
    static void * threadInitDist(void * This);
    static void * threadInitStock(void * This);
    static void * threadInitCust(void * This);
    static void * threadInitHist(void * This);
    static void * threadInitOrder(void * This);
*/
    static void * threadInitWarehouse(void * This);
};

extern std::string TPCC_schema_string;

