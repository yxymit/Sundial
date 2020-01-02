#pragma once

#include "store_procedure.h"
class itemid_t;

class TPCCStoreProcedure : public StoreProcedure
{
public:
    TPCCStoreProcedure(TxnManager * txn_man, QueryBase * query);
    ~TPCCStoreProcedure();

    RC              execute();
    RC              process_remote_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data);

    void            txn_abort();
private:
    void            init();

    RC              execute_payment();
    RC              execute_new_order();
    RC              execute_order_status();
    RC              execute_delivery();
    RC              execute_stock_level();

    uint32_t        get_txn_type();

    // Need to maintain some states so a txn can be pended and restarted.
    uint32_t        _curr_step;
    uint32_t        _curr_ol_number;

    // for remote txn return value
    uint64_t        _s_quantity;

    // for NEW ORDER
    int64_t         _o_id;
    int64_t         _i_price[15];
    double          _w_tax;
    double          _d_tax;
    double          _c_discount;
    int64_t         _ol_num;

    // for ORDER STATUS
    int64_t         _c_id;

    // for DELIVERY
    uint32_t        _curr_dist;
    itemid_t *      _curr_item;
    double          _ol_amount;

    // For STOCK LEVEL
    set<int64_t>    _items;
    set<int64_t>::iterator _item_iter;
};
