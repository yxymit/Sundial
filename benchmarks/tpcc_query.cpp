#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"
#include "workload.h"
#include "table.h"
#include "manager.h"

#if WORKLOAD == TPCC

// In distributed system, w_id ranges from [0, g_num_wh * g_num_nodes)
// global_w_id = local_w_id + g_node_id * g_num_wh

QueryTPCC::QueryTPCC()
    : QueryBase()
{
    // generate the local warehouse id.
    uint32_t local_w_id = URand(1, g_num_wh);
    // global warehouse id
    w_id = g_num_wh * g_node_id + local_w_id;
}

QueryTPCC::QueryTPCC(QueryTPCC * query)
{
    type = query->type;
    w_id = query->w_id;
    d_id = query->d_id;
    c_id = query->c_id;
}

///////////////////////////////////////////
// Payment
///////////////////////////////////////////

QueryPaymentTPCC::QueryPaymentTPCC()
    : QueryTPCC()
{
    type = TPCC_PAYMENT;
    d_w_id = w_id;

    d_id = URand(1, DIST_PER_WARE);
    uint32_t x = URand(1, 100);
    uint32_t y = URand(1, 100);

    if(x >= g_payment_remote_perc) {
        // home warehouse
        c_d_id = d_id;
        c_w_id = w_id;
    } else {
        // remote warehouse
        c_d_id = URand(1, DIST_PER_WARE);
        if(g_num_wh * g_num_nodes > 1) {
            do {
                c_w_id = URand(1, g_num_wh * g_num_nodes);
            } while(c_w_id == w_id);
        } else
            c_w_id = w_id;
    }
    if(y <= 60) {
        // by last name
        by_last_name = true;
        Lastname( NURand(255, 0, 999), c_last );
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, g_cust_per_dist);
    }
    h_amount = URand(1, 5000);
}


QueryPaymentTPCC::QueryPaymentTPCC(char * data)
{
    assert(false);
    //memcpy(this, data, sizeof(*this));
}

///////////////////////////////////////////
// New Order
///////////////////////////////////////////

QueryNewOrderTPCC::QueryNewOrderTPCC()
    : QueryTPCC()
{
    type = TPCC_NEW_ORDER;

    d_id = URand(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, g_cust_per_dist);
    uint32_t rbk = URand(1, 100);
    ol_cnt = URand(5, 15);
    o_entry_d = 2013;
    items = new Item_no[ol_cnt];
    remote = false;

    for (uint32_t oid = 0; oid < ol_cnt; oid ++) {
        items[oid].ol_i_id = NURand(8191, 1, g_max_items);
        // handle roll back. invalid ol_i_id.
        if (oid == ol_cnt - 1 && rbk == 1)
            items[oid].ol_i_id = 0;
        uint32_t x = URand(1, 100);
        if (x > g_new_order_remote_perc || (g_num_wh == 1 && g_num_nodes == 1))
            items[oid].ol_supply_w_id = w_id;
        else  {
            do {
                items[oid].ol_supply_w_id = RAND(g_num_wh * g_num_nodes) + 1;
            } while (items[oid].ol_supply_w_id == w_id);
            remote = true;
        }
        items[oid].ol_quantity = URand(1, 10);
    }
    // Remove duplicate items
    for (uint32_t i = 0; i < ol_cnt; i ++) {
        for (uint32_t j = 0; j < i; j++) {
            if (items[i].ol_i_id == items[j].ol_i_id) {
                items[i] = items[ol_cnt - 1];
                ol_cnt --;
                i--;
            }
        }
    }
}

QueryNewOrderTPCC::QueryNewOrderTPCC(char * data)
{
    assert(false);
    //memcpy(this, data, sizeof(*this));
    //items = new Item_no;
    //memcpy(items, data + sizeof(*this), sizeof(Item_no));
}

QueryNewOrderTPCC::~QueryNewOrderTPCC()
{
    assert(items);
    delete [] items;
}

uint32_t
QueryNewOrderTPCC::serialize(char * &raw_data)
{
    assert(false);
    return 0;
    //uint32_t size = sizeof(*this);
    //size += sizeof(Item_no);

    //raw_data = new char[size];
    //memcpy(raw_data, this, sizeof(*this));
    //memcpy(raw_data + sizeof(*this), items, sizeof(Item_no));
    //`return size;
}

///////////////////////////////////////////
// Order Status
///////////////////////////////////////////

QueryOrderStatusTPCC::QueryOrderStatusTPCC()
    : QueryTPCC()
{
    type = TPCC_ORDER_STATUS;
    d_id = URand(1, DIST_PER_WARE);
    uint32_t y = URand(1, 100);
    if(y <= 60) {
        // by last name
        by_last_name = true;
        Lastname( NURand(255, 0, 999), c_last );
    } else {
        // by cust id
        by_last_name = false;
        c_id = NURand(1023, 1, g_cust_per_dist);
    }
}

///////////////////////////////////////////
// Delivery
///////////////////////////////////////////
QueryDeliveryTPCC::QueryDeliveryTPCC()
{
    // generate the local warehouse id.
    type = TPCC_DELIVERY;
    d_id = URand(1, DIST_PER_WARE);
    o_carrier_id = URand(1, 10);
    ol_delivery_d = 2017;
}

///////////////////////////////////////////
// Stock Level
///////////////////////////////////////////
QueryStockLevelTPCC::QueryStockLevelTPCC()
{
    // generate the local warehouse id.
    type = TPCC_STOCK_LEVEL;
    _isolation_level = NO_ACID;

    d_id = URand(1, DIST_PER_WARE);

    threshold = URand(10, 20);
}

#endif
