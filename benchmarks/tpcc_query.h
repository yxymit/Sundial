#pragma once

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;


// items of new order transaction
struct Item_no {
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
};

class QueryTPCC : public QueryBase
{
public:
    QueryTPCC();
    QueryTPCC(QueryTPCC * query);
    virtual ~QueryTPCC() {};
    uint32_t type;

    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
};

class QueryPaymentTPCC : public QueryTPCC
{
public:
    QueryPaymentTPCC();
    QueryPaymentTPCC(QueryPaymentTPCC * query)
      : QueryTPCC(query)
    {
        d_w_id = query->d_w_id;
        c_w_id = query->c_w_id;
        c_d_id = query->c_d_id;
        by_last_name = query->by_last_name;
        memcpy(c_last, query->c_last, LASTNAME_LEN);
        h_amount = query->h_amount;
    };
    QueryPaymentTPCC(char * data);

    uint64_t d_w_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    bool by_last_name;
    char c_last[LASTNAME_LEN];
    double h_amount;
};

class QueryNewOrderTPCC : public QueryTPCC
{
public:
    QueryNewOrderTPCC();
    QueryNewOrderTPCC(QueryNewOrderTPCC * query)
      : QueryTPCC(query)
    {
        remote = query->remote;
        ol_cnt = query->ol_cnt;
        o_entry_d = query->o_entry_d;
        items = new Item_no[ol_cnt];
        memcpy(items, query->items, sizeof(Item_no) * ol_cnt);
    }
    QueryNewOrderTPCC(char * data);
    ~QueryNewOrderTPCC();

    uint32_t serialize(char * &raw_data);

    Item_no * items;
    bool remote;
    uint64_t ol_cnt;
    uint64_t o_entry_d;
};

class QueryOrderStatusTPCC : public QueryTPCC
{
public:
    QueryOrderStatusTPCC();
    QueryOrderStatusTPCC(QueryOrderStatusTPCC * query)
      : QueryTPCC(query)
    {
        by_last_name = query->by_last_name;
        memcpy(c_last, query->c_last, LASTNAME_LEN);
    }

    bool by_last_name;
    char c_last[LASTNAME_LEN];
};

class QueryDeliveryTPCC : public QueryTPCC
{
public:
    QueryDeliveryTPCC();
    QueryDeliveryTPCC(QueryDeliveryTPCC * query)
      : QueryTPCC(query)
    {
        d_id = query->d_id;
        o_carrier_id = query->o_carrier_id;
        ol_delivery_d = query->ol_delivery_d;
    }
    uint64_t d_id;
    int64_t o_carrier_id;
    int64_t ol_delivery_d;
};

class QueryStockLevelTPCC : public QueryTPCC
{
public:
    QueryStockLevelTPCC();
    QueryStockLevelTPCC(QueryStockLevelTPCC * query)
      : QueryTPCC(query)
    {
        threshold = query->threshold;
    }

    int64_t threshold;
};
