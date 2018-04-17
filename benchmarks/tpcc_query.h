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
	QueryPaymentTPCC(QueryPaymentTPCC * query) {
		memcpy(this, query, sizeof(*this));
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
	QueryNewOrderTPCC(QueryNewOrderTPCC * query) {
		memcpy(this, query, sizeof(*this));
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
	QueryOrderStatusTPCC(QueryOrderStatusTPCC * query) {
		memcpy(this, query, sizeof(*this));
	}
	
	bool by_last_name;
	char c_last[LASTNAME_LEN];
};

class QueryDeliveryTPCC : public QueryTPCC
{
public:
	QueryDeliveryTPCC();
	QueryDeliveryTPCC(QueryDeliveryTPCC * query) {
		memcpy(this, query, sizeof(*this));
	}
	
	uint64_t d_id;
	int64_t o_carrier_id;
	int64_t	ol_delivery_d; 	
};

class QueryStockLevelTPCC : public QueryTPCC
{
public:
	QueryStockLevelTPCC();
	QueryStockLevelTPCC(QueryStockLevelTPCC * query) {
		memcpy(this, query, sizeof(*this));
	}

	int64_t threshold;
};
