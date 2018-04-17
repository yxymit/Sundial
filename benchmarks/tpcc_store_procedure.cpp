#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "tpcc_const.h"
#include "tpcc_store_procedure.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "index_base.h"
#include "catalog.h"

#if WORKLOAD == TPCC

TPCCStoreProcedure::TPCCStoreProcedure(TxnManager * txn_man, QueryBase * query)
	: StoreProcedure(txn_man, query)
{
	init();
}

TPCCStoreProcedure::~TPCCStoreProcedure()
{
}

void 
TPCCStoreProcedure::init()
{
	StoreProcedure::init();
	_curr_step = 0;
	_curr_ol_number = 0;

	_curr_dist = 0;
	_ol_amount = 0;
	_ol_num = 0;
}
	
uint32_t
TPCCStoreProcedure::get_txn_type()
{
	return ((QueryTPCC *)_query)->type;
}

RC
TPCCStoreProcedure::execute()
{
	QueryTPCC * query = (QueryTPCC *) _query;
	assert(!_txn->is_sub_txn());
	switch (query->type) {
		case TPCC_PAYMENT:  	return execute_payment(); 
		case TPCC_NEW_ORDER: 	return execute_new_order(); 
		case TPCC_ORDER_STATUS: return execute_order_status(); 
		case TPCC_DELIVERY:		return execute_delivery();
		case TPCC_STOCK_LEVEL:	return execute_stock_level();
		default:				assert(false);
	}
}

RC 
TPCCStoreProcedure::execute_payment() 
{
	QueryPaymentTPCC * query = (QueryPaymentTPCC *) _query;
	WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD; 

	RC rc = RCOK;
	uint64_t key;
	Catalog * schema;
	INDEX * index;

    uint64_t c_w_id = query->c_w_id;
	if (_curr_step == 0) {
		// WAREHOUSE
		key = query->w_id;
		schema = wl->t_warehouse->get_schema();	
		index = wl->i_warehouse;

		GET_DATA(key, wl->i_warehouse, WR);
		/*====================================================+
    		EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
			WHERE w_id=:w_id;
		+====================================================*/
		/*===================================================================+
			EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
			INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
			FROM warehouse
			WHERE w_id=:w_id;
		+===================================================================*/
		LOAD_VALUE(double, w_ytd, schema, _curr_data, W_YTD);
		w_ytd += query->h_amount;
		STORE_VALUE(w_ytd, schema, _curr_data, W_YTD);
	
		__attribute__((unused)) LOAD_VALUE(char *, w_name, schema, _curr_data, W_NAME);
		__attribute__((unused)) LOAD_VALUE(char *, w_street_1, schema, _curr_data, W_STREET_1);
		__attribute__((unused)) LOAD_VALUE(char *, w_street_2, schema, _curr_data, W_STREET_2);
		__attribute__((unused)) LOAD_VALUE(char *, w_city, schema, _curr_data, W_CITY);
		__attribute__((unused)) LOAD_VALUE(char *, w_state, schema, _curr_data, W_STATE);
		__attribute__((unused)) LOAD_VALUE(char *, w_zip, schema, _curr_data, W_ZIP);

		_curr_step = 1;
	}
	if (_curr_step == 1) {
		// access DISTRICT table
		key = distKey(query->d_w_id, query->d_id);
		schema = wl->t_district->get_schema();
		GET_DATA(key, wl->i_district, WR);
		/*=====================================================+
			EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
			WHERE d_w_id=:w_id AND d_id=:d_id;
		+=====================================================*/
		LOAD_VALUE(double, d_ytd, schema, _curr_data, D_YTD);
		d_ytd += query->h_amount;
		STORE_VALUE(d_ytd, schema, _curr_data, D_YTD);
	
		__attribute__((unused)) LOAD_VALUE(char *, d_name, schema, _curr_data, D_NAME);
		__attribute__((unused)) LOAD_VALUE(char *, d_street_1, schema, _curr_data, D_STREET_1);
		__attribute__((unused)) LOAD_VALUE(char *, d_street_2, schema, _curr_data, D_STREET_2);
		__attribute__((unused)) LOAD_VALUE(char *, d_city, schema, _curr_data, D_CITY);
		__attribute__((unused)) LOAD_VALUE(char *, d_state, schema, _curr_data, D_STATE);
		__attribute__((unused)) LOAD_VALUE(char *, d_zip, schema, _curr_data, D_ZIP);

		_curr_step = 2;
	}
	if (_curr_step == 2) {
		uint32_t node_id = TPCCHelper::wh_to_node(c_w_id);
		key = (query->by_last_name)? 
			  custNPKey(query->c_last, query->c_d_id, query->c_w_id) 
		  	: custKey(query->c_w_id, query->c_d_id, query->c_id);
		index = query->by_last_name? wl->i_customer_last : wl->i_customer_id;
		schema = wl->t_customer->get_schema();
		uint32_t index_id = (query->by_last_name)? IDX_CUSTOMER_LAST : IDX_CUSTOMER_ID;
		if (node_id == g_node_id) {
			GET_DATA(key, index, WR);
			_phase = 2;
		}
		if (_phase == 0) {
			REMOTE_ACCESS(node_id, key, WR, TAB_CUSTOMER, index_id);
			_phase = 1;
			return LOCAL_MISS;
		}
		if (_phase == 1) {
			_phase = 2;
			assert(!remote_requests.empty());
			return RCOK;
		}
		if (_phase == 2) {
			// the last access is to the CUSTOMER table. 
			_curr_data = get_cc_manager()->get_data(key, TAB_CUSTOMER);

			LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
			c_balance -= query->h_amount;
			STORE_VALUE(c_balance, schema, _curr_data, C_BALANCE);

			LOAD_VALUE(double, c_ytd_payment, schema, _curr_data, C_YTD_PAYMENT);
			c_ytd_payment -= query->h_amount;
			STORE_VALUE(c_ytd_payment, schema, _curr_data, C_YTD_PAYMENT);

			LOAD_VALUE(uint64_t, c_payment_cnt, schema, _curr_data, C_PAYMENT_CNT);
			c_payment_cnt += 1;
			STORE_VALUE(c_payment_cnt, schema, _curr_data, C_PAYMENT_CNT);

			__attribute__((unused)) LOAD_VALUE(char *, c_first, schema, _curr_data, C_FIRST);
			__attribute__((unused)) LOAD_VALUE(char *, c_middle, schema, _curr_data, C_MIDDLE);
			__attribute__((unused)) LOAD_VALUE(char *, c_street_1, schema, _curr_data, C_STREET_1);
			__attribute__((unused)) LOAD_VALUE(char *, c_street_2, schema, _curr_data, C_STREET_2);
			__attribute__((unused)) LOAD_VALUE(char *, c_city, schema, _curr_data, C_CITY);
			__attribute__((unused)) LOAD_VALUE(char *, c_state, schema, _curr_data, C_STATE);
			__attribute__((unused)) LOAD_VALUE(char *, c_zip, schema, _curr_data, C_ZIP);
			__attribute__((unused)) LOAD_VALUE(char *, c_phone, schema, _curr_data, C_PHONE);
			__attribute__((unused)) LOAD_VALUE(int64_t, c_since, schema, _curr_data, C_SINCE);
			__attribute__((unused)) LOAD_VALUE(char *, c_credit, schema, _curr_data, C_CREDIT);
			__attribute__((unused)) LOAD_VALUE(int64_t, c_credit_lim, schema, _curr_data, C_CREDIT_LIM);
			__attribute__((unused)) LOAD_VALUE(int64_t, c_discount, schema, _curr_data, C_DISCOUNT);
			
			_phase = 0;
			remote_requests.clear();
			return RCOK;
		}
	}
	assert(false);
	return RCOK;
}
	
RC 
TPCCStoreProcedure::execute_new_order()
{
	RC rc = RCOK;
	uint64_t key;
	//itemid_t * item;
	Catalog * schema = NULL;
	char * _curr_data;

	QueryNewOrderTPCC * query = (QueryNewOrderTPCC *) _query;
	WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD; 
	
	uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	Item_no * items = query->items;	

	if (_curr_step == 0) {
		// WAREHOUSE
		//////////////////////////////////////////////////////////////////
		//	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		//	INTO :c_discount, :c_last, :c_credit, :w_tax
		//	FROM customer, warehouse
		//	WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
		//////////////////////////////////////////////////////////////////
		key = w_id;
		schema = wl->t_warehouse->get_schema(); 
		GET_DATA(key, wl->i_warehouse, RD);

		LOAD_VALUE(double, w_tax, schema, _curr_data, W_TAX);
		_w_tax = w_tax;
		_curr_step = 1;
	}
	if (_curr_step == 1) {
		// CUSTOMER
		key = custKey(w_id, d_id, c_id);
		schema = wl->t_customer->get_schema();
		GET_DATA(key, wl->i_customer_id, RD);
		LOAD_VALUE(uint64_t, c_discount, schema, _curr_data, C_DISCOUNT);
		_c_discount = c_discount;
		__attribute__((unused)) LOAD_VALUE(char *, c_last, schema, _curr_data, C_LAST);
		__attribute__((unused)) LOAD_VALUE(char *, c_credit, schema, _curr_data, C_CREDIT);

		_curr_step = 2;
	}

	if (_curr_step == 2) {
		// DISTRICT
		//////////////////////////////////////////////////////////////////
		// 	EXEC SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax 
		// 	FROM district
		// 	WHERE d_id = :d_id AND d_w_id = :w_id;

		// 	EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1 
		// 	WHERE d_id = :d_id AND d_w_id = :w_id;
		//////////////////////////////////////////////////////////////////

		key = distKey(w_id, d_id);
		schema = wl->t_district->get_schema();
		GET_DATA(key, wl->i_district, WR);
		LOAD_VALUE(double, d_tax, schema, _curr_data, D_TAX);
		_d_tax = d_tax;
		LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
		_o_id = o_id;;
		o_id ++;
		STORE_VALUE(o_id, schema, _curr_data, D_NEXT_O_ID);

		_curr_step = 3;
	}
	if (_curr_step == 3) {
		// insert to ORDER 
		/////////////////////////////////////////////////////////////////
		//	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		//	VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
		/////////////////////////////////////////////////////////////////
		key = orderKey(w_id, d_id, _o_id); 
		schema = wl->t_order->get_schema();

		row_t * row = new row_t(wl->t_order);
		
		row->set_value(O_ID, &_o_id);
		row->set_value(O_C_ID, &c_id);
		row->set_value(O_D_ID, &d_id);
		row->set_value(O_W_ID, &w_id);
		row->set_value(O_ENTRY_D, &query->o_entry_d);
		row->set_value(O_OL_CNT, &ol_cnt);
		int64_t all_local = (query->remote? 0 : 1);
		row->set_value(O_ALL_LOCAL, &all_local);
		rc = get_cc_manager()->row_insert(wl->t_order, row);
		_curr_step = 4;
		if (rc != RCOK)
			return rc;
	} 
	if (_curr_step == 4) {
		// insert to NEWORDER
		/////////////////////////////////////////////////////////////////
		//	EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        //	VALUES (:o_id, :d_id, :w_id);
		/////////////////////////////////////////////////////////////////
		key = neworderKey(w_id, d_id);
		schema = wl->t_neworder->get_schema();
		
		row_t * row = new row_t (wl->t_neworder);
		row->set_value(NO_O_ID, &_o_id);
		row->set_value(NO_D_ID, &d_id);
		row->set_value(NO_W_ID, &w_id);
		rc = get_cc_manager()->row_insert(wl->t_neworder, row);
		_curr_step = 5;
		if (rc != RCOK)
			return rc;
	}
	if (_curr_step == 5) {
		// access ITEM tables 
		/*===========================================+
			EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
#if !REPLICATE_ITEM_TABLE
		if (_phase == 0) {
			assert(remote_requests.empty());
			for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
				key = items[_curr_ol_number].ol_i_id;
				if (key == 0) {
					_self_abort = true;
					return ABORT;
				}
				uint32_t node_id = TPCCHelper::item_to_node( key );
				REMOTE_ACCESS(node_id, key, RD, TAB_ITEM, IDX_ITEM);
			}
			_phase = 1;
			_curr_ol_number = 0;
			if (!remote_requests.empty()) 
				return LOCAL_MISS;
		}
		if (_phase == 1) {
			for (; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
                key = items[_curr_ol_number].ol_i_id;
                uint32_t node_id = TPCCHelper::item_to_node( key );
				if (node_id == g_node_id) {
					schema = wl->t_item->get_schema();

					set<row_t *> * rows = NULL; 
					rc = get_cc_manager()->index_read(wl->i_item, key, rows);	
					assert(rc == RCOK && rows);
					_curr_row = *rows->begin();
					rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
					assert(rc == RCOK);
				}
			}
			_phase = 2;
			_curr_ol_number = 0;
			if (!remote_requests.empty())
				return RCOK;
		}
		if (_phase == 2) {
			for (; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
                key = items[_curr_ol_number].ol_i_id;
				schema = wl->t_item->get_schema();
				char * _curr_data = get_cc_manager()->get_data(key, TAB_ITEM);
				__attribute__((unused)) LOAD_VALUE(int64_t, i_price, schema, _curr_data, I_PRICE);
				_i_price[_curr_ol_number] = i_price;
				__attribute__((unused)) LOAD_VALUE(char *, i_name, schema, _curr_data, I_NAME);
				__attribute__((unused)) LOAD_VALUE(char *, i_data, schema, _curr_data, I_DATA);

			}
		}
#else // REPLICATE_ITEM_TABLE
		for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
			key = items[_curr_ol_number].ol_i_id;
			schema = wl->t_item->get_schema();

			set<row_t *> * rows = NULL; 
			rc = get_cc_manager()->index_read(wl->i_item, key, rows);	
			assert(rc == RCOK);
			if (rc == RCOK && !rows) {
				_self_abort = true;
				return ABORT;
			}
			_curr_row = *rows->begin();
			rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);

			__attribute__((unused)) LOAD_VALUE(int64_t, i_price, schema, _curr_data, I_PRICE);
			_i_price[_curr_ol_number] = i_price;
			__attribute__((unused)) LOAD_VALUE(char *, i_name, schema, _curr_data, I_NAME);
			__attribute__((unused)) LOAD_VALUE(char *, i_data, schema, _curr_data, I_DATA);
		}
#endif
		_curr_ol_number = 0;
		_curr_step = 6;
		_phase = 0;
		remote_requests.clear();
	}
	if (_curr_step == 6) {
		// access STOCK table.
		if (_phase == 0) {
			assert(remote_requests.empty());
			for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
				Item_no * it = &items[_curr_ol_number];
				uint32_t node_id = TPCCHelper::wh_to_node( it->ol_supply_w_id);
				key = stockKey(it->ol_supply_w_id, it->ol_i_id);
				REMOTE_ACCESS(node_id, key, WR, TAB_STOCK, IDX_STOCK);
			}
			_phase = 1;
			_curr_ol_number = 0;
			if (!remote_requests.empty()) 
				return LOCAL_MISS;
		}
		if (_phase == 1) {
			for (; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
				Item_no * it = &items[_curr_ol_number];
				uint32_t node_id = TPCCHelper::wh_to_node( it->ol_supply_w_id);
				if (node_id == g_node_id) {
					key = stockKey(it->ol_supply_w_id, it->ol_i_id);
					schema = wl->t_stock->get_schema();
					GET_DATA(key, wl->i_stock, WR);	
				}
			}
			_phase = 2;
			_curr_ol_number = 0;
			if (!remote_requests.empty())
				return RCOK;
		}
		if (_phase == 2) {
			for (; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
				Item_no * it = &items[_curr_ol_number];
				key = stockKey(it->ol_supply_w_id, it->ol_i_id);
				schema = wl->t_stock->get_schema();
				char * _curr_data = get_cc_manager()->get_data(key, TAB_STOCK);
			
				LOAD_VALUE(uint64_t, s_quantity, schema, _curr_data, S_QUANTITY);
				if (s_quantity > it->ol_quantity + 10) 
					s_quantity = s_quantity - it->ol_quantity;
				else 
					s_quantity = s_quantity - it->ol_quantity + 91;
				STORE_VALUE(s_quantity, schema, _curr_data, S_QUANTITY);
				__attribute__((unused)) LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
#if !TPCC_SMALL
				LOAD_VALUE(int64_t, s_ytd, schema, _curr_data, S_YTD);
				s_ytd += it->ol_quantity;
				STORE_VALUE(s_ytd, schema, _curr_data, S_YTD);

				LOAD_VALUE(int64_t, s_order_cnt, schema, _curr_data, S_ORDER_CNT);
				s_order_cnt ++;
				STORE_VALUE(s_order_cnt, schema, _curr_data, S_ORDER_CNT);

				__attribute__((unused)) LOAD_VALUE(char *, s_data, schema, _curr_data, S_DATA);
#endif
				if (it->ol_supply_w_id != w_id) {
					LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
					s_remote_cnt ++;
					STORE_VALUE(s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
				}
			}
		}
		_curr_step = 7;
		_phase = 0;
		_curr_ol_number = 0;
		remote_requests.clear();
	}
	if (_curr_step == 7) {
		key = orderlineKey(w_id, d_id, _o_id);
		for (_ol_num = 0; _ol_num < (int64_t)ol_cnt; _ol_num ++) {
			Item_no * it = &items[_ol_num];
			// all rows (local or remote) are inserted locally. 
			double ol_amount = it->ol_quantity * _i_price[_ol_num] * (1 + _w_tax + _d_tax) * (1 - _c_discount);
			schema = wl->t_orderline->get_schema();
	
			row_t * row = new row_t(wl->t_orderline);
			row->set_value(OL_O_ID, &_o_id);
			row->set_value(OL_D_ID, &d_id);
			row->set_value(OL_W_ID, &w_id);
			row->set_value(OL_NUMBER, &_ol_num);
			row->set_value(OL_I_ID, &it->ol_i_id);
			row->set_value(OL_SUPPLY_W_ID, &it->ol_supply_w_id);
			row->set_value(OL_QUANTITY, &it->ol_quantity);
			row->set_value(OL_AMOUNT, &ol_amount);

			rc = get_cc_manager()->row_insert(wl->t_orderline, row);
			_ol_num ++;
			if (rc != RCOK) return rc;
		}
		return RCOK;
	} 
	assert(_curr_step == 8);
	return RCOK;
}

RC
TPCCStoreProcedure::execute_order_status()
{
	RC rc = RCOK;
	QueryOrderStatusTPCC * query = (QueryOrderStatusTPCC *) _query;
	WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD; 

	uint64_t key;
	Catalog * schema;
	INDEX * index;

	if (_curr_step == 0) {
		// CUSTOMER
		key = (query->by_last_name)? 
			  custNPKey(query->c_last, query->d_id, query->w_id) 
		  	: custKey(query->w_id, query->d_id, query->c_id);
		index = query->by_last_name? wl->i_customer_last : wl->i_customer_id;
		schema = wl->t_customer->get_schema();

		GET_DATA(key, index, RD);
		if (query->by_last_name) {
			LOAD_VALUE(int64_t, c_id, schema, _curr_data, C_ID);
			_c_id = c_id;
		} else {
			_c_id = query->c_id;  
			__attribute__((unused)) LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
			__attribute__((unused)) LOAD_VALUE(char *, c_first, schema, _curr_data, C_FIRST);
			__attribute__((unused)) LOAD_VALUE(char *, c_middle, schema, _curr_data, C_MIDDLE);
			__attribute__((unused)) LOAD_VALUE(char *, c_last, schema, _curr_data, C_LAST);
		}
		_curr_step = 1;
	}
	if (_curr_step == 1) {
		// ORDER
		key = custKey(query->w_id, query->d_id, _c_id);
		index = wl->i_order;
		schema = wl->t_order->get_schema();
		GET_DATA(key, index, RD);
		// TODO. should pick the largest O_ID. 
		// Right now, picking the last one which should also be the largest.
		LOAD_VALUE(int64_t, o_id, schema, _curr_data, O_ID);
		_o_id = o_id;
		__attribute__((unused)) LOAD_VALUE(int64_t, o_entry_d, schema, _curr_data, O_ENTRY_D);
		__attribute__((unused)) LOAD_VALUE(int64_t, o_carrier_id, schema, _curr_data, O_CARRIER_ID);
		_curr_step = 2;
	}
	if (_curr_step == 2) {
		// ORDER-LINE
		key = orderlineKey(query->w_id, query->d_id, _o_id);
		index = wl->i_orderline;
		schema = wl->t_orderline->get_schema();

		set<row_t *> * rows = NULL;
		rc = get_cc_manager()->index_read(index, key, rows);
		if (rc != RCOK) return rc;
		assert(rows->size() <= 15 && rows->size() >= 5);
		for (set<row_t *>::iterator it = rows->begin(); it != rows->end(); it ++) {
			_curr_row = *it;
			rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
			assert(rc == RCOK);
			__attribute__((unused)) LOAD_VALUE(int64_t, ol_i_id, schema, _curr_data, OL_I_ID);
			__attribute__((unused)) LOAD_VALUE(int64_t, ol_supply_w_id, schema, _curr_data, OL_SUPPLY_W_ID);
			__attribute__((unused)) LOAD_VALUE(int64_t, ol_quantity, schema, _curr_data, OL_QUANTITY);
			__attribute__((unused)) LOAD_VALUE(double, ol_amount, schema, _curr_data, OL_AMOUNT);
			__attribute__((unused)) LOAD_VALUE(int64_t, ol_delivery_d, schema, _curr_data, OL_DELIVERY_D);
		}
	}
	return RCOK;
}


RC 
TPCCStoreProcedure::execute_delivery()
{
	RC rc = RCOK;
	QueryDeliveryTPCC * query = (QueryDeliveryTPCC *) _query;
	WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD; 

	uint64_t key;
	Catalog * schema;
	INDEX * index;

	_curr_dist = query->d_id;
	if (_curr_step == 0) {
		// Delete from NEW ORDER table. 
		key = neworderKey(query->w_id, _curr_dist);
		index = wl->i_neworder;
		schema = wl->t_neworder->get_schema();
		set<row_t *> * rows = NULL;

		// TODO. should pick the row with the lower NO_O_ID. need to do a scan here.
		// However, HSTORE just pick one row using LIMIT 1. So we also just pick a row.   
		rc = get_cc_manager()->index_read(index, key, rows, 1);
		if (rc != RCOK) return rc;
		if (!rows)
			return RCOK;
		_curr_row = *rows->begin(); 
		
		rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
		assert(rc == RCOK);
		
		LOAD_VALUE(int64_t, o_id, schema, _curr_data, NO_O_ID);
		_o_id = o_id;
		rc = get_cc_manager()->row_delete( _curr_row );
		_curr_step = 1;
		if (rc != RCOK) return rc;
	}
	if (_curr_step == 1) {
		// access the ORDER table. 
		key = orderKey(query->w_id, _curr_dist, _o_id);	
		index = wl->i_order;
		schema = wl->t_order->get_schema();
		GET_DATA(key, index, WR);
		LOAD_VALUE(int64_t, o_c_id, schema, _curr_data, O_C_ID);
		_c_id = o_c_id;
		STORE_VALUE(query->o_carrier_id, schema, _curr_data, O_CARRIER_ID);
		_curr_step = 2;
	}
	if (_curr_step == 2) {
		// access the ORDER LINE table.
		key = orderlineKey(query->w_id, _curr_dist, _o_id);
		index = wl->i_orderline;
		schema = wl->t_orderline->get_schema();
		set<row_t *> * rows = NULL;
		rc = get_cc_manager()->index_read(index, key, rows);
		if (rc != RCOK) return rc;
		if (rows == NULL)
			return ABORT; 
		for (set<row_t *>::iterator it = rows->begin(); it != rows->end(); it ++) {
			_curr_row = *it; 
			rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
			assert(rc == RCOK);
			LOAD_VALUE(double, ol_amount, schema, _curr_data, OL_AMOUNT);
			_ol_amount += ol_amount;
		}
		_curr_step = 3;
	}
	if (_curr_step == 3) {
		// update CUSTOMER
		key = custKey(query->w_id, _curr_dist, _c_id);
		index = wl->i_customer_id;
		schema = wl->t_customer->get_schema();
		GET_DATA(key, index, WR);
		
		LOAD_VALUE(double, c_balance, schema, _curr_data, C_BALANCE);
		c_balance += _ol_amount;
		STORE_VALUE(c_balance, schema, _curr_data, C_BALANCE);
			
		LOAD_VALUE(int64_t, c_delivery_cnt, schema, _curr_data, C_DELIVERY_CNT);
		c_delivery_cnt ++;
		STORE_VALUE(c_delivery_cnt, schema, _curr_data, C_DELIVERY_CNT);
	}
	_curr_step = 0;
	return RCOK;
}

RC
TPCCStoreProcedure::execute_stock_level()
{
	RC rc = RCOK;
	QueryStockLevelTPCC * query = (QueryStockLevelTPCC *) _query;
	WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD; 

	uint64_t key;
	Catalog * schema;
	INDEX * index;
	if (_curr_step == 0) {
		// Read DISTRICT table
		key = distKey(query->w_id, query->d_id);
		index = wl->i_district;
		schema = wl->t_district->get_schema();
		GET_DATA(key, index, RD);
		
		LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
		_o_id = o_id;
		_curr_ol_number = o_id - 20;
		_curr_step = 1;
	}
	if (_curr_step == 1) {
		// Read ORDER LINE table
		for (; _curr_ol_number < _o_id; _curr_ol_number ++) {
			key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
			index = wl->i_orderline;
			schema = wl->t_orderline->get_schema();
			
			set<row_t *> * rows = NULL;
			rc = get_cc_manager()->index_read(index, key, rows);
			if (rc != RCOK) return rc;
			if (!rows)
				continue; 
			for (set<row_t *>::iterator it = rows->begin(); it != rows->end(); it ++) {
				_curr_row = *it; 
				rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
				assert(rc == RCOK);
				
				LOAD_VALUE(int64_t, i_id, schema, _curr_data, OL_I_ID);
				_items.insert(i_id);
			}
		}
		_item_iter = _items.begin();
		_curr_step = 2;
	}
	return RCOK;
	if (_curr_step == 2) {
		// Read STOCK table
		for (; _item_iter != _items.end(); _item_iter ++) {
			key = stockKey(query->w_id, *_item_iter);
			index = wl->i_stock;
			schema = wl->t_stock->get_schema();
			GET_DATA(key, index, RD);

			__attribute__((unused)) LOAD_VALUE(int64_t, s_quantity, schema, _curr_data, S_QUANTITY);
		}
	}
	return RCOK;
}

void 
TPCCStoreProcedure::txn_abort()
{
	_curr_step = 0;
	_curr_ol_number = 0;

	_curr_dist = 0;
	_ol_amount = 0;
	_ol_num = 0;

	StoreProcedure::txn_abort();
}

#endif
