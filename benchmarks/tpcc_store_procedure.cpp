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
    //assert(!_txn->is_sub_txn());
    switch (query->type) {
        case TPCC_PAYMENT:          { return execute_payment(); }
        case TPCC_NEW_ORDER:        { return execute_new_order(); }
        case TPCC_ORDER_STATUS:     { return execute_order_status(); }
        case TPCC_DELIVERY:         { return execute_delivery(); }
        case TPCC_STOCK_LEVEL:      { return execute_stock_level(); }
        default:                    assert(false); return RCOK;
    }
}

// TODO. need to support waiting for locks.
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

    // Step 1: access WAREHOUSE
    // ========================
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

    // Step 2: access DISTRICT
    // =======================
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

    // Step 3: access CUSTOMER
    // =======================
    uint32_t node_id = TPCCHelper::wh_to_node(c_w_id);
    key = (query->by_last_name)?
        custNPKey(query->c_last, query->c_d_id, query->c_w_id)
        : custKey(query->c_w_id, query->c_d_id, query->c_id);
    index = query->by_last_name? wl->i_customer_last : wl->i_customer_id;
    schema = wl->t_customer->get_schema();
    if (node_id == g_node_id) {
        GET_DATA(key, index, WR);
    } else {
        uint32_t index_id = (query->by_last_name)? IDX_CUSTOMER_LAST : IDX_CUSTOMER_ID;
        rc = _txn->send_remote_read_request(node_id, key, index_id, TAB_CUSTOMER, WR);
        if (rc == ABORT) return rc;
    }

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

    return COMMIT;
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

    // Step 1: access WAREHOUSE
    // ========================
    //    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
    //    INTO :c_discount, :c_last, :c_credit, :w_tax
    //    FROM customer, warehouse
    //    WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
    key = w_id;
    schema = wl->t_warehouse->get_schema();
    GET_DATA(key, wl->i_warehouse, RD);
    LOAD_VALUE(double, w_tax, schema, _curr_data, W_TAX);
    _w_tax = w_tax;

    // Step 2: accesss CUSTOMER
    // ========================
    key = custKey(w_id, d_id, c_id);
    schema = wl->t_customer->get_schema();
    GET_DATA(key, wl->i_customer_id, RD);
    LOAD_VALUE(uint64_t, c_discount, schema, _curr_data, C_DISCOUNT);
    _c_discount = c_discount;
    __attribute__((unused)) LOAD_VALUE(char *, c_last, schema, _curr_data, C_LAST);
    __attribute__((unused)) LOAD_VALUE(char *, c_credit, schema, _curr_data, C_CREDIT);

    // Step 3: access DISTRICT
    // =======================
    //     EXEC SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
    //     FROM district
    //     WHERE d_id = :d_id AND d_w_id = :w_id;
    //     EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
    //     WHERE d_id = :d_id AND d_w_id = :w_id;

    key = distKey(w_id, d_id);
    schema = wl->t_district->get_schema();
    GET_DATA(key, wl->i_district, WR);
    LOAD_VALUE(double, d_tax, schema, _curr_data, D_TAX);
    _d_tax = d_tax;
    LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
    _o_id = o_id;;
    o_id ++;
    STORE_VALUE(o_id, schema, _curr_data, D_NEXT_O_ID);

    // Step 4: insert to ORDER
    // =======================
    //    EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
    //    VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
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
    if (rc != RCOK)
        return rc;

    // Step 5: insert to NEWORDER
    // ==========================
    //    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
    //    VALUES (:o_id, :d_id, :w_id);
    key = neworderKey(w_id, d_id);
    schema = wl->t_neworder->get_schema();

    row = new row_t (wl->t_neworder);
    row->set_value(NO_O_ID, &_o_id);
    row->set_value(NO_D_ID, &d_id);
    row->set_value(NO_W_ID, &w_id);
    rc = get_cc_manager()->row_insert(wl->t_neworder, row);
    if (rc != RCOK)
        return rc;

    // Step 6: access ITEM
    // ===================
    //    EXEC SQL SELECT i_price, i_name , i_data
    //    INTO :i_price, :i_name, :i_data
    //    FROM item
    //    WHERE i_id = :ol_i_id;
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
    // Step 7: access STOCK
    // ====================
    for (_curr_ol_number = 0; _curr_ol_number < ol_cnt; _curr_ol_number ++) {
        Item_no * it = &items[_curr_ol_number];
        key = stockKey(it->ol_supply_w_id, it->ol_i_id);

        // TODO. if any access is remote, the current thread blocks until
        // the response come back. This can be implemented using asynchronous
        // RPC. E.g., make multiple RPC calls and then wait for them one by one.
        schema = wl->t_stock->get_schema();
        uint32_t node_id = TPCCHelper::wh_to_node( it->ol_supply_w_id);
        if (node_id == g_node_id) {
            GET_DATA(key, wl->i_stock, WR);
        } else {
            rc = _txn->send_remote_read_request(node_id, key, IDX_STOCK, TAB_STOCK, WR);
            if (rc == ABORT) return rc;
        }

        char * _curr_data = get_cc_manager()->get_data(key, TAB_STOCK);

        LOAD_VALUE(uint64_t, s_quantity, schema, _curr_data, S_QUANTITY);
        if (s_quantity > it->ol_quantity + 10)
            s_quantity = s_quantity - it->ol_quantity;
        else
            s_quantity = s_quantity - it->ol_quantity + 91;
        STORE_VALUE(s_quantity, schema, _curr_data, S_QUANTITY);
        __attribute__((unused)) LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);

        LOAD_VALUE(int64_t, s_ytd, schema, _curr_data, S_YTD);
        s_ytd += it->ol_quantity;
        STORE_VALUE(s_ytd, schema, _curr_data, S_YTD);

        LOAD_VALUE(int64_t, s_order_cnt, schema, _curr_data, S_ORDER_CNT);
        s_order_cnt ++;
        STORE_VALUE(s_order_cnt, schema, _curr_data, S_ORDER_CNT);

        __attribute__((unused)) LOAD_VALUE(char *, s_data, schema, _curr_data, S_DATA);

        if (it->ol_supply_w_id != w_id) {
            LOAD_VALUE(int64_t, s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
            s_remote_cnt ++;
            STORE_VALUE(s_remote_cnt, schema, _curr_data, S_REMOTE_CNT);
        }
    }
    // Step 8: insert into ORDERLINE
    // =============================
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
    return COMMIT;
}

// TODO. this is a read-only transaction. With multi-version storage, the logic
// can be simplified.
RC
TPCCStoreProcedure::execute_order_status()
{
    RC rc = RCOK;
    QueryOrderStatusTPCC * query = (QueryOrderStatusTPCC *) _query;
    WorkloadTPCC * wl = (WorkloadTPCC *) GET_WORKLOAD;

    uint64_t key;
    Catalog * schema;
    INDEX * index;

    // Step 1: access CUSTOMER
    // =======================
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

    // Step 2: access ORDER
    // ====================
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

    // Step 3: access ORDERLINE
    // ========================
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
    return COMMIT;
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
    // Step 1: Delete from NEWORDER
    // ============================
    //cout << "step 1" << endl;
    key = neworderKey(query->w_id, _curr_dist);
    index = wl->i_neworder;
    schema = wl->t_neworder->get_schema();
    set<row_t *> * rows = NULL;
    // TODO. should pick the row with the lower NO_O_ID. need to do a scan here.
    // However, HSTORE just pick one row using LIMIT 1. So we also just pick a row.
    rc = get_cc_manager()->index_read(index, key, rows, 1);
    if (rc != RCOK) return rc;
    if (!rows)
        return COMMIT;
    _curr_row = *rows->begin();

    rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
    assert(rc == RCOK);

    LOAD_VALUE(int64_t, o_id, schema, _curr_data, NO_O_ID);
    _o_id = o_id;
    rc = get_cc_manager()->row_delete( _curr_row );
    if (rc != RCOK) return rc;

    // Step 2: access ORDER
    // ====================
    //cout << "step 2" << endl;
    key = orderKey(query->w_id, _curr_dist, _o_id);
    index = wl->i_order;
    schema = wl->t_order->get_schema();
    GET_DATA(key, index, WR);
    LOAD_VALUE(int64_t, o_c_id, schema, _curr_data, O_C_ID);
    _c_id = o_c_id;
    STORE_VALUE(query->o_carrier_id, schema, _curr_data, O_CARRIER_ID);

    // Step 3: access ORDERLINE
    // ========================
    //cout << "step 3" << endl;
    key = orderlineKey(query->w_id, _curr_dist, _o_id);
    index = wl->i_orderline;
    schema = wl->t_orderline->get_schema();
    rows = NULL;
    rc = get_cc_manager()->index_read(index, key, rows);
    if (rc != RCOK) return rc;
    // TODO: how can rows be NULL? The order is found but there are no
    // orderline?
    assert(rows);
    //if (rows == NULL)
    //    return ABORT;
    for (set<row_t *>::iterator it = rows->begin(); it != rows->end(); it ++) {
        _curr_row = *it;
        rc = get_cc_manager()->get_row(_curr_row, RD, _curr_data, key);
        assert(rc == RCOK);
        LOAD_VALUE(double, ol_amount, schema, _curr_data, OL_AMOUNT);
        _ol_amount += ol_amount;
    }

    // Step 4: update CUSTOMER
    // =======================
    //cout << "step 4" << endl;
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
    return COMMIT;
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

    // Step 1: Read DISTRICT
    // =====================
    key = distKey(query->w_id, query->d_id);
    index = wl->i_district;
    schema = wl->t_district->get_schema();
    GET_DATA(key, index, RD);

    LOAD_VALUE(int64_t, o_id, schema, _curr_data, D_NEXT_O_ID);
    _o_id = o_id;
    _curr_ol_number = o_id - 20;

    // Step 2: Read ORDERLINE
    // ======================
    for (; _curr_ol_number < _o_id; _curr_ol_number ++) {
        key = orderlineKey(query->w_id, query->d_id, _curr_ol_number);
        index = wl->i_orderline;
        schema = wl->t_orderline->get_schema();

        set<row_t *> * rows = NULL;
        rc = get_cc_manager()->index_read(index, key, rows);
        if (rc != RCOK) return rc;
        // Again, how can rows be NULL?
        assert(rows);
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

    // Step 3: Read STOCK
    // ==================
    for (_item_iter = _items.begin(); _item_iter != _items.end(); _item_iter ++) {
        key = stockKey(query->w_id, *_item_iter);
        index = wl->i_stock;
        schema = wl->t_stock->get_schema();
        GET_DATA(key, index, RD);

        __attribute__((unused)) LOAD_VALUE(int64_t, s_quantity, schema, _curr_data, S_QUANTITY);
    }
    return COMMIT;
}

void
TPCCStoreProcedure::txn_abort()
{
    _curr_ol_number = 0;

    _curr_dist = 0;
    _ol_amount = 0;
    _ol_num = 0;

    StoreProcedure::txn_abort();
}

#endif
