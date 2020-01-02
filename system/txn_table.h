#pragma once

#include "global.h"

// For Distributed DBMS

class TxnManager;

// States of all active transactions are maintained in the per-node TxnTable.
class TxnTable
{
public:
    TxnTable();
    // TODO. right now, get_txn removes the txn from the table.
    // should support 3 methods: add_txn, get_txn, remove_txn
    void add_txn(TxnManager * txn);
    void remove_txn(TxnManager * txn);
    void print_txn();

    TxnManager * get_txn(uint64_t txn_id);
    void remove_txn(uint64_t txn_id);
    uint32_t get_size();
private:
    struct Node {
        TxnManager * txn;
        Node * next;
    };
    struct Bucket {
        Node * first;
        volatile bool latch;
    };

    Bucket ** _buckets;
    uint32_t _txn_table_size;
};
