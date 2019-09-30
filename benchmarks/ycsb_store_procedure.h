#pragma once

#include "store_procedure.h"
class row_t;

class YCSBStoreProcedure : public StoreProcedure
{
public:
    YCSBStoreProcedure(TxnManager * txn_man, QueryBase * query);
    ~YCSBStoreProcedure();

    RC execute();
    RC process_remote_req(uint32_t size, char * data, uint32_t &resp_size, char * &resp_data);

    void txn_abort();
};
