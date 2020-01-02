#pragma once
#include "global.h"
#include "helper.h"

#if WORKLOAD == TPCC

uint64_t distKey(uint64_t w_id, uint64_t d_id);
uint64_t custKey(uint64_t w_id, uint64_t d_id, uint64_t c_id);
uint64_t orderKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t orderlinePrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t olnum);
uint64_t neworderKey(uint64_t w_id, uint64_t d_id);
uint64_t stockKey(uint64_t s_w_id, uint64_t s_i_id);

// non-primary key
uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id);


uint64_t Lastname(uint64_t num, char* name);
// return random data from [0, max-1]
uint64_t RAND(uint64_t max);
// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y);
// non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y);
// random string with random length beteen min and max.
uint64_t MakeAlphaString(int min, int max, char * str);
uint64_t MakeNumberString(int min, int max, char* str);

class TPCCHelper {
public:
    static uint32_t wh_to_node(uint32_t wh_id);
    static const char * get_txn_name(uint32_t txn_type_id);
};

#endif
