#include "tpcc_helper.h"
#include "manager.h"
#include "tpcc_const.h"

#if WORKLOAD == TPCC

uint64_t distKey(uint64_t w_id, uint64_t d_id)
{
    uint64_t num_wh = g_num_wh * g_num_nodes;
    return d_id * num_wh + w_id;
}

uint64_t custKey(uint64_t w_id, uint64_t d_id, uint64_t c_id)
{
    uint64_t num_wh = g_num_wh * g_num_nodes;
    return (c_id * DIST_PER_WARE + d_id) * num_wh + w_id;
}

uint64_t orderKey(uint64_t w_id, uint64_t d_id, uint64_t o_id)
{
    uint64_t num_wh = g_num_wh * g_num_nodes;
    return (o_id * DIST_PER_WARE + d_id) * num_wh + w_id;
}

uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id)
{
    return orderKey(w_id, d_id, o_id);
}

uint64_t orderlinePrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t olnum)
{
    assert(olnum < 16);
    return orderlineKey(w_id, d_id, o_id) * 16 + olnum;
}

uint64_t neworderKey(uint64_t w_id, uint64_t d_id)
{
    return distKey(w_id, d_id);
}

uint64_t stockKey(uint64_t s_w_id, uint64_t s_i_id) {
    uint64_t num_wh = g_num_wh * g_num_nodes;
    return s_i_id * num_wh + s_w_id;
}

uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id) {
    uint64_t key = 0;
    char offset = 'A';
    for (uint32_t i = 0; i < strlen(c_last); i++)
        key = (key << 2) + (c_last[i] - offset);
    key = key << 3;
    key += c_w_id * DIST_PER_WARE + c_d_id;
    return key;
}



uint64_t Lastname(uint64_t num, char* name) {
      static const char *n[] =
        {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
         "ESE", "ANTI", "CALLY", "ATION", "EING"};
      strcpy(name, n[num / 100]);
      strcat(name, n[(num / 10) % 10]);
      strcat(name, n[num % 10]);
      return strlen(name);
}

uint64_t RAND(uint64_t max) {
    return glob_manager->rand_uint64() % max;
}

uint64_t URand(uint64_t x, uint64_t y) {
    return x + RAND(y - x + 1);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y) {
  static bool C_255_init = false;
  static bool C_1023_init = false;
  static bool C_8191_init = false;
  static uint64_t C_255, C_1023, C_8191;
  int C = 0;
  switch(A) {
    case 255:
      if(!C_255_init) {
        C_255 = (uint64_t) URand(0,255);
        C_255_init = true;
      }
      C = C_255;
      break;
    case 1023:
      if(!C_1023_init) {
        C_1023 = (uint64_t) URand(0,1023);
        C_1023_init = true;
      }
      C = C_1023;
      break;
    case 8191:
      if(!C_8191_init) {
        C_8191 = (uint64_t) URand(0,8191);
        C_8191_init = true;
      }
      C = C_8191;
      break;
    default:
      M_ASSERT(false, "Error! NURand\n");
      exit(-1);
  }
  return(((URand(0,A) | URand(x,y))+C)%(y-x+1))+x;
}

uint64_t MakeAlphaString(int min, int max, char* str) {
    char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
    uint64_t cnt = URand(min, max);
    for (uint32_t i = 0; i < cnt; i++)
        str[i] = char_list[URand(0L, 60L)];
    for (int i = cnt; i < max; i++)
        str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str) {

  uint64_t cnt = URand(min, max);
  for (uint32_t i = 0; i < cnt; i++) {
    uint64_t r = URand(0L,9L);
    str[i] = '0' + r;
  }
  return cnt;
}

uint32_t
TPCCHelper::wh_to_node(uint32_t wh_id)
{
    return (wh_id - 1) / g_num_wh;
}

const char *
TPCCHelper::get_txn_name(uint32_t txn_type_id)
{
    switch(txn_type_id) {
        case TPCC_PAYMENT:
            return "Payment";
        case TPCC_NEW_ORDER:
            return "New Order";
        case TPCC_ORDER_STATUS:
            return "Order Status";
        case TPCC_DELIVERY:
            return "Delivery";
        case TPCC_STOCK_LEVEL:
            return "Stock Level";
        default: {
            assert(false);
            return NULL;
        }
    }
}

#endif
