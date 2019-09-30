#pragma once

#include "global.h"
#include "row_lock.h"

#if CC_ALG == F_ONE

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 63)
#define RTS_LEN (15)
#define WTS_LEN (63 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#endif

class TxnManager;
class row_t;

class Row_f1 : public Row_lock {
public:
    Row_f1() : Row_lock() { _wts = 0; }
    Row_f1(row_t * row) : Row_lock(row) { _wts = 0; }
    RC                     read(TxnManager * txn, char * data, uint64_t &wts, bool latch = true);

    void                write_data(char * data, ts_t wts);

    uint64_t            get_wts() { return _wts; }
    void                 update_ts() { _wts++; }
private:
    uint64_t            _wts;
};

#endif
