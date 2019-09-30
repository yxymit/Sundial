#include "row_f1.h"
#include "row.h"
#include "txn.h"
#include <mm_malloc.h>

#if CC_ALG==F_ONE

RC
Row_f1::read(TxnManager * txn, char * data, uint64_t &wts, bool latch)
{
    if (latch)
        pthread_mutex_lock( &_latch );
    wts = _wts;
    if (data)
        memcpy(data, _row->get_data(), _row->get_tuple_size());
    if (latch)
        pthread_mutex_unlock( &_latch );
    return RCOK;
}

void
Row_f1::write_data(char * data, ts_t wts)
{
    _wts = wts;
    _row->copy(data);
}

#endif
