#pragma once 

#include "global.h"

#if CC_ALG == IDEAL_MVCC

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else 

#define LOCK_BIT (1UL << 63)

#endif

class TxnManager;
class CCManager;
class MVCCManager;
class row_t;

class Row_MVCC {
public:
	Row_MVCC();
	Row_MVCC(row_t * row) : Row_MVCC()
	{ _row = row; }

	RC 					read(TxnManager * txn, char * data, 
							 uint64_t &wts, uint64_t &rts, uint64_t &cts, bool latch = true); 
	RC					write(TxnManager * txn, uint64_t &wts, uint64_t &rts, uint64_t &cts, bool latch = true);
	RC 					update(char * data, uint64_t wts, uint64_t rts);

	void 				latch();
	void 				unlatch();

	void 				lock();
	bool  				try_lock();
	RC  				try_lock(TxnManager * txn);
	void 				release(TxnManager * txn, RC rc);

#if SPECULATE
	RC					write_speculate(row_t * data, ts_t version, bool spec_read); 
#endif
	void				write_data(char * data, ts_t wts);
	void 				update_ts(uint64_t cts);
	void				write_ptr(row_t * data, ts_t wts, char *& data_to_free);
	bool 				try_renew(ts_t rts);
	bool 				try_renew(ts_t wts, ts_t rts, ts_t &new_rts);
	bool 				renew(ts_t wts, ts_t rts, ts_t &new_rts);

	uint64_t 			get_wts();
	uint64_t 			get_rts();
	void 				get_ts(uint64_t &wts, uint64_t &rts);
	void				set_ts(uint64_t wts, uint64_t rts); 
#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK
  	TxnManager *		_lock_owner; 
	#define MAN(txn) ((MVCCManager *) (txn)->get_cc_manager())
	struct CompareWait {
		bool operator() (TxnManager * en1, TxnManager * en2) const;
	};
	std::set<TxnManager *, CompareWait> _waiting_set; 
	uint32_t _max_num_waits;
#endif
	
	bool 				is_deleted() { return _deleted; }
	void 				delete_row(uint64_t del_ts);
	enum State {
		SHARED,
		EXCLUSIVE
	};
	State 				_state;
	uint32_t			_owner; // host node ID

	row_t * 			_row;
	private:
	uint64_t			_wts; // last write timestamp
	uint64_t			_rts; // end lease timestamp
	uint64_t	   		_pending_cts;

	// TODO _wr_latch can be removed using the atomic word trick. 
	pthread_mutex_t * 	_latch;   	 // to guarantee read/write consistency
	bool 				_blatch;   	 // to guarantee read/write consistency
	bool				_ts_lock;	 // wts/rts cannot be changed if _ts_lock is true.

	bool				_deleted;
	uint64_t			_delete_timestamp;
	// for locality predictor
	uint32_t 			_num_remote_reads; // should cache a local copy if this number is too large.
} __attribute__ ((aligned(64)));

#endif
