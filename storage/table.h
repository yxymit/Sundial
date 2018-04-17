#pragma once 

#include "global.h"

// TODO sequential scan is not supported yet.
// only index access is supported for table. 
class Catalog;
class row_t;

class table_t
{
public:
	void init(Catalog * schema);
	// row lookup should be done with index. But index does not have
	// records for new rows. get_new_row returns the pointer to a 
	// new row.	
	RC get_new_row(row_t *& row); // this is equivalent to insert()
	RC get_new_row(row_t *& row, uint64_t part_id);

	void delete_row(); // TODO delete_row is not supportet yet

	uint64_t get_table_size() { return cur_tab_size; };
	Catalog * get_schema() { return schema; };
	const char * get_table_name();

	uint32_t		get_table_id() { return _table_id; }
	void 			set_table_id(uint32_t table_id) { _table_id = table_id; }
	
	INDEX * 		get_index();
	void 			get_indexes(set<INDEX *> * indexes);
	Catalog * 		schema;
#if ENABLE_LOCAL_CACHING
	bool			is_read_intensive() { return 1.0 * _num_writes / _num_reads < 0.015; };
	uint32_t		get_lease() { return _curr_lease; }
#endif
	void 			update_max_rts(uint64_t rts);
	void 			update_max_wts(uint64_t wts);
	uint64_t		get_max_wts() { return _max_wts; }
	uint64_t		get_max_rts() { return _max_rts; }
private:
	const char * 	table_name;
	uint32_t 		_table_id; 
	uint64_t  		cur_tab_size;
#if ENABLE_LOCAL_CACHING
	static __thread uint32_t 	_write_intensity;
	static __thread uint32_t 	_num_writes;
	static __thread uint32_t 	_num_reads;
	volatile uint32_t 			_curr_lease;
	// for readonly tables.
	// The table cannot be written before _min_rts.
	// For a renewal, can increase _min_rts to renew all rows in the tuple.   
	volatile uint64_t 			_min_rts;
#endif
	volatile uint64_t			_max_wts; 
	volatile uint64_t			_max_rts; 
} __attribute__ ((aligned(64)));
