#pragma once

#define DISTRIBUTED                     true
#define NUM_NODES                       2


// number of server threads on each node
#define NUM_WORKER_THREADS              32//64//4096 //2048 //1024
#define NUM_RPC_SERVER_THREADS          1024

// only a limited number of active threads are allowed. This configuration is
// effective only when LOG_ENABLE == true.
#define ENABLE_ADMISSION_CONTROL        true
#define MAX_NUM_ACTIVE_TXNS             20//16

// WORKLOAD can be YCSB or TPCC
#define WORKLOAD                        TPCC

// Statistics
// ==========
// COLLECT_LATENCY: when set to true, will collect transaction latency information
#define COLLECT_LATENCY                 true
#define STATS_ENABLE                    true
#define TIME_ENABLE                     true
#define STATS_CP_INTERVAL               1000 // in ms

// Concurrency Control
// ===================
// Supported concurrency control algorithms: WAIT_DIE, NO_WAIT, TICTOC, F_ONE
#define CC_ALG                          NO_WAIT

#define ABORT_PENALTY                   10000000  // in nanoseconds

// [ INDEX ]
#define INDEX_STRUCT                    IDX_HASH
#define BTREE_ORDER                     16

// [Two Phase Locking]
#define NO_LOCK                         false // NO_LOCK=true : used to model H-Store
// [TIMESTAMP]
#define TS_ALLOC                        TS_CLOCK
#define TS_BATCH_ALLOC                  false
#define TS_BATCH_NUM                    1
// [MVCC]
#define MIN_TS_INTVL                    5000000 //5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET                   10
#define PER_ROW_VALID                   true
// [TICTOC]
#define WRITE_COPY_FORM                 "data" // ptr or data
#define TICTOC_MV                       false
#define WR_VALIDATION_SEPARATE          true
#define WRITE_PERMISSION_LOCK           false
#define ATOMIC_TIMESTAMP                "false"

// when WAW_LOCK is true, lock a tuple before write.
// essentially, WW conflicts are handled as 2PL.
#define OCC_WAW_LOCK                    true
// if SKIP_READONLY_PREPARE is true, then a readonly subtxn will forget
// about its states after returning. If no renewal is required, this remote
// node will not participate in the 2PC protocol.
#define SKIP_READONLY_PREPARE           false
#define MAX_NUM_WAITS                   4
#define READ_INTENSITY_THRESH           0.8

// [Caching in TicToc]
#define ENABLE_LOCAL_CACHING            false
#define CACHING_POLICY                  ALWAYS_CHECK
#define RO_LEASE                        false
#define LOCAL_CACHE_SIZE                (1024*1024) // in KB
#define REUSE_FRESH_DATA                false
#define REUSE_IF_NO_REMOTE              false

#define LOCK_ALL_BEFORE_COMMIT          false
#define LOCK_ALL_DEBUG                  false
#define TRACK_LAST                      false
#define LOCK_TRIAL                      3
#define MULTI_VERSION                   false

// [TICTOC, SILO]
#define OCC_LOCK_TYPE                   WAIT_DIE
#define PRE_ABORT                       true
#define ATOMIC_WORD                     false
#define UPDATE_TABLE_TS                 true

// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions.
#define HSTORE_LOCAL_TS                 false

// Logging
// =======
#define LOG_ENABLE                      true
#define CONTROLLED_LOCK_VIOLATION       true

// Benchmark
// =========
// max number of rows touched per transaction
#define RUN_TIME                        60 // in second
#define MAX_TUPLE_SIZE                  1024 // in bytes
#define INIT_PARALLELISM                8

// [YCSB]
// Number of tuples per node
#define SYNTH_TABLE_SIZE                (1024 * 10) // * 1024)
#define ZIPF_THETA                      0.6
#define READ_PERC                       0.9
#define PERC_READONLY_DATA              0
#define PERC_REMOTE                     0
#define SINGLE_PART_ONLY                false // access single partition only
#define REQ_PER_QUERY                   16
#define THINK_TIME                      0  // in us
#define SOCIAL_NETWORK                  false
// KEY_ORDER: when set to true, each transaction accesses tuples in the primary key order.
#define SORT_KEY_ORDER                  false


// [TPCC]
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL                      false
#define NUM_WH                          16

// In the current implementation, the standard delivery transaction is modeled
// as 10 seperate transactions (one for each district). All the percentage
// numbers are accordingly adjusted.
#define PERC_PAYMENT                    0.316
#define PERC_NEWORDER                   0.331
#define PERC_ORDERSTATUS                0.029
#define PERC_DELIVERY                   0.294
#define PERC_STOCKLEVEL                 0.03

#define PAYMENT_REMOTE_PERC             15 // 15% customers are remote
#define NEW_ORDER_REMOTE_PERC           1  // 1% order lines are remote
#define FIRSTNAME_MINLEN                8
#define FIRSTNAME_LEN                   16
#define LASTNAME_LEN                    16
#define DIST_PER_WARE                   10

// TODO centralized CC management
// ==============================
#define MAX_LOCK_CNT                    (20 * THREAD_CNT)
#define TSTAB_SIZE                      50 * THREAD_CNT
#define TSTAB_FREE                      TSTAB_SIZE
#define TSREQ_FREE                      4 * TSTAB_FREE
#define MVHIS_FREE                      4 * TSTAB_FREE
#define SPIN                            false

// Distributed DBMS
// ================
#define ASYNC_RPC                       true
#define START_PORT                      35777
#define INOUT_QUEUE_SIZE                1024
#define NUM_INPUT_THREADS               1
#define NUM_OUTPUT_THREADS              1
#define ENABLE_MSG_BUFFER               false
#define MAX_MESSAGE_SIZE                16384
#define RECV_BUFFER_SIZE                32768
#define SEND_BUFFER_SIZE                32768

#define MAX_CLOCK_SKEW                  0 // in us

// Constant
// ========
// index structure
#define IDX_HASH                        1
#define IDX_BTREE                       2
// WORKLOAD
#define YCSB                            1
#define TPCC                            2
// Concurrency Control Algorithm
#define NO_WAIT                         1
#define WAIT_DIE                        2
#define F_ONE                           3
#define MAAT                            4
#define IDEAL_MVCC                      5
#define NAIVE_TICTOC                    6
#define TICTOC                          7
#define TCM                             8
// TIMESTAMP allocation method.
#define TS_MUTEX                        1
#define TS_CAS                          2
#define TS_HW                           3
#define TS_CLOCK                        4
// Caching policy
#define ALWAYS_READ                     1    // always read cached data
#define ALWAYS_CHECK                    2    // always contact remote node
#define READ_INTENSIVE                  3    // only read cached data that is read-intensive
