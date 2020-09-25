#include "global.h"
#include "helper.h"
#include "manager.h"
#include "row_tictoc.h"

void print_usage() {
    printf("[usage]:\n");

    printf("\t-SINT       ; NUM_WORKER_THREADS\n");
    printf("\t-mINT       ; MULTI_VERSION (if turned on)\n");
    printf("\t-GaINT      ; ABORT_PENALTY (in us)\n");
    printf("\t-GtINT      ; TS_ALLOC\n");
    printf("\t-GkINT      ; SORT_KEY_ORDER\n");
    printf("\t-GTFLOAT    ; RUN_TIME\n");
    printf("\t-GbINT      ; TS_BATCH_ALLOC\n");
    printf("\t-GuINT      ; TS_BATCH_NUM\n");
    printf("\t-GsINT      ; MAX_CLOCK_SKEW\n");
    printf("\t-o STRING   ; output file\n\n");
    printf("Benchmarks\n");
    printf("[YCSB]:\n");
    printf("\t-rFLOAT     ; READ_PERC\n");
    printf("\t-zFLOAT     ; ZIPF_THETA\n");
    printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
    printf("\t-RINT       ; REQ_PER_QUERY\n");
    printf("\t-YrFLOAT    ; PERC_REMOTE\n");
    printf("\t-YoFLOAT    ; PERC_READONLY_DATA\n");
    printf("[TPCC]:\n");
    printf("\t-nINT       ; NUM_WH\n");
    printf("\t-TrINT      ; PAYMENT_REMOTE_PERC\n");
    printf("\t-ToINT      ; NEW_ORDER_REMOTE_PERC\n");
    printf("\t-TpFLOAT    ; PERC_PAYMENT\n");
    printf("\t-TnFLOAT    ; PERC_NEWORDER\n");
    printf("\t-TsFLOAT    ; PERC_ORDERSTATUS\n");
    printf("\t-TdFLOAT    ; PERC_DELIVERY\n");
    printf("Concurrency Control Algorithms\n");
    printf("[TICTOC]:\n");
    printf("\t-CwINT      ; MAX_NUM_WAITS\n");
    printf("\t-CrINT      ; READ_INTENSITY_THRESH\n");
    printf("[Distributed DBMS]:\n");
    printf("\t-DxINT      ; MAX_NUM_ACTIVE_TXNS\n");
    printf("\t-DiINT      ; NUM_INPUT_THREADS (NUM_OUTPUT_THREADS)\n");
    printf("\t-Df STRING  ; ifconfig file\n");
    printf("\t-DcINT      ; LOCAL_CACHE_SIZE\n");
    printf("\n");
}

void parser(int argc, char * argv[]) {
    if (CONTROLLED_LOCK_VIOLATION) static_assert(LOG_ENABLE);
    //if (DISTRIBUTED) assert(NUM_NODES > 1);
    M_ASSERT(INDEX_STRUCT != IDX_BTREE, "btree is not supported yet\n");
    // The current admission control is designed for logging
    if (ENABLE_ADMISSION_CONTROL) static_assert( LOG_ENABLE );
    for (int i = 1; i < argc; i++) {
        assert(argv[i][0] == '-');
        if (argv[i][1] == 'm')
#if MULTI_VERSION && CC_ALG==TICTOC
            Row_tictoc::_history_num = atoi( &argv[i][2]);
#else
            printf("Ignore option -m\n");
#endif
        else if (argv[i][1] == 'r')
            g_read_perc = atof( &argv[i][2] );
        else if (argv[i][1] == 'z')
            g_zipf_theta = atof( &argv[i][2] );
        else if (argv[i][1] == 'S')
            g_num_worker_threads = atoi( &argv[i][2] );
        else if (argv[i][1] == 's')
            g_synth_table_size = atoi( &argv[i][2] );
        else if (argv[i][1] == 'R')
            g_req_per_query = atoi( &argv[i][2] );
        else if (argv[i][1] == 'n')
            g_num_wh = atoi( &argv[i][2] );
        else if (argv[i][1] == 'G') {
            // Global
            if (argv[i][2] == 'a')
                g_abort_penalty = atoi( &argv[i][3] );
            else if (argv[i][2] == 't')
                g_ts_alloc = atoi( &argv[i][3] );
            else if (argv[i][2] == 'k')
                g_sort_key_order = atoi( &argv[i][3] );
            else if (argv[i][2] == 'b')
                g_ts_batch_alloc = atoi( &argv[i][3] );
            else if (argv[i][2] == 'u')
                g_ts_batch_num = atoi( &argv[i][3] );
            else if (argv[i][2] == 's')
                g_max_clock_skew = atoi( &argv[i][3] );
            else if (argv[i][2] == 'T')
                g_run_time = atof( &argv[i][3] );
            else if (argv[i][2] == 'N')
                g_node_id= atoi( &argv[i][3] );    
            else
                exit(0);
        } else if (argv[i][1] == 'Y') {
            if (argv[i][2] == 'r')
                g_perc_remote = atof( &argv[i][3] );
            else if (argv[i][2] == 'o')
                g_readonly_perc = atof( &argv[i][3] );
        } else if (argv[i][1] == 'T') {
            // TPCC
            if (argv[i][2] == 'r')
                g_payment_remote_perc = atoi( &argv[i][3] );
            else if (argv[i][2] == 'o')
                g_new_order_remote_perc = atoi( &argv[i][3] );
            else if (argv[i][2] == 'p')
                g_perc_payment = atof( &argv[i][3] );
            else if (argv[i][2] == 'n')
                g_perc_new_order = atof( &argv[i][3] );
            else if (argv[i][2] == 's')
                g_perc_order_status = atof( &argv[i][3] );
            else if (argv[i][2] == 'd')
                g_perc_delivery = atof( &argv[i][3] );
            else
                exit(0);
        } else if (argv[i][1] == 'C') {
            // TICTOC
            if (argv[i][2] == 'w')
                g_max_num_waits = atoi( &argv[i][3] );
            else if (argv[i][2] == 'r')
                g_read_intensity_thresh = atof( &argv[i][3] );
            else assert(false);
        } else if (argv[i][1] == 'D') {
            if (argv[i][2] == 'x')
                g_max_num_active_txns = atoi( &argv[i][3] );
            else if (argv[i][2] == 'i') {
                g_num_input_threads = atoi( &argv[i][3] );
                g_num_output_threads = g_num_input_threads;
            } else if (argv[i][2] == 'f')
                strcpy( ifconfig_file, argv[++i]);
            else if (argv[i][2] == 'c')
                g_local_cache_size = atoi( &argv[i][3] );
            else
              exit(0);
        } else if (argv[i][1] == 'o') {
            i++;
            output_file = argv[i];
        } else if (argv[i][1] == 'h') {
            print_usage();
            exit(0);
        }
        else
            exit(0);
    }
    if (g_num_worker_threads < g_init_parallelism)
        g_init_parallelism = g_num_worker_threads;
    if (ENABLE_ADMISSION_CONTROL)
        assert( g_max_num_active_txns <= g_num_worker_threads );
}
