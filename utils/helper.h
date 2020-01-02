#pragma once

#include <cstdlib>
#include <iostream>
#include <stdint.h>
#include <chrono>
#include "global.h"

extern double g_cpu_freq;

#define BILLION 1000000000UL

// atomic operations
// =================
#define ATOM_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_ADD_FETCH(dest, value) \
    __sync_add_and_fetch(&(dest), value)
#define ATOM_SUB(dest, value) \
    __sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
    __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_SUB_FETCH(dest, value) \
    __sync_sub_and_fetch(&(dest), value)
#define ATOM_COUT(msg) \
    stringstream sstream; sstream << msg; cout << sstream.str();

#define COMPILER_BARRIER asm volatile("" ::: "memory");
// on draco, each pause takes approximately 3.7 ns.
#define PAUSE __asm__ ( "pause;" );
#define PAUSE10 \
    PAUSE PAUSE PAUSE PAUSE PAUSE \
    PAUSE PAUSE PAUSE PAUSE PAUSE

// about 370 ns.
#define PAUSE100 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10 \
    PAUSE10    PAUSE10    PAUSE10    PAUSE10    PAUSE10

#define NANOSLEEP(t) { \
    timespec time {0, t}; \
    nanosleep(&time, NULL); }

// DEBUG print
// ===========
#define DEBUG_PRINT(...) \
    if (false) \
        printf(__VA_ARGS__);

// ASSERT Helper
// =============
#define M_ASSERT(cond, ...) \
    if (!(cond)) {\
        printf("ASSERTION FAILURE [%s : %d] ", __FILE__, __LINE__); \
        printf(__VA_ARGS__);\
        fprintf(stderr, "ASSERTION FAILURE [%s : %d] ", \
        __FILE__, __LINE__); \
        fprintf(stderr, __VA_ARGS__);\
        exit(0); \
    }

#define ASSERT(cond) assert(cond)

// Global Data Structure
// =====================
#define GET_WORKLOAD glob_manager->get_workload()
#define GLOBAL_NODE_ID g_node_id
#define GET_THD_ID glob_manager->get_thd_id()


// STACK helper (push & pop)
// =========================
#define STACK_POP(stack, top) { \
    if (stack == NULL) top = NULL; \
    else {    top = stack;     stack=stack->next; } }
#define STACK_PUSH(stack, entry) {\
    entry->next = stack; stack = entry; }

// LIST helper (read from head & write to tail)
// ============================================
#define LIST_GET_HEAD(lhead, ltail, en) {\
    en = lhead; \
    lhead = lhead->next; \
    if (lhead) lhead->prev = NULL; \
    else ltail = NULL; \
    en->next = NULL; }
#define LIST_PUT_TAIL(lhead, ltail, en) {\
    en->next = NULL; \
    en->prev = NULL; \
    if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
    else { lhead = en; ltail = en; }}
#define LIST_INSERT_BEFORE(entry, newentry) { \
    newentry->next = entry; \
    newentry->prev = entry->prev; \
    if (entry->prev) entry->prev->next = newentry; \
    entry->prev = newentry; }
#define LIST_REMOVE(entry) { \
    if (entry->next) entry->next->prev = entry->prev; \
    if (entry->prev) entry->prev->next = entry->next; }
#define LIST_REMOVE_HT(entry, head, tail) { \
    if (entry->next) entry->next->prev = entry->prev; \
    else { assert(entry == tail); tail = entry->prev; } \
    if (entry->prev) entry->prev->next = entry->next; \
    else { assert(entry == head); head = entry->next; } \
}

// STATS helper
// ============
#define INC_STATS(tid, name, value) \
    ;

#define INC_TMP_STATS(tid, name, value) \
    ;

#define INC_FLOAT_STATS(name, value) { \
    if (STATS_ENABLE) \
        glob_stats->_stats[GET_THD_ID]->_float_stats[STAT_##name] += value; }

#define INC_INT_STATS(name, value) {{ \
    if (STATS_ENABLE) \
        glob_stats->_stats[GET_THD_ID]->_int_stats[STAT_##name] += value; }}

#define STAT_SUM(type, sum, name) \
    type sum = 0; \
    for (int ii = 0; ii < g_total_num_threads; ii++) \
        sum += _stats[ii]->name;

// Malloc helper
// =============
#define MALLOC(size) malloc(size)
#define NEW(name, type, ...) \
    { name = (type *) MALLOC(sizeof(type)); \
      new(name) type(__VA_ARGS__); }

#define FREE(block, size) free(block)
#define DELETE(type, block) { delete block; }


#define ALIGNED(x) __attribute__ ((aligned(x)))

int get_thdid_from_txnid(uint64_t txnid);

//uint64_t key_to_part(uint64_t key);
//uint64_t get_part_id(void * addr);

//uint64_t txn_id_to_node_id(uint64_t txn_id);
//uint64_t txn_id_to_thread_id(uint64_t txn_id);

inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
    ret = (uint64_t) ((double)ret / g_cpu_freq);
#else
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t get_sys_clock() {
#if TIME_ENABLE
    return get_server_clock();
#else
    return 0;
#endif
}

