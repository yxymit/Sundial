#pragma once

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <iomanip>
#include <cassert>

#define length 6
#define tableSize 10
#define readPerc 90
#define numTxns 10

#define MVCC false //true

using std::min;
using std::max;
using std::cout;
using std::endl;
using std::setw;

struct Access {
  int key;
  bool isWrite;
};

struct Txn {
  void sort_by_key() {
    for (int i = 0; i < length; i++) {
      for (int j = i + 1; j < length; j ++) {
        Access &acc1 = accesses[i];
        Access &acc2 = accesses[j];
        if (acc1.key > acc2.key) {
          Access temp = acc1;
          acc1 = acc2;
          acc2 = temp;
        }
      }
    }
  }
  Access accesses[length];
};

class Workload {
public:
  Workload() {
    // TODO supports only YCSB transactions.
    for (int i = 0; i < numTxns; i++) {
      for (int a = 0; a < length; a ++) {
        bool duplicate;
        do {
          duplicate = false;
          txns[i].accesses[a].key = rand() % tableSize;
          txns[i].accesses[a].isWrite = (rand() % 100 >= readPerc);
          for (int k = 0; k < a; k ++) {
            if (txns[i].accesses[a].key == txns[i].accesses[k].key)
              duplicate = true;
          }
        } while(duplicate);
      }
    }
  }
  Workload(Workload * other) {
    memcpy(txns, other->txns, sizeof(Txn) * numTxns);
  }
  void sort_by_key() {
    for (int i = 0; i < numTxns; i++)
      txns[i].sort_by_key();
  }
  void print() {
    for (int a = 0; a < length; a++)
      cout << "txns[0].accesses[" << a << "] = ("
           << txns[0].accesses[a].key << ", " << txns[0].accesses[a].isWrite << ")" << endl;
  }
  Txn txns[numTxns];
};

class Scheduler {
public:
  Scheduler(Workload * wl, int maxSteps)
    : workload(wl)
    , maxSteps(maxSteps)
    , runtime(0)
    , num_aborts(0)
    , num_waits(0)
    , cpu_cycles(0) { }
  virtual void schedule() = 0;
  void print_stats() {
    cout << scheduler_name << endl;
    if (runtime > maxSteps) {
      cout << "Schedule not found!" << endl;
      return;
    }
    // runtime, num_abort, num_waits, cpu_cycles
    cout << "runtime=" << runtime << ", aborts=" << num_aborts
         << ", waits=" << num_waits << ", cpu_cycles=" << cpu_cycles << endl << endl;
  }

  Workload * workload;
  int maxSteps;
  std::string scheduler_name;
  // stats
  int runtime;
  int num_aborts;
  int num_waits;
  int cpu_cycles;
};


