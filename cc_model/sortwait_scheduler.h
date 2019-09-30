#include "scheduler.h"

class SortWaitScheduler : public Scheduler {
public:
  SortWaitScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "SortWait Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
    sorted_workload = new Workload(wl);
    sorted_workload->sort_by_key();
    //sorted_workload->print();
  }
  void visualize() {
    int width = 6;
    cout << "SortWait Schedule:" << endl;
    if (runtime == maxSteps) {
      cout << "Schedule not found!" << endl;
      //return;
    }
    cout << setw(width) << "";
    for (int i = 0; i < numTxns; i++) {
      cout << setw(width - 1) << "T" << i;
    }
    cout << endl << endl;

    for (int step = 0; step < runtime; step ++) {
      cout << setw(width) << step;
      for (int i = 0; i < numTxns; i ++) {
        int a = history[step][i];
        if (a == length)
          cout << setw(width) << "";
        else {
          if (a == -1) cout << setw(width) << "A";
          else {
            cout << setw(width - 3) << a;
            cout << "-";
            Access &access = sorted_workload->txns[i].accesses[ a ];
            std::string type = access.isWrite? "W" : "R";
            cout << type << access.key;
          }
        }
      }
      cout << endl;
    }
  }
  void schedule() {
    int curr_step = 0;
    while (!all_txns_done()) {
      for (int i = 0; i < numTxns; i ++) {
        if (txn_states[i] == length) continue; // skip committed transactions
        cpu_cycles ++;
        int step = txn_states[i];
        Access &access = sorted_workload->txns[i].accesses[ step ];
        int ret = locktable.tryLock(access.key, i, access.isWrite? 2:1);
        if (ret == 1) {
          txn_states[i] ++;
          if (txn_states[i] == length) // txn commits
            locktable.unlock_all(i);
        } else { // wait
          num_waits ++;
          assert(ret == 2);
        }
      }
      curr_step ++;
      runtime = curr_step;
      if (curr_step == maxSteps)
        break;
      memcpy(history[curr_step], txn_states, sizeof(int) * numTxns);
    }
  }
private:
  class LockTable {
  public:
    LockTable() {
      memset(table, 0, sizeof(int) * tableSize * numTxns);
      memset(wait_pri, 0, sizeof(int) * tableSize * numTxns);
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    // return value:
    // 1: success
    // 2: wait
    int tryLock(int key, int txn_id, int type) {
      int ret = 1;
      if (table[key][txn_id] == 0) {
        int pri = 0;
        for (int i = 0; i < numTxns; i ++)
          if (wait_pri[key][i] > pri)
            pri = wait_pri[key][i];
        wait_pri[key][txn_id] = pri + 1;
      }
      table[key][txn_id] = type;
      for (int i = 0; i < numTxns; i ++) {
        if (i == txn_id) continue;
        if (conflict(table[key][i], type)
            && wait_pri[key][txn_id] > wait_pri[key][i])
          return 2;
      }
      return 1;
    }
    void unlock_all(int txn_id) {
      for (int i = 0; i < tableSize; i ++)
        table[i][txn_id] = 0;
    }
  private:
    // 0: no lock
    // 1: shared lock
    // 2: exclusive lock
    int table[tableSize][numTxns];
    int wait_pri[tableSize][numTxns];
  };

  bool all_txns_done() {
    for (int i = 0; i < numTxns; i++)
      if (txn_states[i] != length)
        return false;
    return true;
  };
  // 0 <= value < L: the current step of the txn
  // L: the txn has committed.
  // if value == -1, the txn just aborted.
  int txn_states[numTxns];
  int ** history; //[maxSteps][numTxns];
  LockTable locktable;
  Workload * sorted_workload;
};

