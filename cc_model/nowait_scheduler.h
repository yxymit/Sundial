#include "scheduler.h"

class NoWaitScheduler : public Scheduler {
public:
  NoWaitScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "NoWait Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
  }
  void visualize() {
    int width = 6;
    cout << "NoWait Schedule:" << endl;
    if (runtime == maxSteps) {
      cout << "Schedule not found!" << endl;
      return;
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
          cout << setw(width-3) << history[step][i] << "-";
          Access &access = workload->txns[i].accesses[a];
          std::string type = access.isWrite? "W" : "R";
          cout << type << access.key;
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
        Access &access = workload->txns[i].accesses[ txn_states[i] ];
        bool success = locktable.tryLock(access.key, i, access.isWrite? 2:1);
        if (success) {
          txn_states[i] ++;
          if (txn_states[i] == length) // txn commits
            locktable.unlock_all(i);
        } else {
          num_aborts ++;
          locktable.unlock_all(i);
          txn_states[i] = 0;
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
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    bool tryLock(int key, int txn_id, int type) {
      for (int i = 0; i < numTxns; i ++) {
        if (conflict(table[key][i], type)) return false;
      }
      table[key][txn_id] = type;
      return true;
    }
    void unlock(int key, int txn_id) {
      table[key][txn_id] = 0;
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
  };

  bool all_txns_done() {
    for (int i = 0; i < numTxns; i++)
      if (txn_states[i] != length)
        return false;
    return true;
  };
  // 0 <= value < L: the current step of the txn
  // L: the txn has committed.
  // -1: the txn aborted and is waiting for restart.
  int txn_states[numTxns];
  int ** history; //[maxSteps][numTxns];
  LockTable locktable;
};

