#include "scheduler.h"

class WaitDieScheduler : public Scheduler {
public:
  WaitDieScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "WaitDie Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
  }
  void visualize() {
    int width = 6;
    cout << "WaitDie Schedule:" << endl;
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
          if (a == -1) cout << setw(width) << "A";
          else {
            cout << setw(width - 3) << a;
            cout << "-";
            Access &access = workload->txns[i].accesses[ a ];
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
        assert(step >= 0);
        Access &access = workload->txns[i].accesses[ txn_states[i] ];
        int ret = locktable.tryLock(access.key, i, access.isWrite? 2:1);
        if (ret == 1) {
          txn_states[i] ++;
          if (txn_states[i] == length) // txn commits
            locktable.unlock_all(i);
        } else if (ret == 0) {  //abort
          num_aborts ++;
          locktable.unlock_all(i);
          txn_states[i] = 0;
          history[curr_step][i] = -1;
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
      memset(is_waiting, false, numTxns);
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    // return value:
    // 0: failure
    // 1: success
    // 2: has to wait
    // Policy: can wait only for txns with smaller ID
    int tryLock(int key, int txn_id, int type) {
      int ret = 1;
      for (int i = 0; i < numTxns; i ++) {
        if (i == txn_id) continue;
        if (conflict(table[key][i], type)) {
          if (txn_id > i) // cannot wait for txn with a smaller ID
            return 0;
          else
            ret = 2;
        }
      }
      table[key][txn_id] = type;
      is_waiting[txn_id] = (ret == 2);
      return ret;
    }
    //void unlock(int key, int txn_id) {
    //  table[key][txn_id] = 0;
    //}
    void unlock_all(int txn_id) {
      for (int i = 0; i < tableSize; i ++)
        table[i][txn_id] = 0;
      is_waiting[txn_id] = false;
    }
  private:
    // 0: no lock
    // 1: shared lock
    // 2: exclusive lock
    int table[tableSize][numTxns];
    bool is_waiting[numTxns];
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
};

