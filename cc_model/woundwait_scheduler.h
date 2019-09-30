#include "scheduler.h"

class WoundWaitScheduler : public Scheduler {
public:
  WoundWaitScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "WoundWait Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
  }
  void visualize() {
    int width = 6;
    cout << "WoundWait Schedule:" << endl;
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
        int aborted_txn_list[numTxns];
        memset(aborted_txn_list, 0, sizeof(int) * numTxns);
        int ret = locktable.tryLock(access.key, i, access.isWrite? 2:1, aborted_txn_list);
        if (ret == 1) {
          txn_states[i] ++;
          if (txn_states[i] == length) // txn commits
            locktable.unlock_all(i);
        } else { // wait
          num_waits ++;
          assert(ret == 2);
        }
        for (int k = 0; k < numTxns; k ++) {
          if (aborted_txn_list[k]) {
            num_aborts ++;
            locktable.unlock_all(k);
            txn_states[k] = 0;
            history[curr_step][k] = -1;
          }
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
      for (int i = 0; i < numTxns; i ++)
        is_waiting_on[i] = -1;
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    // return value:
    // 1: success
    // 2: wait
    // Policy: can wait only for txns with smaller ID
    int tryLock(int key, int txn_id, int type, int * aborted_txn_list) {

      int prev_waiting_on = is_waiting_on[txn_id];
      is_waiting_on[txn_id] = -1;
      table[key][txn_id] = type;
      for (int i = 0; i < txn_id; i ++)
        if (conflict(table[key][i], type))
          is_waiting_on[txn_id] = key;
      if (key == 9 && txn_id == 2)
        cout << "here" << endl;
      if (prev_waiting_on == -1) // new coming txn
        for (int i = txn_id + 1; i < numTxns; i++)
          if (conflict(table[key][i], type) && is_waiting_on[i] != key)
            aborted_txn_list[i] = 1;

      if (is_waiting_on[txn_id] == key) return 2;
      return 1;
    }

    void unlock_all(int txn_id) {
      for (int i = 0; i < tableSize; i ++)
        table[i][txn_id] = 0;
      is_waiting_on[txn_id] = -1;
    }
  private:
    // 0: no lock
    // 1: shared lock
    // 2: exclusive lock
    int table[tableSize][numTxns];
    int is_waiting_on[numTxns]; // what record the transaction is waiting on. -1 means not waiting.
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

