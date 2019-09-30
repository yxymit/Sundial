#include "scheduler.h"

class OCCScheduler : public Scheduler {
public:
  OCCScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "OCC Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
    memset(versions, 0, sizeof(int) * tableSize);
    memset(access_set, 0, sizeof(int) * numTxns * length);
  }

  void visualize() {
    int width = 6;
    cout << "OCC Schedule:" << endl;
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
        if (a > length)
          cout << setw(width) << "";
        else if (a == length) {
          cout << setw(width) << "V";
        } else {
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
        int step = txn_states[i];
        if (step > length) continue; // skip committed transactions
        cpu_cycles ++;
        if (step < length) {
          Access &access = workload->txns[i].accesses[ step ];
          access_set[i][step] = versions[access.key];
          txn_states[i] ++;
        } else { // step == length
          // validate the transaction
          Access * accesses = workload->txns[i].accesses;
          for (int a = 0; a < length; a++)
            if (versions[accesses[a].key] != access_set[i][a]) {
              // abort
              txn_states[i] = 0;
              num_aborts ++;
            }
          if (txn_states[i] != 0) {
            // update versions and commit
            for (int a = 0; a < length; a++) {
              if (accesses[a].isWrite)
                versions[accesses[a].key] ++;
            }
            txn_states[i] ++;
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
  bool all_txns_done() {
    for (int i = 0; i < numTxns; i++)
      if (txn_states[i] != length + 1)
        return false;
    return true;
  };
  // 0 <= value < L: the current step of the txn
  // L: the validation phase of the txn
  // L + 1: the txn has committed.
  int txn_states[numTxns];
  int access_set[numTxns][length];
  int ** history; //[maxSteps][numTxns];
  int versions[tableSize];
};

