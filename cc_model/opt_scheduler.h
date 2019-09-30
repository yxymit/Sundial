#include "scheduler.h"
#include "matrix.h"

class OptScheduler : public Scheduler {
public:
  OptScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "Optimal Scheduler";
  }
  void visualize() {
    int width = 6;
    cout << "Optimal Schedule:" << endl;
    if (currMinRuntime > maxSteps) {
      cout << "Schedule not found!" << endl;
      return;
    }
    int min_offset = maxSteps, max_offset = -maxSteps;
    cout << setw(width) << "";
    for (int i = 0; i < numTxns; i++) {
      min_offset = min(min_offset, currSchedule[i]);
      max_offset = max(max_offset, currSchedule[i] + length - 1);
      cout << setw(width - 1) << "T" << i;
    }
    cout << endl << endl;
    for (int step = min_offset; step <= max_offset; step ++) {
      cout << setw(width) << step - min_offset;
      for (int i = 0; i < numTxns; i++) {
        if (step < currSchedule[i] || step >= currSchedule[i] + length)
          cout << setw(width) << "";
        else {
          int a = step - currSchedule[i];
          Access &access = workload->txns[i].accesses[a];
          std::string type = access.isWrite? "W" : "R";
          cout << setw(width - 1) << type << access.key;
        }
      }
      cout << endl;
    }
  }
  void schedule() {
    currMinRuntime = maxSteps + 3;
    offset[0] = 0;
    compute(1, 0, length - 1);
    runtime = currMinRuntime;
    cpu_cycles = length * numTxns;
  }
private:
  bool compute(int txn_id, int min_offset, int max_offset) {
    // An optimization: if the current trial cannot beat the current best, stop
    // trying.
    if (max_offset - min_offset + 1 >= currMinRuntime)
      return false;
    if (txn_id == numTxns) {
      // calculate the current results
      int runtime = max_offset - min_offset + 1;
      if (runtime <= currMinRuntime) {
        currMinRuntime = runtime;
        // a better schedule is found.
        memcpy(currSchedule, offset, sizeof(int) * numTxns);
      }
      return true;
    }
    // Searching starts from 0
    for (int curr_offset = 0;
         curr_offset >= max_offset - maxSteps + 1;
         curr_offset --) {
      offset[txn_id] = curr_offset;
      if (check(txn_id)) {
        compute(txn_id + 1,
                min(min_offset, curr_offset),
                max(max_offset, curr_offset + length - 1));
      }
    }
    for (int curr_offset = 0;
         curr_offset <= min_offset + maxSteps - length;
         curr_offset ++) {
      offset[txn_id] = curr_offset;
      if (check(txn_id)) {
        compute(txn_id + 1,
                min(min_offset, curr_offset),
                max(max_offset, curr_offset + length - 1));
      }
    }
    return false;
  };

  bool check(int txn_id) {
    mat.clear(txn_id);
    // Identify new dependency information
    for (int i = 0; i < txn_id; i ++) {
      // a1 indexes the accesses of txn.id = i
      // a2 indexes the accesses of txn.id = txn_id
      for (int a1 = 0; a1 < length; a1 ++) {
        for (int a2 = 0; a2 < length; a2 ++) {
          Access &acc1 = workload->txns[i].accesses[a1];
          Access &acc2 = workload->txns[txn_id].accesses[a2];
          if (acc1.key != acc2.key) continue;
          if (!acc1.isWrite && !acc2.isWrite) continue;
          // now they access the same key and one of them is a write
          int offset1 = offset[i] + a1;
          int offset2 = offset[txn_id] + a2;
          if (offset1 > offset2) mat.set(i, txn_id);
          else if (offset1 < offset2) mat.set(txn_id, i);
          else { // offset1 == offset2
            //if (acc1.isWrite && acc2.isWrite) return false; // cannot write the same key in the same cycle.
            //else if (acc1.isWrite) mat.set(i, txn_id);
            if (txn_id < i) mat.set(i, txn_id);
            else mat.set(txn_id, i);
          }
        }
      }
    }

    // Detect cycles.
    Matrix mat1(mat);
    Matrix mat2;
    Matrix * matptr_in = &mat1;
    Matrix * matptr_out = &mat2;
    int size = txn_id + 1;
    for (int i = 0; i < size; i ++) {
      Matrix::multiply(*matptr_out, *matptr_in, mat, size);
      if (matptr_out->trace(size) > 0) return false;
      Matrix * tmp = matptr_out;
      matptr_out = matptr_in;
      matptr_in = tmp;
    }
    return true;
  }

  int currMinRuntime;
  int currSchedule[numTxns];
  // offset of each transaction
  int offset[numTxns];
  // adjacency matrix of the graph
  Matrix mat;
};

