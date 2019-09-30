#include "scheduler.h"

class LockViolationScheduler : public Scheduler {
public:
  LockViolationScheduler(Workload * wl, int maxSteps) : Scheduler(wl, maxSteps) {
    scheduler_name = "LockViolation Scheduler";
    memset(txn_states, 0, sizeof(int) * numTxns);
    history = new int * [maxSteps];
    for (int i = 0; i < maxSteps; i ++) {
      history[i] = new int[numTxns];
      memset(history[i], 0, sizeof(int) * numTxns);
    }
  }
  void visualize() {
    int width = 6;
    cout << "LockViolation Schedule:" << endl;
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
        //if (curr_step == 2 && i == 0)
        //  cout << "here" << endl;
        if (txn_states[i] == length) continue; // skip committed transactions
        cpu_cycles ++;
        int step = txn_states[i];
        assert(step >= 0);
        Access &access = workload->txns[i].accesses[ txn_states[i] ];
        // TODO: detect cycle for each access, abort current if cycle is
        // detected.
        int aborted_txn_list[numTxns];
        memset(aborted_txn_list, 0, sizeof(int) * numTxns);
        int ret = locktable.tryLock(access.key, i, access.isWrite? 2:1, aborted_txn_list);
        txn_states[i] ++;
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
  class Cube {
  public:
    Cube() {
      //memset(&matrix, 0, (numTxns + 1) * (numTxns + 1));
      memset(&cube, 0, (numTxns + 1) * (numTxns + 1) * tableSize);
    }
    Cube (Cube &other) {
      //memcpy(&matrix, &other, sizeof(bool) * (numTxns + 1) * (numTxns + 1));
      memcpy(&cube, &other, sizeof(bool) * (numTxns + 1) * (numTxns + 1) * tableSize);
    }
    /*static void multiply(Matrix &out, Matrix &left, Matrix &right, int size) {
      for (int i = 0; i < size; i ++)
        for (int j = 0; j < size; j++) {
          out.matrix[i][j] = 0;
          for (int k = 0; k < size; k ++)
            out.matrix[i][j] += left.matrix[i][k] * right.matrix[k][j];
        }
    };

    int trace(int size) {
      int trace = 0;
      for (int i = 0; i < size; i ++)
        trace += matrix[i][i];
      return trace;
    }*/
    void clear(int level) {
      for (int i = 0; i < numTxns + 1; i++) {
        for (int v = 0; v < tableSize; v ++) {
          cube[level][i][v] = 0;
          cube[i][level][v] = 0;
        }
      }
    }
    void i_depends_on(int i, int * txns) {
      for (int k = 0; k < numTxns; k++)
        for (int v = 0; v < tableSize; v++)
          if (cube[i][k][v])
            txns[k] = 1;
    }
    void get_raw_waw_successors(int txn_id, int * successors) {
      for (int i = 0; i < numTxns; i ++) {
        for (int v = 0; v < tableSize; v++) {
          if (cube[i][txn_id][v] == 1 || cube[i][txn_id][v] == 2) {
            if (successors[i] == 0) {
              successors[i] = 1;
              get_raw_waw_successors(i, successors);
            }
          }
        }
      }
    }
    // transaction i depends on transaction j
    // type: 1: RAW, 2: WAW, 3: WAR
    void set(int i, int j, int v, int type) {
      cube[i][j][v] = type;
    }
    void calculate_war() {
      // If i WAW depends on j thr v
      // && k RAW depends on j thr v
      // -> i WAR depends on k thr v
      for (int i = 0; i < numTxns; i ++)
        for (int j = 0; j < numTxns + 1; j ++)
          for (int k = 0; k < numTxns; k++)
            for (int v = 0; v < tableSize; v ++)
              if (cube[i][j][v] == 2 && cube[k][j][v] == 1) {
                assert(cube[i][k][v] == 0);
                cube[i][k][v] = 3;
              }
    }
    /*void get_non_zero_levels(int * non_zero_levels) {
      for (int i = 0; i < numTxns; i ++) {
        if (matrix[i][i] != 0)
          non_zero_levels[i] = 1;
      }
    }
    bool has_cycle(int size, int * txns_in_cycle) {
      // Detect cycles.
      Matrix mat1(*this);
      Matrix mat2;
      Matrix * matptr_in = &mat1;
      Matrix * matptr_out = &mat2;
      for (int i = 0; i < size; i ++) {
        Matrix::multiply(*matptr_out, *matptr_in, *this, size);
        if (matptr_out->trace(size) > 0) {
          matptr_out->get_non_zero_levels(txns_in_cycle);
          return true;
        }
        Matrix * tmp = matptr_out;
        matptr_out = matptr_in;
        matptr_in = tmp;
      }
      return false;
    }*/
  private:
    // matrix[i][j] = 1: i somehow depends on j
    //bool matrix[numTxns + 1][numTxns + 1];
    // cube[i][j][v] = 1: i RAW depends on j through record v
    // cube[i][j][v] = 2: i WAW depends on j through record v
    // cube[i][j][v] = 3: i WAR depends on j through record v
    // j == numTxns: initial state --- used to capture WAR for txns reading/writing initial states.
    bool cube[numTxns + 1][numTxns + 1][tableSize];
  };

  // This is not a real lock table, but just a place to track data dependencies.
  class LockTable {
  public:
    LockTable() {
      memset(table, 0, sizeof(int) * tableSize * numTxns);
    }
    bool conflict(int type1, int type2) {
      return type1 + type2 >= 3;
    }
    // type: 1: read 2: write
    // return value:
    // 0: failure
    // 1: success
    int tryLock(int key, int txn_id, int type, int * aborted_txn_list) {
      int ret = 1;
      int last_writer_before_curr = numTxns;
      bool abort = false;

      // find the laster writer
      for (int i = 0; i < txn_id; i ++)
        if (table[key][i])
          last_writer_before_curr = i;

      if (type == 1) {
        // record RAW dependency
        cube.set(txn_id, last_writer_before_curr, key, 1);
#if !MVCC
        for (int i = txn_id; i < numTxns; i ++)
          if (table[key][i])
            aborted_txn_list[i] = 1;
#endif
      } else { // type == 2
        // record WAW dependencies
        cube.set(txn_id, last_writer_before_curr, key, 2);
        cube.calculate_war();
        for (int i = txn_id; i < numTxns; i ++)
          if (table[key][i])
            aborted_txn_list[i] = 1;
        table[key][txn_id] = 1;
      }
      for (int k = txn_id + 1; k < numTxns; k++)
        if (aborted_txn_list[k])
          get_successors(k, aborted_txn_list);
      aborted_txn_list[txn_id] = 0;
      return 1;
    }
    void get_successors(int txn_id, int * successors) {
      cube.get_raw_waw_successors(txn_id, successors);
    }
    void unlock_all(int txn_id) {
      for (int i = 0; i < tableSize; i ++)
        table[i][txn_id] = 0;
      cube.clear(txn_id);
    }
  private:
    // 0: no lock
    // 1: shared lock
    // 2: exclusive lock
    int table[tableSize][numTxns];
    Cube cube;
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

