#pragma once

class Matrix {
public:
  Matrix() { memset(&matrix, 0, numTxns*numTxns); }
  Matrix (Matrix &other) {
    memcpy(&matrix, &other, sizeof(bool) * numTxns * numTxns);
  }
  static void multiply(Matrix &out, Matrix &left, Matrix &right, int size) {
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
  }
  void get_non_zero_levels(int * non_zero_levels) {
    for (int i = 0; i < numTxns; i ++) {
      if (matrix[i][i] != 0)
        non_zero_levels[i] = 1;
    }
  }
  void clear(int level) {
    for (int i = 0; i < numTxns; i++) {
      matrix[level][i] = 0;
     matrix[i][level] = 0;
    }
  }
  // transaction i depends on transaction j
  void set(int i, int j) {
    matrix[i][j] = 1;
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
  }
  bool is_txn_waiting(int txn_id) {
    for (int i = 0; i < numTxns; i ++) {
      if (matrix[txn_id][i])
        return true;
    }
    return false;
  }
private:
  bool matrix[numTxns][numTxns];
};


