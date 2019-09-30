#include <iostream>
#include <cstdlib>
#include <cstring>

#include "opt_scheduler.h"
#include "nowait_scheduler.h"
#include "limitdepth_scheduler.h"
#include "occ_scheduler.h"
#include "waitdie_scheduler.h"
#include "sortwait_scheduler.h"
#include "dldetect_scheduler.h"
#include "lockviolation_scheduler.h"
#include "woundwait_scheduler.h"

int main() {
#if MVCC
  cout << "NOTE: Multi-versioning is supported only in LockViolation Scheduler" << endl;
#endif
  Workload * workload = new Workload;
  OptScheduler opt_scheduler(workload, 8);
  opt_scheduler.schedule();
  opt_scheduler.visualize();
  opt_scheduler.print_stats();

  /*NoWaitScheduler nowait_scheduler(workload, 100);
  nowait_scheduler.schedule();
  nowait_scheduler.visualize();
  nowait_scheduler.print_stats();

  LimitDepthScheduler limitdepth_scheduler(workload, 100);
  limitdepth_scheduler.schedule();
  limitdepth_scheduler.visualize();
  limitdepth_scheduler.print_stats();

  OCCScheduler occ_scheduler(workload, 100);
  occ_scheduler.schedule();
  occ_scheduler.visualize();
  occ_scheduler.print_stats();
  */
  WaitDieScheduler waitdie_scheduler(workload, 1000);
  waitdie_scheduler.schedule();
  waitdie_scheduler.visualize();
  waitdie_scheduler.print_stats();
  /*
  SortWaitScheduler sortwait_scheduler(workload, 100);
  sortwait_scheduler.schedule();
  sortwait_scheduler.visualize();
  sortwait_scheduler.print_stats();

  DLDetectScheduler dldetect_scheduler(workload, 200);
  dldetect_scheduler.schedule();
  dldetect_scheduler.visualize();
  dldetect_scheduler.print_stats();
  */

  LockViolationScheduler lockviolation_scheduler(workload, 200);
  lockviolation_scheduler.schedule();
  lockviolation_scheduler.visualize();
  lockviolation_scheduler.print_stats();

  WoundWaitScheduler woundwait_scheduler(workload, 1000);
  woundwait_scheduler.schedule();
  woundwait_scheduler.visualize();
  woundwait_scheduler.print_stats();
}
