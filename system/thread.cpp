#include "thread.h"
#include "manager.h"

BaseThread::BaseThread(uint64_t thd_id, ThreadType thread_type)
    : _thd_id(thd_id)
    , _thread_type(thread_type)
{
    _wl = glob_manager->get_workload();
}
