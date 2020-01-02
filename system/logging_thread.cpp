#include "logging_thread.h"
#include "log.h"
#include "manager.h"

#if LOG_ENABLE

LoggingThread::LoggingThread(uint64_t thd_id)
    : BaseThread(thd_id, LOGGING_THREAD)
{ }

RC LoggingThread::run()
{
    glob_manager->init_rand( get_thd_id() );
    glob_manager->set_thd_id( get_thd_id() );
    assert( glob_manager->get_thd_id() == get_thd_id() );
    pthread_barrier_wait( &global_barrier );

    return log_manager->run();
}

#endif
