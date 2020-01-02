#pragma once

#include "global.h"
#include "thread.h"

class LoggingThread : public BaseThread
{
public:
    LoggingThread(uint64_t thd_id);
    RC run();
};
