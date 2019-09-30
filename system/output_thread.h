#pragma once

#include "global.h"
#include "thread.h"
#include "message.h"

class Transport;

class OutputThread : public Thread
{
public:
    OutputThread(uint64_t thd_id);
    RC run();

    void output();
private:
    bool _terminated;
    Transport * _transport;
    // synchronize all nodes.
    void global_sync();
    void measure_bw();
    void terminate();

    // Stats
    // message breakdown
    uint64_t _msg_count[Message::NUM_MSG_TYPES];
    uint64_t _msg_size[Message::NUM_MSG_TYPES];

    uint64_t _start_time;
    uint64_t _last_output_time;
};

