#pragma once 

#include "global.h"
#include "thread.h"
#include "message.h"

class Transport;

class InputThread : public Thread 
{
public:
	InputThread(uint64_t thd_id); 
	RC run();
private:
	Transport * _transport;

	// synchronize all nodes. 
	void global_sync();
	void measure_bw();
    void dealwithMsg(Message * msg, uint64_t t1);

	uint64_t _msg_count[Message::NUM_MSG_TYPES];
	uint64_t _msg_size[Message::NUM_MSG_TYPES];
	
	// DEBUG
	uint64_t _start_time;
	uint64_t _last_input_time;
};
