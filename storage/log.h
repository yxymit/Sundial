#pragma once
#include "global.h"
#include "helper.h"

class LogManager {
public:
    LogManager();
    void log(uint32_t size, char * record);
private:
    uint32_t _buffer_size;
    char * _buffer;
    uint32_t _lsn;
};
