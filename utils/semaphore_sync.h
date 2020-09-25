#pragma once

#include "global.h"

class SemaphoreSync {
public:
    SemaphoreSync();
    void              incr();
    void              decr();
    void              wait();
    void              print();
private:
    uint32_t          _semaphore;
    pthread_cond_t *  _cond;
    pthread_mutex_t * _mutex;
};

