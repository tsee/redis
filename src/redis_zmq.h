#ifndef __REDIS_ZMQ_H
#define __REDIS_ZMQ_H

#include "redis.h"

extern unsigned int redis_zmq_num_endpoints;
extern char **redis_zmq_endpoints;
extern uint64_t redis_zmq_hwm;

extern uint32_t redis_zmq_hash_max_expire_cycles;
extern uint32_t redis_zmq_hash_expire_delay_ms;
extern uint32_t redis_zmq_hash_expire_delay_jitter_ms;


int dispatchExpirationMessage(redisDb *db, robj *key);
void redis_zmq_init();

#endif
