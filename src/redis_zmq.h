#ifndef __REDIS_ZMQ_H
#define __REDIS_ZMQ_H

#include "redis.h"

extern unsigned int redis_zmq_num_endpoints;
extern char **redis_zmq_endpoints;
extern unsigned int redis_zmq_hwm;

void dispatchExpiryMessage(redisDb *db, robj *key);

#endif
