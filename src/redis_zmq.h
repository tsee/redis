#ifndef __REDIS_ZMQ_H
#define __REDIS_ZMQ_H

#include "redis.h"

extern char *redis_zmq_endpoint;
extern unsigned int redis_zmq_hwm;

void dispatchExpiryMessage(redisDb *db, robj *key);

#endif
