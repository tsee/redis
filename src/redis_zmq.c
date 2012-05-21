#include "redis_zmq.h"
#include "sds.h"
#include "dict.h"
#include "zipmap.h"

#include <zmq.h>

#define REDIS_ZMQ_TYPE_STRING 0
#define REDIS_ZMQ_TYPE_HASH 1

static void *redis_zmq_context = NULL;
static void *redis_zmq_socket = NULL;
/* static zmq_msg_t redis_zmq_msg; */

unsigned int redis_zmq_num_endpoints = 0;
char **redis_zmq_endpoints = NULL;
uint64_t redis_zmq_hwm = 0;

uint32_t redis_zmq_hash_max_expire_cycles = 0;
uint32_t redis_zmq_hash_expire_delay_ms = 10*1000; /* 10s */
uint32_t redis_zmq_hash_expire_delay_jitter_ms = 10*1000; /* 10s */

/* write raw data to rio */
static int rio_write_raw(rio *r, void *p, uint32_t len) {
    if (r && rioWrite(r, p, len) == 0)
        return -1;
    return len;
}

/* write a length to rio */
static inline int rio_write_unsigned_32bit(rio *r, uint32_t num) {
    return rio_write_raw(r, &num, sizeof(uint32_t));
}

/* write string to rio as string */
static int rio_write_raw_string(rio *r, unsigned char *s, uint32_t len) {
    int n, nwritten = 0;

    /* Store verbatim */
    if ((n = rio_write_unsigned_32bit(r, len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rio_write_raw(r, s, len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

static int rio_write_longlong_as_string(rio *r, long long num) {
    /* Encode as string */
    unsigned char buf[32];
    int enclen;
    enclen = ll2string((char*)buf, 32, num);
    redisAssert(enclen < 32);
    return rio_write_raw_string(r, buf, enclen);
}

static int rio_write_string_object(rio *r, robj *obj) {
    if (obj->encoding == REDIS_ENCODING_INT) {
        return rio_write_longlong_as_string(r, (long long)obj->ptr);
    } else if (obj->encoding == REDIS_ENCODING_RAW) {
        return rio_write_raw_string(r, obj->ptr, sdslen(obj->ptr));
    } else {
        redisPanic("Not a string encoding we can handle");
    }
}

static int rio_write_value(rio *r, robj *o) {
    int n, nwritten = 0;
    if (o->type == REDIS_STRING) {
        /* Save a string value */
        if ((n = rio_write_string_object(r, o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == REDIS_HASH) {
        /* Save a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *zl;
            unsigned char *fptr;
            unsigned int zl_len;
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;
            int ret;

            zl = o->ptr;
            zl_len = ziplistLen(zl);
            rio_write_unsigned_32bit(r, (uint32_t)zl_len);

            fptr = ziplistIndex(zl, 0);
            /* ziplist element iteration, key and value treated the same -- a number or a string */
            while (fptr != NULL) {
                ret = ziplistGet(fptr, &vstr, &vlen, &vll);
                redisAssert(ret);

                if (vstr) {
                    if ((n=rio_write_raw_string(r, vstr, vlen)) == -1)
                        return -1;
                    nwritten += n;
                } else {
                    if ((n=rio_write_longlong_as_string(r, vll)) == -1)
                        return -1;
                    nwritten += n;
                }

                fptr = ziplistNext(zl, fptr);
            }
        } else if (o->encoding == REDIS_ENCODING_HT) {
            dict *d = o->ptr;
            dictIterator *di = dictGetIterator(d);
            dictEntry *de;

            if ((n = rio_write_unsigned_32bit(r, dictSize(d)*2)) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);

                if ((n = rio_write_string_object(r, key)) == -1) return -1;
                nwritten += n;
                if ((n = rio_write_string_object(r, val)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            redisPanic("Unknown hash encoding");
        }
    } else {
        redisPanic("Unknown or unsupported object type");
    }
    return nwritten;
}


static int zeromqSend(char *str, size_t len, int flags, char *on_error) {
    int rc;
    /* size_t bytes; */
    zmq_msg_t msg;
    zmq_msg_init_size(&msg, len);
    memcpy(zmq_msg_data(&msg), str, len);
    /* bytes = zmq_msg_size(&msg); */
    rc = zmq_send(redis_zmq_socket, &msg, flags);
    zmq_msg_close(&msg);
    if (rc == -1) {
        redisLog(REDIS_WARNING, on_error, zmq_strerror(zmq_errno()));
    }
    return rc;
}

static void zeromqDumpObject(redisDb *db, robj *key, robj *val) {
    int rc;
    /* char event[2]; */
    uint16_t header[2];
    rio payload;
    rio keystr;
    sds payload_buf = sdsempty();
    sds keystr_buf = sdsempty();

    rioInitWithBuffer(&payload, payload_buf);
    rioInitWithBuffer(&keystr, keystr_buf);

    header[0] = (uint16_t)db->id;
    if (val->type == REDIS_STRING) {
        header[1] = (uint16_t)REDIS_ZMQ_TYPE_STRING;
    } else if (val->type == REDIS_HASH) {
        header[1] = (uint16_t)REDIS_ZMQ_TYPE_HASH;
    }
    else {
        redisPanic("Cannot handle types other than strings and hashes!");
    }


    /*if (val) {
        rioInitWithBuffer(&payload,sdsempty());
        redisAssertWithInfo(NULL, key, rio_write_string_object(&payload, key));
        redisAssertWithInfo(NULL, val, rio_write_value(&payload, val) != -1);
    }*/
    redisAssertWithInfo(NULL, key, rio_write_string_object(&keystr, key) != -1);
    redisAssertWithInfo(NULL, val, rio_write_value(&payload, val) != -1);

    rc = zeromqSend((char *)header, (size_t)4, ZMQ_SNDMORE, "Could not send header: %s");
    rc = zeromqSend((char *)keystr.io.buffer.ptr, (size_t)sdslen(keystr.io.buffer.ptr), ZMQ_SNDMORE, "Could not send key: %s");
    rc = zeromqSend((char *)payload.io.buffer.ptr, (size_t)sdslen(payload.io.buffer.ptr), 0, "Could not send payload: %s");

    sdsfree(keystr.io.buffer.ptr);
    sdsfree(payload.io.buffer.ptr);
/*    if (rc_db != -1 && rc_event != -1 && rc_key != -1 && rc_val != -1)
        server.stat_zeromq_events++;
*/
}


/* Attempts to set up context, socket, connection. Don't call unless
 * redis_zmq_num_endpoints is not 0. */
void redis_zmq_init() {
    int status;
    unsigned int iendpoint;
    redisLog(REDIS_DEBUG,"Initializing 0MQ for expiry.");

    if (redis_zmq_context == NULL) {
        redis_zmq_context = zmq_init(1);
        if (redis_zmq_context == NULL) {
            redisLog(REDIS_WARNING,"Failed to init 0MQ context");
        }
    }

    if (redis_zmq_socket == NULL && redis_zmq_context != NULL) {
        redis_zmq_socket = zmq_socket(redis_zmq_context, ZMQ_PUSH);
        if (redis_zmq_socket == NULL) {
            redisLog(REDIS_WARNING,"Failed to init 0MQ socket");
        }
        else {
            if (zmq_setsockopt(redis_zmq_socket, ZMQ_HWM, &redis_zmq_hwm, sizeof(redis_zmq_hwm)) == -1) {
                zmq_close(redis_zmq_socket);
                redisLog(REDIS_WARNING,"Failed to set HWM on 0MQ socket with error %i", errno);
                return;
            }
            for (iendpoint = 0; iendpoint < redis_zmq_num_endpoints; ++iendpoint) {
                /* status = zmq_connect(redis_zmq_socket, redis_zmq_endpoints[iendpoint]); */

                redisLog(REDIS_NOTICE,"Binding 0MQ socket to '%s'", redis_zmq_endpoints[iendpoint]);
                status = zmq_bind(redis_zmq_socket, redis_zmq_endpoints[iendpoint]);
                if (status != 0) {
                    zmq_close(redis_zmq_socket);
                    redisLog(REDIS_WARNING,"Failed to bind 0MQ socket");
                    break;
                }
            }
        }
    }

    return;
}

/* Returns 0 for anything but hashes.
 * For hashes, returns the number of elapsed expire cycles
 * and increments the number if applicable. */
static int redis_zmq_check_expire_cycles(redisDb *db, robj *key, robj *o) {
    long long nexpirecycles = 0;
    int need_reset_expire = 0;

    if (o->type == REDIS_HASH) {
        /* Save a hash value */
        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *zl;
            unsigned char *val = NULL;
            unsigned int vlen = UINT_MAX;
            char buf[64];
            int ret;

            zl = o->ptr;
            /* fetch num. expire cycles elapsed */
            ret = zipmapGet(zl, (unsigned char *)"_expire_cycles", 14, &val, &vlen);
            if (ret)
                nexpirecycles = atoll((char *)val);

            /* increment num. expire cycles */
            if (nexpirecycles < redis_zmq_hash_max_expire_cycles) {
                sprintf(buf, "%u", (unsigned int)(nexpirecycles+1));
                zl = zipmapSet(zl, (unsigned char *)"_expire_cycles", 14, (unsigned char *)buf, strlen(buf), NULL);
                o->ptr = zl;
                need_reset_expire = 1;
            }

        } else if (o->encoding == REDIS_ENCODING_HT) {
            dict *d = o->ptr;
            unsigned char *val = NULL;
            robj *newval;

            val = dictFetchValue(d, "_expire_cycles");
            if (val != NULL) {
                redisAssert( getLongLongFromObject((robj *)val, &nexpirecycles) );
            }
            if (nexpirecycles < redis_zmq_hash_max_expire_cycles) {
                newval = createStringObjectFromLongLong(nexpirecycles+1);
                dictReplace(d, "_expire_cycles", newval);
                /* FIXME check newval refcount */
                need_reset_expire = 1;
            }

        } else {
            redisPanic("Unknown hash encoding");
        }
    }

    if (need_reset_expire != 0) {
        long long when = mstime()
                         + redis_zmq_hash_expire_delay_ms
                         + (long long)(
                            ((double)random() / (double)RAND_MAX)
                            * (double)redis_zmq_hash_expire_delay_jitter_ms
                           );
        setExpire(db, key, when);
    }

    return (int)nexpirecycles;
}


/* Called from the propagateExpire function. Sends a 0MQ message
 * containing the unsigned 32bit (native endianess) key length,
 * the key string, the unsigned 32bit (native endianess) value length,
 * and the value string in that order.
 *
 * Handles only Redis "scalar" string values and hashes!
 */
/* Returns 0 if the key is not to be deleted after all */
int dispatchExpiryMessage(redisDb *db, robj *key) {
    /* uint32_t rc; */
    /* size_t msg_len; */
    /* char *buf; */
    robj *val;

    /* Abuse endpoint setting to see whether we need to send
     * expiry messages at all. */
    if (redis_zmq_num_endpoints == 0)
        return 1;

    val = lookupKey(db, key); /* FIXME this updates expire time... Silly. *gnash teeth* */

    /* We only support dispatching expiry messages on STRING and HASH values for now. */
    /* Technically, it would be vastly more elegant to reuse the actual Redis protocol for
     * transmitting this information, but for my nefarious purposes, this is good enough.
     * Also, the Redis functions that encode Redis data structures for output appear
     * to like writing to a global buffer. Probably missed something obvious in my
     * sleep-deprived stupor. */
    if ( val == NULL
         || (val->type != REDIS_STRING && val->type != REDIS_HASH) )
        return 1;

    /* Set up context, socket, and connection. */
    redis_zmq_init();

    if (redis_zmq_socket == NULL)
        return 1;

    zeromqDumpObject(db, key, val);

    /* Check whether we need to cycle the key back to the db and do so
     * if necessary. */
    if (redis_zmq_hash_max_expire_cycles != 0) {
        int ncycles = redis_zmq_check_expire_cycles(db, key, val);
        /* Do not delete if we haven't hit the cycle limit yet */
        if (ncycles < redis_zmq_hash_max_expire_cycles)
            return 0;
    }

    return 1;
}

