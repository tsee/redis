#include "redis_zmq.h"
#include "sds.h"

#include <zmq.h>

#define REDIS_ZMQ_TYPE_STRING 0
#define REDIS_ZMQ_TYPE_HASH 1

static void *redis_zmq_context = NULL;
static void *redis_zmq_socket = NULL;
/* static zmq_msg_t redis_zmq_msg; */

unsigned int redis_zmq_num_endpoints = 0;
char **redis_zmq_endpoints = NULL;
uint64_t redis_zmq_hwm = 0;

/* write raw data to rio */
static int rio_write_raw(rio *r, void *p, size_t len) {
    if (r && rioWrite(r,p,len) == 0)
        return -1;
    return len;
}

/* write string to rio as string */
static int rio_write_raw_string(rio *r, unsigned char *s, size_t len) {
    int n, nwritten = 0;

    /* Store verbatim */
    if ((n = rio_write_raw(r, &len, sizeof(size_t))) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rio_write_raw(r, s, len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* from rdb.c */
static int rio_write_string_object(rio *r, robj *obj) {
    if (obj->encoding == REDIS_ENCODING_INT) {
        /* Encode as string */
        unsigned char buf[32];
        int enclen;
        enclen = ll2string((char*)buf, 32, (long)obj->ptr);
        redisAssert(enclen < 32);
        return rio_write_raw_string(r, buf, enclen);
    } else if (obj->encoding == REDIS_ENCODING_RAW) {
        return rio_write_raw_string(r, obj->ptr, sdslen(obj->ptr));
    }
    else {
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
            /*size_t l = ziplistBlobLen((unsigned char*)o->ptr);

            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) return -1;
            nwritten += n;
            */
            /* FIXME */
        } else if (o->encoding == REDIS_ENCODING_HT) {
            /*
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di)) != NULL) {
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);

                if ((n = rdbSaveStringObject(rdb,key)) == -1) return -1;
                nwritten += n;
                if ((n = rdbSaveStringObject(rdb,val)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
            FIXME
            */

        } else {
            redisPanic("Unknown hash encoding");
        }
    } else {
        redisPanic("Unknown or unsupported object type");
    }
    return nwritten;
}


int zeromqSend(char *str, size_t len, int flags, char *on_error) {
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

void zeromqDumpObject(redisDb *db, robj *key, robj *val) {
    int rc;
    /* char event[2]; */
    char header[4];
    rio payload;
    rio keystr;
    sds payload_buf = sdsempty();
    sds keystr_buf = sdsempty();

    rioInitWithBuffer(&payload, payload_buf);
    rioInitWithBuffer(&keystr, keystr_buf);

    /* sprintf(event, "%d", key_event); */
    ((uint16_t *)header)[0] = (uint16_t)db->id;
    if (val->type == REDIS_STRING) {
        ((uint16_t *)header)[1] = (uint16_t)REDIS_ZMQ_TYPE_STRING;
    } else if (val->type == REDIS_HASH) {
        ((uint16_t *)header)[1] = (uint16_t)REDIS_ZMQ_TYPE_HASH;
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

    rc = zeromqSend(header, (size_t)4, ZMQ_SNDMORE, "Could not send header: %s");
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

/* Called from the propagateExpire function. Sends a 0MQ message
 * containing the unsigned 32bit (native endianess) key length,
 * the key string, the unsigned 32bit (native endianess) value length,
 * and the value string in that order.
 *
 * Handles only Redis "scalar" string values!
 */
void dispatchExpiryMessage(redisDb *db, robj *key) {
    /* uint32_t rc; */
    /* size_t msg_len; */
    /* char *buf; */
    robj *val;

    /* Abuse endpoint setting to see whether we need to send
     * expiry messages at all. */
    if (redis_zmq_num_endpoints == 0)
        return;

    val = lookupKey(db, key); /* FIXME this updates expire time... Silly. *gnash teeth* */

    /* We only support dispatching expiry messages on STRING and HASH values for now. */
    /* Technically, it would be vastly more elegant to reuse the actual Redis protocol for
     * transmitting this information, but for my nefarious purposes, this is good enough.
     * Also, the Redis functions that encode Redis data structures for output appear
     * to like writing to a global buffer. Probably missed something obvious in my
     * sleep-deprived stupor. */
    if ( val == NULL
         || (val->type != REDIS_STRING && val->type != REDIS_HASH) )
        return;

    /* Set up context, socket, and connection. */
    redis_zmq_init();

    if (redis_zmq_socket == NULL)
        return;

    zeromqDumpObject(db, key, val);

    return;
}

