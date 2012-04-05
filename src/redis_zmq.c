#include "redis_zmq.h"
#include "sds.h"

#include <zmq.h>

static void *redis_zmq_context = NULL;
static void *redis_zmq_socket = NULL;
static zmq_msg_t redis_zmq_msg;

unsigned int redis_zmq_num_endpoints = 0;
char **redis_zmq_endpoints = NULL;
unsigned int redis_zmq_hwm = 0;

/* 0MQ callback for freeing the msg buffer */
void my_msg_free (void *data, void *hint)
{
    hint = hint; /* silence warning */
    zfree(data);
}


/* Attempts to set up context, socket, connection. Don't call unless
 * redis_zmq_num_endpoints is not 0. */
static void redis_zmq_init() {
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
        redis_zmq_socket = zmq_socket(redis_zmq_context, ZMQ_PUB);
        if (redis_zmq_socket == NULL) {
            redisLog(REDIS_WARNING,"Failed to init 0MQ socket");
        }
        else {
            /* printf("SOCKET CREATED\n"); */
            for (iendpoint = 0; iendpoint < redis_zmq_num_endpoints; ++iendpoint) {
                status = zmq_connect(redis_zmq_socket, redis_zmq_endpoints[iendpoint]);
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
    char *buf, *buf_ptr;
    uint32_t val_len, key_len, rc;
    robj *val;

    /* Abuse endpoint setting to see whether we need to send
     * expiry messages at all. */
    if (redis_zmq_num_endpoints == 0)
        return;

    val = lookupKey(db, key); /* FIXME this updates expire time... Silly. *gnash teeth* */

    /* We only support dispatching expiry messages on STRING values for now. */
    /* Technically, it would be vastly more elegant to reuse the actual Redis protocol for
     * transmitting this information, but for my nefarious purposes, this is good enough.
     * Also, the Redis functions that encode Redis data structures for output appear
     * to like writing to a global buffer. Probably missed something obvious in my
     * sleep-deprived stupor. */
    if (val == NULL || val->type != REDIS_STRING)
        return;

    /* Set up context, socket, and connection. */
    redis_zmq_init();

    if (redis_zmq_socket == NULL)
    return;
    if (key->encoding == REDIS_ENCODING_INT)
        key = getDecodedObject(key);
    if (val->encoding == REDIS_ENCODING_INT)
        val = getDecodedObject(val);

    if (key->encoding == REDIS_ENCODING_RAW) {
        key_len = sdslen(key->ptr);
    } else {
        return; /* normally, panic */
    }
    if (val->encoding == REDIS_ENCODING_RAW) {
        val_len = sdslen(val->ptr);
    } else {
        return; /* normally, panic */
    }

    /* malloc */
    buf = zmalloc(sizeof(uint32_t) + key_len + sizeof(uint32_t) +val_len);
    buf_ptr = buf;

    /* copy */
    /* keylen, key */
    memcpy(buf_ptr, &key_len, sizeof(uint32_t));
    buf_ptr += sizeof(uint32_t);
    memcpy(buf_ptr, key->ptr, key_len);
    buf_ptr += key_len;

    /* value len, value */
    memcpy(buf_ptr, &val_len, sizeof(uint32_t));
    buf_ptr += sizeof(uint32_t);
    memcpy(buf_ptr, val->ptr, val_len);
    buf_ptr += val_len;

    /* 0MQ takes ownership of our buffer. */
    rc = zmq_msg_init_data(&redis_zmq_msg, buf, buf_ptr-buf, my_msg_free, NULL);
    if (rc != 0) {
        redisLog(REDIS_WARNING,"Failed to init 0MQ msg with data! Dropping message!");
        return; /* PANIC! */
    }

    /* ... and finally dispatch the message ... */
    zmq_send(redis_zmq_socket, &redis_zmq_msg, 0);
    /* zmq_send(redis_zmq_socket, &redis_zmq_msg, ZMQ_NOBLOCK); */

    return;
}

