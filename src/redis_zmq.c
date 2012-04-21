#include "redis_zmq.h"
#include "sds.h"

#include <zmq.h>

static void *redis_zmq_context = NULL;
static void *redis_zmq_socket = NULL;
/* static zmq_msg_t redis_zmq_msg; */

unsigned int redis_zmq_num_endpoints = 0;
char **redis_zmq_endpoints = NULL;
uint64_t redis_zmq_hwm = 0;

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
    int rc_db, rc_event, rc_key, rc_val;
    /* char event[2]; */
    char db_num[2];
    rio payload;

    /* sprintf(event, "%d", key_event); */
    sprintf(db_num, "%d", db->id);

    if (val) {
        rioInitWithBuffer(&payload,sdsempty());
        redisAssertWithInfo(NULL, val,rdbSaveObjectType(&payload,val));
        redisAssertWithInfo(NULL, val,rdbSaveObject(&payload,val) != -1);
    }

    rc_db = zeromqSend(db_num, (size_t)2, ZMQ_SNDMORE, "Could not send DB num: %s");
    rc_key = zeromqSend((char *)key->ptr, (size_t)sdslen(key->ptr), (val? ZMQ_SNDMORE : 0), "Could not send key: %s");
    rc_val = zeromqSend((char *)payload.io.buffer.ptr, (size_t)sdslen(payload.io.buffer.ptr), 0, "Could not send payload: %s");

    if (val)
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

