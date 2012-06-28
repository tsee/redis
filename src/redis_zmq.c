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

uint32_t redis_zmq_main_db_num = 0; /* FIXME this is just very wrong */
uint32_t redis_zmq_expire_loop_db_num = 1; /* FIXME this is just very wrong */

#define MAIN_DB (&server.db[redis_zmq_main_db_num])
#define EXPIRE_DB (&server.db[redis_zmq_expire_loop_db_num])

static inline long long redis_zmq_expire_loop_expire_time() {
    return(
        mstime()
        + redis_zmq_hash_expire_delay_ms
        + (long long)(
            ((double)random() / (double)RAND_MAX)
            * (double)redis_zmq_hash_expire_delay_jitter_ms
        )
    );
}

/* makes static void zeromqSendObject(redisDb *db, robj *key, robj *val) available */
#include "redis_zmq_snd_msg.h"


/* Attempts to set up context, socket, connection. Don't call unless
 * redis_zmq_num_endpoints is not 0. */
void redis_zmq_init() {
    int status;
    unsigned int iendpoint;

    if (redis_zmq_context == NULL) {
        redisLog(REDIS_VERBOSE, "Initializing 0MQ for expiration.");
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
 * Handles only Redis "scalar" string values and hashes!
 */
/* Returns 0 if the key is not to be deleted after all */
int dispatchExpirationMessage(redisDb *db, robj *key) {
    robj *val;

    /* Abuse endpoint setting to see early whether we need to send
     * expiration messages at all. */
    if (redis_zmq_num_endpoints == 0)
        return 1;

    val = lookupKey(db, key);

    /* Technically, it would be vastly more elegant to reuse the actual Redis protocol for
     * transmitting this information, but for my nefarious purposes, this is good enough
     * AND likely more efficient.
     * Also, the Redis functions that encode Redis data structures for output appear
     * to like writing to a global buffer. Probably missed something obvious in my
     * sleep-deprived stupor. */
    if ( val == NULL
         || (val->type != REDIS_STRING && val->type != REDIS_HASH) )
        return 1;

    /* Set up context, socket, and connection. */
    redis_zmq_init();

    /* 0MQ init failed, should have been reported already. Soft fail. */
    if (redis_zmq_socket == NULL)
        return 1;

    /* Actually send the initial object dump now if it's a hash. */
    redisLog(REDIS_DEBUG, "Detected expiration of key with type %s", (val->type == REDIS_STRING ? "string" : "hash"));
    if (val->type == REDIS_HASH) {
        /* Only sending messages for hashes because that's what *I* need. */
        zeromqSendObject(db, key, val);
    }

    /* Check whether we need to cycle the key back to the db and do so
     * if necessary. */
    if (val->type == REDIS_HASH
        && redis_zmq_hash_max_expire_cycles != 0
        && db == MAIN_DB)
    {
        /* This, by definition, is the first time the hash-type value expires. That is
         * because after they expired once from the main db, hash-type values have their
         * expiration removed.
         * Here's what needs to happen in this case:
         * - get rid of expiration for the hash-type value
         * - mark the hash-type value as having expired in the past
         *   (Right now, this be determined form the lack of an expire)
         * - insert key => count (==1) into the expire db
         * - set expiration on that
         * - return 0 from this function to avoid having the key delete.
         *
         * If we weren't using the expiration of the key itself as a marker to say
         * whether the key needs externally-triggered expiration, then we'd need to
         * assert that the marker about previous expiration gets removed on writes.
         */
        robj *cnt;

        /* Expiration now controlled by key in expire-db.
         * The lack of an expiration on a hash-type value will be used by the
         * expiration of the key in the expire-db to decide whether a given key
         * has been written to. There may be a very tiny race condition in this,
         * but it would not be of catastrophic consequences.
         */
        removeExpire(db, key);

        /* debug output only */
        if (1) {
            robj *k = getDecodedObject(key);
            redisLog(REDIS_DEBUG, "HASH: First expiration for key ('%s')", k->ptr);
            decrRefCount(k);
        }

        cnt = createStringObjectFromLongLong(1); /* Could also try createStringObject("1", 1) */
        /* TODO check whether we can use the same key robj* for another database */
        /* Create key in expire loop database, let that expire */
        setKey(EXPIRE_DB, key, cnt);
        setExpire(EXPIRE_DB,
                  key,
                  redis_zmq_expire_loop_expire_time());
        decrRefCount(cnt);

        return 0;
    }
    else if (val->type == REDIS_STRING
             && redis_zmq_hash_max_expire_cycles != 0
             && db == EXPIRE_DB)
    {
        /* We enter this branch if we're expiring a string key from the expire-loop db.
         * This conceptually happens as the second or later time that a record expires
         * from the database since we inserted it into the expire-db only after it
         * expired from the main db once.
         * In this branch, we have to:
         *   - check whether the main hash-type value has been rewritten since it first expired.
         *     => if so, drop this key from expire db and move on.
         *     => TODO Does this need an explicit marker or does it work to
         *        just check whether it has an expiration set?
         *   - dispatch a copy of the main-db hash-type value as a 0MQ msg
         *   - get number of times we've dispatched it including this time.
         *   - If we hit the expire-loop limit on it, delete from both dbs.
         *   - if not, increment count in expire-db.
         *   - reset expiration in expire-db
         */
        long long expireTime;
        robj *hashVal;
        robj *cnt;
        long long numTimesDispatched = 0;

        /* Just drop this key/value if the main db's key does not exist
         * or if it has no expiration set. */
        expireTime = getExpire(MAIN_DB, key);
        if (expireTime != -1) {
            return 1; /* yes, delete from expire db */
        }

        /* Get hash-type value from main db. Just delete the currently-expiring
         * key/value if it's not a hash in the main db. */
        hashVal = lookupKey(MAIN_DB, key);
        if (hashVal == NULL || hashVal->type != REDIS_HASH) {
            robj *k = getDecodedObject(key);
            redisLog(REDIS_DEBUG, "Expire cycle for key '%s' - key not found or not a hash in main db", k->ptr);
            decrRefCount(k);
            return 1; /* yes, delete from expire db */
        }

        /* Send copy of hash-type value from main db to expire workers. */
        zeromqSendObject(MAIN_DB, key, hashVal);

        /* Get number of expire cycles elapsed */
        if (REDIS_OK != getLongLongFromObject(val, &numTimesDispatched)) {
            robj *k = getDecodedObject(key);
            redisLog(REDIS_WARNING, "Expire db appears to contain a value (for key '%s')"
                                    " that is not a number. This is very unexpected.", k->ptr);
            decrRefCount(k);
            return 1; /* yes, delete from expire db */
        }

        /* Cycled enough. Ditch main db key and expire db key. */
        if (numTimesDispatched >= redis_zmq_hash_max_expire_cycles) {
            /* FIXME will this be propagated to slaves? */
            dbDelete(MAIN_DB, key);
            return 1; /* yes, delete from expire db */
        }

        /* Increment expire/message count in expire db, reset expiration */
        ++numTimesDispatched;
        cnt = createStringObjectFromLongLong(numTimesDispatched);
        dbDelete(EXPIRE_DB, key);
        setKey(EXPIRE_DB, key, cnt);
        decrRefCount(cnt);
        setExpire(EXPIRE_DB, key, redis_zmq_expire_loop_expire_time());
        return 0; /* no, do not delete from expire db */
    }

    /* Anything else can just be deleted. */
    return 1;
}

