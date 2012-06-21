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



static int getValueFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    redisAssert(o->encoding == REDIS_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return -1;
    *value = dictGetVal(de);
    return 0;
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
            long long vll = LLONG_MAX;
            char buf[64];
            int ret;

            unsigned char *fptr;
            unsigned char *vptr = NULL;
            zl = o->ptr;

            fptr = ziplistIndex(zl, ZIPLIST_HEAD);
            if (fptr != NULL) {
                fptr = ziplistFind(fptr, (unsigned char *)"_expire_cycles", 14, 1);
                if (fptr != NULL) {
                    /* Grab pointer to the value (fptr points to the field) */
                    vptr = ziplistNext(zl, fptr);
                    redisAssert(vptr != NULL);
                }
            }

            if (vptr != NULL) {
                ret = ziplistGet(vptr, &val, &vlen, &vll);
                redisAssert(ret);
                nexpirecycles = val ? atoll((char *)val) : vll;
            }

            /* increment num. expire cycles */
            if (nexpirecycles < redis_zmq_hash_max_expire_cycles) {
                sprintf(buf, "%u", (unsigned int)(nexpirecycles+1));
                if (nexpirecycles > 0) { /* already have a setting */
                    zl = ziplistDelete(zl, &vptr);
                    zl = ziplistInsert(zl, vptr, (unsigned char *)buf, strlen(buf));
                }
                else { /* need to set completely new key */
                    /* Push new field/value pair onto the tail of the ziplist */
                    zl = ziplistPush(zl, (unsigned char *)"_expire_cycles", 14, ZIPLIST_TAIL);
                    zl = ziplistPush(zl, (unsigned char *)buf, strlen(buf), ZIPLIST_TAIL);
                }
                o->ptr = zl;

                need_reset_expire = 1;
            }

        } else if (o->encoding == REDIS_ENCODING_HT) {
            unsigned char *val = NULL;
            robj *val2 = NULL;
            robj *name = createStringObject("_expire_cycles", 14);

            getValueFromHashTable(o, name, &val2);
            if (val2 != NULL)
                getLongLongFromObject(val2, &nexpirecycles);

            if (val != NULL) {
                redisAssert( getLongLongFromObject(val2, &nexpirecycles) );
            }
            if (nexpirecycles < redis_zmq_hash_max_expire_cycles) {
                robj *newval = createStringObjectFromLongLong(nexpirecycles+1);
                hashTypeSet(o, name, newval);
                decrRefCount(newval);
                need_reset_expire = 1;
            }

            decrRefCount(name);
        } else {
            redisPanic("Unknown hash encoding");
        }
    }

    if (need_reset_expire != 0) {
        robj *cnt = createStringObjectFromLongLong(nexpirecycles+1);
        redisLog(REDIS_DEBUG, "need_reset_expire = 1 => setting expire loop key to %u", nexpirecycles+1);

        /* Create key in expire loop database, let that expire */
        setKey(EXPIRE_DB, key, cnt);
        setExpire(EXPIRE_DB,
                  key,
                  redis_zmq_expire_loop_expire_time());
        /* main key no longer needs to expire */
        removeExpire(MAIN_DB, key);
        decrRefCount(cnt);
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

    redisLog(REDIS_DEBUG, "Sending Expire message for %s", (val->type == REDIS_STRING ? "string" : "hash"));
    if (val->type == REDIS_HASH) {
        /* only send messages for hashes because that's what *I* need */
        zeromqSendObject(db, key, val);
        removeExpire(db, key);
    }

    /* Check whether we need to cycle the key back to the db and do so
     * if necessary. */
    if (val->type == REDIS_HASH
        && redis_zmq_hash_max_expire_cycles != 0)
    {
        int ncycles_prev = redis_zmq_check_expire_cycles(db, key, val)+1;
        robj *k = getDecodedObject(key);
        redisLog(REDIS_DEBUG, "HASH: Current expire cycles for key ('%s'): %i in db %u (main db %u, expire db %u)", k->ptr, ncycles_prev, db, MAIN_DB, EXPIRE_DB);
        decrRefCount(k);

        /* Do not delete if we haven't hit the cycle limit yet */
        if (ncycles_prev < redis_zmq_hash_max_expire_cycles) {
            return 0;
        }
        else {
            /* delete from expire loop db, too */
            redisLog(REDIS_DEBUG, "Deleting from expire loop db");
            dbDelete(EXPIRE_DB, key);
        }
    }
    else if (val->type == REDIS_STRING
             && redis_zmq_hash_max_expire_cycles != 0
             && db == EXPIRE_DB)
    {
        /* We enter this branch if we're expiring a string key from the expire-loop db.
         * We need to check whether the number of expire cycles above the limit. If so,
         * we check whether there's a corresponding key in the main db. Again, if so,
         * then we can dispatch an expire message for that key in the main db. */

        /* Get elapsed expire cycles */
        long long ncycles_prev = 0;
        dictEntry *de = dictFind(db->dict, key->ptr);
        if (de) {
            robj *val = dictGetVal(de);
            if (getLongLongFromObject(val, &ncycles_prev)) {
                redisLog(REDIS_WARNING, "Found non-integer value in expire loop db. This is a critical failure.");
            }
        } else {
            return 1;
        }
        ++ncycles_prev;
        robj *k = getDecodedObject(key);
        redisLog(REDIS_DEBUG, "STRING: Current expire cycles for key ('%s'): %i in db %u (main db %u, expire db %u)", k->ptr, ncycles_prev, db, MAIN_DB, EXPIRE_DB);
        decrRefCount(k);

        /* dispatch expiration message for key in main db */
        /* FIXME can we "repurpose" the key this way for another db (same key name)? */
        { /* scope */
            robj *val;
            redisDb *db;
            db = MAIN_DB;
            val = lookupKey(db, key); /* FIXME this updates expire time? Silly. *gnash teeth* */

            if ( val != NULL && val->type == REDIS_HASH) {
                redisLog(REDIS_DEBUG, "Sending Expire message for %s", (val->type == REDIS_STRING ? "string" : "hash"));
                zeromqSendObject(db, key, val);
                removeExpire(db, key);
            }
        } /* end scope */

        /* Do not delete if we haven't hit the cycle limit yet */
        if (ncycles_prev < redis_zmq_hash_max_expire_cycles) {
            /* Re-set the expire key */
            robj *cnt = createStringObjectFromLongLong(ncycles_prev);
            dbDelete(EXPIRE_DB, key);
            setKey(EXPIRE_DB, key, cnt);
            decrRefCount(cnt);
            setExpire(EXPIRE_DB, key, redis_zmq_expire_loop_expire_time());
            return 0;
        }
        else {
            /* Nuke from the main db */
            redisLog(REDIS_DEBUG, "Deleting key from main db");
            //removeExpire(MAIN_DB, key);
            dbDelete(MAIN_DB, key);
        }
    }

    return 1;
}

