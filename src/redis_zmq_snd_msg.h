/* only to be included from src/redis_zmq.c */

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


static int zeromqSendMsgFragment(char *str, size_t len, int flags, char *on_error) {
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


static void zeromqSendObject(redisDb *db, robj *key, robj *val) {
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

    rc = zeromqSendMsgFragment((char *)header,
                               (size_t)4,
                               ZMQ_SNDMORE,
                               "Could not send header: %s");
    rc = zeromqSendMsgFragment((char *)keystr.io.buffer.ptr,
                               (size_t)sdslen(keystr.io.buffer.ptr),
                               ZMQ_SNDMORE,
                               "Could not send key: %s");
    rc = zeromqSendMsgFragment((char *)payload.io.buffer.ptr,
                               (size_t)sdslen(payload.io.buffer.ptr),
                               0,
                               "Could not send payload: %s");

    sdsfree(keystr.io.buffer.ptr);
    sdsfree(payload.io.buffer.ptr);
/*    if (rc_db != -1 && rc_event != -1 && rc_key != -1 && rc_val != -1)
        server.stat_zeromq_events++;
*/
}

