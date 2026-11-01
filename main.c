#include <cjson/cJSON.h>
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/time.h>
#include <unistd.h>

#include "types.h"

#define MAX_NODES 128
#define MAX_NODE_NAME_LEN 16
#define MAX_PENDING 256
#define PENDING_READ_TIMEOUT_MS 5000

#define MAX_LOGS 1024
#define MAX_LOG_KEY_LEN 16
#define MAX_LOG_KEY_LEN 16
#define LOG_RING_BUF_SIZE 128

_Static_assert((LOG_RING_BUF_SIZE & (LOG_RING_BUF_SIZE - 1)) == 0,
               "LOG_RING_BUF_SIZE must be a power of two");

#define MAX_KV_ENTRIES 4096
#define MAX_KV_KEY_LEN 16
#define KV_BUF_SIZE 1024

static const u8 ERR_MALFORMED_INIT[] = "malformed init message";
static const u8 ERR_NODE_ID_TOO_LONG[] = "node id too long";
static const u8 ERR_ECHO_TYPE[] = "incorrect echo type";
static const u8 ERR_GENERATE_BEFORE_INIT[] = "generate received before init";
static const u8 ERR_GENERATE_ID[] = "could not build generate id";
static const u8 ERR_BROADCAST_FULL[] = "broadcast buffer full";
static const u8 ERR_BROADCAST_TYPE[] = "incorrect broadcast type";
static const u8 ERR_TOO_MANY_PENDING_READS[] = "too many pending reads";
static const u8 ERR_TOPOLOGY_TYPE[] = "incorrect topology type";
static const u8 ERR_INVALID_ADD[] = "invalid add msg";
static const u8 ERR_TOO_MANY_PENDING_ADDS[] = "too many pending adds";
static const u8 ERR_MALFORMED_SEND[] = "malformed send message";
static const u8 ERR_KEY_TOO_LONG[] = "key too long";
static const u8 ERR_TOO_MANY_LOGS[] = "can not support more logs";
static const u8 ERR_LOG_FULL[] = "ran out of buffer space";
static const u8 ERR_MALFORMED_POLL[] = "malformed poll request";
static const u8 ERR_INVALID_READ[] = "invalid read";
static const u8 ERR_MALFORMED_COMMIT[] = "malformed commit msg";
static const u8 ERR_MALFORMED_LIST[] = "malformed list committed msg";
static const u8 ERR_KV_FULL[] = "kv table full";
static const u8 ERR_MALFORMED_TXN[] = "malformed txn msg";
static const u8 ERR_NOT_SUPPORTED[] = "not supported";
static const u8 ERR_TIMEOUT[] = "timeout";

typedef struct {
    i32 msg_vals[4096];
    usize size;
    usize cap;
} BroadcastMessages;

typedef struct {
    bool used;
    u64 kv_msg_id;
    u64 client_msg_id;
    u8 client_src[MAX_NODE_NAME_LEN];
} PendingKVAdd;

static PendingKVAdd pending_adds[MAX_PENDING];

static PendingKVAdd *find_pending_add(u64 kv_msg_id) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (pending_adds[i].used && pending_adds[i].kv_msg_id == kv_msg_id) {
            return &pending_adds[i];
        }
    }
    return NULL;
}

typedef struct {
    bool used;
    u64 client_msg_id;
    u8 client_src[MAX_NODE_NAME_LEN];
    u64 kv_read_msg_ids[MAX_NODES];
    usize kv_count;
    usize remaining;
    i64 sum;
    i64 deadline_ms;
} PendingKVReadAll;

static PendingKVReadAll pending_kv_read_alls[MAX_PENDING];

static PendingKVReadAll *alloc_pending_kv_read_all(void) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!pending_kv_read_alls[i].used) {
            return &pending_kv_read_alls[i];
        }
    }
    return NULL;
}

static PendingKVReadAll *find_pending_kv_read_all(u64 kv_msg_id) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!pending_kv_read_alls[i].used) {
            continue;
        }
        for (usize j = 0; j < pending_kv_read_alls[i].kv_count; ++j)
            if (pending_kv_read_alls[i].kv_read_msg_ids[j] == kv_msg_id) {
                return &pending_kv_read_alls[i];
            }
    }
    return NULL;
}

typedef struct {
    u8 keys[MAX_LOGS][MAX_LOG_KEY_LEN];
    i64 logs[MAX_LOGS][LOG_RING_BUF_SIZE];
    usize next_log_idx;
    u64 next_log_offsets[MAX_LOGS];
    u64 uncomitted_offset_start[MAX_LOGS];
} LogBuffers;

static i64 find_log_idx_by_key(LogBuffers *lb, const u8 *key) {
    for (usize i = 0; i < lb->next_log_idx; ++i) {
        if (!strncmp((const char *)key, (const char *)lb->keys[i], MAX_LOG_KEY_LEN)) {
            return (i64)i;
        }
    }
    return -1;
}

typedef struct {
    i64 keys[MAX_KV_ENTRIES];
    i64 vals[MAX_KV_ENTRIES];
    usize empty_idx;
} KVStore;

static i64 find_kv_entry_idx_by_key(KVStore *kvs, i64 key) {
    for (usize i = 0; i < kvs->empty_idx; ++i) {
        if (kvs->keys[i] == key) {
            return (i64)i;
        }
    }
    return -1;
}

static i64 now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (i64)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Prints a message and stops the program. Use only when a reply cannot be built.
static _Noreturn void fatal(const char *message) {
    fprintf(stderr, "fatal: %s\n", message);
    exit(EXIT_FAILURE);
}

// Builds a reply body with the fields every reply needs.
static cJSON *reply_body(u64 msg_id, const u8 *type, u64 in_reply_to) {
    cJSON *body = cJSON_CreateObject();
    if (!body) {
        fatal("could not create reply body");
    }
    if (!cJSON_AddStringToObject(body, "type", (const char *)type) ||
        !cJSON_AddNumberToObject(body, "msg_id", (double)msg_id) ||
        !cJSON_AddNumberToObject(body, "in_reply_to", (double)in_reply_to)) {
        cJSON_Delete(body);
        fatal("could not build reply body");
    }
    return body;
}

// Wraps a body into an envelope and writes it to stdout.
static void send(const u8 *src, const u8 *dest, cJSON *body) {
    cJSON *res = cJSON_CreateObject();
    if (!res) {
        cJSON_Delete(body);
        fatal("could not create reply envelope");
    }
    if (!cJSON_AddStringToObject(res, "src", (const char *)src) ||
        !cJSON_AddStringToObject(res, "dest", (const char *)dest) ||
        !cJSON_AddItemToObject(res, "body", body)) {
        cJSON_Delete(res);
        cJSON_Delete(body);
        fatal("could not build reply envelope");
    }

    char *out = cJSON_PrintUnformatted(res);
    if (!out) {
        cJSON_Delete(res);
        fatal("could not serialize reply");
    }
    if (printf("%s\n", out) < 0 || fflush(stdout) != 0) {
        free(out);
        cJSON_Delete(res);
        fatal("could not write reply");
    }

    free(out);
    cJSON_Delete(res);
}

static void send_error(const u8 *src, const u8 *dest, u64 msg_id, u64 in_reply_to, i32 code,
                       const u8 *text) {
    cJSON *body = reply_body(msg_id, (const u8 *)"error", in_reply_to);
    if (!cJSON_AddNumberToObject(body, "code", code) ||
        !cJSON_AddStringToObject(body, "text", (const char *)text)) {
        cJSON_Delete(body);
        fatal("could not build error reply");
    }
    send(src, dest, body);
}

// Answers expired pending reads with a timeout error and releases their slots.
static void reap_pending_reads(i64 now, const u8 *node_id, u64 *next_msg_id) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        PendingKVReadAll *pr = &pending_kv_read_alls[i];
        if (!pr->used || now < pr->deadline_ms) {
            continue;
        }
        send_error(node_id, pr->client_src, (*next_msg_id)++, pr->client_msg_id, 0, ERR_TIMEOUT);
        pr->used = false;
    }
}

// Returns the poll timeout for the earliest pending read, or -1 when none is pending.
static int pending_read_timeout_ms(i64 now) {
    bool found = false;
    i64 earliest = 0;
    for (usize i = 0; i < MAX_PENDING; ++i) {
        const PendingKVReadAll *pr = &pending_kv_read_alls[i];
        if (!pr->used) {
            continue;
        }
        if (!found || pr->deadline_ms < earliest) {
            earliest = pr->deadline_ms;
            found = true;
        }
    }
    if (!found) {
        return -1;
    }
    i64 remaining = earliest - now;
    if (remaining <= 0) {
        return 0;
    }
    if (remaining > INT_MAX) {
        return INT_MAX;
    }
    return (int)remaining;
}

static PendingKVAdd *alloc_pending_read(void) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!pending_adds[i].used) {
            return &pending_adds[i];
        }
    }
    return NULL;
}

int main(void) {
    u8 NODE_ID[MAX_NODE_NAME_LEN];
    bool node_id_set = false;

    u8 NODE_IDS[MAX_NODES][MAX_NODE_NAME_LEN];
    usize next_node_idx = 0;

    i64 PREV_GENERATE_TIME_MS = now_ms();
    i32 GENERATE_MS_COUNTER = 0;

    const usize JSON_PARSE_BUF_LEN = 65536;

    u8 msg[JSON_PARSE_BUF_LEN];
    u64 next_msg_id = 1;

    BroadcastMessages bd_msg = {{0}, 0, 4096};

    i64 counter = 0;

    LogBuffers *lb = calloc(1, sizeof *lb);
    if (!lb) {
        fatal("could not allocate log buffers");
    }

    KVStore *kvs = calloc(1, sizeof *kvs);
    if (!kvs) {
        fatal("could not allocate kv buffer");
    }

    if (setvbuf(stdin, NULL, _IONBF, 0) != 0) {
        fatal("could not disable stdin buffering");
    }

    for (;;) {
        i64 loop_now = now_ms();
        reap_pending_reads(loop_now, NODE_ID, &next_msg_id);

        struct pollfd pfd = {.fd = STDIN_FILENO, .events = POLLIN};
        int ready = poll(&pfd, 1, pending_read_timeout_ms(loop_now));
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            fatal("could not poll stdin");
        }
        if (ready == 0) {
            continue;
        }

        if (!fgets((char *)msg, (i32)sizeof msg, stdin)) {
            break;
        }
        cJSON *msg_json = cJSON_Parse((const char *)msg);

        if (!msg_json) {
            fprintf(stderr, "couldn't parse msg: %s\n", (const char *)msg);
            continue;
        }

        if (!cJSON_IsObject(msg_json)) {
            fprintf(stderr, "message is not a JSON object\n");
            cJSON_Delete(msg_json);
            continue;
        }

        const cJSON *src, *dest, *body, *type, *msg_id;

        if (!cJSON_HasObjectItem(msg_json, "src") || !cJSON_HasObjectItem(msg_json, "dest") ||
            !cJSON_HasObjectItem(msg_json, "body")) {
            fprintf(stderr, "field/s src, dest or body not found\n");
            cJSON_Delete(msg_json);
            continue;
        }

        src = cJSON_GetObjectItem(msg_json, "src");
        dest = cJSON_GetObjectItem(msg_json, "dest");
        body = cJSON_GetObjectItem(msg_json, "body");
        if (!cJSON_IsString(src) || src->valuestring == NULL || !cJSON_IsString(dest) ||
            dest->valuestring == NULL || !cJSON_IsObject(body)) {
            fprintf(stderr, "incorrect type for src, dest or body\n");
            cJSON_Delete(msg_json);
            continue;
        }

        if (strlen(src->valuestring) >= MAX_NODE_NAME_LEN ||
            strlen(dest->valuestring) >= MAX_NODE_NAME_LEN) {
            fprintf(stderr, "src or dest too long\n");
            cJSON_Delete(msg_json);
            continue;
        }

        type = cJSON_GetObjectItem(body, "type");
        if (!cJSON_IsString(type) || type->valuestring == NULL) {
            fprintf(stderr, "field/s type not found or not a string\n");
            cJSON_Delete(msg_json);
            continue;
        }

        if (!strcmp(type->valuestring, "read_ok")) {
            if (!cJSON_HasObjectItem(body, "value")) {
                cJSON_Delete(msg_json);
                continue;
            }
            cJSON *value_ptr = cJSON_GetObjectItem(body, "value");
            if (!cJSON_IsNumber(value_ptr)) {
                cJSON_Delete(msg_json);
                continue;
            }

            double value = value_ptr->valuedouble;

            cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
            if (!cJSON_IsNumber(reply_ptr)) {
                cJSON_Delete(msg_json);
                continue;
            }
            u64 in_reply_to = (u64)reply_ptr->valuedouble;

            PendingKVReadAll *pr = find_pending_kv_read_all(in_reply_to);
            if (!pr) {
                cJSON_Delete(msg_json);
                continue;
            }

            pr->sum += (i64)value;
            if (--pr->remaining == 0) {
                cJSON *res = reply_body(next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
                if (!cJSON_AddNumberToObject(res, "value", (double)pr->sum)) {
                    cJSON_Delete(res);
                    fatal("could not build read reply");
                }
                send(NODE_ID, pr->client_src, res);
                pr->used = 0;
            }

            cJSON_Delete(msg_json);
            continue;
        }

        if (!strcmp(type->valuestring, "write_ok")) {
            cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
            if (!cJSON_IsNumber(reply_ptr)) {
                cJSON_Delete(msg_json);
                continue;
            }
            u64 in_reply_to = (u64)reply_ptr->valuedouble;

            PendingKVAdd *p = find_pending_add(in_reply_to);
            if (p) {
                cJSON *res = reply_body(next_msg_id++, (const u8 *)"add_ok", p->client_msg_id);
                send(NODE_ID, p->client_src, res);
                p->used = 0;
            }

            cJSON_Delete(msg_json);
            continue;
        }

        if (!strcmp(type->valuestring, "error")) {
            cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
            if (!cJSON_IsNumber(reply_ptr)) {
                cJSON_Delete(msg_json);
                continue;
            }
            u64 in_reply_to = (u64)reply_ptr->valuedouble;

            const cJSON *code_ptr = cJSON_GetObjectItem(body, "code");
            i32 code = cJSON_IsNumber(code_ptr) ? code_ptr->valueint : -1;

            if (code == 20) {
                PendingKVReadAll *pr = find_pending_kv_read_all(in_reply_to);
                if (pr && --pr->remaining == 0) {
                    cJSON *res =
                        reply_body(next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
                    if (!cJSON_AddNumberToObject(res, "value", (double)pr->sum)) {
                        cJSON_Delete(res);
                        fatal("could not build read reply");
                    }
                    send(NODE_ID, pr->client_src, res);
                    pr->used = 0;
                }
            }

            cJSON_Delete(msg_json);
            continue;
        }

        msg_id = cJSON_GetObjectItem(body, "msg_id");
        if (!cJSON_IsNumber(msg_id)) {
            fprintf(stderr, "incorrect type for type or msg_id\n");
            cJSON_Delete(msg_json);
            continue;
        }
        u64 in_reply_to = (u64)msg_id->valuedouble;

        if (!strcmp(type->valuestring, "init")) {
            if (!cJSON_HasObjectItem(body, "node_id") || !cJSON_HasObjectItem(body, "node_ids")) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_INIT);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *node_id = cJSON_GetObjectItem(body, "node_id"),
                  *node_ids = cJSON_GetObjectItem(body, "node_ids");

            if (!cJSON_IsString(node_id) || node_id->valuestring == NULL ||
                !cJSON_IsArray(node_ids)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_INIT);
                cJSON_Delete(msg_json);
                continue;
            }

            usize node_id_len = strlen(node_id->valuestring);
            if (node_id_len >= sizeof NODE_ID) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 12, ERR_NODE_ID_TOO_LONG);
                cJSON_Delete(msg_json);
                continue;
            }

            bool node_ids_too_long = false;
            cJSON *it = NULL;
            cJSON_ArrayForEach(it, node_ids) {
                if (!cJSON_IsString(it) || it->valuestring == NULL) {
                    continue;
                }
                if (strlen(it->valuestring) >= MAX_NODE_NAME_LEN) {
                    node_ids_too_long = true;
                    break;
                }
            }
            if (node_ids_too_long) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 12, ERR_NODE_ID_TOO_LONG);
                cJSON_Delete(msg_json);
                continue;
            }

            memcpy(NODE_ID, node_id->valuestring, node_id_len + 1);
            node_id_set = true;

            cJSON_ArrayForEach(it, node_ids) {
                if (!cJSON_IsString(it) || it->valuestring == NULL) {
                    continue;
                }
                if (next_node_idx >= MAX_NODES) {
                    break;
                }
                usize name_len = strlen(it->valuestring);
                memcpy(NODE_IDS[next_node_idx], it->valuestring, name_len + 1);
                ++next_node_idx;
            }

            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"init_ok", in_reply_to);
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "echo")) {
            const cJSON *echo = cJSON_GetObjectItem(body, "echo");
            if (!cJSON_IsString(echo) || echo->valuestring == NULL) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 13, ERR_ECHO_TYPE);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"echo_ok", in_reply_to);
            if (!cJSON_AddStringToObject(res_body, "echo", echo->valuestring)) {
                cJSON_Delete(res_body);
                fatal("could not build echo reply");
            }
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);

        } else if (!strcmp(type->valuestring, "generate")) {
            if (!node_id_set) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_GENERATE_BEFORE_INIT);
                cJSON_Delete(msg_json);
                continue;
            }

            i64 now = now_ms();
            if (now == PREV_GENERATE_TIME_MS) {
                if ((GENERATE_MS_COUNTER + 1) >= (1 << 12)) {
                    do {
                        now = now_ms();
                    } while (PREV_GENERATE_TIME_MS == now);
                    PREV_GENERATE_TIME_MS = now;
                    GENERATE_MS_COUNTER = 0;
                } else {
                    GENERATE_MS_COUNTER++;
                }
            } else {
                PREV_GENERATE_TIME_MS = now;
                GENERATE_MS_COUNTER = 0;
            }
            u8 id[64];
            int id_len = snprintf((char *)id, sizeof id, "%s-%lld-%d", (const char *)NODE_ID,
                                  (long long)now, (int)GENERATE_MS_COUNTER);
            if (id_len < 0 || (usize)id_len >= sizeof id) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 13, ERR_GENERATE_ID);
                cJSON_Delete(msg_json);
                continue;
            }
            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"generate_ok", in_reply_to);
            if (!cJSON_AddStringToObject(res_body, "id", (const char *)id)) {
                cJSON_Delete(res_body);
                fatal("could not build generate reply");
            }
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "broadcast")) {
            if (bd_msg.size >= bd_msg.cap) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 11, ERR_BROADCAST_FULL);
                cJSON_Delete(msg_json);
                continue;
            }

            if (!cJSON_HasObjectItem(body, "message")) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_BROADCAST_TYPE);
                cJSON_Delete(msg_json);
                continue;
            }

            const cJSON *msg_val = cJSON_GetObjectItem(body, "message");
            if (!cJSON_IsNumber(msg_val)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_BROADCAST_TYPE);
                cJSON_Delete(msg_json);
                continue;
            }

            bd_msg.msg_vals[bd_msg.size++] = msg_val->valueint;
            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"broadcast_ok", in_reply_to);
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "read")) {
            // cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"read_ok",
            // in_reply_to); cJSON *msg_arr = cJSON_AddArrayToObject(res_body,
            // "messages"); for (usize i = 0; i < bd_msg.size; ++i) {
            //   if (!cJSON_AddItemToArray(msg_arr,
            //   cJSON_CreateNumber((double)bd_msg.msg_vals[i])))
            //   {
            //     return 1;
            //   }
            // }
            // send_reply(dest->valuestring, src->valuestring, res_body);

            PendingKVReadAll *pr = alloc_pending_kv_read_all();
            if (!pr) {
                send_error(NODE_ID, (const u8 *)src->valuestring, next_msg_id++, in_reply_to, 11,
                           ERR_TOO_MANY_PENDING_READS);
                cJSON_Delete(msg_json);
                continue;
            }

            pr->used = true;
            pr->client_msg_id = in_reply_to;
            usize src_len = strlen(src->valuestring);
            memcpy(pr->client_src, src->valuestring, src_len + 1);
            pr->sum = 0;
            pr->remaining = next_node_idx;
            pr->kv_count = 0;
            pr->deadline_ms = now_ms() + PENDING_READ_TIMEOUT_MS;

            for (usize i = 0; i < next_node_idx; ++i) {
                cJSON *res_body = cJSON_CreateObject();
                if (!res_body) {
                    fatal("could not create read request");
                }
                u64 kv_id = next_msg_id++;

                if (!cJSON_AddStringToObject(res_body, "type", "read") ||
                    !cJSON_AddStringToObject(res_body, "key", (const char *)NODE_IDS[i]) ||
                    !cJSON_AddNumberToObject(res_body, "msg_id", (double)kv_id)) {
                    cJSON_Delete(res_body);
                    fatal("could not build read request");
                }

                pr->kv_read_msg_ids[pr->kv_count++] = kv_id;
                send(NODE_ID, (const u8 *)"seq-kv", res_body);
            }
        } else if (!strcmp(type->valuestring, "topology")) {
            if (!cJSON_HasObjectItem(body, "topology")) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_TOPOLOGY_TYPE);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"topology_ok", in_reply_to);
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "add")) {
            if (!cJSON_HasObjectItem(body, "delta")) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_INVALID_ADD);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *delta_obj = cJSON_GetObjectItem(body, "delta");
            if (!cJSON_IsNumber(delta_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_INVALID_ADD);
                cJSON_Delete(msg_json);
                continue;
            }

            PendingKVAdd *pa = alloc_pending_read();
            if (!pa) {
                send_error(NODE_ID, (const u8 *)src->valuestring, next_msg_id++, in_reply_to, 11,
                           ERR_TOO_MANY_PENDING_ADDS);
                cJSON_Delete(msg_json);
                continue;
            }

            pa->used = true;
            pa->kv_msg_id = next_msg_id++;
            pa->client_msg_id = in_reply_to;
            usize src_len = strlen(src->valuestring);
            memcpy(pa->client_src, src->valuestring, src_len + 1);

            counter += (i64)delta_obj->valuedouble;
            cJSON *res_body = cJSON_CreateObject();
            if (!res_body) {
                fatal("could not create write request");
            }
            if (!cJSON_AddStringToObject(res_body, "type", "write") ||
                !cJSON_AddStringToObject(res_body, "key", (const char *)NODE_ID) ||
                !cJSON_AddNumberToObject(res_body, "value", (double)counter) ||
                !cJSON_AddNumberToObject(res_body, "msg_id", (double)pa->kv_msg_id)) {
                cJSON_Delete(res_body);
                fatal("could not build write request");
            }
            send(NODE_ID, (const u8 *)"seq-kv", res_body);
        } else if (!strcmp(type->valuestring, "send")) {
            if (!cJSON_HasObjectItem(body, "key") || !cJSON_HasObjectItem(body, "msg")) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_SEND);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *key_obj = cJSON_GetObjectItem(body, "key"),
                  *msg_obj = cJSON_GetObjectItem(body, "msg");

            if (!cJSON_IsString(key_obj) || key_obj->valuestring == NULL ||
                !cJSON_IsNumber(msg_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_SEND);
                cJSON_Delete(msg_json);
                continue;
            }

            usize key_len = strlen(key_obj->valuestring);
            if (key_len >= MAX_LOG_KEY_LEN) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 12, ERR_KEY_TOO_LONG);
                cJSON_Delete(msg_json);
                continue;
            }

            i64 log_idx = find_log_idx_by_key(lb, (const u8 *)key_obj->valuestring);

            if (log_idx == -1) {
                if (lb->next_log_idx >= MAX_LOGS) {
                    send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                               next_msg_id++, in_reply_to, 11, ERR_TOO_MANY_LOGS);
                    cJSON_Delete(msg_json);
                    continue;
                }

                log_idx = (i64)lb->next_log_idx++;
                memcpy(lb->keys[log_idx], key_obj->valuestring, key_len + 1);
            }

            if (lb->next_log_offsets[log_idx] >= LOG_RING_BUF_SIZE) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 11, ERR_LOG_FULL);
                cJSON_Delete(msg_json);
                continue;
            }

            lb->logs[log_idx][lb->next_log_offsets[log_idx] & (LOG_RING_BUF_SIZE - 1)] =
                (i64)msg_obj->valuedouble;
            ++lb->next_log_offsets[log_idx];

            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"send_ok", in_reply_to);
            if (!cJSON_AddNumberToObject(res_body, "offset",
                                         (double)lb->next_log_offsets[log_idx] - 1)) {
                cJSON_Delete(res_body);
                fatal("could not build send reply");
            }

            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "poll")) {
            cJSON *offsets_obj = cJSON_GetObjectItem(body, "offsets");
            if (!cJSON_IsObject(offsets_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_POLL);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *offset_items = offsets_obj->child;

            cJSON *poll_res_obj = reply_body(next_msg_id++, (const u8 *)"poll_ok", in_reply_to);

            cJSON *msgs_obj = cJSON_AddObjectToObject(poll_res_obj, "msgs");
            if (!msgs_obj) {
                cJSON_Delete(poll_res_obj);
                fatal("could not build poll reply");
            }

            bool error_during_iterarion = false;
            cJSON *it = offset_items;
            while (it) {
                const char *key = it->string;
                if (key == NULL || !cJSON_IsNumber(it)) {
                    send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                               next_msg_id++, in_reply_to, 69, ERR_MALFORMED_POLL);
                    error_during_iterarion = true;
                    break;
                }
                const u64 offset = (u64)it->valuedouble;

                i64 log_idx = find_log_idx_by_key(lb, (const u8 *)key);
                if (log_idx == -1) {
                    it = it->next;
                    continue;
                }

                if (offset > lb->next_log_offsets[log_idx]) {
                    send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                               next_msg_id++, in_reply_to, 69, ERR_INVALID_READ);
                    error_during_iterarion = true;
                    break;
                }

                cJSON *log_res_arr = cJSON_AddArrayToObject(msgs_obj, key);
                if (!log_res_arr) {
                    cJSON_Delete(poll_res_obj);
                    fatal("could not build poll reply");
                }

                for (u64 i = offset; i < lb->next_log_offsets[log_idx]; ++i) {
                    u64 idx = i & (LOG_RING_BUF_SIZE - 1);
                    cJSON *res_offset_obj = cJSON_CreateNumber((double)i);
                    if (!res_offset_obj) {
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }

                    cJSON *res_val_obj = cJSON_CreateNumber((double)lb->logs[log_idx][idx]);
                    if (!res_val_obj) {
                        cJSON_Delete(res_offset_obj);
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }

                    cJSON *arr = cJSON_CreateArray();
                    if (!arr) {
                        cJSON_Delete(res_offset_obj);
                        cJSON_Delete(res_val_obj);
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }

                    if (!cJSON_AddItemToArray(arr, res_offset_obj)) {
                        cJSON_Delete(res_offset_obj);
                        cJSON_Delete(res_val_obj);
                        cJSON_Delete(arr);
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }
                    if (!cJSON_AddItemToArray(arr, res_val_obj)) {
                        cJSON_Delete(res_val_obj);
                        cJSON_Delete(arr);
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }
                    if (!cJSON_AddItemToArray(log_res_arr, arr)) {
                        cJSON_Delete(arr);
                        cJSON_Delete(poll_res_obj);
                        fatal("could not build poll reply");
                    }
                }

                it = it->next;
            }

            if (error_during_iterarion) {
                cJSON_Delete(poll_res_obj);
                cJSON_Delete(msg_json);
                continue;
            }

            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, poll_res_obj);
        } else if (!strcmp(type->valuestring, "commit_offsets")) {
            cJSON *offsets_obj = cJSON_GetObjectItem(body, "offsets");
            if (!cJSON_IsObject(offsets_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_COMMIT);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *it = offsets_obj->child;
            bool malformed = false;
            while (it) {
                const u8 *key = (const u8 *)it->string;
                if (key == NULL || !cJSON_IsNumber(it)) {
                    send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                               next_msg_id++, in_reply_to, 69, ERR_MALFORMED_COMMIT);
                    malformed = true;
                    break;
                }
                const u64 offset_val = (u64)it->valuedouble;

                i64 log_idx = find_log_idx_by_key(lb, key);
                if (log_idx == -1) {
                    it = it->next;
                    continue;
                }

                u64 next_uncommitted = offset_val + 1;
                if (next_uncommitted > lb->uncomitted_offset_start[log_idx]) {
                    lb->uncomitted_offset_start[log_idx] = next_uncommitted;
                }

                it = it->next;
            }

            if (malformed) {
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *res_body =
                reply_body(next_msg_id++, (const u8 *)"commit_offsets_ok", in_reply_to);
            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "list_committed_offsets")) {
            cJSON *keys_arr_obj = cJSON_GetObjectItem(body, "keys");
            if (!keys_arr_obj || !cJSON_IsArray(keys_arr_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_LIST);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *res_body =
                reply_body(next_msg_id++, (const u8 *)"list_committed_offsets_ok", in_reply_to);

            cJSON *res_offsets_obj = cJSON_AddObjectToObject(res_body, "offsets");
            if (!res_offsets_obj) {
                cJSON_Delete(res_body);
                fatal("could not build offsets reply");
            }

            cJSON *it;
            bool error_in_for_loop = false;
            cJSON_ArrayForEach(it, keys_arr_obj) {
                if (!cJSON_IsString(it) || it->valuestring == NULL) {
                    error_in_for_loop = true;
                    break;
                }

                const u8 *key_str = (const u8 *)it->valuestring;

                i64 log_idx = find_log_idx_by_key(lb, key_str);
                if (log_idx == -1) {
                    continue;
                }

                if (!cJSON_AddNumberToObject(res_offsets_obj, (const char *)key_str,
                                             (double)lb->uncomitted_offset_start[log_idx] - 1)) {
                    cJSON_Delete(res_body);
                    fatal("could not build offsets reply");
                }
            }

            if (error_in_for_loop) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_LIST);
                cJSON_Delete(msg_json);
                cJSON_Delete(res_body);
                continue;
            }

            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else if (!strcmp(type->valuestring, "txn")) {
            cJSON *txn_arr_obj = cJSON_GetObjectItem(body, "txn");
            if (!txn_arr_obj || !cJSON_IsArray(txn_arr_obj)) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_TXN);
                cJSON_Delete(msg_json);
                continue;
            }

            cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"txn_ok", in_reply_to);
            cJSON *res_txn_arr = cJSON_AddArrayToObject(res_body, "txn");
            if (!res_txn_arr) {
                cJSON_Delete(res_body);
                fatal("could not build txn reply");
            }

            bool error_in_foreach = false;
            bool kv_full = false;
            cJSON *it;
            cJSON_ArrayForEach(it, txn_arr_obj) {
                if (!cJSON_IsArray(it)) {
                    error_in_foreach = true;
                    break;
                }

                cJSON *op = cJSON_GetArrayItem(it, 0), *key = cJSON_GetArrayItem(it, 1),
                      *val = cJSON_GetArrayItem(it, 2);

                if (!op || !cJSON_IsString(op) || op->valuestring == NULL || !key ||
                    !cJSON_IsNumber(key)) {
                    error_in_foreach = true;
                    break;
                }

                if (!strcmp(op->valuestring, "w") && (!val || !cJSON_IsNumber(val))) {
                    error_in_foreach = true;
                    break;
                }

                i64 idx = find_kv_entry_idx_by_key(kvs, (i64)key->valuedouble);

                cJSON *op_res_arr = cJSON_CreateArray();
                if (!op_res_arr) {
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                if (!cJSON_AddItemToArray(res_txn_arr, op_res_arr)) {
                    cJSON_Delete(op_res_arr);
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                cJSON *op_str_obj = cJSON_CreateString(op->valuestring);
                if (!op_str_obj) {
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                if (!cJSON_AddItemToArray(op_res_arr, op_str_obj)) {
                    cJSON_Delete(op_str_obj);
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                cJSON *key_res_obj = cJSON_CreateNumber((double)key->valuedouble);
                if (!key_res_obj) {
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                if (!cJSON_AddItemToArray(op_res_arr, key_res_obj)) {
                    cJSON_Delete(key_res_obj);
                    cJSON_Delete(res_body);
                    fatal("could not build txn reply");
                }

                if (!strcmp(op->valuestring, "r")) {
                    cJSON *val_res_obj;
                    if (idx == -1) {
                        val_res_obj = cJSON_CreateNull();
                    } else {
                        i64 read_val = kvs->vals[idx];
                        val_res_obj = cJSON_CreateNumber((double)read_val);
                    }

                    if (!val_res_obj) {
                        cJSON_Delete(res_body);
                        fatal("could not build txn reply");
                    }

                    if (!cJSON_AddItemToArray(op_res_arr, val_res_obj)) {
                        cJSON_Delete(val_res_obj);
                        cJSON_Delete(res_body);
                        fatal("could not build txn reply");
                    }
                } else if (!strcmp(op->valuestring, "w")) {
                    if (idx == -1) {
                        if (kvs->empty_idx >= MAX_KV_ENTRIES) {
                            kv_full = true;
                            break;
                        }
                        idx = (i64)kvs->empty_idx++;
                        kvs->keys[idx] = (i64)key->valuedouble;
                    }
                    kvs->vals[idx] = (i64)val->valuedouble;

                    cJSON *val_write_obj = cJSON_CreateNumber((double)val->valuedouble);
                    if (!val_write_obj) {
                        cJSON_Delete(res_body);
                        fatal("could not build txn reply");
                    }

                    if (!cJSON_AddItemToArray(op_res_arr, val_write_obj)) {
                        cJSON_Delete(val_write_obj);
                        cJSON_Delete(res_body);
                        fatal("could not build txn reply");
                    }
                } else {
                    error_in_foreach = true;
                    break;
                }
            }

            if (kv_full) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 11, ERR_KV_FULL);
                cJSON_Delete(msg_json);
                cJSON_Delete(res_body);
                continue;
            }

            if (error_in_foreach) {
                send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring,
                           next_msg_id++, in_reply_to, 69, ERR_MALFORMED_TXN);
                cJSON_Delete(msg_json);
                cJSON_Delete(res_body);
                continue;
            }

            send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
        } else {
            send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                       in_reply_to, 10, ERR_NOT_SUPPORTED);
            cJSON_Delete(msg_json);
            continue;
        }

        cJSON_Delete(msg_json);
    }

    if (ferror(stdin)) {
        fprintf(stderr, "could not read stdin\n");
        free(kvs);
        free(lb);
        return EXIT_FAILURE;
    }

    free(kvs);
    free(lb);
    return EXIT_SUCCESS;
}
