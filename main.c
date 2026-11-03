#include <cjson/cJSON.h>
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <time.h>
#include <unistd.h>

#include "types.h"

#define MAX_NODES 128
#define MAX_NODE_NAME_LEN 16
#define MAX_PENDING 256

#define MAX_OUTGOING (MAX_PENDING + 1)

#define PENDING_READ_TIMEOUT_MS 5000

#define MAX_LOGS 1024
#define MAX_LOG_KEY_LEN 16
#define LOG_RING_BUF_SIZE 128

_Static_assert((LOG_RING_BUF_SIZE & (LOG_RING_BUF_SIZE - 1)) == 0,
               "LOG_RING_BUF_SIZE must be a power of two");

#define MAX_KV_ENTRIES 4096
#define MAX_KV_KEY_LEN 16
#define KV_BUF_SIZE 1024

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

typedef enum {
    err_code_timeout = 0,
    err_code_not_supported = 10,
    err_code_temporarily_unavailable = 11,
    err_code_malformed_request = 12,
    err_code_crash = 13,
    err_code_abort = 14,
    err_code_key_does_not_exist = 20,
} ErrorCode;

static const u8 err_malformed_init[] = "malformed init message";
static const u8 err_node_id_too_long[] = "node id too long";
static const u8 err_echo_type[] = "incorrect echo type";
static const u8 err_generate_before_init[] = "generate received before init";
static const u8 err_generate_id[] = "could not build generate id";
static const u8 err_broadcast_full[] = "broadcast buffer full";
static const u8 err_broadcast_type[] = "incorrect broadcast type";
static const u8 err_too_many_pending_reads[] = "too many pending reads";
static const u8 err_topology_type[] = "incorrect topology type";
static const u8 err_invalid_add[] = "invalid add msg";
static const u8 err_too_many_pending_adds[] = "too many pending adds";
static const u8 err_malformed_send[] = "malformed send message";
static const u8 err_key_too_long[] = "key too long";
static const u8 err_too_many_logs[] = "can not support more logs";
static const u8 err_log_full[] = "ran out of buffer space";
static const u8 err_malformed_poll[] = "malformed poll request";
static const u8 err_invalid_read[] = "invalid read";
static const u8 err_malformed_commit[] = "malformed commit msg";
static const u8 err_malformed_list[] = "malformed list committed msg";
static const u8 err_kv_full[] = "kv table full";
static const u8 err_malformed_txn[] = "malformed txn msg";
static const u8 err_not_supported[] = "not supported";
static const u8 err_timeout[] = "timeout";

typedef struct {
    const char *src;
    const char *dest;
    const cJSON *body;
    u64 msg_id;
    bool has_msg_id;
} Request;

typedef struct {
    u8 src[MAX_NODE_NAME_LEN];
    u8 dest[MAX_NODE_NAME_LEN];
    cJSON *body;
} Response;

typedef struct {
    Response items[MAX_OUTGOING];
    usize count;
} PendingResponseArr;

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

typedef struct {
    u8 keys[MAX_LOGS][MAX_LOG_KEY_LEN];
    i64 logs[MAX_LOGS][LOG_RING_BUF_SIZE];
    usize next_log_idx;
    u64 next_log_offsets[MAX_LOGS];
    u64 uncomitted_offset_start[MAX_LOGS];
} LogBuffers;

typedef struct {
    i64 keys[MAX_KV_ENTRIES];
    i64 vals[MAX_KV_ENTRIES];
    usize empty_idx;
} KVStore;

typedef struct {
    u8 node_id[MAX_NODE_NAME_LEN];
    bool node_id_set;
    u8 node_ids[MAX_NODES][MAX_NODE_NAME_LEN];
    usize next_node_idx;
    u64 next_msg_id;
    BroadcastMessages bd_msg;
    i64 counter;
    LogBuffers *lb;
    KVStore *kvs;
    PendingKVAdd pending_adds[MAX_PENDING];
    PendingKVReadAll pending_read_alls[MAX_PENDING];
} ServerContext;

typedef PendingResponseArr (*RequestHandler)(ServerContext *ctx, const Request *req);

typedef struct {
    const char *name;
    RequestHandler handler;
} RequestHandlerEntry;

static _Noreturn void fatal(const char *message) {
    fprintf(stderr, "fatal: %s\n", message);
    exit(EXIT_FAILURE);
}

static i64 now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (i64)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

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

static void res_add(PendingResponseArr *res, const u8 *src, const u8 *dest, cJSON *body) {
    usize src_len = strlen((const char *)src);
    usize dest_len = strlen((const char *)dest);
    if (src_len >= MAX_NODE_NAME_LEN || dest_len >= MAX_NODE_NAME_LEN) {
        cJSON_Delete(body);
        fatal("outgoing node name too long");
    }
    if (res->count >= MAX_OUTGOING) {
        cJSON_Delete(body);
        fatal("too many outgoing messages");
    }

    Response *out = &res->items[res->count++];
    memcpy(out->src, src, src_len + 1);
    memcpy(out->dest, dest, dest_len + 1);
    out->body = body;
}

static void res_error(PendingResponseArr *res, const u8 *src, const u8 *dest, u64 msg_id,
                      u64 in_reply_to, ErrorCode code, const u8 *text) {
    cJSON *body = reply_body(msg_id, (const u8 *)"error", in_reply_to);
    if (!cJSON_AddNumberToObject(body, "code", code) ||
        !cJSON_AddStringToObject(body, "text", (const char *)text)) {
        cJSON_Delete(body);
        fatal("could not build error reply");
    }
    res_add(res, src, dest, body);
}

static void response_free(PendingResponseArr *res) {
    for (usize i = 0; i < res->count; ++i) {
        cJSON_Delete(res->items[i].body);
    }
    res->count = 0;
}

static int server_context_new(ServerContext **out) {
    *out = NULL;

    ServerContext *ctx = calloc(1, sizeof *ctx);
    if (!ctx) {
        return -ENOMEM;
    }

    ctx->lb = calloc(1, sizeof *ctx->lb);
    if (!ctx->lb) {
        free(ctx);
        return -ENOMEM;
    }

    ctx->kvs = calloc(1, sizeof *ctx->kvs);
    if (!ctx->kvs) {
        free(ctx->lb);
        free(ctx);
        return -ENOMEM;
    }

    ctx->bd_msg.cap = 4096;
    ctx->next_msg_id = 1;
    *out = ctx;
    return 0;
}

static void server_context_free(ServerContext *ctx) {
    if (ctx == NULL) {
        return;
    }
    free(ctx->kvs);
    free(ctx->lb);
    free(ctx);
}

static bool find_pending_add(ServerContext *ctx, u64 kv_msg_id, PendingKVAdd **out) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (ctx->pending_adds[i].used && ctx->pending_adds[i].kv_msg_id == kv_msg_id) {
            *out = &ctx->pending_adds[i];
            return true;
        }
    }
    return false;
}

static int alloc_pending_kv_read_all(ServerContext *ctx, PendingKVReadAll **out) {
    *out = NULL;
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!ctx->pending_read_alls[i].used) {
            *out = &ctx->pending_read_alls[i];
            return 0;
        }
    }
    return -ENOSPC;
}

static bool find_pending_kv_read_all(ServerContext *ctx, u64 kv_msg_id, PendingKVReadAll **out) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!ctx->pending_read_alls[i].used) {
            continue;
        }
        for (usize j = 0; j < ctx->pending_read_alls[i].kv_count; ++j) {
            if (ctx->pending_read_alls[i].kv_read_msg_ids[j] == kv_msg_id) {
                *out = &ctx->pending_read_alls[i];
                return true;
            }
        }
    }
    return false;
}

static bool find_log_idx_by_key(LogBuffers *lb, const u8 *key, usize *out) {
    for (usize i = 0; i < lb->next_log_idx; ++i) {
        if (!strncmp((const char *)key, (const char *)lb->keys[i], MAX_LOG_KEY_LEN)) {
            *out = i;
            return true;
        }
    }
    return false;
}

static bool find_kv_entry_idx_by_key(KVStore *kvs, i64 key, usize *out) {
    for (usize i = 0; i < kvs->empty_idx; ++i) {
        if (kvs->keys[i] == key) {
            *out = i;
            return true;
        }
    }
    return false;
}

// Answers expired pending reads with a timeout error and releases their slots.
static void reap_pending_reads(ServerContext *ctx, i64 now, PendingResponseArr *out) {
    for (usize i = 0; i < MAX_PENDING; ++i) {
        PendingKVReadAll *pr = &ctx->pending_read_alls[i];
        if (!pr->used || now < pr->deadline_ms) {
            continue;
        }
        res_error(out, ctx->node_id, pr->client_src, ctx->next_msg_id++, pr->client_msg_id,
                  err_code_timeout, err_timeout);
        pr->used = false;
    }
}

// Writes the poll timeout for the earliest pending read. Returns false when no read is pending.
static bool pending_read_timeout_ms(const ServerContext *ctx, i64 now, int *out) {
    bool found = false;
    i64 earliest = 0;
    for (usize i = 0; i < MAX_PENDING; ++i) {
        const PendingKVReadAll *pr = &ctx->pending_read_alls[i];
        if (!pr->used) {
            continue;
        }
        if (!found || pr->deadline_ms < earliest) {
            earliest = pr->deadline_ms;
            found = true;
        }
    }
    if (!found) {
        return false;
    }
    i64 remaining = earliest - now;
    if (remaining <= 0) {
        *out = 0;
    } else if (remaining > INT_MAX) {
        *out = INT_MAX;
    } else {
        *out = (int)remaining;
    }
    return true;
}

static int alloc_pending_read(ServerContext *ctx, PendingKVAdd **out) {
    *out = NULL;
    for (usize i = 0; i < MAX_PENDING; ++i) {
        if (!ctx->pending_adds[i].used) {
            *out = &ctx->pending_adds[i];
            return 0;
        }
    }
    return -ENOSPC;
}

static int request_msg_id(const Request *req, u64 *msg_id) {
    if (!req->has_msg_id) {
        fprintf(stderr, "incorrect type for type or msg_id\n");
        return -EINVAL;
    }
    *msg_id = req->msg_id;
    return 0;
}

static PendingResponseArr handle_read_ok(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;

    if (!cJSON_HasObjectItem(req->body, "value")) {
        goto done;
    }
    const cJSON *value_ptr = cJSON_GetObjectItem(req->body, "value");
    if (!cJSON_IsNumber(value_ptr)) {
        goto done;
    }

    double value = value_ptr->valuedouble;

    const cJSON *reply_ptr = cJSON_GetObjectItem(req->body, "in_reply_to");
    if (!cJSON_IsNumber(reply_ptr)) {
        goto done;
    }
    u64 in_reply_to = (u64)reply_ptr->valuedouble;

    PendingKVReadAll *pr;
    if (!find_pending_kv_read_all(ctx, in_reply_to, &pr)) {
        goto done;
    }

    pr->sum += (i64)value;
    if (--pr->remaining == 0) {
        body = reply_body(ctx->next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
        if (!cJSON_AddNumberToObject(body, "value", (double)pr->sum)) {
            cJSON_Delete(body);
            fatal("could not build read reply");
        }
        res_add(&res, ctx->node_id, pr->client_src, body);
        body = NULL;
        pr->used = false;
    }
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_write_ok(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;

    const cJSON *reply_ptr = cJSON_GetObjectItem(req->body, "in_reply_to");
    if (!cJSON_IsNumber(reply_ptr)) {
        goto done;
    }
    u64 in_reply_to = (u64)reply_ptr->valuedouble;

    PendingKVAdd *p;
    if (find_pending_add(ctx, in_reply_to, &p)) {
        body = reply_body(ctx->next_msg_id++, (const u8 *)"add_ok", p->client_msg_id);
        res_add(&res, ctx->node_id, p->client_src, body);
        body = NULL;
        p->used = false;
    }
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_error(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;

    const cJSON *reply_ptr = cJSON_GetObjectItem(req->body, "in_reply_to");
    if (!cJSON_IsNumber(reply_ptr)) {
        goto done;
    }
    u64 in_reply_to = (u64)reply_ptr->valuedouble;

    const cJSON *code_ptr = cJSON_GetObjectItem(req->body, "code");
    i32 code = cJSON_IsNumber(code_ptr) ? code_ptr->valueint : -1;

    if (code == err_code_key_does_not_exist) {
        PendingKVReadAll *pr;
        if (find_pending_kv_read_all(ctx, in_reply_to, &pr) && --pr->remaining == 0) {
            body = reply_body(ctx->next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
            if (!cJSON_AddNumberToObject(body, "value", (double)pr->sum)) {
                cJSON_Delete(body);
                fatal("could not build read reply");
            }
            res_add(&res, ctx->node_id, pr->client_src, body);
            body = NULL;
            pr->used = false;
        }
    }
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_init(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (!cJSON_HasObjectItem(req->body, "node_id") || !cJSON_HasObjectItem(req->body, "node_ids")) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_init);
        goto done;
    }

    const cJSON *node_id = cJSON_GetObjectItem(req->body, "node_id"),
                *node_ids = cJSON_GetObjectItem(req->body, "node_ids");

    if (!cJSON_IsString(node_id) || node_id->valuestring == NULL || !cJSON_IsArray(node_ids)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_init);
        goto done;
    }

    usize node_id_len = strlen(node_id->valuestring);
    if (node_id_len >= sizeof ctx->node_id) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_node_id_too_long);
        goto done;
    }

    bool node_ids_too_long = false;
    const cJSON *it = NULL;
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
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_node_id_too_long);
        goto done;
    }

    memcpy(ctx->node_id, node_id->valuestring, node_id_len + 1);
    ctx->node_id_set = true;

    cJSON_ArrayForEach(it, node_ids) {
        if (!cJSON_IsString(it) || it->valuestring == NULL) {
            continue;
        }
        if (ctx->next_node_idx >= MAX_NODES) {
            break;
        }
        usize name_len = strlen(it->valuestring);
        memcpy(ctx->node_ids[ctx->next_node_idx], it->valuestring, name_len + 1);
        ++ctx->next_node_idx;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"init_ok", msg_id);
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_echo(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    const cJSON *echo = cJSON_GetObjectItem(req->body, "echo");
    if (!cJSON_IsString(echo) || echo->valuestring == NULL) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_echo_type);
        goto done;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"echo_ok", msg_id);
    if (!cJSON_AddStringToObject(body, "echo", echo->valuestring)) {
        cJSON_Delete(body);
        fatal("could not build echo reply");
    }
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_generate(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (!ctx->node_id_set) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_generate_before_init);
        goto done;
    }

    u8 rand_bytes[16];
    usize rand_filled = 0;
    while (rand_filled < sizeof rand_bytes) {
        ssize_t n = getrandom(rand_bytes + rand_filled, sizeof rand_bytes - rand_filled, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_crash, err_generate_id);
            goto done;
        }
        rand_filled += (usize)n;
    }
    u64 id_hi;
    u64 id_lo;
    memcpy(&id_hi, rand_bytes, sizeof id_hi);
    memcpy(&id_lo, rand_bytes + sizeof id_hi, sizeof id_lo);
    u8 id[64];
    int id_len = snprintf((char *)id, sizeof id, "%016llx%016llx", (unsigned long long)id_hi,
                          (unsigned long long)id_lo);
    if (id_len < 0 || (usize)id_len >= sizeof id) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_crash, err_generate_id);
        goto done;
    }
    body = reply_body(ctx->next_msg_id++, (const u8 *)"generate_ok", msg_id);
    if (!cJSON_AddStringToObject(body, "id", (const char *)id)) {
        cJSON_Delete(body);
        fatal("could not build generate reply");
    }
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_broadcast(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (ctx->bd_msg.size >= ctx->bd_msg.cap) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_broadcast_full);
        goto done;
    }

    if (!cJSON_HasObjectItem(req->body, "message")) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_broadcast_type);
        goto done;
    }

    const cJSON *msg_val = cJSON_GetObjectItem(req->body, "message");
    if (!cJSON_IsNumber(msg_val)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_broadcast_type);
        goto done;
    }

    ctx->bd_msg.msg_vals[ctx->bd_msg.size++] = msg_val->valueint;
    body = reply_body(ctx->next_msg_id++, (const u8 *)"broadcast_ok", msg_id);
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_read(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    PendingKVReadAll *pr;
    if (alloc_pending_kv_read_all(ctx, &pr) < 0) {
        res_error(&res, ctx->node_id, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_too_many_pending_reads);
        goto done;
    }

    pr->used = true;
    pr->client_msg_id = msg_id;
    usize src_len = strlen(req->src);
    memcpy(pr->client_src, req->src, src_len + 1);
    pr->sum = 0;
    pr->remaining = ctx->next_node_idx;
    pr->kv_count = 0;
    pr->deadline_ms = now_ms() + PENDING_READ_TIMEOUT_MS;

    for (usize i = 0; i < ctx->next_node_idx; ++i) {
        cJSON *body = cJSON_CreateObject();
        if (!body) {
            fatal("could not create read request");
        }
        u64 kv_id = ctx->next_msg_id++;

        if (!cJSON_AddStringToObject(body, "type", "read") ||
            !cJSON_AddStringToObject(body, "key", (const char *)ctx->node_ids[i]) ||
            !cJSON_AddNumberToObject(body, "msg_id", (double)kv_id)) {
            cJSON_Delete(body);
            fatal("could not build read request");
        }

        pr->kv_read_msg_ids[pr->kv_count++] = kv_id;
        res_add(&res, ctx->node_id, (const u8 *)"seq-kv", body);
    }
done:
    return res;
}

static PendingResponseArr handle_topology(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (!cJSON_HasObjectItem(req->body, "topology")) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_topology_type);
        goto done;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"topology_ok", msg_id);
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_add(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (!cJSON_HasObjectItem(req->body, "delta")) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_invalid_add);
        goto done;
    }

    const cJSON *delta_obj = cJSON_GetObjectItem(req->body, "delta");
    if (!cJSON_IsNumber(delta_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_invalid_add);
        goto done;
    }

    PendingKVAdd *pa;
    if (alloc_pending_read(ctx, &pa) < 0) {
        res_error(&res, ctx->node_id, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_too_many_pending_adds);
        goto done;
    }

    pa->used = true;
    pa->kv_msg_id = ctx->next_msg_id++;
    pa->client_msg_id = msg_id;
    usize src_len = strlen(req->src);
    memcpy(pa->client_src, req->src, src_len + 1);

    ctx->counter += (i64)delta_obj->valuedouble;
    cJSON *body = cJSON_CreateObject();
    if (!body) {
        fatal("could not create write request");
    }
    if (!cJSON_AddStringToObject(body, "type", "write") ||
        !cJSON_AddStringToObject(body, "key", (const char *)ctx->node_id) ||
        !cJSON_AddNumberToObject(body, "value", (double)ctx->counter) ||
        !cJSON_AddNumberToObject(body, "msg_id", (double)pa->kv_msg_id)) {
        cJSON_Delete(body);
        fatal("could not build write request");
    }
    res_add(&res, ctx->node_id, (const u8 *)"seq-kv", body);
done:
    return res;
}

static PendingResponseArr handle_send(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    if (!cJSON_HasObjectItem(req->body, "key") || !cJSON_HasObjectItem(req->body, "msg")) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_send);
        goto done;
    }

    const cJSON *key_obj = cJSON_GetObjectItem(req->body, "key"),
                *msg_obj = cJSON_GetObjectItem(req->body, "msg");

    if (!cJSON_IsString(key_obj) || key_obj->valuestring == NULL || !cJSON_IsNumber(msg_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_send);
        goto done;
    }

    usize key_len = strlen(key_obj->valuestring);
    if (key_len >= MAX_LOG_KEY_LEN) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_key_too_long);
        goto done;
    }

    usize log_idx;

    if (!find_log_idx_by_key(ctx->lb, (const u8 *)key_obj->valuestring, &log_idx)) {
        if (ctx->lb->next_log_idx >= MAX_LOGS) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_temporarily_unavailable, err_too_many_logs);
            goto done;
        }

        log_idx = ctx->lb->next_log_idx++;
        memcpy(ctx->lb->keys[log_idx], key_obj->valuestring, key_len + 1);
    }

    if (ctx->lb->next_log_offsets[log_idx] >= LOG_RING_BUF_SIZE) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_log_full);
        goto done;
    }

    ctx->lb->logs[log_idx][ctx->lb->next_log_offsets[log_idx] & (LOG_RING_BUF_SIZE - 1)] =
        (i64)msg_obj->valuedouble;
    ++ctx->lb->next_log_offsets[log_idx];

    body = reply_body(ctx->next_msg_id++, (const u8 *)"send_ok", msg_id);
    if (!cJSON_AddNumberToObject(body, "offset", (double)ctx->lb->next_log_offsets[log_idx] - 1)) {
        cJSON_Delete(body);
        fatal("could not build send reply");
    }

    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_poll(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *poll_res_obj = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    const cJSON *offsets_obj = cJSON_GetObjectItem(req->body, "offsets");
    if (!cJSON_IsObject(offsets_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_poll);
        goto done;
    }

    const cJSON *it = offsets_obj->child;

    poll_res_obj = reply_body(ctx->next_msg_id++, (const u8 *)"poll_ok", msg_id);

    cJSON *msgs_obj = cJSON_AddObjectToObject(poll_res_obj, "msgs");
    if (!msgs_obj) {
        cJSON_Delete(poll_res_obj);
        fatal("could not build poll reply");
    }

    while (it) {
        const char *key = it->string;
        if (key == NULL || !cJSON_IsNumber(it)) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_malformed_request, err_malformed_poll);
            goto done;
        }
        const u64 offset = (u64)it->valuedouble;

        usize log_idx;
        if (!find_log_idx_by_key(ctx->lb, (const u8 *)key, &log_idx)) {
            it = it->next;
            continue;
        }

        if (offset > ctx->lb->next_log_offsets[log_idx]) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_malformed_request, err_invalid_read);
            goto done;
        }

        cJSON *log_res_arr = cJSON_AddArrayToObject(msgs_obj, key);
        if (!log_res_arr) {
            cJSON_Delete(poll_res_obj);
            fatal("could not build poll reply");
        }

        for (u64 i = offset; i < ctx->lb->next_log_offsets[log_idx]; ++i) {
            u64 idx = i & (LOG_RING_BUF_SIZE - 1);
            cJSON *res_offset_obj = cJSON_CreateNumber((double)i);
            if (!res_offset_obj) {
                cJSON_Delete(poll_res_obj);
                fatal("could not build poll reply");
            }

            cJSON *res_val_obj = cJSON_CreateNumber((double)ctx->lb->logs[log_idx][idx]);
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

    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, poll_res_obj);
    poll_res_obj = NULL;
done:
    cJSON_Delete(poll_res_obj);
    return res;
}

static PendingResponseArr handle_commit_offsets(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    const cJSON *offsets_obj = cJSON_GetObjectItem(req->body, "offsets");
    if (!cJSON_IsObject(offsets_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_commit);
        goto done;
    }

    const cJSON *it = offsets_obj->child;
    while (it) {
        const char *key = it->string;
        if (key == NULL || !cJSON_IsNumber(it)) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_malformed_request, err_malformed_commit);
            goto done;
        }
        const u64 offset_val = (u64)it->valuedouble;

        usize log_idx;
        if (!find_log_idx_by_key(ctx->lb, (const u8 *)key, &log_idx)) {
            it = it->next;
            continue;
        }

        u64 next_uncommitted = offset_val + 1;
        if (next_uncommitted > ctx->lb->uncomitted_offset_start[log_idx]) {
            ctx->lb->uncomitted_offset_start[log_idx] = next_uncommitted;
        }

        it = it->next;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"commit_offsets_ok", msg_id);
    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_list_committed_offsets(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    const cJSON *keys_arr_obj = cJSON_GetObjectItem(req->body, "keys");
    if (!keys_arr_obj || !cJSON_IsArray(keys_arr_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_list);
        goto done;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"list_committed_offsets_ok", msg_id);

    cJSON *res_offsets_obj = cJSON_AddObjectToObject(body, "offsets");
    if (!res_offsets_obj) {
        cJSON_Delete(body);
        fatal("could not build offsets reply");
    }

    const cJSON *it = NULL;
    cJSON_ArrayForEach(it, keys_arr_obj) {
        if (!cJSON_IsString(it) || it->valuestring == NULL) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_malformed_request, err_malformed_list);
            goto done;
        }

        const u8 *key_str = (const u8 *)it->valuestring;

        usize log_idx;
        if (!find_log_idx_by_key(ctx->lb, key_str, &log_idx)) {
            continue;
        }

        if (!cJSON_AddNumberToObject(res_offsets_obj, (const char *)key_str,
                                     (double)ctx->lb->uncomitted_offset_start[log_idx] - 1)) {
            cJSON_Delete(body);
            fatal("could not build offsets reply");
        }
    }

    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static PendingResponseArr handle_txn(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};
    cJSON *body = NULL;
    u64 msg_id;
    if (request_msg_id(req, &msg_id) < 0) {
        goto done;
    }

    const cJSON *txn_arr_obj = cJSON_GetObjectItem(req->body, "txn");
    if (!txn_arr_obj || !cJSON_IsArray(txn_arr_obj)) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_txn);
        goto done;
    }

    body = reply_body(ctx->next_msg_id++, (const u8 *)"txn_ok", msg_id);
    cJSON *res_txn_arr = cJSON_AddArrayToObject(body, "txn");
    if (!res_txn_arr) {
        cJSON_Delete(body);
        fatal("could not build txn reply");
    }

    int status = 0;
    const cJSON *it = NULL;
    cJSON_ArrayForEach(it, txn_arr_obj) {
        if (!cJSON_IsArray(it)) {
            status = -EINVAL;
            break;
        }

        cJSON *op = cJSON_GetArrayItem(it, 0), *key = cJSON_GetArrayItem(it, 1),
              *val = cJSON_GetArrayItem(it, 2);

        if (!op || !cJSON_IsString(op) || op->valuestring == NULL || !key || !cJSON_IsNumber(key)) {
            status = -EINVAL;
            break;
        }

        if (!strcmp(op->valuestring, "w") && (!val || !cJSON_IsNumber(val))) {
            status = -EINVAL;
            break;
        }

        usize idx;
        bool found = find_kv_entry_idx_by_key(ctx->kvs, (i64)key->valuedouble, &idx);

        cJSON *op_res_arr = cJSON_CreateArray();
        if (!op_res_arr) {
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        if (!cJSON_AddItemToArray(res_txn_arr, op_res_arr)) {
            cJSON_Delete(op_res_arr);
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        cJSON *op_str_obj = cJSON_CreateString(op->valuestring);
        if (!op_str_obj) {
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        if (!cJSON_AddItemToArray(op_res_arr, op_str_obj)) {
            cJSON_Delete(op_str_obj);
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        cJSON *key_res_obj = cJSON_CreateNumber((double)key->valuedouble);
        if (!key_res_obj) {
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        if (!cJSON_AddItemToArray(op_res_arr, key_res_obj)) {
            cJSON_Delete(key_res_obj);
            cJSON_Delete(body);
            fatal("could not build txn reply");
        }

        if (!strcmp(op->valuestring, "r")) {
            cJSON *val_res_obj;
            if (!found) {
                val_res_obj = cJSON_CreateNull();
            } else {
                i64 read_val = ctx->kvs->vals[idx];
                val_res_obj = cJSON_CreateNumber((double)read_val);
            }

            if (!val_res_obj) {
                cJSON_Delete(body);
                fatal("could not build txn reply");
            }

            if (!cJSON_AddItemToArray(op_res_arr, val_res_obj)) {
                cJSON_Delete(val_res_obj);
                cJSON_Delete(body);
                fatal("could not build txn reply");
            }
        } else if (!strcmp(op->valuestring, "w")) {
            if (!found) {
                if (ctx->kvs->empty_idx >= MAX_KV_ENTRIES) {
                    status = -ENOSPC;
                    break;
                }
                idx = ctx->kvs->empty_idx++;
                ctx->kvs->keys[idx] = (i64)key->valuedouble;
            }
            ctx->kvs->vals[idx] = (i64)val->valuedouble;

            cJSON *val_write_obj = cJSON_CreateNumber((double)val->valuedouble);
            if (!val_write_obj) {
                cJSON_Delete(body);
                fatal("could not build txn reply");
            }

            if (!cJSON_AddItemToArray(op_res_arr, val_write_obj)) {
                cJSON_Delete(val_write_obj);
                cJSON_Delete(body);
                fatal("could not build txn reply");
            }
        } else {
            status = -EINVAL;
            break;
        }
    }

    if (status == -ENOSPC) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_temporarily_unavailable, err_kv_full);
        goto done;
    }

    if (status < 0) {
        res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                  err_code_malformed_request, err_malformed_txn);
        goto done;
    }

    res_add(&res, (const u8 *)req->dest, (const u8 *)req->src, body);
    body = NULL;
done:
    cJSON_Delete(body);
    return res;
}

static const RequestHandlerEntry handlers[] = {
    {"read_ok", handle_read_ok},
    {"write_ok", handle_write_ok},
    {"error", handle_error},
    {"init", handle_init},
    {"echo", handle_echo},
    {"generate", handle_generate},
    {"broadcast", handle_broadcast},
    {"read", handle_read},
    {"topology", handle_topology},
    {"add", handle_add},
    {"send", handle_send},
    {"poll", handle_poll},
    {"commit_offsets", handle_commit_offsets},
    {"list_committed_offsets", handle_list_committed_offsets},
    {"txn", handle_txn},
};

static bool find_handler(const char *name, const RequestHandlerEntry **out) {
    for (usize i = 0; i < ARRAY_SIZE(handlers); ++i) {
        if (strcmp(name, handlers[i].name) == 0) {
            *out = &handlers[i];
            return true;
        }
    }
    return false;
}

static PendingResponseArr handle_request(ServerContext *ctx, const Request *req) {
    PendingResponseArr res = {0};

    if (req->src == NULL || req->dest == NULL || !cJSON_IsObject(req->body)) {
        return res;
    }

    const cJSON *type = cJSON_GetObjectItem(req->body, "type");
    if (!cJSON_IsString(type) || type->valuestring == NULL) {
        return res;
    }

    const RequestHandlerEntry *entry;
    if (!find_handler(type->valuestring, &entry)) {
        u64 msg_id;
        if (request_msg_id(req, &msg_id) == 0) {
            res_error(&res, (const u8 *)req->dest, (const u8 *)req->src, ctx->next_msg_id++, msg_id,
                      err_code_not_supported, err_not_supported);
        }
        return res;
    }

    return entry->handler(ctx, req);
}

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

static void send_response(PendingResponseArr *res) {
    for (usize i = 0; i < res->count; ++i) {
        cJSON *body = res->items[i].body;
        res->items[i].body = NULL;
        send(res->items[i].src, res->items[i].dest, body);
    }
    response_free(res);
}

int main(void) {
    const usize json_parse_buf_len = 65536;
    u8 msg[json_parse_buf_len];

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        fatal("could not ignore SIGPIPE");
    }

    ServerContext *ctx;
    if (server_context_new(&ctx) < 0) {
        fatal("could not allocate server context");
    }

    if (setvbuf(stdin, NULL, _IONBF, 0) != 0) {
        fatal("could not disable stdin buffering");
    }

    for (;;) {
        i64 loop_now = now_ms();

        PendingResponseArr timeout_res = {0};
        reap_pending_reads(ctx, loop_now, &timeout_res);
        send_response(&timeout_res);

        struct pollfd pfd = {.fd = STDIN_FILENO, .events = POLLIN};
        int poll_timeout_ms;
        if (!pending_read_timeout_ms(ctx, loop_now, &poll_timeout_ms)) {
            poll_timeout_ms = -1;
        }
        int ready = poll(&pfd, 1, poll_timeout_ms);
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

        Request req = {0};
        PendingResponseArr res = {0};
        const cJSON *src, *dest, *body, *type;

        if (!cJSON_IsObject(msg_json)) {
            fprintf(stderr, "message is not a JSON object\n");
            goto next;
        }

        if (!cJSON_HasObjectItem(msg_json, "src") || !cJSON_HasObjectItem(msg_json, "dest") ||
            !cJSON_HasObjectItem(msg_json, "body")) {
            fprintf(stderr, "field/s src, dest or body not found\n");
            goto next;
        }

        src = cJSON_GetObjectItem(msg_json, "src");
        dest = cJSON_GetObjectItem(msg_json, "dest");
        body = cJSON_GetObjectItem(msg_json, "body");
        if (!cJSON_IsString(src) || src->valuestring == NULL || !cJSON_IsString(dest) ||
            dest->valuestring == NULL || !cJSON_IsObject(body)) {
            fprintf(stderr, "incorrect type for src, dest or body\n");
            goto next;
        }

        if (strlen(src->valuestring) >= MAX_NODE_NAME_LEN ||
            strlen(dest->valuestring) >= MAX_NODE_NAME_LEN) {
            fprintf(stderr, "src or dest too long\n");
            goto next;
        }

        type = cJSON_GetObjectItem(body, "type");
        if (!cJSON_IsString(type) || type->valuestring == NULL) {
            fprintf(stderr, "field/s type not found or not a string\n");
            goto next;
        }

        req.src = src->valuestring;
        req.dest = dest->valuestring;
        req.body = body;

        const cJSON *msg_id_obj = cJSON_GetObjectItem(body, "msg_id");
        if (cJSON_IsNumber(msg_id_obj)) {
            req.msg_id = (u64)msg_id_obj->valuedouble;
            req.has_msg_id = true;
        }

        res = handle_request(ctx, &req);
        send_response(&res);

    next:
        response_free(&res);
        cJSON_Delete(msg_json);
    }

    if (ferror(stdin)) {
        fprintf(stderr, "could not read stdin\n");
        server_context_free(ctx);
        return EXIT_FAILURE;
    }

    server_context_free(ctx);
    return EXIT_SUCCESS;
}
