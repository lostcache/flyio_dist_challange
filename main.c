#include <cjson/cJSON.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/time.h>
#include <time.h>

#include "types.h"

#define MAX_NODES 128
#define NODE_NAME_LEN 16
#define MAX_PENDING 256

typedef struct {
  i32 msg_vals[4096];
  usize size;
  usize cap;
} BroadcastMessages;

typedef struct {
  bool used;
  u64 kv_msg_id;
  u64 client_msg_id;
  u8 client_src[NODE_NAME_LEN];
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
  u8 client_src[NODE_NAME_LEN];
  u64 kv_read_msg_ids[MAX_NODES];
  usize kv_count;
  usize remaining;
  i64 sum;
} PendingKVReadAll;

static PendingKVReadAll pending_kv_read_alls[MAX_PENDING];

PendingKVReadAll *alloc_pending_kv_read_all(void) {
  for (usize i = 0; i < MAX_PENDING; ++i) {
    if (!pending_kv_read_alls[i].used) {
      return &pending_kv_read_alls[i];
    }
  }
  return NULL;
}

PendingKVReadAll *find_pending_kv_read_all(u64 kv_msg_id) {
  for (usize i = 0; i < MAX_PENDING; ++i) {
    if (!pending_kv_read_alls[i].used)
      continue;
    for (usize j = 0; j < pending_kv_read_alls[i].kv_count; ++j)
      if (pending_kv_read_alls[i].kv_read_msg_ids[j] == kv_msg_id) {
        return &pending_kv_read_alls[i];
      }
  }
  return NULL;
}

static i64 now_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (i64)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Builds a reply body with the fields every reply needs.
static cJSON *reply_body(u64 msg_id, const u8 *type, u64 in_reply_to) {
  cJSON *body = cJSON_CreateObject();
  cJSON_AddStringToObject(body, "type", (const char *)type);
  cJSON_AddNumberToObject(body, "msg_id", (double)msg_id);
  cJSON_AddNumberToObject(body, "in_reply_to", (double)in_reply_to);
  return body;
}

// Wraps a body into an envelope and writes it to stdout.
static void send(const u8 *src, const u8 *dest, cJSON *body) {
  cJSON *res = cJSON_CreateObject();
  cJSON_AddStringToObject(res, "src", (const char *)src);
  cJSON_AddStringToObject(res, "dest", (const char *)dest);
  cJSON_AddItemToObject(res, "body", body);

  u8 *out = (u8 *)cJSON_PrintUnformatted(res);
  printf("%s\n", (const char *)out);
  fflush(stdout);

  free(out);
  cJSON_Delete(res);
}

static void send_error(const u8 *src, const u8 *dest, u64 msg_id, u64 in_reply_to, i32 code,
                       const u8 *text) {
  cJSON *body = reply_body(msg_id, (const u8 *)"error", in_reply_to);
  cJSON_AddNumberToObject(body, "code", code);
  cJSON_AddStringToObject(body, "text", (const char *)text);
  send(src, dest, body);
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
  u8 NODE_ID[NODE_NAME_LEN];
  bool node_id_set = false;

  u8 NODE_IDS[MAX_NODES][NODE_NAME_LEN];
  usize next_node_idx = 0;

  i64 PREV_GENERATE_TIME_MS = now_ms();
  i32 GENERATE_MS_COUNTER = 0;

  const usize JSON_PARSE_BUF_LEN = 4096;

  u8 msg[JSON_PARSE_BUF_LEN];
  u64 next_msg_id = 1;

  BroadcastMessages bd_msg = {{0}, 0, 4096};

  i64 counter = 0;

  while (fgets((char *)msg, sizeof msg, stdin)) {
    cJSON *msg_json = cJSON_Parse((const char *)msg);
    if (!msg_json) {
      fprintf(stderr, "couldn't parse msg: %s\n", (const char *)msg);
      continue;
    }

    const cJSON *src, *dest, *body, *type, *msg_id;

    if (!cJSON_HasObjectItem(msg_json, "src") || !cJSON_HasObjectItem(msg_json, "dest") ||
        !cJSON_HasObjectItem(msg_json, "body")) {
      fprintf(stderr, "field/s src, dest or body not found\n");
      cJSON_Delete(msg_json);
      continue;
    }

    // TODO: validate lengths
    src = cJSON_GetObjectItem(msg_json, "src");
    dest = cJSON_GetObjectItem(msg_json, "dest");

    // TODO: validate lengths
    body = cJSON_GetObjectItem(msg_json, "body");
    if (!cJSON_IsString(src) || !cJSON_IsString(dest) || !cJSON_IsObject(body)) {
      fprintf(stderr, "incorrect type for src, dest or body\n");
      cJSON_Delete(msg_json);
      continue;
    }

    if (!cJSON_HasObjectItem(body, "type")) {
      fprintf(stderr, "field/s type, msg_id not found\n");
      cJSON_Delete(msg_json);
      continue;
    }
    type = cJSON_GetObjectItem(body, "type");

    if (!strcmp(type->valuestring, "read_ok")) {
      if (!cJSON_HasObjectItem(body, "value")) {
        cJSON_Delete(msg_json);
        continue;
      }
      cJSON *value_ptr = cJSON_GetObjectItem(body, "value");
      double value = value_ptr->valuedouble;

      if (!cJSON_HasObjectItem(body, "in_reply_to")) {
        cJSON_Delete(msg_json);
        continue;
      }
      cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
      u64 in_reply_to = (u64)reply_ptr->valuedouble;

      PendingKVReadAll *pr = find_pending_kv_read_all(in_reply_to);

      if (!pr)
        continue;

      pr->sum += value;
      if (--pr->remaining == 0) {
        cJSON *res = reply_body(next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
        cJSON_AddNumberToObject(res, "value", (double)pr->sum);
        send(NODE_ID, pr->client_src, res);
        pr->used = 0;
      }

      continue;
    }

    if (!strcmp(type->valuestring, "write_ok")) {
      if (!cJSON_HasObjectItem(body, "in_reply_to")) {
        cJSON_Delete(msg_json);
        continue;
      }
      cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
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
      if (!cJSON_HasObjectItem(body, "in_reply_to")) {
        cJSON_Delete(msg_json);
        continue;
      }
      cJSON *reply_ptr = cJSON_GetObjectItem(body, "in_reply_to");
      u64 in_reply_to = (u64)reply_ptr->valuedouble;

      const cJSON *code_ptr = cJSON_GetObjectItem(body, "code");
      i32 code = cJSON_IsNumber(code_ptr) ? code_ptr->valueint : -1;

      if (code == 20) {
        PendingKVReadAll *pr = find_pending_kv_read_all(in_reply_to);
        if (pr && --pr->remaining == 0) {
          cJSON *res = reply_body(next_msg_id++, (const u8 *)"read_ok", pr->client_msg_id);
          cJSON_AddNumberToObject(res, "value", (double)pr->sum);
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
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"correct init type");
        cJSON_Delete(msg_json);
        continue;
      }

      cJSON *node_id = cJSON_GetObjectItem(body, "node_id"),
            *node_ids = cJSON_GetObjectItem(body, "node_ids");

      if (!cJSON_IsString(node_id) || !cJSON_IsArray(node_ids)) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"correct init type");
        cJSON_Delete(msg_json);
        continue;
      }

      if (strlen(node_id->valuestring) >= sizeof NODE_ID) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 12, (const u8 *)"node id too long");
        cJSON_Delete(msg_json);
        continue;
      }
      usize node_id_len = strlen(node_id->valuestring);
      memcpy(NODE_ID, node_id->valuestring, node_id_len + 1);
      node_id_set = true;

      cJSON *it = NULL;
      cJSON_ArrayForEach(it, node_ids) {
        if (!cJSON_IsString(it)) {
          continue;
        }
        if (next_node_idx >= MAX_NODES) {
          break;
        }
        snprintf((char *)NODE_IDS[next_node_idx], NODE_NAME_LEN, "%s", it->valuestring);
        ++next_node_idx;
      }

      cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"init_ok", in_reply_to);
      send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
    } else if (!strcmp(type->valuestring, "echo")) {
      const cJSON *echo = cJSON_GetObjectItem(body, "echo");
      if (!cJSON_IsString(echo)) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 13, (const u8 *)"incorrect echo type");
        cJSON_Delete(msg_json);
        continue;
      }

      cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"echo_ok", in_reply_to);
      cJSON_AddStringToObject(res_body, "echo", echo->valuestring);
      send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);

    } else if (!strcmp(type->valuestring, "generate")) {
      if (!node_id_set) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"generate received before init");
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
      snprintf((char *)id, sizeof id, "%s-%lld-%d", (const char *)NODE_ID, (long long)now,
               (int)GENERATE_MS_COUNTER);
      cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"generate_ok", in_reply_to);
      cJSON_AddStringToObject(res_body, "id", (const char *)id);
      send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
    } else if (!strcmp(type->valuestring, "broadcast")) {
      if (bd_msg.size >= bd_msg.cap) {
        cJSON_Delete(msg_json);
        return 1;
      }

      if (!cJSON_HasObjectItem(body, "message")) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"incorrect broadcast type");
        cJSON_Delete(msg_json);
        continue;
      }

      const cJSON *msg_val = cJSON_GetObjectItem(body, "message");
      if (!cJSON_IsNumber(msg_val)) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"incorrect broadcast type");
        cJSON_Delete(msg_json);
        continue;
      }

      bd_msg.msg_vals[bd_msg.size++] = msg_val->valueint;
      cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"broadcast_ok", in_reply_to);
      send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
    } else if (!strcmp(type->valuestring, "read")) {
      // cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"read_ok", in_reply_to);
      // cJSON *msg_arr = cJSON_AddArrayToObject(res_body, "messages");
      // for (usize i = 0; i < bd_msg.size; ++i) {
      //   if (!cJSON_AddItemToArray(msg_arr, cJSON_CreateNumber((double)bd_msg.msg_vals[i])))
      //   {
      //     return 1;
      //   }
      // }
      // send_reply(dest->valuestring, src->valuestring, res_body);

      PendingKVReadAll *pr = alloc_pending_kv_read_all();
      if (!pr) {
        send_error(NODE_ID, (const u8 *)src->valuestring, next_msg_id++, in_reply_to, 13,
                   (const u8 *)"too many pending reads");
        cJSON_Delete(msg_json);
        continue;
      }

      pr->used = true;
      pr->client_msg_id = in_reply_to;
      snprintf((char *)pr->client_src, sizeof pr->client_src, "%s", src->valuestring);
      pr->sum = 0;
      pr->remaining = next_node_idx;
      pr->kv_count = 0;

      bool read_send_errored = false;
      for (usize i = 0; i < next_node_idx; ++i) {
        cJSON *body = cJSON_CreateObject();
        u64 kv_id = next_msg_id++;

        if (!cJSON_AddStringToObject(body, "type", "read") ||
            !cJSON_AddStringToObject(body, "key", (const char *)NODE_IDS[i]) ||
            !cJSON_AddNumberToObject(body, "msg_id", (double)kv_id)) {
          read_send_errored = true;
          cJSON_Delete(body);
          break;
        }

        pr->kv_read_msg_ids[pr->kv_count++] = kv_id;
        send(NODE_ID, (const u8 *)"seq-kv", body);
      }

      if (read_send_errored) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69,
                   (const u8 *)"something went wrong while sending read request to kv-seq node");
        cJSON_Delete(msg_json);
        continue;
      }
    } else if (!strcmp(type->valuestring, "topology")) {
      if (!cJSON_HasObjectItem(body, "topology")) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"incorrect topology type");
        cJSON_Delete(msg_json);
        continue;
      }

      cJSON *res_body = reply_body(next_msg_id++, (const u8 *)"topology_ok", in_reply_to);
      send((const u8 *)dest->valuestring, (const u8 *)src->valuestring, res_body);
    } else if (!strcmp(type->valuestring, "add")) {
      if (!cJSON_HasObjectItem(body, "delta")) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"invalid add msg");
        cJSON_Delete(msg_json);
        continue;
      }

      cJSON *delta_obj = cJSON_GetObjectItem(body, "delta");
      if (!cJSON_IsNumber(delta_obj)) {
        send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                   in_reply_to, 69, (const u8 *)"invalid add msg");
        cJSON_Delete(msg_json);
        continue;
      }

      PendingKVAdd *pa = alloc_pending_read();
      if (!pa) {
        send_error(NODE_ID, (const u8 *)src->valuestring, next_msg_id++, in_reply_to, 13,
                   (const u8 *)"too many pending adds");
        cJSON_Delete(msg_json);
        continue;
      }

      pa->used = true;
      pa->kv_msg_id = next_msg_id++;
      pa->client_msg_id = in_reply_to;
      snprintf((char *)pa->client_src, sizeof pa->client_src, "%s", src->valuestring);

      counter += delta_obj->valuedouble;
      cJSON *res_body = cJSON_CreateObject();
      cJSON_AddItemToObject(res_body, "type", cJSON_CreateString("write"));
      cJSON_AddItemToObject(res_body, "key", cJSON_CreateString((const char *)NODE_ID));
      cJSON_AddItemToObject(res_body, "value", cJSON_CreateNumber(counter));
      cJSON_AddItemToObject(res_body, "msg_id", cJSON_CreateNumber((double)pa->kv_msg_id));
      send(NODE_ID, (const u8 *)"seq-kv", res_body);
    } else {
      send_error((const u8 *)dest->valuestring, (const u8 *)src->valuestring, next_msg_id++,
                 in_reply_to, 10, (const u8 *)"not supported");
      cJSON_Delete(msg_json);
      continue;
    }

    cJSON_Delete(msg_json);
  }

  return 0;
}
