#include <cjson/cJSON.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/time.h>
#include <time.h>

/*

{
  "src": "c1",
  "dest": "n1",
  "body": {
    "type": "echo",
    "msg_id": 1,
    "echo": "Please echo 35"
  }
}


{
  "src": "n1",
  "dest": "c1",
  "body": {
    "type": "echo_ok",
    "msg_id": 1,
    "in_reply_to": 1,
    "echo": "Please echo 35"
  }
}

*/

static int64_t now_ms() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Builds a reply body with the fields every reply needs.
static cJSON *reply_body(long msg_id, const char *type, long in_reply_to) {
  cJSON *body = cJSON_CreateObject();
  cJSON_AddStringToObject(body, "type", type);
  cJSON_AddNumberToObject(body, "msg_id", (double)msg_id);
  cJSON_AddNumberToObject(body, "in_reply_to", (double)in_reply_to);
  return body;
}

// Wraps a body into an envelope and writes it to stdout.
static void send_reply(const char *src, const char *dest, cJSON *body) {
  cJSON *res = cJSON_CreateObject();
  cJSON_AddStringToObject(res, "src", src);
  cJSON_AddStringToObject(res, "dest", dest);
  cJSON_AddItemToObject(res, "body", body);

  char *out = cJSON_PrintUnformatted(res);
  printf("%s\n", out);
  fflush(stdout);

  free(out);
  cJSON_Delete(res);
}

static void send_error(const char *src, const char *dest, long msg_id, long in_reply_to, int code,
                       const char *text) {
  cJSON *body = reply_body(msg_id, "error", in_reply_to);
  cJSON_AddNumberToObject(body, "code", code);
  cJSON_AddStringToObject(body, "text", text);
  send_reply(src, dest, body);
}

int main() {
  uint32_t NODE_ID;
  if (getrandom(&NODE_ID, sizeof NODE_ID, 0) != sizeof NODE_ID) {
    perror("getrandom");
    return 1;
  }

  int64_t PREV_GENERATE_TIME_MS = now_ms();
  int GENERATE_MS_COUNTER = 0;

  const size_t JSON_PARSE_BUF_LEN = 4096;

  char msg[JSON_PARSE_BUF_LEN];
  long next_msg_id = 1;

  while (fgets(msg, sizeof msg, stdin)) {
    cJSON *msg_json = cJSON_Parse(msg);
    if (!msg_json) {
      fprintf(stderr, "couldn't parse msg: %s\n", msg);
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

    if (!cJSON_HasObjectItem(body, "type") || !cJSON_HasObjectItem(body, "msg_id")) {
      fprintf(stderr, "field/s type, msg_id not found\n");
      cJSON_Delete(msg_json);
      continue;
    }

    type = cJSON_GetObjectItem(body, "type");
    msg_id = cJSON_GetObjectItem(body, "msg_id");
    if (!cJSON_IsString(type) || !cJSON_IsNumber(msg_id)) {
      fprintf(stderr, "incorrect type for type or msg_id\n");
      cJSON_Delete(msg_json);
      continue;
    }
    long in_reply_to = (long)msg_id->valuedouble;

    if (!strcmp(type->valuestring, "init")) {
      cJSON *res_body = reply_body(next_msg_id++, "init_ok", in_reply_to);
      send_reply(dest->valuestring, src->valuestring, res_body);
    } else if (!strcmp(type->valuestring, "echo")) {
      const cJSON *echo = cJSON_GetObjectItem(body, "echo");
      if (!cJSON_IsString(echo)) {
        send_error(dest->valuestring, src->valuestring, next_msg_id++, in_reply_to, 13,
                   "incorrect echo type");
      } else {
        cJSON *res_body = reply_body(next_msg_id++, "echo_ok", in_reply_to);
        cJSON_AddStringToObject(res_body, "echo", echo->valuestring);
        send_reply(dest->valuestring, src->valuestring, res_body);
      }

    } else if (!strcmp(type->valuestring, "generate")) {
      int64_t now = now_ms();
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
      char id[64];
      snprintf(id, sizeof id, "%u-%lld-%d", NODE_ID, (long long)now, GENERATE_MS_COUNTER);
      cJSON *res_body = reply_body(next_msg_id++, "generate_ok", in_reply_to);
      cJSON_AddStringToObject(res_body, "id", id);
      send_reply(dest->valuestring, src->valuestring, res_body);
    } else {
      send_error(dest->valuestring, src->valuestring, next_msg_id++, in_reply_to, 10,
                 "not supported");
    }

    cJSON_Delete(msg_json);
  }

  return 0;
}
