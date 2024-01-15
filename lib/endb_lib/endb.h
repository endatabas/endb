#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct endb_server_http_response endb_server_http_response;

typedef struct endb_server_http_sender endb_server_http_sender;

typedef struct endb_server_http_websocket_stream endb_server_http_websocket_stream;

typedef struct endb_server_one_shot_sender endb_server_one_shot_sender;

/**
 * ABI-compatible struct for `ArrayStream` from C Stream Interface
 * See <https://arrow.apache.org/docs/format/CStreamInterface.html#structure-definitions>
 * This was created by bindgen
 */
typedef struct FFI_ArrowArrayStream {
  int (*get_schema)(struct FFI_ArrowArrayStream *arg1, FFI_ArrowSchema *out);
  int (*get_next)(struct FFI_ArrowArrayStream *arg1, FFI_ArrowArray *out);
  const char *(*get_last_error)(struct FFI_ArrowArrayStream *arg1);
  void (*release)(struct FFI_ArrowArrayStream *arg1);
  void *private_data;
} FFI_ArrowArrayStream;

typedef void (*endb_on_error_callback)(const char*);

typedef void (*endb_arrow_array_stream_consumer_on_init_stream_callback)(struct FFI_ArrowArrayStream*);

typedef void (*endb_arrow_array_stream_consumer_on_success_callback)(const uint8_t*, uintptr_t);

typedef void (*endb_parse_sql_cst_on_open_callback)(const uint8_t*, uint32_t);

typedef void (*endb_parse_sql_cst_on_close_callback)(void);

typedef void (*endb_parse_sql_cst_on_literal_callback)(const uint8_t*, uint32_t, uint32_t);

typedef void (*endb_parse_sql_cst_on_pattern_callback)(uint32_t, uint32_t);

typedef void (*endb_render_json_error_report_on_success_callback)(const char*);

typedef void (*endb_init_logger_on_success_callback)(const char*);

typedef void (*endb_start_tokio_on_init)(void);

typedef void (*endb_start_server_on_query_on_abort_callback)(void);

typedef void (*endb_start_server_on_query_on_response_init_callback)(struct endb_server_http_response*,
                                                                     struct endb_server_one_shot_sender*,
                                                                     uint16_t,
                                                                     const char*,
                                                                     endb_start_server_on_query_on_abort_callback);

typedef void (*endb_start_server_on_query_on_response_send_callback)(struct endb_server_http_sender*,
                                                                     const uint8_t*,
                                                                     uintptr_t,
                                                                     endb_start_server_on_query_on_abort_callback);

typedef void (*endb_start_server_on_query_callback)(struct endb_server_http_response*,
                                                    struct endb_server_http_sender*,
                                                    struct endb_server_one_shot_sender*,
                                                    const char*,
                                                    const char*,
                                                    const char*,
                                                    const char*,
                                                    const char*,
                                                    endb_start_server_on_query_on_response_init_callback,
                                                    endb_start_server_on_query_on_response_send_callback);

typedef void (*endb_start_server_on_websocket_init_callback)(const char*);

typedef void (*endb_start_server_on_websocket_close_callback)(const char*);

typedef void (*endb_start_server_on_websocket_message_on_abort_callback)(void);

typedef void (*endb_start_server_on_websocket_message_on_response_send_callback)(struct endb_server_http_websocket_stream*,
                                                                                 const uint8_t*,
                                                                                 uintptr_t,
                                                                                 endb_start_server_on_websocket_message_on_abort_callback);

typedef void (*endb_start_server_on_websocket_message_callback)(const char*,
                                                                struct endb_server_http_websocket_stream*,
                                                                const uint8_t*,
                                                                uintptr_t,
                                                                endb_start_server_on_websocket_message_on_response_send_callback);

typedef void (*endb_parse_command_line_to_json_on_success_callback)(const char*);

typedef void (*endb_version_on_success_callback)(const char*);

typedef void (*endb_base64_encode_on_success_callback)(const char*);

typedef void (*endb_base64_decode_on_success_callback)(const uint8_t*, uintptr_t);

typedef void (*endb_sha1_on_success_callback)(const char*);

typedef void (*endb_uuid_v4_on_success_callback)(const char*);

typedef void (*endb_uuid_str_on_success_callback)(const char*);

void endb_arrow_array_stream_producer(struct FFI_ArrowArrayStream *stream,
                                      const uint8_t *buffer_ptr,
                                      uintptr_t buffer_size,
                                      endb_on_error_callback on_error);

void endb_arrow_array_stream_consumer(endb_arrow_array_stream_consumer_on_init_stream_callback on_init_stream,
                                      endb_arrow_array_stream_consumer_on_success_callback on_success,
                                      endb_on_error_callback on_error,
                                      char ipc_stream);

void endb_parse_sql_cst(const char *filename,
                        const char *input,
                        endb_parse_sql_cst_on_open_callback on_open,
                        endb_parse_sql_cst_on_close_callback on_close,
                        endb_parse_sql_cst_on_literal_callback on_literal,
                        endb_parse_sql_cst_on_pattern_callback on_pattern,
                        endb_on_error_callback on_error);

void endb_render_json_error_report(const char *report_json,
                                   endb_render_json_error_report_on_success_callback on_success,
                                   endb_on_error_callback on_error);

void endb_init_logger(endb_init_logger_on_success_callback on_success,
                      endb_on_error_callback on_error);

void endb_log_error(const char *target, const char *message);

void endb_log_warn(const char *target, const char *message);

void endb_log_info(const char *target, const char *message);

void endb_log_debug(const char *target, const char *message);

void endb_log_trace(const char *target, const char *message);

void endb_start_tokio(endb_start_tokio_on_init on_init, endb_on_error_callback on_error);

void endb_start_server(endb_start_server_on_query_callback on_query,
                       endb_on_error_callback on_error,
                       endb_start_server_on_websocket_init_callback on_ws_init,
                       endb_start_server_on_websocket_close_callback on_ws_close,
                       endb_start_server_on_websocket_message_callback on_ws_message);

void endb_set_panic_hook(endb_on_error_callback on_panic);

void endb_parse_command_line_to_json(endb_parse_command_line_to_json_on_success_callback on_success);

void endb_version(endb_version_on_success_callback on_success);

void endb_base64_encode(const uint8_t *buffer_ptr,
                        uintptr_t buffer_size,
                        endb_base64_encode_on_success_callback on_success);

void endb_base64_decode(const char *string,
                        endb_base64_decode_on_success_callback on_success,
                        endb_on_error_callback on_error);

void endb_sha1(const uint8_t *buffer_ptr,
               uintptr_t buffer_size,
               endb_sha1_on_success_callback on_success);

void endb_uuid_v4(endb_uuid_v4_on_success_callback on_success);

void endb_uuid_str(const uint8_t *buffer_ptr,
                   uintptr_t buffer_size,
                   endb_uuid_str_on_success_callback on_success,
                   endb_on_error_callback on_error);

uint64_t endb_xxh64(const uint8_t *buffer_ptr, uintptr_t buffer_size, uint64_t seed);

uint8_t *endb_memcpy(uint8_t *dest_buffer_ptr,
                     const uint8_t *src_buffer_ptr,
                     uintptr_t buffer_size);
