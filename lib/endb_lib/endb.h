#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum Keyword {
  Select,
  From,
  Where,
  GroupBy,
  Having,
  OrderBy,
  Lt,
  Le,
  Gt,
  Ge,
  Eq,
  Ne,
  Is,
  In,
  InQuery,
  Between,
  Like,
  Case,
  Exists,
  ScalarSubquery,
  Else,
  Plus,
  Minus,
  Mul,
  Div,
  Mod,
  Lsh,
  Rsh,
  And,
  Or,
  Not,
  Function,
  AggregateFunction,
  Count,
  CountStar,
  Avg,
  Sum,
  Min,
  Max,
  Total,
  GroupConcat,
  Cast,
  Asc,
  Desc,
  Distinct,
  All,
  True,
  False,
  Null,
  Limit,
  Offset,
  Join,
  Type,
  Left,
  Inner,
  On,
  Except,
  Intersect,
  Union,
  UnionAll,
  Values,
  Insert,
  ColumnNames,
  Delete,
  Update,
  CreateIndex,
  DropIndex,
  CreateView,
  DropView,
  IfExists,
  CreateTable,
  DropTable,
  MultipleStatements,
  Date,
  Time,
  Timestamp,
  Array,
  Object,
  Access,
  AsOf,
  With,
  ArrayAgg,
  ObjectAgg,
  ArrayQuery,
  Unnest,
  WithOrdinality,
  Objects,
  Parameter,
  Concat,
  ShorthandProperty,
  SpreadProperty,
  ComputedProperty,
  Duration,
  CurrentDate,
  CurrentTime,
  CurrentTimestamp,
  Unset,
  Recursive,
  Overlaps,
  Contains,
  Precedes,
  Succeeds,
  ImmediatelyPrecedes,
  ImmediatelySucceeds,
  Year,
  Month,
  Day,
  Hour,
  Minute,
  Second,
  Interval,
  OnConflict,
  Blob,
  Glob,
  Regexp,
  Patch,
  Match,
  BitNot,
  BitAnd,
  BitOr,
  Hash,
  Path,
  CreateAssertion,
  DropAssertion,
  Extract,
  Erase,
} Keyword;

typedef struct Vec_Ast Vec_Ast;

typedef struct endb_server_http_response endb_server_http_response;

typedef struct endb_server_http_sender endb_server_http_sender;

typedef struct endb_server_one_shot_sender endb_server_one_shot_sender;

typedef enum Ast_Tag {
  List,
  KW,
  Integer,
  Float,
  Id,
  String,
} Ast_Tag;

typedef struct Id_Body {
  int32_t start;
  int32_t end;
} Id_Body;

typedef struct String_Body {
  int32_t start;
  int32_t end;
} String_Body;

typedef struct Ast {
  Ast_Tag tag;
  union {
    struct {
      struct Vec_Ast list;
    };
    struct {
      enum Keyword kw;
    };
    struct {
      i128 integer;
    };
    struct {
      double float_;
    };
    Id_Body id;
    String_Body string;
  };
} Ast;

typedef void (*endb_parse_sql_on_success_callback)(const struct Ast*);

typedef void (*endb_on_error_callback)(const char*);

typedef void (*endb_annotate_input_with_error_on_success_callback)(const char*);

/**
 * ABI-compatible struct for [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions)
 */
typedef struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *arg1);
  void *private_data;
} ArrowSchema;

/**
 * ABI-compatible struct for [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions)
 */
typedef struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *arg1);
  void *private_data;
} ArrowArray;

/**
 * ABI-compatible struct for [`ArrowArrayStream`](https://arrow.apache.org/docs/format/CStreamInterface.html).
 */
typedef struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *arg1, struct ArrowSchema *out);
  int (*get_next)(struct ArrowArrayStream *arg1, struct ArrowArray *out);
  const char *(*get_last_error)(struct ArrowArrayStream *arg1);
  void (*release)(struct ArrowArrayStream *arg1);
  void *private_data;
} ArrowArrayStream;

typedef void (*endb_arrow_array_stream_consumer_on_init_stream_callback)(struct ArrowArrayStream*);

typedef void (*endb_arrow_array_stream_consumer_on_success_callback)(const uint8_t*, uintptr_t);

typedef void (*endb_parse_sql_cst_on_open_callback)(const uint8_t*, uintptr_t);

typedef void (*endb_parse_sql_cst_on_close_callback)(void);

typedef void (*endb_parse_sql_cst_on_literal_callback)(const uint8_t*,
                                                       uintptr_t,
                                                       uintptr_t,
                                                       uintptr_t);

typedef void (*endb_parse_sql_cst_on_pattern_callback)(uintptr_t, uintptr_t);

typedef void (*endb_render_json_error_report_on_success_callback)(const char*);

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

typedef void (*endb_parse_command_line_to_json_on_success_callback)(const char*);

void endb_parse_sql(const char *input,
                    endb_parse_sql_on_success_callback on_success,
                    endb_on_error_callback on_error);

void endb_annotate_input_with_error(const char *input,
                                    const char *message,
                                    uintptr_t start,
                                    uintptr_t end,
                                    endb_annotate_input_with_error_on_success_callback on_success);

uintptr_t endb_ast_vec_len(const struct Vec_Ast *ast);

const struct Ast *endb_ast_vec_ptr(const struct Vec_Ast *ast);

uintptr_t endb_ast_size(void);

const struct Ast *endb_ast_vec_element(const struct Vec_Ast *ast, uintptr_t idx);

void endb_arrow_array_stream_producer(struct ArrowArrayStream *stream,
                                      const uint8_t *buffer_ptr,
                                      uintptr_t buffer_size,
                                      endb_on_error_callback on_error);

void endb_arrow_array_stream_consumer(endb_arrow_array_stream_consumer_on_init_stream_callback on_init_stream,
                                      endb_arrow_array_stream_consumer_on_success_callback on_success,
                                      endb_on_error_callback on_error);

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

void endb_init_logger(endb_on_error_callback on_error);

void endb_log_error(const char *target, const char *message);

void endb_log_warn(const char *target, const char *message);

void endb_log_info(const char *target, const char *message);

void endb_log_debug(const char *target, const char *message);

void endb_log_trace(const char *target, const char *message);

void endb_start_server(endb_start_server_on_query_callback on_query,
                       endb_on_error_callback on_error);

void endb_set_panic_hook(endb_on_error_callback on_panic);

void endb_parse_command_line_to_json(endb_parse_command_line_to_json_on_success_callback on_success);
