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

void endb_parse_sql(const char *input,
                    void (*on_success)(const struct Ast*),
                    void (*on_error)(const char*));

void endb_annotate_input_with_error(const char *input,
                                    const char *message,
                                    uintptr_t start,
                                    uintptr_t end,
                                    void (*on_success)(const char*),
                                    void (*on_error)(const char*));

uintptr_t endb_ast_vec_len(const struct Vec_Ast *ast);

const struct Ast *endb_ast_vec_ptr(const struct Vec_Ast *ast);

uintptr_t endb_ast_size(void);

const struct Ast *endb_ast_vec_element(const struct Vec_Ast *ast, uintptr_t idx);

void endb_arrow_array_stream_producer(struct ArrowArrayStream *stream,
                                      const uint8_t *buffer_ptr,
                                      uintptr_t buffer_size,
                                      void (*on_error)(const char*));

void endb_arrow_array_stream_consumer(void (*init_stream)(struct ArrowArrayStream*),
                                      void (*on_success)(const uint8_t*, uintptr_t),
                                      void (*on_error)(const char*));

void endb_parse_sql_cst(const char *filename,
                        const char *input,
                        void (*on_open)(const uint8_t*, uintptr_t),
                        void (*on_close)(void),
                        void (*on_literal)(const uint8_t*, uintptr_t, uintptr_t, uintptr_t),
                        void (*on_pattern)(uintptr_t, uintptr_t),
                        void (*on_error)(const char*));

void endb_render_json_error_report(const char *report_json,
                                   void (*on_success)(const char*),
                                   void (*on_error)(const char*));

void endb_init_logger(void);

void endb_log_error(const char *target, const char *message);

void endb_log_warn(const char *target, const char *message);

void endb_log_info(const char *target, const char *message);

void endb_log_debug(const char *target, const char *message);

void endb_log_trace(const char *target, const char *message);

void endb_start_server(void (*on_init)(const char*),
                       void (*on_query)(const char*,
                                        const char*,
                                        const char*,
                                        const char*,
                                        const char*,
                                        void(*)(uint16_t, const char*, const char*)),
                       void (*on_error)(const char*));
