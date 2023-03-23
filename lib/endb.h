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
  Gt,
  And,
  Function,
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
} Keyword;

typedef struct Vec_Ast Vec_Ast;

typedef enum Ast_Tag {
  List,
  KW,
  Integer,
  Float,
  Id,
  String,
  Binary,
} Ast_Tag;

typedef struct Id_Body {
  uintptr_t start;
  uintptr_t end;
} Id_Body;

typedef struct String_Body {
  uintptr_t start;
  uintptr_t end;
} String_Body;

typedef struct Binary_Body {
  uintptr_t start;
  uintptr_t end;
} Binary_Body;

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
      int64_t integer;
    };
    struct {
      double float_;
    };
    Id_Body id;
    String_Body string;
    Binary_Body binary;
  };
} Ast;

void endb_parse_sql(const char *input,
                    void (*on_success)(const struct Ast*),
                    void (*on_error)(const char*));

uintptr_t endb_ast_vec_len(const struct Vec_Ast *ast);

const struct Ast *endb_ast_vec_ptr(const struct Vec_Ast *ast);

uintptr_t endb_ast_size(void);

const struct Ast *endb_ast_vec_element(const struct Vec_Ast *ast, uintptr_t idx);
