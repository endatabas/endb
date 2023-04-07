pub mod parser;

use chumsky::error::Rich;
use chumsky::extra::{Default, Err};
use chumsky::prelude::Boxed;
use chumsky::Parser;

use crate::parser::{ast::Ast, sql_parser};

std::thread_local! {
    pub static SQL_AST_PARSER_NO_ERRORS: Boxed<'static, 'static, &'static str, Ast, Default> = sql_parser::sql_ast_parser_no_errors().boxed();
    pub static SQL_AST_PARSER_WITH_ERRORS: Boxed<'static, 'static, &'static str, Ast, Err<Rich<'static, char>>> = sql_parser::sql_ast_parser_with_errors().boxed();
}
