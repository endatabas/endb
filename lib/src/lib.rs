pub mod parser;

use chumsky::error::Rich;
use chumsky::extra::Err;
use chumsky::prelude::{Parser, Recursive};

use libc::c_char;
use std::ffi::{CStr, CString};

use parser::Ast;

std::thread_local! {
  pub static SQL_AST_PARSER: Recursive<dyn Parser<'static, &'static str, Ast, Err<Rich<'static, char>>>> = parser::sql_ast_parser();
}

#[no_mangle]
pub extern "C" fn endb_parse_sql(
    input: *const c_char,
    on_success: extern "C" fn(&Ast),
    on_error: extern "C" fn(*const c_char),
) {
    SQL_AST_PARSER.with(|parser| {
        let c_str = unsafe { CStr::from_ptr(input) };
        let input_str = c_str.to_str().unwrap();
        let result = parser.parse(input_str);
        if result.has_errors() {
            let error_str = parser::parse_errors_to_string(input_str, result.into_errors());
            let c_error_str = CString::new(error_str).unwrap();
            on_error(c_error_str.as_ptr());
        } else {
            on_success(&result.into_output().unwrap());
        }
    })
}

#[no_mangle]
pub extern "C" fn endb_ast_vec_len(ast: &Vec<Ast>) -> usize {
    ast.len()
}

#[no_mangle]
pub extern "C" fn endb_ast_vec_ptr(ast: &Vec<Ast>) -> *const Ast {
    ast.as_ptr()
}

#[no_mangle]
pub extern "C" fn endb_ast_size() -> usize {
    std::mem::size_of::<Ast>()
}

#[no_mangle]
pub extern "C" fn endb_ast_vec_element(ast: &Vec<Ast>, idx: usize) -> *const Ast {
    &ast[idx]
}
