use libc::c_char;
use std::ffi::{CStr, CString};

use arrow2::ffi::ArrowArrayStream;
use chumsky::Parser;
use endb_parser::parser::ast::Ast;
use endb_parser::parser::sql_parser;
use endb_parser::{SQL_AST_PARSER_NO_ERRORS, SQL_AST_PARSER_WITH_ERRORS};
use endb_storage::arrow;

use std::panic;

fn handle_error(error_str: String, on_error: extern "C" fn(*const c_char)) {
    let c_error_str = CString::new(error_str).unwrap();
    on_error(c_error_str.as_ptr());
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_parse_sql(
    input: *const c_char,
    on_success: extern "C" fn(&Ast),
    on_error: extern "C" fn(*const c_char),
) {
    if let Err(err) = panic::catch_unwind(|| {
        SQL_AST_PARSER_NO_ERRORS.with(|parser| {
            let c_str = unsafe { CStr::from_ptr(input) };
            let input_str = c_str.to_str().unwrap();
            let result = parser.parse(input_str);
            if result.has_output() {
                on_success(&result.into_output().unwrap());
            } else {
                SQL_AST_PARSER_WITH_ERRORS.with(|parser| {
                    let result = parser.parse(input_str);
                    let error_str =
                        sql_parser::parse_errors_to_string(input_str, result.into_errors());
                    handle_error(error_str, on_error);
                });
            }
        })
    }) {
        let msg = err.downcast_ref::<&str>().unwrap_or(&"unknown panic!!");
        handle_error(msg.to_string(), on_error);
    }
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
#[allow(clippy::ptr_arg)]
pub extern "C" fn endb_ast_vec_element(ast: &Vec<Ast>, idx: usize) -> *const Ast {
    &ast[idx]
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_arrow_array_stream_producer(
    stream: &mut ArrowArrayStream,
    buffer_ptr: *const u8,
    buffer_size: usize,
    on_error: extern "C" fn(*const c_char),
) {
    match panic::catch_unwind(|| {
        let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
        arrow::read_arrow_array_stream_from_ipc_buffer(buffer)
    }) {
        Ok(Ok(exported_stream)) => unsafe {
            std::ptr::write(stream, exported_stream);
        },
        Ok(Err(err)) => {
            handle_error(err.to_string(), on_error);
        }
        Err(err) => {
            let msg = err.downcast_ref::<&str>().unwrap_or(&"unknown panic!!");
            handle_error(msg.to_string(), on_error);
        }
    }
}

#[no_mangle]
pub extern "C" fn endb_arrow_array_stream_consumer(
    init_stream: extern "C" fn(&mut ArrowArrayStream),
    on_success: extern "C" fn(*const u8, usize),
    on_error: extern "C" fn(*const c_char),
) {
    match panic::catch_unwind(|| {
        let mut stream = ArrowArrayStream::empty();
        init_stream(&mut stream);
        arrow::write_arrow_array_stream_to_ipc_buffer(stream)
    }) {
        Ok(Ok(buffer)) => on_success(buffer.as_ptr(), buffer.len()),
        Ok(Err(err)) => {
            handle_error(err.to_string(), on_error);
        }
        Err(err) => {
            let msg = err.downcast_ref::<&str>().unwrap_or(&"unknown panic!!");
            handle_error(msg.to_string(), on_error);
        }
    }
}
