#![allow(non_camel_case_types)]

use libc::c_char;
use std::ffi::{CStr, CString};

use arrow2::ffi::ArrowArrayStream;
use base64::Engine;
use chumsky::Parser;
use endb_arrow::arrow;
use endb_parser::parser::ast::Ast;
use endb_parser::parser::sql_parser;
use endb_parser::{SQL_AST_PARSER_NO_ERRORS, SQL_AST_PARSER_WITH_ERRORS};

use std::panic;

fn string_callback<T: Into<Vec<u8>>>(s: T, cb: extern "C" fn(*const c_char)) {
    let c_string = CString::new(s).unwrap();
    cb(c_string.as_ptr());
}

type endb_on_error_callback = extern "C" fn(*const c_char);

type endb_parse_sql_on_success_callback = extern "C" fn(&Ast);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_parse_sql(
    input: *const c_char,
    on_success: endb_parse_sql_on_success_callback,
    on_error: endb_on_error_callback,
) {
    SQL_AST_PARSER_NO_ERRORS.with(|parser| {
        let input = unsafe { CStr::from_ptr(input).to_str().unwrap() };
        let result = parser.parse(input);
        if result.has_output() {
            on_success(&result.into_output().unwrap());
        } else {
            SQL_AST_PARSER_WITH_ERRORS.with(|parser| {
                let result = parser.parse(input);
                let error = sql_parser::parse_errors_to_string(input, result.into_errors());
                string_callback(error, on_error);
            });
        }
    });
}

type endb_annotate_input_with_error_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_annotate_input_with_error(
    input: *const c_char,
    message: *const c_char,
    start: usize,
    end: usize,
    on_success: endb_annotate_input_with_error_on_success_callback,
) {
    let input = unsafe { CStr::from_ptr(input).to_str().unwrap() };
    let message = unsafe { CStr::from_ptr(message).to_str().unwrap() };

    let error = sql_parser::annotate_input_with_error(input, message, start, end);
    string_callback(error, on_success);
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
    on_error: endb_on_error_callback,
) {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    match arrow::read_arrow_array_stream_from_ipc_buffer(buffer) {
        Ok(exported_stream) => unsafe {
            std::ptr::write(stream, exported_stream);
        },
        Err(err) => {
            string_callback(err.to_string(), on_error);
        }
    }
}

type endb_arrow_array_stream_consumer_on_init_stream_callback =
    extern "C" fn(&mut ArrowArrayStream);

type endb_arrow_array_stream_consumer_on_success_callback = extern "C" fn(*const u8, usize);

#[no_mangle]
pub extern "C" fn endb_arrow_array_stream_consumer(
    on_init_stream: endb_arrow_array_stream_consumer_on_init_stream_callback,
    on_success: endb_arrow_array_stream_consumer_on_success_callback,
    on_error: endb_on_error_callback,
) {
    let mut stream = ArrowArrayStream::empty();
    on_init_stream(&mut stream);
    match arrow::write_arrow_array_stream_to_ipc_buffer(stream) {
        Ok(buffer) => on_success(buffer.as_ptr(), buffer.len()),
        Err(err) => {
            string_callback(err.to_string(), on_error);
        }
    }
}

type endb_parse_sql_cst_on_open_callback = extern "C" fn(*const u8, usize);

type endb_parse_sql_cst_on_close_callback = extern "C" fn();

type endb_parse_sql_cst_on_literal_callback = extern "C" fn(*const u8, usize, usize, usize);

type endb_parse_sql_cst_on_pattern_callback = extern "C" fn(usize, usize);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_parse_sql_cst(
    filename: *const c_char,
    input: *const c_char,
    on_open: endb_parse_sql_cst_on_open_callback,
    on_close: endb_parse_sql_cst_on_close_callback,
    on_literal: endb_parse_sql_cst_on_literal_callback,
    on_pattern: endb_parse_sql_cst_on_pattern_callback,
    on_error: endb_on_error_callback,
) {
    let filename = unsafe { CStr::from_ptr(filename).to_str().unwrap() };
    let input = unsafe { CStr::from_ptr(input).to_str().unwrap() };

    let mut state = endb_cst::ParseState::default();

    match endb_cst::sql::sql_stmt_list(input, 0, &mut state) {
        Ok(_) => {
            for e in state.events {
                match e {
                    endb_cst::Event::Open { label, .. } => {
                        on_open(label.as_ptr(), label.len());
                    }
                    endb_cst::Event::Close {} => {
                        on_close();
                    }
                    endb_cst::Event::Literal { literal, range } => {
                        on_literal(literal.as_ptr(), literal.len(), range.start, range.end);
                    }
                    endb_cst::Event::Pattern { range, .. } => {
                        on_pattern(range.start, range.end);
                    }
                    endb_cst::Event::Error { .. } => {}
                }
            }
        }
        Err(_) => {
            let mut state = endb_cst::ParseState {
                track_errors: true,
                ..endb_cst::ParseState::default()
            };
            let _ = endb_cst::sql::sql_stmt_list(input, 0, &mut state);

            string_callback(
                endb_cst::parse_errors_to_string(
                    filename,
                    input,
                    &endb_cst::events_to_errors(&state.errors),
                )
                .unwrap(),
                on_error,
            );
        }
    };
}

type endb_render_json_error_report_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_render_json_error_report(
    report_json: *const c_char,
    on_success: endb_render_json_error_report_on_success_callback,
    on_error: endb_on_error_callback,
) {
    let report_json = unsafe { CStr::from_ptr(report_json).to_str().unwrap() };

    match endb_cst::json_error_report_to_string(report_json) {
        Ok(report) => {
            string_callback(report, on_success);
        }
        Err(err) => string_callback(err.to_string(), on_error),
    }
}

#[no_mangle]
pub extern "C" fn endb_init_logger(on_error: endb_on_error_callback) {
    if let Err(err) = endb_server::init_logger() {
        string_callback(err.to_string(), on_error);
    }
}

fn do_log(level: log::Level, target: *const c_char, message: *const c_char) {
    let target = unsafe { CStr::from_ptr(target).to_str().unwrap() };
    let message = unsafe { CStr::from_ptr(message).to_str().unwrap() };

    log::log!(target: target, level, "{}", message);
}

#[no_mangle]
pub extern "C" fn endb_log_error(target: *const c_char, message: *const c_char) {
    do_log(log::Level::Error, target, message);
}

#[no_mangle]
pub extern "C" fn endb_log_warn(target: *const c_char, message: *const c_char) {
    do_log(log::Level::Warn, target, message);
}

#[no_mangle]
pub extern "C" fn endb_log_info(target: *const c_char, message: *const c_char) {
    do_log(log::Level::Info, target, message);
}

#[no_mangle]
pub extern "C" fn endb_log_debug(target: *const c_char, message: *const c_char) {
    do_log(log::Level::Debug, target, message);
}

#[no_mangle]
pub extern "C" fn endb_log_trace(target: *const c_char, message: *const c_char) {
    do_log(log::Level::Trace, target, message);
}

pub struct endb_server_http_response(endb_server::HttpResponse);

pub struct endb_server_http_sender(endb_server::HttpSender);

pub struct endb_server_one_shot_sender(endb_server::OneShotSender);

type endb_start_server_on_query_on_abort_callback = extern "C" fn();

type endb_start_server_on_query_on_response_init_callback = extern "C" fn(
    *mut endb_server_http_response,
    *mut endb_server_one_shot_sender,
    u16,
    *const c_char,
    endb_start_server_on_query_on_abort_callback,
);

type endb_start_server_on_query_on_response_send_callback = extern "C" fn(
    *mut endb_server_http_sender,
    *const u8,
    usize,
    endb_start_server_on_query_on_abort_callback,
);

type endb_start_server_on_query_callback = extern "C" fn(
    *mut endb_server_http_response,
    *mut endb_server_http_sender,
    *mut endb_server_one_shot_sender,
    *const c_char,
    *const c_char,
    *const c_char,
    *const c_char,
    *const c_char,
    endb_start_server_on_query_on_response_init_callback,
    endb_start_server_on_query_on_response_send_callback,
);

#[no_mangle]
pub extern "C" fn endb_start_server(
    on_query: endb_start_server_on_query_callback,
    on_error: endb_on_error_callback,
) {
    if let Err(err) =
        endb_server::start_server(move |response, sender, tx, method, media_type, q, p, m| {
            let method_cstring = CString::new(method).unwrap();
            let media_type_cstring = CString::new(media_type).unwrap();
            let q_cstring = CString::new(q).unwrap();
            let p_cstring = CString::new(p).unwrap();
            let m_cstring = CString::new(m).unwrap();

            extern "C" fn on_response_init_callback(
                response: *mut endb_server_http_response,
                tx: *mut endb_server_one_shot_sender,
                status: u16,
                content_type: *const c_char,
                on_abort: endb_start_server_on_query_on_abort_callback,
            ) {
                let content_type = unsafe { CStr::from_ptr(content_type).to_str().unwrap() };

                let response = unsafe { Box::from_raw(response as *mut endb_server::HttpResponse) };
                let tx = unsafe { Box::from_raw(tx as *mut endb_server::OneShotSender) };

                if endb_server::on_response_init(*response, *tx, status, content_type).is_err() {
                    on_abort();
                };
            }
            extern "C" fn on_response_send_callback(
                sender: *mut endb_server_http_sender,
                body_ptr: *const u8,
                body_size: usize,
                on_abort: endb_start_server_on_query_on_abort_callback,
            ) {
                let body = unsafe { std::slice::from_raw_parts(body_ptr, body_size) };
                let sender = unsafe { &mut *(sender as *mut endb_server::HttpSender) };

                if endb_server::on_response_send(sender, body).is_err() {
                    on_abort();
                }
            }

            on_query(
                Box::into_raw(response.into()) as *mut endb_server_http_response,
                sender as *mut _ as *mut endb_server_http_sender,
                Box::into_raw(tx.into()) as *mut endb_server_one_shot_sender,
                method_cstring.as_ptr(),
                media_type_cstring.as_ptr(),
                q_cstring.as_ptr(),
                p_cstring.as_ptr(),
                m_cstring.as_ptr(),
                on_response_init_callback,
                on_response_send_callback,
            );
        })
    {
        string_callback(err.to_string(), on_error);
    }
}

#[no_mangle]
pub extern "C" fn endb_set_panic_hook(on_panic: endb_on_error_callback) {
    let prev = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        string_callback(info.to_string(), on_panic);
        prev(info);
    }));
}

type endb_parse_command_line_to_json_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
pub extern "C" fn endb_parse_command_line_to_json(
    on_success: endb_parse_command_line_to_json_on_success_callback,
) {
    endb_server::parse_command_line_to_json(|config_json| string_callback(config_json, on_success));
}

type endb_version_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
pub extern "C" fn endb_version(on_success: endb_version_on_success_callback) {
    string_callback(endb_server::ENDB_FULL_VERSION, on_success);
}

type endb_base64_encode_on_success_callback = extern "C" fn(*const c_char);

type endb_base64_decode_on_success_callback = extern "C" fn(*const u8, usize);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_base64_encode(
    buffer_ptr: *const u8,
    buffer_size: usize,
    on_success: endb_base64_encode_on_success_callback,
) {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    string_callback(
        base64::engine::general_purpose::STANDARD.encode(buffer),
        on_success,
    );
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_base64_decode(
    string: *const c_char,
    on_success: endb_base64_decode_on_success_callback,
    on_error: endb_on_error_callback,
) {
    let string = unsafe { CStr::from_ptr(string).to_str().unwrap() };
    match base64::engine::general_purpose::STANDARD.decode(string) {
        Ok(buffer) => on_success(buffer.as_ptr(), buffer.len()),
        Err(err) => string_callback(err.to_string(), on_error),
    }
}

type endb_sha1_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_sha1(
    buffer_ptr: *const u8,
    buffer_size: usize,
    on_success: endb_sha1_on_success_callback,
) {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    let mut m = sha1_smol::Sha1::new();
    m.update(buffer);
    string_callback(m.digest().to_string(), on_success);
}

type endb_uuid_v4_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_uuid_v4(on_success: endb_uuid_v4_on_success_callback) {
    string_callback(uuid::Uuid::new_v4().to_string(), on_success);
}

type endb_uuid_str_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_uuid_str(
    buffer_ptr: *const u8,
    buffer_size: usize,
    on_success: endb_uuid_str_on_success_callback,
    on_error: endb_on_error_callback,
) {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    match uuid::Uuid::from_slice(buffer) {
        Ok(uuid) => string_callback(uuid.to_string(), on_success),
        Err(err) => string_callback(err.to_string(), on_error),
    }
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_xxh64(buffer_ptr: *const u8, buffer_size: usize, seed: u64) -> u64 {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    xxhash_rust::xxh64::xxh64(buffer, seed)
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_memcpy(
    dest_buffer_ptr: *mut u8,
    src_buffer_ptr: *const u8,
    buffer_size: usize,
) -> *mut u8 {
    unsafe {
        std::ptr::copy_nonoverlapping(src_buffer_ptr, dest_buffer_ptr, buffer_size);
        dest_buffer_ptr
    }
}
