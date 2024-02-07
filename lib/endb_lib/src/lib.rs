#![allow(non_camel_case_types)]

use libc::c_char;
use std::ffi::{CStr, CString};

use base64::Engine;

use std::panic;

fn string_callback<T: Into<Vec<u8>>>(s: T, cb: extern "C" fn(*const c_char)) {
    let c_string = CString::new(s).unwrap();
    cb(c_string.as_ptr());
}

type endb_on_error_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_arrow_array_stream_producer(
    stream: &mut arrow::ffi_stream::FFI_ArrowArrayStream,
    buffer_ptr: *const u8,
    buffer_size: usize,
    on_error: endb_on_error_callback,
) {
    let buffer = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_size) };
    match endb_arrow::read_arrow_array_stream_from_ipc_buffer(buffer) {
        Ok(exported_stream) => unsafe {
            std::ptr::write(stream, exported_stream);
        },
        Err(err) => {
            string_callback(err.to_string(), on_error);
        }
    }
}

type endb_arrow_array_stream_consumer_on_init_stream_callback =
    extern "C" fn(&mut arrow::ffi_stream::FFI_ArrowArrayStream);

type endb_arrow_array_stream_consumer_on_success_callback = extern "C" fn(*const u8, usize);

#[no_mangle]
pub extern "C" fn endb_arrow_array_stream_consumer(
    on_init_stream: endb_arrow_array_stream_consumer_on_init_stream_callback,
    on_success: endb_arrow_array_stream_consumer_on_success_callback,
    on_error: endb_on_error_callback,
    ipc_stream: c_char,
) {
    let mut stream = arrow::ffi_stream::FFI_ArrowArrayStream::empty();
    on_init_stream(&mut stream);
    match endb_arrow::write_arrow_array_stream_to_ipc_buffer(stream, ipc_stream != 0) {
        Ok(buffer) => on_success(buffer.as_ptr(), buffer.len()),
        Err(err) => {
            string_callback(err.to_string(), on_error);
        }
    }
}

type endb_parse_sql_cst_on_open_callback = extern "C" fn(*const u8, u32);

type endb_parse_sql_cst_on_close_callback = extern "C" fn();

type endb_parse_sql_cst_on_literal_callback = extern "C" fn(*const u8, u32, u32);

type endb_parse_sql_cst_on_pattern_callback = extern "C" fn(u32, u32);

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
            for e in state.events.iter().rev() {
                match e {
                    endb_cst::Event::Open {
                        hide: false, label, ..
                    } => {
                        on_open(label.as_ptr(), label.len().try_into().unwrap());
                    }
                    endb_cst::Event::Close { hide: false, .. } => {
                        on_close();
                    }
                    endb_cst::Event::Literal { literal, range } => {
                        on_literal(literal.as_ptr(), range.start, range.end);
                    }
                    endb_cst::Event::Pattern { range, .. } => {
                        on_pattern(range.start, range.end);
                    }
                    _ => {}
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

type endb_init_logger_on_success_callback = extern "C" fn(*const c_char);

#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn endb_init_logger(
    on_success: endb_init_logger_on_success_callback,
    on_error: endb_on_error_callback,
) {
    #[cfg(feature = "server")]
    match endb_server::init_logger() {
        Ok(level) => string_callback(level.to_string(), on_success),
        Err(err) => string_callback(err.to_string(), on_error),
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

#[cfg(feature = "server")]
type endb_start_tokio_on_init = extern "C" fn();

#[cfg(feature = "server")]
#[no_mangle]
#[allow(clippy::redundant_closure)]
pub extern "C" fn endb_start_tokio(
    on_init: endb_start_tokio_on_init,
    on_error: endb_on_error_callback,
) {
    if let Err(err) = endb_server::start_tokio(move || on_init()) {
        string_callback(err.to_string(), on_error);
    }
}

type endb_trace_span_in_scope = extern "C" fn();

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_trace_span(
    span: *const c_char,
    kvs_json: *const c_char,
    in_scope: endb_trace_span_in_scope,
) {
    let span = unsafe { CStr::from_ptr(span).to_str().unwrap() };
    let span = match span {
        "buffer_pool_eviction" => {
            tracing::error_span!("buffer_pool_eviction")
        }
        "commit" => tracing::error_span!("commit", tx_id = tracing::field::Empty,),
        "compaction" => {
            tracing::error_span!("compaction", compaction_table = tracing::field::Empty,)
        }
        "constraints" => {
            tracing::error_span!("constraints")
        }
        "gc" => tracing::error_span!("gc"),
        "index" => tracing::error_span!("index", index_id = tracing::field::Empty,),
        "log_replay" => tracing::error_span!("log_replay"),
        "log_rotation" => tracing::error_span!("log_rotation"),
        "object_store_delete" => {
            tracing::error_span!("object_store_delete", path = tracing::field::Empty,)
        }
        "object_store_get" => {
            tracing::error_span!("object_store_get", path = tracing::field::Empty,)
        }
        "object_store_list" => {
            tracing::error_span!("object_store_list", prefix = tracing::field::Empty,)
        }
        "object_store_put" => {
            tracing::error_span!("object_store_put", path = tracing::field::Empty,)
        }
        "query" => tracing::error_span!(
            "query",
            query_id = tracing::field::Empty,
            query_base_tx_id = tracing::field::Empty,
            query_interactive_tx_id = tracing::field::Empty,
        ),
        "shutdown" => tracing::error_span!("shutdown"),
        "snapshot" => tracing::error_span!("snapshot", snapshot_tx_id = tracing::field::Empty,),
        "startup" => tracing::error_span!("startup"),
        "wal_append_entry" => {
            tracing::error_span!("wal_append_entry", path = tracing::field::Empty,)
        }
        "wal_read_next_entry" => {
            tracing::error_span!("wal_read_next_entry")
        }
        "wal_fsync" => {
            tracing::error_span!("wal_fsync")
        }
        _ => todo!("unknown span: {}", span),
    };

    if !kvs_json.is_null() {
        let kvs_json = unsafe { CStr::from_ptr(kvs_json).to_str().unwrap() };
        if let Ok(serde_json::Value::Object(json)) =
            serde_json::from_slice::<serde_json::Value>(kvs_json.as_ref())
        {
            for (k, v) in json {
                if let serde_json::Value::String(v) = v {
                    span.record(k.as_str(), v);
                } else {
                    span.record(k.as_str(), v.to_string());
                }
            }
        }
    }

    let _enter = span.enter();
    in_scope();
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_metric_monotonic_counter(name: *const c_char, value: usize) {
    let name = unsafe { CStr::from_ptr(name).to_str().unwrap() };
    match name {
        "websocket_message_internal_errors_total" => {
            tracing::trace!(monotonic_counter.websocket_message_internal_errors_total = value)
        }
        "object_store_read_bytes_total" => {
            tracing::trace!(monotonic_counter.object_store_read_bytes_total = value)
        }
        "object_store_written_bytes_total" => {
            tracing::trace!(monotonic_counter.object_store_written_bytes_total = value)
        }
        "queries_total" => tracing::trace!(monotonic_counter.queries_total = value),
        "transactions_conflicted_total" => {
            tracing::trace!(monotonic_counter.transactions_conflicted_total = value)
        }
        "transactions_committed_total" => {
            tracing::trace!(monotonic_counter.transactions_committed_total = value)
        }
        "transactions_prepared_total" => {
            tracing::trace!(monotonic_counter.transactions_prepared_total = value)
        }
        "transactions_retried_total" => {
            tracing::trace!(monotonic_counter.transactions_retried_total = value)
        }
        "wal_read_bytes_total" => tracing::trace!(monotonic_counter.wal_read_bytes_total = value),
        "wal_written_bytes_total" => {
            tracing::trace!(monotonic_counter.wal_written_bytes_total = value)
        }
        _ => todo!("unknown monotonic counter: {}", name),
    };
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_metric_counter(name: *const c_char, value: isize) {
    let name = unsafe { CStr::from_ptr(name).to_str().unwrap() };
    match name {
        "queries_active" => tracing::trace!(counter.queries_active = value),
        "interactive_transactions_active" => {
            tracing::trace!(counter.interactive_transactions_active = value)
        }
        "buffer_pool_usage_bytes" => {
            tracing::trace!(counter.buffer_pool_usage_bytes = value)
        }
        "dynamic_space_usage_bytes" => tracing::trace!(counter.dynamic_space_usage_bytes = value),
        _ => todo!("unknown counter: {}", name),
    };
}

#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn endb_metric_histogram(name: *const c_char, value: f64) {
    let name = unsafe { CStr::from_ptr(name).to_str().unwrap() };
    match name {
        "query_real_time_duration_seconds" => {
            tracing::trace!(histogram.query_real_time_duration_seconds = value)
        }
        "query_gc_run_time_duration_seconds" => {
            tracing::trace!(histogram.query_gc_run_time_duration_seconds = value)
        }
        "query_consed_bytes" => tracing::trace!(histogram.query_consed_bytes = value),
        _ => todo!("unknown histogram: {}", name),
    };
}

#[cfg(feature = "server")]
pub struct endb_server_http_response(endb_server::HttpResponse);

#[cfg(feature = "server")]
pub struct endb_server_http_sender(endb_server::HttpSender);

#[cfg(feature = "server")]
pub struct endb_server_one_shot_sender(endb_server::OneShotSender);

#[cfg(feature = "server")]
type endb_start_server_on_query_on_abort_callback = extern "C" fn();

#[cfg(feature = "server")]
type endb_start_server_on_query_on_response_init_callback = extern "C" fn(
    *mut endb_server_http_response,
    *mut endb_server_one_shot_sender,
    u16,
    *const c_char,
    endb_start_server_on_query_on_abort_callback,
);

#[cfg(feature = "server")]
type endb_start_server_on_query_on_response_send_callback = extern "C" fn(
    *mut endb_server_http_sender,
    *const u8,
    usize,
    endb_start_server_on_query_on_abort_callback,
);

#[cfg(feature = "server")]
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

#[cfg(feature = "server")]
pub struct endb_server_http_websocket_stream(endb_server::HttpWebsocketStream);

#[cfg(feature = "server")]
type endb_start_server_on_websocket_init_callback = extern "C" fn(*const c_char);

#[cfg(feature = "server")]
type endb_start_server_on_websocket_close_callback = extern "C" fn(*const c_char);

#[cfg(feature = "server")]
type endb_start_server_on_websocket_message_on_abort_callback = extern "C" fn();

#[cfg(feature = "server")]
type endb_start_server_on_websocket_message_on_response_send_callback = extern "C" fn(
    *mut endb_server_http_websocket_stream,
    *const u8,
    usize,
    endb_start_server_on_websocket_message_on_abort_callback,
);

#[cfg(feature = "server")]
type endb_start_server_on_websocket_message_callback = extern "C" fn(
    *const c_char,
    *mut endb_server_http_websocket_stream,
    *const u8,
    usize,
    endb_start_server_on_websocket_message_on_response_send_callback,
);

#[cfg(feature = "server")]
#[no_mangle]
pub extern "C" fn endb_start_server(
    on_query: endb_start_server_on_query_callback,
    on_error: endb_on_error_callback,
    on_ws_init: endb_start_server_on_websocket_init_callback,
    on_ws_close: endb_start_server_on_websocket_close_callback,
    on_ws_message: endb_start_server_on_websocket_message_callback,
) {
    if let Err(err) = endb_server::start_server(
        move |response, sender, tx, method, media_type, q, p, m| {
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
        },
        move |remote_addr| {
            let remote_addr_cstring = CString::new(remote_addr).unwrap();
            on_ws_init(remote_addr_cstring.as_ptr());
        },
        move |remote_addr| {
            let remote_addr_cstring = CString::new(remote_addr).unwrap();
            on_ws_close(remote_addr_cstring.as_ptr());
        },
        move |remote_addr, ws_stream, message| {
            let remote_addr_cstring = CString::new(remote_addr).unwrap();

            extern "C" fn on_websocket_send_callback(
                ws_stream: *mut endb_server_http_websocket_stream,
                message_ptr: *const u8,
                message_size: usize,
                on_abort: endb_start_server_on_websocket_message_on_abort_callback,
            ) {
                let message = unsafe { std::slice::from_raw_parts(message_ptr, message_size) };
                let ws_stream =
                    unsafe { &mut *(ws_stream as *mut endb_server::HttpWebsocketStream) };

                if endb_server::on_websocket_send(ws_stream, message).is_err() {
                    on_abort();
                }
            }

            on_ws_message(
                remote_addr_cstring.as_ptr(),
                ws_stream as *mut _ as *mut endb_server_http_websocket_stream,
                message.as_ptr(),
                message.len(),
                on_websocket_send_callback,
            );
        },
    ) {
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

#[no_mangle]
pub extern "C" fn endb_shutdown_logger() {
    #[cfg(feature = "server")]
    endb_server::shutdown_logger();
}

#[cfg(feature = "server")]
type endb_parse_command_line_to_json_on_success_callback = extern "C" fn(*const c_char);

#[cfg(feature = "server")]
#[no_mangle]
pub extern "C" fn endb_parse_command_line_to_json(
    on_success: endb_parse_command_line_to_json_on_success_callback,
) {
    endb_server::parse_command_line_to_json(|config_json| string_callback(config_json, on_success));
}

#[cfg(feature = "server")]
type endb_version_on_success_callback = extern "C" fn(*const c_char);

#[cfg(feature = "server")]
#[no_mangle]
pub extern "C" fn endb_version(on_success: endb_version_on_success_callback) {
    string_callback(endb_server::ENDB_GIT_DESCRIBE, on_success);
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
