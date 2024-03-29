use base64::Engine;
use clap::Parser;
use futures::sink::SinkExt;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::marker::{Send, Sync};
use std::pin::Pin;
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::BoxBody<bytes::Bytes, Infallible>;

pub const ENDB_GIT_DESCRIBE: &str = env!("ENDB_GIT_DESCRIBE");

#[derive(Serialize, Deserialize, Debug, Parser)]
#[command(name = "endb", author, version = ENDB_GIT_DESCRIBE, about, long_about = None)]
pub struct CommandLineArguments {
    #[arg(short, long, default_value = "endb_data", env = "ENDB_DATA_DIRECTORY")]
    data_directory: String,
    #[arg(long, env = "ENDB_USERNAME", help_heading = "Authentication")]
    username: Option<String>,
    #[arg(long, env = "ENDB_PASSWORD", help_heading = "Authentication")]
    password: Option<String>,
    #[arg(
        short = 'p',
        long,
        default_value = "3803",
        env = "ENDB_PORT",
        help_heading = "Network"
    )]
    port: u16,
    #[arg(
        long,
        default_value = "0.0.0.0",
        env = "ENDB_BIND_ADDRESS",
        help_heading = "Network"
    )]
    bind_address: String,
    #[arg(long, default_value = "http", value_parser = ["http", "https"], env = "ENDB_PROTOCOL", help_heading = "Network")]
    protocol: String,
    #[arg(
        long,
        env = "ENDB_CERT_FILE",
        required_if_eq("protocol", "https"),
        requires = "key_file",
        help_heading = "Network"
    )]
    cert_file: Option<String>,
    #[arg(
        long,
        env = "ENDB_KEY_FILE",
        required_if_eq("protocol", "https"),
        requires = "cert_file",
        help_heading = "Network"
    )]
    key_file: Option<String>,
}

pub type HttpResponse = Response<BoxBody>;
pub type HttpSender = futures::channel::mpsc::Sender<Result<Frame<bytes::Bytes>, Infallible>>;
pub type OneShotSender = futures::channel::oneshot::Sender<HttpResponse>;

pub fn on_response_init(
    mut response: HttpResponse,
    tx: OneShotSender,
    status_code: u16,
    content_type: &str,
) -> Result<(), Response<BoxBody>> {
    if let Ok(status_code) = StatusCode::from_u16(status_code) {
        *response.status_mut() = status_code;
    }
    if !content_type.is_empty() {
        if let Ok(content_type) = content_type.parse() {
            response
                .headers_mut()
                .insert(hyper::header::CONTENT_TYPE, content_type);
        }
    }
    tx.send(response)
}

pub fn on_response_send(sender: &mut HttpSender, chunk: &[u8]) -> Result<(), Error> {
    Ok(tokio::runtime::Handle::current()
        .block_on(sender.feed(Ok(Frame::data(chunk.to_vec().into()))))?)
}

const REALM: &str = "restricted area";

type OnQueryFn = Arc<
    dyn Fn(HttpResponse, &mut HttpSender, OneShotSender, &str, &str, &str, &str, &str)
        + Sync
        + Send,
>;

pub type HttpWebsocketStream = hyper_tungstenite::HyperWebsocketStream;

type OnWebsocketInitFn = Arc<dyn Fn(&str) + Sync + Send>;
type OnWebsocketCloseFn = Arc<dyn Fn(&str) + Sync + Send>;
type OnWebsocketMessageFn = Arc<dyn Fn(&str, &mut HttpWebsocketStream, &[u8]) + Sync + Send>;

pub fn on_websocket_send(stream: &mut HttpWebsocketStream, message: &[u8]) -> Result<(), Error> {
    tokio::task::block_in_place(|| {
        Ok(tokio::runtime::Handle::current()
            .block_on(stream.send(tungstenite::Message::binary(message.to_vec())))?)
    })
}

#[derive(Clone)]
struct EndbService {
    basic_auth: Option<String>,
    on_query: OnQueryFn,
    on_ws_init: OnWebsocketInitFn,
    on_ws_close: OnWebsocketCloseFn,
    on_ws_message: OnWebsocketMessageFn,
    remote_addr: std::net::SocketAddr,
}

fn empty_response(status_code: StatusCode) -> Result<Response<BoxBody>, Error> {
    Ok(Response::builder()
        .status(status_code)
        .body(Empty::new().boxed())?)
}

async fn sql_response(
    method: Method,
    media_type: &str,
    mut params: HashMap<String, String>,
    on_query: OnQueryFn,
) -> Result<Response<BoxBody>, Error> {
    if let Some(q) = params.remove("q") {
        let media_type = media_type.to_string();
        let p = params.remove("p").unwrap_or_else(|| "[]".to_string());
        let m = params.remove("m").unwrap_or_else(|| "false".to_string());

        let (tx, rx) = futures::channel::oneshot::channel();
        let mut response = empty_response(StatusCode::INTERNAL_SERVER_ERROR)?;

        let child_span = tracing::debug_span!("sql", protocol = "http").or_current();

        tokio::task::spawn_blocking(move || {
            let _entered = child_span.entered();

            let (mut sender, body_rx) = futures::channel::mpsc::channel(0);
            *response.body_mut() = StreamBody::new(body_rx).boxed();

            on_query(
                response,
                &mut sender,
                tx,
                method.as_str(),
                media_type.as_str(),
                q.as_str(),
                p.as_str(),
                m.as_str(),
            );
        });

        Ok(rx.await?)
    } else {
        empty_response(StatusCode::UNPROCESSABLE_ENTITY)
    }
}

async fn serve_websocket(
    websocket: hyper_tungstenite::HyperWebsocket,
    remote_addr: std::net::SocketAddr,
    on_ws_init: OnWebsocketInitFn,
    on_ws_close: OnWebsocketCloseFn,
    on_ws_message: OnWebsocketMessageFn,
) -> Result<(), Error> {
    use futures::StreamExt;

    let mut ws_stream = websocket.await?;
    let remote_addr_string = remote_addr.to_string();

    on_ws_init(remote_addr_string.as_str());
    tracing::trace!(counter.websocket_connections_active = 1);

    while let Some(message) = ws_stream.next().await {
        let message = message?;
        if message.is_text() || message.is_binary() {
            let start_time = std::time::Instant::now();
            on_ws_message(
                remote_addr_string.as_str(),
                &mut ws_stream,
                &message.into_data(),
            );
            let latency = start_time.elapsed();
            tracing::trace!(histogram.websocket_message_duration_seconds = latency.as_secs_f64());
            tracing::trace!(monotonic_counter.websocket_messages_total = 1);
        }
    }

    on_ws_close(remote_addr_string.as_str());
    tracing::trace!(counter.websocket_connections_active = -1);

    Ok(())
}

fn accepted_mime_types<B>(req: &Request<B>) -> Vec<mime::Mime> {
    req.headers()
        .get(hyper::header::ACCEPT)
        .and_then(|x| x.to_str().ok())
        .map(|x| {
            let mut xs = x
                .split(',')
                .flat_map(|x| x.trim().parse::<mime::Mime>())
                .collect::<Vec<_>>();
            xs.sort_by(|x, y| {
                x.get_param("q")
                    .and_then(|x| x.as_str().parse::<f64>().ok())
                    .unwrap_or(1.0)
                    .total_cmp(
                        &y.get_param("q")
                            .and_then(|y| y.as_str().parse::<f64>().ok())
                            .unwrap_or(1.0),
                    )
                    .reverse()
            });
            xs
        })
        .unwrap_or(vec![mime::STAR_STAR])
}

fn content_negotiate<B>(req: &Request<B>) -> Option<&'static str> {
    let accepted = accepted_mime_types(req);
    match req.uri().path() {
        "/metrics" => accepted.iter().find_map(|x| match x.essence_str() {
            "*/*" | "text/plain" if req.uri().path() == "/metrics" => Some(prometheus::TEXT_FORMAT),
            _ => None,
        }),
        "/sql" => accepted.iter().find_map(|x| match x.essence_str() {
            "*/*" | "application/*" | "application/json" => Some("application/json"),
            "application/ld+json" => Some("application/ld+json"),
            "application/x-ndjson" => Some("application/x-ndjson"),
            "application/vnd.apache.arrow.file" => Some("application/vnd.apache.arrow.file"),
            "application/vnd.apache.arrow.stream" => Some("application/vnd.apache.arrow.stream"),
            "text/*" | "text/csv" => Some("text/csv"),
            "multipart/mixed" => Some("multipart/mixed"),
            _ => None,
        }),
        _ => Some("*/*"),
    }
}

impl<B> tower::Service<Request<B>> for EndbService
where
    B: hyper::body::Body + Send + Sync + 'static,
    B::Error: std::error::Error + Sync + Send,
    B::Data: Send,
{
    type Response = Response<BoxBody>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let on_query = self.on_query.clone();
        let on_ws_init = self.on_ws_init.clone();
        let on_ws_close = self.on_ws_close.clone();
        let on_ws_message = self.on_ws_message.clone();
        let remote_addr = self.remote_addr;

        let auth_header_value = if hyper_tungstenite::is_upgrade_request(&req) {
            req.headers()
                .get(hyper::header::AUTHORIZATION)
                .or(req.headers().get(hyper::header::SEC_WEBSOCKET_PROTOCOL))
        } else {
            req.headers().get(hyper::header::AUTHORIZATION)
        };
        let unauthorized = self.basic_auth.is_some()
            && self.basic_auth.as_deref()
                != auth_header_value
                    .and_then(|x| x.to_str().ok())
                    .and_then(|x| percent_encoding::percent_decode_str(x).decode_utf8().ok())
                    .as_deref();

        Box::pin(async move {
            use tracing::Instrument;

            if unauthorized {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(
                        hyper::header::WWW_AUTHENTICATE,
                        format!("Basic realm=\"{}\"", REALM),
                    )
                    .body(Empty::new().boxed())?);
            }

            if hyper_tungstenite::is_upgrade_request(&req) {
                return if req.uri().path() == "/sql" {
                    let (response, websocket) = hyper_tungstenite::upgrade(&mut req, None)?;
                    let child_span =
                        tracing::debug_span!("sql", protocol = "websocket").or_current();
                    tokio::spawn(
                        serve_websocket(
                            websocket,
                            remote_addr,
                            on_ws_init,
                            on_ws_close,
                            on_ws_message,
                        )
                        .instrument(child_span.or_current()),
                    );
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(parts, body.boxed()))
                } else {
                    empty_response(StatusCode::NOT_FOUND)
                };
            }

            let Some(media_type) = content_negotiate(&req) else {
                return empty_response(StatusCode::NOT_ACCEPTABLE);
            };

            let content_type = req
                .headers()
                .get(hyper::header::CONTENT_TYPE)
                .and_then(|x| x.to_str().ok())
                .and_then(|x| x.parse::<mime::Mime>().ok());

            let mut params = if let Some(query) = req.uri().query() {
                url::form_urlencoded::parse(query.as_bytes())
                    .into_owned()
                    .collect::<HashMap<String, String>>()
            } else {
                HashMap::default()
            };

            let method = req.method().clone();

            match (
                req.method(),
                req.uri().path(),
                content_type
                    .clone()
                    .map(|x| x.essence_str().to_string())
                    .as_deref(),
            ) {
                (&Method::GET, "/sql", _) => {
                    sql_response(method, media_type, params, on_query).await
                }
                (&Method::POST, "/sql", Some("application/json" | "application/ld+json")) => {
                    let body = req.collect().await?.to_bytes();
                    if let Ok(serde_json::Value::Object(json)) =
                        serde_json::from_slice::<serde_json::Value>(body.as_ref())
                    {
                        for (k, v) in json {
                            if let serde_json::Value::String(v) = v {
                                params.insert(k, v);
                            } else {
                                params.insert(k, v.to_string());
                            }
                        }
                        sql_response(method, media_type, params, on_query).await
                    } else {
                        empty_response(StatusCode::UNPROCESSABLE_ENTITY)
                    }
                }
                (&Method::POST, "/sql", Some("application/sql")) => {
                    let body = req.collect().await?.to_bytes();
                    if let Ok(q) = std::str::from_utf8(&body) {
                        params.insert("q".to_string(), q.to_string());
                        sql_response(method, media_type, params, on_query).await
                    } else {
                        empty_response(StatusCode::UNPROCESSABLE_ENTITY)
                    }
                }
                (&Method::POST, "/sql", Some("application/x-www-form-urlencoded")) => {
                    let body = req.collect().await?.to_bytes();
                    for (k, v) in url::form_urlencoded::parse(body.as_ref()) {
                        params.insert(k.to_string(), v.to_string());
                    }
                    sql_response(method, media_type, params, on_query).await
                }
                (&Method::POST, "/sql", Some("multipart/form-data")) => {
                    if let Some(boundary) = content_type
                        .and_then(|x| x.get_param(mime::BOUNDARY).map(|x| x.to_string()))
                    {
                        let body = req.collect().await?.to_bytes();
                        let stream = futures::stream::iter([Ok::<_, Infallible>(body)]);
                        let mut mult = multer::Multipart::new(stream, boundary);

                        while let Ok(Some(field)) = mult.next_field().await {
                            if let Some(k) = field.name() {
                                let k = k.to_string();
                                match (
                                    k.as_str(),
                                    field
                                        .content_type()
                                        .unwrap_or(&mime::TEXT_PLAIN)
                                        .essence_str(),
                                ) {
                                    ("q", "text/plain" | "application/sql") => {
                                        let v = field.text().await?;
                                        params.insert(k, v.to_string());
                                    }
                                    (
                                        "p" | "m",
                                        "text/plain" | "application/json" | "application/ld+json",
                                    ) => {
                                        let v = field.text().await?;
                                        params.insert(k, v.to_string());
                                    }
                                    (
                                        "p",
                                        "application/vnd.apache.arrow.file"
                                        | "application/vnd.apache.arrow.stream",
                                    ) => {
                                        let v = field.bytes().await?;
                                        let base64_arrow =
                                            base64::engine::general_purpose::STANDARD.encode(v);
                                        params.insert(
                                            k,
                                            "{\"@value\":\"".to_owned()
                                                + &base64_arrow
                                                + "\",\"@type\":\"xsd:base64Binary\"}",
                                        );
                                    }
                                    ("q" | "p" | "m", _) => {
                                        return Ok(Response::builder()
                                            .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                                            .body(Empty::new().boxed())?)
                                    }
                                    _ => {
                                        return Ok(Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Empty::new().boxed())?)
                                    }
                                }
                            }
                        }
                        sql_response(method, media_type, params, on_query).await
                    } else {
                        empty_response(StatusCode::BAD_REQUEST)
                    }
                }
                (&Method::POST, "/sql", _) => empty_response(StatusCode::UNSUPPORTED_MEDIA_TYPE),
                (&Method::OPTIONS, "/sql", _) => Ok(Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(hyper::header::ALLOW, "OPTIONS, GET, POST")
                    .body(Empty::new().boxed())?),
                (_, "/sql", _) => Ok(Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header(hyper::header::ALLOW, "OPTIONS, GET, POST")
                    .body(Empty::new().boxed())?),
                (&Method::GET, "/metrics", _) => {
                    let encoder = prometheus::TextEncoder::new();
                    let metrics =
                        encoder.encode_to_string(&prometheus::default_registry().gather())?;
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, media_type)
                        .body(Full::from(bytes::Bytes::from(metrics)).boxed())?)
                }
                (&Method::OPTIONS, "/metrics", _) => Ok(Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(hyper::header::ALLOW, "OPTIONS, GET")
                    .body(Empty::new().boxed())?),
                (_, "/metrics", _) => Ok(Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header(hyper::header::ALLOW, "OPTIONS, GET")
                    .body(Empty::new().boxed())?),
                _ => empty_response(StatusCode::NOT_FOUND),
            }
        })
    }

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

fn make_basic_auth_header(username: Option<String>, password: Option<String>) -> Option<String> {
    if let (Some(username), Some(password)) = (username, password) {
        Some(format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password))
        ))
    } else {
        None
    }
}

pub fn start_tokio(on_init: impl Fn() + Sync + Send + 'static) -> Result<(), Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            on_init();
        });
    Ok(())
}

#[derive(Clone, Default)]
struct PrometheusTracing {
    method_and_path: Arc<std::sync::OnceLock<(String, String)>>,
}

impl<B> tower_http::trace::OnRequest<B> for PrometheusTracing {
    fn on_request(&mut self, request: &Request<B>, _: &tracing::Span) {
        if request.uri().path() != "/metrics" {
            let _ = self.method_and_path.set((
                request.method().to_string(),
                request.uri().path().to_string(),
            ));
        }
    }
}

impl<B> tower_http::trace::OnResponse<B> for PrometheusTracing {
    fn on_response(
        self,
        response: &Response<B>,
        latency: std::time::Duration,
        span: &tracing::Span,
    ) {
        if let Some((method, path)) = self.method_and_path.get() {
            let code = response.status().as_u16();
            tracing::trace!(
                histogram.http_request_duration_seconds = latency.as_secs_f64(),
                code,
                method,
                path,
            );
            tracing::trace!(
                monotonic_counter.http_requests_total = 1,
                code,
                method,
                path,
            );
        }
        tower_http::trace::DefaultOnResponse::new()
            .include_headers(true)
            .on_response(response, latency, span);
    }
}

pub fn start_server(
    on_query: impl Fn(HttpResponse, &mut HttpSender, OneShotSender, &str, &str, &str, &str, &str)
        + Sync
        + Send
        + 'static,
    on_ws_init: impl Fn(&str) + Sync + Send + 'static,
    on_ws_close: impl Fn(&str) + Sync + Send + 'static,
    on_ws_message: impl Fn(&str, &mut HttpWebsocketStream, &[u8]) + Sync + Send + 'static,
) -> Result<(), Error> {
    use tracing::Instrument;

    let args = CommandLineArguments::parse();

    let basic_auth = make_basic_auth_header(args.username, args.password);
    let on_query = Arc::new(on_query);
    let on_ws_init = Arc::new(on_ws_init);
    let on_ws_close = Arc::new(on_ws_close);
    let on_ws_message = Arc::new(on_ws_message);

    let addr = format!("{}:{}", args.bind_address, args.port);

    let tls_acceptor = if "https" == args.protocol {
        let mut reader = std::io::BufReader::new(std::fs::File::open(args.cert_file.unwrap())?);
        let cert_chain = rustls_pemfile::certs(&mut reader).flatten().collect();
        let mut reader = std::io::BufReader::new(std::fs::File::open(args.key_file.unwrap())?);
        let key_der = rustls_pemfile::private_key(&mut reader)?
            .ok_or(std::io::Error::other("no private key"))?;
        let mut tls_config = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?;
        tls_config.alpn_protocols =
            vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
        Some(tokio_rustls::TlsAcceptor::from(Arc::new(tls_config)))
    } else {
        None
    };

    let child_span = tracing::error_span!("server").or_current();
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(
            async {
                let listener = tokio::net::TcpListener::bind(addr.clone()).await?;
                tracing::info!(target: "endb/lib/server", "listening on {}://{}", args.protocol, addr);
                tracing::trace!(counter.build_info = 1, version = ENDB_GIT_DESCRIBE);
                loop {
                    let (stream, remote_addr) = listener.accept().await?;
                    let svc = EndbService {
                        basic_auth: basic_auth.clone(),
                        on_query: on_query.clone(),
                        on_ws_init: on_ws_init.clone(),
                        on_ws_close: on_ws_close.clone(),
                        on_ws_message: on_ws_message.clone(),
                        remote_addr,
                    };
                    let prometheus_tracing = PrometheusTracing::default();
                    let svc = tower::ServiceBuilder::new()
                        .layer(
                            tower_http::sensitive_headers::SetSensitiveHeadersLayer::new(
                                std::iter::once(hyper::header::AUTHORIZATION),
                            ),
                        )
                        .layer(tower_http::request_id::SetRequestIdLayer::x_request_id(
                            tower_http::request_id::MakeRequestUuid,
                        ))
                        .layer(
                            tower_http::trace::TraceLayer::new_for_http()
                                .make_span_with(
                                    tower_http::trace::DefaultMakeSpan::new().include_headers(true),
                                )
                                .on_request(prometheus_tracing.clone())
                                .on_response(prometheus_tracing),
                        )
                        .layer(tower_http::request_id::PropagateRequestIdLayer::x_request_id())
                        .layer(tower_http::compression::CompressionLayer::new())
                        .service(svc);
                    let svc = hyper_util::service::TowerToHyperService::new(svc);
                    let mut builder = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    );
                    builder
                        .http1()
                        .title_case_headers(true)
                        .keep_alive(true)
                        .timer(hyper_util::rt::tokio::TokioTimer::new());
                    let tls_acceptor = tls_acceptor.clone();
                    tokio::task::spawn(async move {
                        if let Some(ref tls_acceptor) = tls_acceptor {
                            let tls_stream = tls_acceptor.accept(stream).await;
                            match tls_stream {
                                Ok(tls_stream) => builder.serve_connection_with_upgrades(hyper_util::rt::tokio::TokioIo::new(tls_stream), svc).await,
                                Err(err) => Err(err.into())
                            }
                        } else {
                            builder.serve_connection_with_upgrades(hyper_util::rt::tokio::TokioIo::new(stream), svc).await

                        }
                    });
                }
            }
            .instrument(child_span.or_current()),
        )
    })
}

pub fn parse_command_line_to_json(on_success: impl Fn(&str)) {
    let args = CommandLineArguments::parse();
    on_success(&serde_json::to_string(&args).unwrap());
}

pub fn init_logger() -> Result<tracing_subscriber::filter::LevelFilter, Error> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    let fmt_filter = tracing_subscriber::filter::EnvFilter::builder()
        .with_default_directive("endb=INFO".parse()?)
        .with_env_var("ENDB_LOG_LEVEL")
        .from_env_lossy();

    let ansi = std::env::var("ENDB_LOG_ANSI")
        .map(|x| x == "1" || x.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let thread_ids = std::env::var("ENDB_LOG_THREAD_IDS")
        .map(|x| x == "1" || x.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let fmt_level = fmt_filter
        .max_level_hint()
        .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(thread_ids)
        .with_ansi(ansi)
        .with_filter(fmt_filter);

    let registry = tracing_subscriber::registry().with(fmt_layer);

    let otel = std::env::var("ENDB_TRACING_OTEL")
        .map(|x| x == "1" || x.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let resource = opentelemetry_sdk::Resource::new(vec![
        opentelemetry::KeyValue::new("service.name", "endb"),
        opentelemetry::KeyValue::new("service.version", ENDB_GIT_DESCRIBE),
    ]);

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus::default_registry().clone())
        .with_namespace("endb")
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .build()?;

    let seconds_histogram = opentelemetry_sdk::metrics::Aggregation::ExplicitBucketHistogram {
        boundaries: vec![
            0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
        ],
        record_min_max: true,
    };

    let bytes_histogram = opentelemetry_sdk::metrics::Aggregation::ExplicitBucketHistogram {
        boundaries: vec![
            0.0,
            1048576.0,
            2097152.0,
            4194304.0,
            8388608.0,
            16777216.0,
            33554432.0,
            67108864.0,
            134217728.0,
            268435456.0,
            536870912.0,
            1073741824.0,
            2147483648.0,
            4294967296.0,
            8589934592.0,
        ],
        record_min_max: true,
    };

    let meter_provider_builder = opentelemetry_sdk::metrics::MeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(prometheus_exporter)
        .with_view(opentelemetry_sdk::metrics::new_view(
            opentelemetry_sdk::metrics::Instrument::new().name("*_duration_seconds"),
            opentelemetry_sdk::metrics::Stream::new().aggregation(seconds_histogram.clone()),
        )?)
        .with_view(opentelemetry_sdk::metrics::new_view(
            opentelemetry_sdk::metrics::Instrument::new().name("query_consed_bytes"),
            opentelemetry_sdk::metrics::Stream::new().aggregation(bytes_histogram),
        )?);

    if otel {
        let meter_exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .build_metrics_exporter(
                Box::new(opentelemetry_sdk::metrics::reader::DefaultAggregationSelector::new()),
                Box::new(opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector::new()),
            )?;

        let periodic_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
            meter_exporter,
            opentelemetry_sdk::runtime::Tokio,
        )
        .with_interval(std::time::Duration::from_secs(5))
        .build();

        let meter_provider = meter_provider_builder.with_reader(periodic_reader).build();

        opentelemetry::global::set_meter_provider(meter_provider.clone());

        let tracing_filter = tracing_subscriber::filter::EnvFilter::builder()
            .with_default_directive("endb=DEBUG".parse()?)
            .with_env_var("ENDB_TRACING_LEVEL")
            .from_env_lossy();

        let tracing_level = tracing_filter
            .max_level_hint()
            .unwrap_or(tracing_subscriber::filter::LevelFilter::DEBUG);
        let exporter = opentelemetry_otlp::new_exporter().tonic();
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_trace_config(opentelemetry_sdk::trace::config().with_resource(resource))
            .with_exporter(exporter)
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        registry
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_location(false)
                    .with_filter(tracing_filter),
            )
            .with(tracing_opentelemetry::MetricsLayer::new(
                meter_provider.clone(),
            ))
            .init();

        Ok(tracing_level.max(fmt_level))
    } else {
        let meter_provider = meter_provider_builder.build();

        opentelemetry::global::set_meter_provider(meter_provider.clone());

        registry
            .with(tracing_opentelemetry::MetricsLayer::new(
                meter_provider.clone(),
            ))
            .init();

        Ok(fmt_level)
    }
}

pub fn shutdown_logger() {
    opentelemetry::global::shutdown_tracer_provider();
    opentelemetry::global::shutdown_meter_provider();
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, Full};
    use hyper::service::Service;
    use hyper::{Request, Response, StatusCode};
    use insta::assert_debug_snapshot;
    use std::sync::Arc;

    async fn read_body(response: Response<crate::BoxBody>) -> Response<Full<bytes::Bytes>> {
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        Response::from_parts(parts, Full::new(body))
    }

    fn ok(body: &'static str) -> crate::OnQueryFn {
        Arc::new(
            move |mut response: crate::HttpResponse,
                  sender: &mut crate::HttpSender,
                  tx: crate::OneShotSender,
                  _method: &str,
                  media_type: &str,
                  q: &str,
                  p: &str,
                  m: &str| {
                let headers = response.headers_mut();
                headers.insert("X-q", q.parse().unwrap());
                headers.insert("X-p", p.parse().unwrap());
                headers.insert("X-m", m.parse().unwrap());

                crate::on_response_init(response, tx, StatusCode::OK.into(), media_type).unwrap();
                let (b1, b2) = body.split_at(body.len() / 2);
                crate::on_response_send(sender, b1.as_bytes()).unwrap();
                crate::on_response_send(sender, b2.as_bytes()).unwrap();
            },
        )
    }

    fn unreachable() -> crate::OnQueryFn {
        Arc::new(
            move |_response: crate::HttpResponse,
                  _sender: &mut crate::HttpSender,
                  _tx: crate::OneShotSender,
                  _method: &str,
                  _media_type: &str,
                  _q: &str,
                  _p: &str,
                  _m: &str| {
                unreachable!("should not been called");
            },
        )
    }

    fn service(
        basic_auth: Option<String>,
        on_query: crate::OnQueryFn,
    ) -> hyper_util::service::TowerToHyperService<crate::EndbService> {
        hyper_util::service::TowerToHyperService::new(crate::EndbService {
            basic_auth,
            on_query,
            remote_addr: ([0, 0, 0, 0], 3803).into(),
            on_ws_init: Arc::new(|_| {}),
            on_ws_close: Arc::new(|_| {}),
            on_ws_message: Arc::new(|_, _, _| {}),
        })
    }

    fn get(uri: &str, accept: &str) -> Request<crate::BoxBody> {
        Request::get(uri)
            .header(hyper::header::ACCEPT, accept)
            .body(Empty::new().boxed())
            .unwrap()
    }

    fn post(uri: &str, content_type: &str, body: &str) -> Request<crate::BoxBody> {
        Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, content_type)
            .body(Full::from(bytes::Bytes::from(body.to_string())).boxed())
            .unwrap()
    }

    fn head(uri: &str) -> Request<crate::BoxBody> {
        Request::head(uri).body(Empty::new().boxed()).unwrap()
    }

    fn options(uri: &str) -> Request<crate::BoxBody> {
        Request::options(uri).body(Empty::new().boxed()).unwrap()
    }

    fn add_header<T>(
        mut request: Request<T>,
        key: hyper::header::HeaderName,
        value: &str,
    ) -> Request<T> {
        request.headers_mut().insert(key, value.parse().unwrap());
        request
    }

    #[tokio::test]
    async fn content_type() {
        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
                .call(get("http://localhost:3803/sql?q=SELECT%201", "*/*")).await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "application/json"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "application/ld+json"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/ld+json",
            },
            body: Full {
                data: Some(
                    b"{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("{\"column1\":1}\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "application/x-ndjson"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/x-ndjson",
            },
            body: Full {
                data: Some(
                    b"{\"column1\":1}\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("\"column1\"\r\n1~\r\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "text/csv"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "text/csv",
            },
            body: Full {
                data: Some(
                    b"\"column1\"\r\n1~\r\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(get("http://localhost:3803/sql?q=SELECT%201", "text/xml"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 406,
            version: HTTP/1.1,
            headers: {},
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("\"column1\"\r\n1~\r\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "text/csv; charset=utf-8"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "text/csv",
            },
            body: Full {
                data: Some(
                    b"\"column1\"\r\n1~\r\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("\"column1\"\r\n1~\r\n"))
            .call(get("http://localhost:3803/sql?q=SELECT%201", "application/json; q=0.9, text/csv"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "text/csv",
            },
            body: Full {
                data: Some(
                    b"\"column1\"\r\n1~\r\n",
                ),
            },
        }
        "###);
    }

    #[tokio::test]
    async fn media_type() {
        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
                .call(post("http://localhost:3803/sql", "application/sql", "SELECT 1")).await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "application/x-www-form-urlencoded", "q=SELECT%201"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "multipart/form-data; boundary=12345", "--12345\r\nContent-Disposition: form-data; name=\"q\"\r\n\r\nSELECT 1\r\n--12345--"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "multipart/form-data; boundary=12345", "--12345\r\nContent-Disposition: form-data; name=\"q\"\r\nContent-Type: application/sql\r\n\r\nSELECT 1\r\n--12345--"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "application/json", "{\"q\":\"SELECT 1\"}"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n"))
                .call(add_header(post("http://localhost:3803/sql", "application/ld+json", "{\"q\":\"SELECT 1\"}"), hyper::header::ACCEPT, "application/ld+json"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/ld+json",
            },
            body: Full {
                data: Some(
                    b"{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(post("http://localhost:3803/sql", "text/plain", "SELECT 1"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 415,
            version: HTTP/1.1,
            headers: {},
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "multipart/form-data; boundary=12345", "--12345\r\nContent-Disposition: form-data; name=\"q\"\r\nContent-Type: text/xml\r\n\r\nSELECT 1\r\n--12345--"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 415,
            version: HTTP/1.1,
            headers: {},
            body: Full {
                data: None,
            },
        }
        "###);
    }

    #[tokio::test]
    async fn parameters() {
        assert_debug_snapshot!(
            service(None, ok("[[3]]\n"))
                .call(post("http://localhost:3803/sql", "application/json", "{\"q\":\"SELECT :a + :b\",\"p\":{\"a\":1,\"b\":2}}"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT :a + :b",
                "x-p": "{\"a\":1,\"b\":2}",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[3]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[1]]\n"))
            .call(post("http://localhost:3803/sql", "application/x-www-form-urlencoded", "q=SELECT%20?&p=[[1]]&m=true"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT ?",
                "x-p": "[[1]]",
                "x-m": "true",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, ok("[[3]]\n"))
            .call(post("http://localhost:3803/sql", "multipart/form-data; boundary=12345", "--12345\r\nContent-Disposition: form-data; name=\"q\"\r\n\r\nSELECT ?, ?\r\n--12345\r\nContent-Disposition: form-data; name=\"p\"\r\n\r\n[1,2]\r\n--12345--"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT ?, ?",
                "x-p": "[1,2]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[3]]\n",
                ),
            },
        }
        "###);
    }

    #[tokio::test]
    async fn errors() {
        assert_debug_snapshot!(
            service(None, unreachable())
            .call(head("http://localhost:3803/sql"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 405,
            version: HTTP/1.1,
            headers: {
                "allow": "OPTIONS, GET, POST",
            },
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(get("http://localhost:3803/foo", "*/*"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 404,
            version: HTTP/1.1,
            headers: {},
            body: Full {
                data: None,
            },
        }
        "###);
    }

    #[tokio::test]
    async fn method_options() {
        assert_debug_snapshot!(
            service(None, unreachable())
            .call(options("http://localhost:3803/sql"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 204,
            version: HTTP/1.1,
            headers: {
                "allow": "OPTIONS, GET, POST",
            },
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(options("http://localhost:3803/metrics"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 204,
            version: HTTP/1.1,
            headers: {
                "allow": "OPTIONS, GET",
            },
            body: Full {
                data: None,
            },
        }
        "###);
    }

    #[tokio::test]
    async fn metrics() {
        assert_debug_snapshot!(
            service(None, unreachable())
            .call(head("http://localhost:3803/metrics"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 405,
            version: HTTP/1.1,
            headers: {
                "allow": "OPTIONS, GET",
            },
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(get("http://localhost:3803/metrics", "*/*"))
                .await.map(read_body).unwrap().await.into_parts().0
                , @r###"
        Parts {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "content-type": "text/plain; version=0.0.4",
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
                .call(get("http://localhost:3803/metrics", prometheus::TEXT_FORMAT))
            .await.map(read_body).unwrap().await.into_parts().0
        , @r###"
        Parts {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "content-type": "text/plain; version=0.0.4",
            },
        }
        "###);

        assert_debug_snapshot!(
            service(None, unreachable())
            .call(get("http://localhost:3803/metrics", "application/json"))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 406,
            version: HTTP/1.1,
            headers: {},
            body: Full {
                data: None,
            },
        }
        "###);
    }

    #[tokio::test]
    async fn websocket_upgrade() {
        assert_debug_snapshot!(
            service(None, unreachable())
                .call(add_header(add_header(add_header(add_header(get("ws://localhost:3803/sql", "*/*"),
                                            hyper::header::UPGRADE, "websocket"),
                                                       hyper::header::CONNECTION, "Upgrade"),
                                            hyper::header::SEC_WEBSOCKET_VERSION, "13"),
                                 hyper::header::SEC_WEBSOCKET_KEY, "x3JJHMbDL1EzLkh9GBhXDw=="))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 101,
            version: HTTP/1.1,
            headers: {
                "connection": "upgrade",
                "upgrade": "websocket",
                "sec-websocket-accept": "HSmrc0sMlYUkAGmm5OPpG2HaGWk=",
            },
            body: Full {
                data: Some(
                    b"switching to websocket protocol",
                ),
            },
        }
        "###);
    }

    #[tokio::test]
    async fn websocket_auth_via_subprotocol() {
        let basic_auth =
            crate::make_basic_auth_header(Some("foo".to_string()), Some("foo".to_string()));

        assert_debug_snapshot!(
            service(basic_auth.clone(), unreachable())
                .call(add_header(add_header(add_header(add_header(get("ws://localhost:3803/sql", "*/*"),
                                            hyper::header::UPGRADE, "websocket"),
                                                       hyper::header::CONNECTION, "Upgrade"),
                                            hyper::header::SEC_WEBSOCKET_VERSION, "13"),
                                 hyper::header::SEC_WEBSOCKET_KEY, "x3JJHMbDL1EzLkh9GBhXDw=="))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 401,
            version: HTTP/1.1,
            headers: {
                "www-authenticate": "Basic realm=\"restricted area\"",
            },
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(basic_auth.clone(), unreachable())
                .call(add_header(add_header(add_header(add_header(add_header(get("ws://localhost:3803/sql", "*/*"),
                                            hyper::header::UPGRADE, "websocket"),
                                                       hyper::header::CONNECTION, "Upgrade"),
                                            hyper::header::SEC_WEBSOCKET_VERSION, "13"),
                                            hyper::header::SEC_WEBSOCKET_KEY, "x3JJHMbDL1EzLkh9GBhXDw=="),
                                 hyper::header::SEC_WEBSOCKET_PROTOCOL, "Basic%20Zm9vOmZvbw%3D%3D" ))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 101,
            version: HTTP/1.1,
            headers: {
                "connection": "upgrade",
                "upgrade": "websocket",
                "sec-websocket-accept": "HSmrc0sMlYUkAGmm5OPpG2HaGWk=",
            },
            body: Full {
                data: Some(
                    b"switching to websocket protocol",
                ),
            },
        }
        "###);

        assert_debug_snapshot!(
            service(basic_auth, unreachable())
                .call(add_header(add_header(add_header(add_header(add_header(get("ws://localhost:3803/sql", "*/*"),
                                            hyper::header::UPGRADE, "websocket"),
                                                       hyper::header::CONNECTION, "Upgrade"),
                                            hyper::header::SEC_WEBSOCKET_VERSION, "13"),
                                            hyper::header::SEC_WEBSOCKET_KEY, "x3JJHMbDL1EzLkh9GBhXDw=="),
                                 hyper::header::AUTHORIZATION, "Basic%20Zm9vOmZvbw%3D%3D" ))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 101,
            version: HTTP/1.1,
            headers: {
                "connection": "upgrade",
                "upgrade": "websocket",
                "sec-websocket-accept": "HSmrc0sMlYUkAGmm5OPpG2HaGWk=",
            },
            body: Full {
                data: Some(
                    b"switching to websocket protocol",
                ),
            },
        }
        "###);
    }

    #[tokio::test]
    async fn basic_auth() {
        let basic_auth =
            crate::make_basic_auth_header(Some("foo".to_string()), Some("foo".to_string()));
        assert_debug_snapshot!(
            service(basic_auth.clone(), unreachable())
                .call(get("http://localhost:3803/sql?q=SELECT%201", "*/*"))
                .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 401,
            version: HTTP/1.1,
            headers: {
                "www-authenticate": "Basic realm=\"restricted area\"",
            },
            body: Full {
                data: None,
            },
        }
        "###);

        assert_debug_snapshot!(
            service(basic_auth, ok("[[1]]\n"))
                .call(add_header(get("http://localhost:3803/sql?q=SELECT%201", "*/*"), hyper::header::AUTHORIZATION, "Basic Zm9vOmZvbw=="))
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 200,
            version: HTTP/1.1,
            headers: {
                "x-q": "SELECT 1",
                "x-p": "[]",
                "x-m": "false",
                "content-type": "application/json",
            },
            body: Full {
                data: Some(
                    b"[[1]]\n",
                ),
            },
        }
        "###);
    }
}
