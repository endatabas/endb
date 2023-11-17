use base64::Engine;
use clap::Parser;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::marker::{Send, Sync};
use std::pin::Pin;
use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync>;

const ENDB_FULL_VERSION: &str = env!("ENDB_FULL_VERSION");

#[derive(Serialize, Deserialize, Debug, Parser)]
#[command(name = "endb", author, version = ENDB_FULL_VERSION, about, long_about = None)]
pub struct CommandLineArguments {
    #[arg(short, long, default_value = "endb_data", env = "ENDB_DATA_DIRECTORY")]
    data_directory: String,
    #[arg(short = 'p', long, default_value = "3803", env = "ENDB_HTTP_PORT")]
    http_port: u16,
    #[arg(long, env = "ENDB_USERNAME")]
    username: Option<String>,
    #[arg(long, env = "ENDB_PASSWORD")]
    password: Option<String>,
}

pub type HttpResponse = Response<Body>;
pub type HttpSender = hyper::body::Sender;
pub type OneShotSender = tokio::sync::oneshot::Sender<HttpResponse>;

pub fn on_response_init(
    mut response: HttpResponse,
    tx: OneShotSender,
    status_code: u16,
    content_type: &str,
) -> Result<(), Response<Body>> {
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

pub fn on_response_send(sender: &mut HttpSender, chunk: &str) -> Result<(), Error> {
    Ok(tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(async { sender.send_data(chunk.to_string().into()).await })
    })?)
}

const REALM: &str = "restricted area";

type OnQueryFn<'a> = Arc<
    dyn Fn(HttpResponse, &mut HttpSender, OneShotSender, &str, &str, &str, &str, &str)
        + Sync
        + Send
        + 'a,
>;

struct EndbService<'a> {
    basic_auth: Option<String>,
    on_query: OnQueryFn<'a>,
}

fn empty_response(status_code: StatusCode) -> Result<Response<Body>, Error> {
    Ok(Response::builder().status(status_code).body("".into())?)
}

async fn sql_response(
    method: Method,
    media_type: &str,
    mut params: HashMap<String, String>,
    on_query: OnQueryFn<'static>,
) -> Result<Response<Body>, Error> {
    if let Some(q) = params.remove("q") {
        let media_type = media_type.to_string();
        let p = params.remove("p").unwrap_or_else(|| "[]".to_string());
        let m = params.remove("m").unwrap_or_else(|| "false".to_string());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut response = empty_response(StatusCode::INTERNAL_SERVER_ERROR)?;

        tokio::spawn(async move {
            let (mut sender, body) = Body::channel();
            *response.body_mut() = body;

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

impl Service<Request<Body>> for EndbService<'static> {
    type Response = Response<Body>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let on_query = self.on_query.clone();
        let unauthorized = self.basic_auth.is_some()
            && self.basic_auth.as_deref()
                != req
                    .headers()
                    .get(hyper::header::AUTHORIZATION)
                    .and_then(|x| x.to_str().ok());
        Box::pin(async move {
            if unauthorized {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(
                        hyper::header::WWW_AUTHENTICATE,
                        format!("Basic realm=\"{}\"", REALM),
                    )
                    .body("".into())?);
            }

            let accept = req
                .headers()
                .get(hyper::header::ACCEPT)
                .and_then(|x| x.to_str().ok())
                .and_then(|x| x.split(',').next())
                .and_then(|x| x.parse::<mime::Mime>().ok())
                .unwrap_or(mime::STAR_STAR);

            let media_type = match accept.essence_str() {
                "*/*" | "application/*" | "application/json" => "application/json",
                "application/ld+json" => "application/ld+json",
                "application/x-ndjson" => "application/x-ndjson",
                "text/*" | "text/csv" => "text/csv",
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_ACCEPTABLE)
                        .body("".into())?);
                }
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
                    let body = hyper::body::to_bytes(req).await?;
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
                    let body = hyper::body::to_bytes(req).await?;
                    if let Ok(q) = std::str::from_utf8(&body) {
                        params.insert("q".to_string(), q.to_string());
                        sql_response(method, media_type, params, on_query).await
                    } else {
                        empty_response(StatusCode::UNPROCESSABLE_ENTITY)
                    }
                }
                (&Method::POST, "/sql", Some("application/x-www-form-urlencoded")) => {
                    let body = hyper::body::to_bytes(req).await?;
                    for (k, v) in url::form_urlencoded::parse(body.as_ref()) {
                        params.insert(k.to_string(), v.to_string());
                    }
                    sql_response(method, media_type, params, on_query).await
                }
                (&Method::POST, "/sql", Some("multipart/form-data")) => {
                    if let Some(boundary) = content_type
                        .and_then(|x| x.get_param(mime::BOUNDARY).map(|x| x.to_string()))
                    {
                        let body = hyper::body::to_bytes(req).await?;
                        let mut mult = multer::Multipart::new(Body::from(body), boundary);

                        while let Ok(Some(field)) = mult.next_field().await {
                            if let Some(k) = field.name() {
                                let k = k.to_string();
                                if let Ok(v) = field.text().await {
                                    params.insert(k, v.to_string());
                                }
                            }
                        }
                        sql_response(method, media_type, params, on_query).await
                    } else {
                        empty_response(StatusCode::BAD_REQUEST)
                    }
                }
                (&Method::POST, "/sql", _) => empty_response(StatusCode::UNSUPPORTED_MEDIA_TYPE),
                (_, "/sql", _) => Ok(Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header(hyper::header::ALLOW, "GET, POST")
                    .body("".into())?),
                _ => empty_response(StatusCode::NOT_FOUND),
            }
        })
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

pub fn start_server(
    on_init: impl Fn(&str),
    on_query: impl Fn(HttpResponse, &mut HttpSender, OneShotSender, &str, &str, &str, &str, &str)
        + Sync
        + Send
        + 'static,
) -> Result<(), Error> {
    let args = CommandLineArguments::parse();

    let full_version = env!("ENDB_FULL_VERSION");
    log::info!("version {}", full_version);

    on_init(&serde_json::to_string(&args).unwrap());

    let basic_auth = make_basic_auth_header(args.username, args.password);
    let on_query = Arc::new(on_query);
    let make_svc = hyper::service::make_service_fn(|_conn| {
        let svc = EndbService {
            basic_auth: basic_auth.clone(),
            on_query: on_query.clone(),
        };
        async { Ok::<_, Error>(svc) }
    });

    let addr = ([0, 0, 0, 0], args.http_port).into();

    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match hyper::Server::try_bind(&addr) {
                Ok(builder) => {
                    let server = builder.serve(make_svc);
                    log::info!("listening on port {}", args.http_port);
                    server.await
                }
                Err(err) => Err(err),
            }
        })?)
}

pub fn init_logger() -> Result<(), Error> {
    Ok(env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_env("ENDB_LOG_LEVEL")
        .try_init()?)
}

#[cfg(test)]
mod tests {
    use hyper::service::Service;
    use hyper::{Body, Request, Response, StatusCode};
    use insta::assert_debug_snapshot;
    use std::sync::Arc;

    async fn read_body(response: Response<Body>) -> Response<Body> {
        let (parts, body) = response.into_parts();
        let body = Body::from(hyper::body::to_bytes(body).await.unwrap());
        Response::from_parts(parts, body)
    }

    fn ok(body: &str) -> crate::OnQueryFn {
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
                crate::on_response_send(sender, b1).unwrap();
                crate::on_response_send(sender, b2).unwrap();
            },
        )
    }

    fn unreachable<'a>() -> crate::OnQueryFn<'a> {
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

    fn service(basic_auth: Option<String>, on_query: crate::OnQueryFn) -> crate::EndbService {
        crate::EndbService {
            basic_auth,
            on_query,
        }
    }

    fn get(uri: &str, accept: &str) -> Request<Body> {
        Request::get(uri)
            .header(hyper::header::ACCEPT, accept)
            .body(Body::empty())
            .unwrap()
    }

    fn post(uri: &str, content_type: &str, body: &str) -> Request<Body> {
        Request::post(uri)
            .header(hyper::header::CONTENT_TYPE, content_type)
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn add_header(
        mut request: Request<Body>,
        key: hyper::header::HeaderName,
        value: &str,
    ) -> Request<Body> {
        request.headers_mut().insert(key, value.parse().unwrap());
        request
    }

    #[tokio::test(flavor = "multi_thread")]
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"{\"column1\":1}\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"\"column1\"\r\n1~\r\n",
                ),
            ),
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
            body: Body(
                Empty,
            ),
        }
        "###);
    }

    #[tokio::test(flavor = "multi_thread")]
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}\n",
                ),
            ),
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
            body: Body(
                Empty,
            ),
        }
        "###);
    }

    #[tokio::test(flavor = "multi_thread")]
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
            body: Body(
                Full(
                    b"[[3]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
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
            body: Body(
                Full(
                    b"[[3]]\n",
                ),
            ),
        }
        "###);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn errors() {
        assert_debug_snapshot!(
            service(None, unreachable())
            .call(Request::head("http://localhost:3803/sql")
            .body(Body::empty())
            .unwrap())
            .await.map(read_body).unwrap().await
        , @r###"
        Response {
            status: 405,
            version: HTTP/1.1,
            headers: {
                "allow": "GET, POST",
            },
            body: Body(
                Empty,
            ),
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
            body: Body(
                Empty,
            ),
        }
        "###);
    }

    #[tokio::test(flavor = "multi_thread")]
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
            body: Body(
                Empty,
            ),
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
            body: Body(
                Full(
                    b"[[1]]\n",
                ),
            ),
        }
        "###);
    }
}
