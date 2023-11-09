use base64::Engine;
use clap::Parser;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::marker::{Send, Sync};
use std::pin::Pin;
use std::sync::Arc;

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

thread_local! {
    static RESPONSE: RefCell<Option<Response<Body>>> = RefCell::default();
}

pub fn on_response(status_code: u16, content_type: &str, body: &str) {
    RESPONSE.with_borrow_mut(|response| {
        *response = Some(
            Response::builder()
                .status(status_code)
                .header(hyper::header::CONTENT_TYPE, content_type)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
    });
}

const REALM: &str = "restricted area";

type OnQueryFn = Arc<dyn Fn(&str, &str, &str, &str, &str) + Sync + Send>;

struct EndbService {
    basic_auth: Option<String>,
    on_query: OnQueryFn,
}

fn empty_response(status_code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status_code)
        .body(Body::from(""))
        .unwrap()
}

fn sql_response(
    method: Method,
    media_type: &str,
    params: HashMap<String, String>,
    on_query: OnQueryFn,
) -> Result<Response<Body>, hyper::Error> {
    if let Some(q) = params.get("q") {
        let p = params.get("p").map(|x| x.as_str()).unwrap_or("[]");
        let m = params.get("m").map(|x| x.as_str()).unwrap_or("false");

        RESPONSE.set(None);
        on_query(method.as_str(), media_type, q, p, m);
        if let Some(response) = RESPONSE.take() {
            Ok(response)
        } else {
            Ok(empty_response(StatusCode::INTERNAL_SERVER_ERROR))
        }
    } else {
        Ok(empty_response(StatusCode::UNPROCESSABLE_ENTITY))
    }
}

impl Service<Request<Body>> for EndbService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        core::task::Poll::Ready(Ok(()))
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
                    .body("".into())
                    .unwrap());
            }

            let accept = req
                .headers()
                .get(hyper::header::ACCEPT)
                .and_then(|x| x.to_str().ok())
                .and_then(|x| x.split(',').next())
                .and_then(|x| x.parse::<mime::Mime>().ok())
                .unwrap_or(mime::STAR_STAR);

            let media_type = match accept.essence_str() {
                "*/*" | "application/*" | "applicatio/json" => "application/json",
                "application/ld+json" => "application/ld+json",
                "application/x-ndjson" => "application/x-ndjson",
                "text/*" | "text/csv" => "text/csv",
                _ => {
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_ACCEPTABLE)
                        .body("".into())
                        .unwrap())
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
                (&Method::GET, "/sql", _) => sql_response(method, media_type, params, on_query),
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
                        sql_response(method, media_type, params, on_query)
                    } else {
                        Ok(empty_response(StatusCode::UNPROCESSABLE_ENTITY))
                    }
                }
                (&Method::POST, "/sql", Some("application/sql")) => {
                    let body = hyper::body::to_bytes(req).await?;
                    if let Ok(q) = std::str::from_utf8(&body) {
                        params.insert("q".to_string(), q.to_string());
                        sql_response(method, media_type, params, on_query)
                    } else {
                        Ok(empty_response(StatusCode::UNPROCESSABLE_ENTITY))
                    }
                }
                (&Method::POST, "/sql", Some("application/x-www-form-urlencoded")) => {
                    let body = hyper::body::to_bytes(req).await?;
                    for (k, v) in url::form_urlencoded::parse(body.as_ref()) {
                        params.insert(k.to_string(), v.to_string());
                    }
                    sql_response(method, media_type, params, on_query)
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
                        sql_response(method, media_type, params, on_query)
                    } else {
                        Ok(empty_response(StatusCode::BAD_REQUEST))
                    }
                }
                (&Method::POST, "/sql", _) => {
                    Ok(empty_response(StatusCode::UNSUPPORTED_MEDIA_TYPE))
                }
                (_, "/sql", _) => Ok(Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header(hyper::header::ALLOW, "GET, POST")
                    .body("".into())
                    .unwrap()),
                _ => Ok(empty_response(StatusCode::NOT_FOUND)),
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
    on_query: impl Fn(&str, &str, &str, &str, &str) + Sync + Send + 'static,
) {
    let args = CommandLineArguments::parse();

    let full_version = env!("ENDB_FULL_VERSION");
    log::info!(target: "endb", "{}", full_version);

    on_init(&serde_json::to_string(&args).unwrap());

    let basic_auth = make_basic_auth_header(args.username, args.password);
    let on_query = Arc::new(on_query);
    let make_svc = hyper::service::make_service_fn(|_conn| {
        let svc = EndbService {
            basic_auth: basic_auth.clone(),
            on_query: on_query.clone(),
        };
        async move { Ok::<_, hyper::Error>(svc) }
    });

    let addr = ([0, 0, 0, 0], args.http_port).into();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let server = hyper::Server::bind(&addr).serve(make_svc);
            log::info!("listening on port {}", args.http_port);

            server.await
        })
        .unwrap();
}

pub fn init_logger() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_env("ENDB_LOG_LEVEL")
        .init();
}

#[cfg(test)]
mod tests {
    use hyper::service::Service;
    use hyper::{Request, StatusCode};
    use std::sync::Arc;

    #[tokio::test]
    async fn content_type() {
        let on_query = Arc::new(
            |_method: &str, _media_type: &str, _q: &str, _p: &str, _m: &str| {
                let json = serde_json::json!([[1]]);
                crate::on_response(
                    StatusCode::OK.into(),
                    "application/json",
                    format!("{}\n", json).as_str(),
                );
            },
        );

        let mut svc = crate::EndbService {
            basic_auth: None,
            on_query,
        };

        let res = svc
            .call(
                Request::get("http://localhost:3803/sql?q=SELECT%201")
                    .body("".into())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, res.status());
        assert_eq!(
            "application/json",
            res.headers().get(hyper::header::CONTENT_TYPE).unwrap()
        );
        let body = hyper::body::to_bytes(res).await.unwrap();
        assert_eq!("[[1]]\n", body);
    }
}

// (test content-type
//   (let* ((db (endb/sql:make-db))
//          (write-db (endb/sql:begin-write-tx db))
//          (app (make-api-handler write-db)))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app :get "/sql" :query "q=SELECT%201")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app :get "/sql" :query "q=SELECT%201" :accept "application/json")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/ld+json")
//                      (format nil "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}~%"))
//                (%req app :get "/sql" :query "q=SELECT%201" :accept "application/ld+json")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/x-ndjson")
//                      (format nil "{\"column1\":1}~%"))
//                (%req app :get "/sql" :query "q=SELECT%201" :accept "application/x-ndjson")))

//     (is (equal (list +http-ok+
//                      '(:content-type "text/csv")
//                      (format nil "\"column1\"~A1~A" endb/http::+crlf+ endb/http::+crlf+))
//                (%req app :get "/sql" :query "q=SELECT%201" :accept "text/csv")))

//     (is (equal (list +http-not-acceptable+
//                      '(:content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app :get "/sql" :query "q=SELECT%201" :accept "text/xml")))))

// (test media-type
//   (let* ((db (endb/sql:make-db))
//          (write-db (endb/sql:begin-write-tx db))
//          (app (make-api-handler write-db)))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "SELECT 1"
//                      :content-type "application/sql")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "q=SELECT%201"
//                      :content-type "application/x-www-form-urlencoded")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body (format nil "--12345~AContent-Disposition: form-data; name=\"q\"~A~ASELECT 1~A--12345--"
//                                    endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+)
//                      :content-type "multipart/form-data; boundary=12345")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"SELECT 1\"}"
//                      :content-type "application/json")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/ld+json")
//                      (format nil "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"SELECT 1\"}"
//                      :content-type "application/ld+json"
//                      :accept "application/ld+json")))

//     (is (equal (list +http-unsupported-media-type+
//                      '(:content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app
//                      :post "/sql"
//                      :body "SELECT 1"
//                      :content-type "text/plain")))))

// (test parameters
//   (let* ((db (endb/sql:make-db))
//          (write-db (endb/sql:begin-write-tx db))
//          (app (make-api-handler write-db)))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"SELECT ?, ?\",\"p\":[{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"},{\"b\":1}]}"
//                      :content-type "application/ld+json")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body (format nil "--12345~AContent-Disposition: form-data; name=\"q\"~A~ASELECT ?, ?~A--12345~AContent-Disposition: form-data; name=\"p\"~A~A[2001-01-01,{b:1}]~A--12345--"
//                                    endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+)
//                      :content-type "multipart/form-data; boundary=12345")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[3]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"SELECT :a + :b\",\"p\":{\"a\":1,\"b\":2}}"
//                      :content-type "application/json")))

//     (is (equal (list +http-created+
//                      '(:content-type "application/json")
//                      (format nil "[[2]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"INSERT INTO foo {:a, :b}\",\"p\":[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}],\"m\":true}"
//                      :content-type "application/json")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1,2],[3,4]]~%"))
//                (%req app
//                      :get "/sql"
//                      :query "q=SELECT%20a%2Cb%20FROM%20foo%20ORDER%20BY%20a")))))

// (test errors
//   (let* ((db (endb/sql:make-db))
//          (write-db (endb/sql:begin-write-tx db))
//          (app (make-api-handler write-db)))

//     (is (equal (list +http-created+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app
//                      :post "/sql"
//                      :body "{\"q\":\"INSERT INTO foo {a: 1, b: 2}\"}"
//                      :content-type "application/json")))

//     (is (equal (list +http-bad-request+
//                      '(:content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app
//                      :get "/sql"
//                      :query "q=DELETE%20FROM%20foo")))

//     (is (equal (list +http-bad-request+
//                      '(:content-type "text/plain")
//                      (list (format nil "Invalid argument types: SIN(\"foo\")~%")))
//                (%req app
//                      :get "/sql"
//                      :query "q=SELECT%20SIN%28%27foo%27%29")))

//     (is (equal (list +http-method-not-allowed+
//                      '(:allow "GET, POST"
//                        :content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app :head "/sql")))

//     (is (equal (list +http-not-found+
//                      '(:content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app :get "/foo")))))

// (test basic-auth
//   (let* ((db (endb/sql:make-db))
//          (write-db (endb/sql:begin-write-tx db))
//          (app (make-api-handler write-db :username "foo" :password "foo" :realm "test realm")))

//     (is (equal (list +http-unauthorized+
//                      '(:www-authenticate "Basic realm=\"test realm\""
//                        :content-type "text/plain"
//                        :content-length 0)
//                      '(""))
//                (%req app :get "/sql" :query "q=SELECT%201")))

//     (is (equal (list +http-ok+
//                      '(:content-type "application/json")
//                      (format nil "[[1]]~%"))
//                (%req app :get "/sql" :query "q=SELECT%201" :headers '("authorization" "Basic Zm9vOmZvbw=="))))))
