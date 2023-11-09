use base64::Engine;
use clap::Parser;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub static RESPONSE: std::cell::RefCell<Option<Response<Body>>> = std::cell::RefCell::default();
}

pub fn start_server(
    on_init: impl Fn(String),
    on_query: impl Fn(String, String, String, String, String, Box<dyn FnMut(u16, String, String)>)
        + std::marker::Sync
        + std::marker::Send
        + 'static
        + Clone,
) {
    let args = CommandLineArguments::parse();

    let full_version = env!("ENDB_FULL_VERSION");
    log::info!(target: "endb", "{}", full_version);

    on_init(serde_json::to_string(&args).unwrap());

    let realm = "restricted area";
    let basic_auth: Option<String> = if let (Some(username), Some(password)) =
        (args.username, args.password)
    {
        Some(format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password))
        ))
    } else {
        None
    };

    let make_svc = hyper::service::make_service_fn(|_conn| {
        let basic_auth = basic_auth.clone();
        let on_query = on_query.clone();
        let www_authenticate = format!("Basic realm=\"{}\"", realm);
        async {
            Ok::<_, hyper::Error>(hyper::service::service_fn(move |req: Request<Body>| {
                let basic_auth = basic_auth.clone();
                let on_query = on_query.clone();
                let www_authenticate = www_authenticate.clone();
                async move {
                    if basic_auth.is_some()
                        && basic_auth.as_deref()
                            != req
                                .headers()
                                .get(hyper::header::AUTHORIZATION)
                                .and_then(|x| x.to_str().ok())
                    {
                        return Ok(Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .header(hyper::header::WWW_AUTHENTICATE, www_authenticate)
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
                    }
                    .to_string();

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

                    let method = req.method().to_string();

                    let responder = |params: HashMap<String, String>| {
                        if let Some(q) = params.get("q") {
                            let p = params.get("p").map_or("[]".to_string(), |x| x.to_string());
                            let m = params
                                .get("m")
                                .map_or("false".to_string(), |x| x.to_string());

                            on_query(
                                method,
                                media_type,
                                q.to_string(),
                                p,
                                m,
                                Box::new(|status, content_type, body| {
                                    RESPONSE.with_borrow_mut(|response| {
                                        *response = Some(
                                            Response::builder()
                                                .status(status)
                                                .header(hyper::header::CONTENT_TYPE, content_type)
                                                .body(Body::from(body))
                                                .unwrap(),
                                        )
                                    })
                                }),
                            );
                            if let Some(response) = RESPONSE.take() {
                                Ok(response)
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(""))
                                    .unwrap())
                            }
                        } else {
                            Ok(Response::builder()
                                .status(StatusCode::UNPROCESSABLE_ENTITY)
                                .body("".into())
                                .unwrap())
                        }
                    };

                    match (
                        req.method(),
                        req.uri().path(),
                        content_type
                            .clone()
                            .map(|x| x.essence_str().to_string())
                            .as_deref(),
                    ) {
                        (&Method::GET, "/sql", _) => responder(params),
                        (
                            &Method::POST,
                            "/sql",
                            Some("application/json" | "application/ld+json"),
                        ) => {
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

                                responder(params)
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body("".into())
                                    .unwrap())
                            }
                        }
                        (&Method::POST, "/sql", Some("application/sql")) => {
                            let body = hyper::body::to_bytes(req).await?;
                            if let Ok(q) = std::str::from_utf8(&body) {
                                params.insert("q".to_string(), q.to_string());
                                responder(params)
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                                    .body("".into())
                                    .unwrap())
                            }
                        }
                        (&Method::POST, "/sql", Some("application/x-www-form-urlencoded")) => {
                            let body = hyper::body::to_bytes(req).await?;
                            for (k, v) in url::form_urlencoded::parse(body.as_ref()) {
                                params.insert(k.to_string(), v.to_string());
                            }
                            responder(params)
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
                                responder(params)
                            } else {
                                Ok(Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body("".into())
                                    .unwrap())
                            }
                        }
                        (&Method::POST, "/sql", _) => Ok(Response::builder()
                            .status(StatusCode::UNSUPPORTED_MEDIA_TYPE)
                            .body("".into())
                            .unwrap()),
                        (_, "/sql", _) => Ok(Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .header(hyper::header::ALLOW, "GET, POST")
                            .body("".into())
                            .unwrap()),
                        _ => Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body("".into())
                                .unwrap(),
                        ),
                    }
                }
            }))
        }
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
