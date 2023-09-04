(defpackage :endb-test/http
  (:use :cl :fiveam :endb/http)
  (:import-from :endb/sql)
  (:import-from :fast-io)
  (:import-from :trivial-utf-8)
  (:import-from :alexandria))
(in-package :endb-test/http)

(in-suite* :http)

(defun %req (handler method path-info &key query body content-type (accept "*/*") headers)
  (let* ((raw-body (when body
                     (trivial-utf-8:string-to-utf-8-bytes body)))
         (env (list :request-method method
                    :path-info path-info
                    :query-string query
                    :content-type content-type
                    :content-length (when raw-body
                                      (length raw-body))
                    :raw-body (when raw-body
                                (make-instance 'fast-io:fast-input-stream
                                               :vector raw-body))
                    :headers (alexandria:plist-hash-table (append headers (list "accept" accept)) :test 'equal)))
         (response (funcall handler env)))
    (etypecase response
      (function (let* ((response-body "")
                       (response-list)
                       (response-closed-p)
                       (responder (lambda (res)
                                    (setf response-list res)
                                    (lambda (content &key close)
                                      (unless response-closed-p
                                        (setf response-body (concatenate 'string response-body content))
                                        (setf response-closed-p close))))))
                  (funcall response responder)
                  (append response-list (list response-body))))
      (list response))))

(test content-type
  (let* ((db (endb/sql:make-db))
         (write-db (endb/sql:begin-write-tx db))
         (app (make-api-handler write-db)))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app :get "/sql" :query "q=SELECT%201")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app :get "/sql" :query "q=SELECT%201" :accept "application/json")))

    (is (equal (list +http-ok+
                     '(:content-type "application/ld+json")
                     (format nil "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}~%"))
               (%req app :get "/sql" :query "q=SELECT%201" :accept "application/ld+json")))

    (is (equal (list +http-ok+
                     '(:content-type "application/x-ndjson")
                     (format nil "{\"column1\":1}~%"))
               (%req app :get "/sql" :query "q=SELECT%201" :accept "application/x-ndjson")))

    (is (equal (list +http-ok+
                     '(:content-type "text/csv")
                     (format nil "\"column1\"~A1~A" endb/http::+crlf+ endb/http::+crlf+))
               (%req app :get "/sql" :query "q=SELECT%201" :accept "text/csv")))

    (is (equal (list +http-not-acceptable+
                     '(:content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app :get "/sql" :query "q=SELECT%201" :accept "text/xml")))))

(test media-type
  (let* ((db (endb/sql:make-db))
         (write-db (endb/sql:begin-write-tx db))
         (app (make-api-handler write-db)))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app
                     :post "/sql"
                     :body "SELECT 1"
                     :content-type "application/sql")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app
                     :post "/sql"
                     :body "q=SELECT%201"
                     :content-type "application/x-www-form-urlencoded")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app
                     :post "/sql"
                     :body (format nil "--12345~AContent-Disposition: form-data; name=\"q\"~A~ASELECT 1~A--12345--"
                                   endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+)
                     :content-type "multipart/form-data; boundary=12345")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"SELECT 1\"}"
                     :content-type "application/json")))

    (is (equal (list +http-ok+
                     '(:content-type "application/ld+json")
                     (format nil "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[{\"column1\":1}]}~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"SELECT 1\"}"
                     :content-type "application/ld+json"
                     :accept "application/ld+json")))

    (is (equal (list +http-unsupported-media-type+
                     '(:content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app
                     :post "/sql"
                     :body "SELECT 1"
                     :content-type "text/plain")))))

(test parameters
  (let* ((db (endb/sql:make-db))
         (write-db (endb/sql:begin-write-tx db))
         (app (make-api-handler write-db)))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"SELECT ?, ?\",\"p\":[{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"},{\"b\":1}]}"
                     :content-type "application/ld+json")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
               (%req app
                     :post "/sql"
                     :body (format nil "--12345~AContent-Disposition: form-data; name=\"q\"~A~ASELECT ?, ?~A--12345~AContent-Disposition: form-data; name=\"p\"~A~A[2001-01-01,{b:1}]~A--12345--"
                                   endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+ endb/http::+crlf+)
                     :content-type "multipart/form-data; boundary=12345")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[3]]~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"SELECT :a + :b\",\"p\":{\"a\":1,\"b\":2}}"
                     :content-type "application/json")))

    (is (equal (list +http-created+
                     '(:content-type "application/json")
                     (format nil "[[2]]~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"INSERT INTO foo {:a, :b}\",\"p\":[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}],\"m\":true}"
                     :content-type "application/json")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1,2],[3,4]]~%"))
               (%req app
                     :get "/sql"
                     :query "q=SELECT%20a,b%20FROM%20foo%20ORDER%20BY%20a")))))

(test errors
  (let* ((db (endb/sql:make-db))
         (write-db (endb/sql:begin-write-tx db))
         (app (make-api-handler write-db)))

    (is (equal (list +http-created+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app
                     :post "/sql"
                     :body "{\"q\":\"INSERT INTO foo {a: 1, b: 2}\"}"
                     :content-type "application/json")))

    (is (equal (list +http-bad-request+
                     '(:content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app
                     :get "/sql"
                     :query "q=DELETE%20FROM%20foo")))

    (is (equal (list +http-method-not-allowed+
                     '(:allow "GET, POST"
                       :content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app :head "/sql")))

    (is (equal (list +http-not-found+
                     '(:content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app :get "/foo")))))

(test basic-auth
  (let* ((db (endb/sql:make-db))
         (write-db (endb/sql:begin-write-tx db))
         (app (make-api-handler write-db :username "foo" :password "foo" :realm "test realm")))

    (is (equal (list +http-unauthorized+
                     '(:www-authenticate "Basic realm=\"test realm\""
                       :content-type "text/plain"
                       :content-length 0)
                     '(""))
               (%req app :get "/sql" :query "q=SELECT%201")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%req app :get "/sql" :query "q=SELECT%201" :headers '("authorization" "Basic Zm9vOmZvbw=="))))))
