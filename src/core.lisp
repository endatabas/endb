(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :bordeaux-threads)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :fset)
  (:import-from :local-time)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/lib/parser)
  (:import-from :endb/sql)
  (:import-from :endb/sql/expr)
  (:import-from :trivial-backtrace)
  (:import-from :uiop))
(in-package :endb/core)

(defvar *db*)
(defvar *write-lock* (bt:make-lock))

(defconstant +http-ok+ 200)
(defconstant +http-created+ 201)
(defconstant +http-bad-request+ 400)
(defconstant +http-conflict+ 409)
(defconstant +http-internal-server-error+ 500)

(defparameter +crlf+ (coerce '(#\return #\linefeed) 'string))

(defun endb-init (config)
  (setf *db* (endb/sql:make-directory-db :directory (fset:lookup config "data_directory") :object-store-path nil)))

(defun %format-csv (x)
  (ppcre:regex-replace-all "\\\\\"" (com.inuoe.jzon:stringify x) "\"\""))

(defun %row-to-json (column-names row)
  (with-output-to-string (out)
    (com.inuoe.jzon:with-writer (writer :stream out)
      (com.inuoe.jzon:with-object writer
        (loop for column in row
              for column-name in column-names
              do (com.inuoe.jzon:write-key writer column-name)
                 (com.inuoe.jzon:write-value writer column))))))

(defun %stream-response (content-type column-names rows)
  (let ((endb/json:*json-ld-scalars* (equal "application/ld+json" content-type)))
    (with-output-to-string (out)
      (cond
        ((equal "application/json" content-type)
         (write-string "["  out)
         (loop for row in rows
               for idx from 0
               unless (zerop idx)
                 do (write-string "," out)
               do (write-string (com.inuoe.jzon:stringify row) out)
               finally (write-string (format nil "]~%") out)))
        ((equal "application/ld+json" content-type)
         (write-string "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[" out)
         (loop for row in rows
               for idx from 0
               unless (zerop idx)
                 do (write-string "," out)
               do (write-string (%row-to-json column-names row) out)
               finally (write-string (format nil "]}~%") out)))
        ((equal "application/x-ndjson" content-type)
         (loop for row in rows
               do (write-string (%row-to-json column-names row) out)
                  (write-string (format nil "~%") out)))
        ((equal "text/csv" content-type)
         (loop for row in (cons column-names rows)
               do (write-string (format nil "~{~A~^,~}~A" (mapcar #'%format-csv row) +crlf+) out)))))))

(defun endb-query (request-method content-type sql parameters manyp on-response)
  (handler-case
      (let* ((endb/sql:*use-cst-parser* (equal "1" (uiop:getenv "ENDB_USE_CST_PARSER")))
             (write-db (endb/sql:begin-write-tx *db*))
             (original-md (endb/sql/expr:db-meta-data write-db))
             (parameters (endb/json:resolve-json-ld-xsd-scalars (endb/sql:interpret-sql-literal parameters)))
             (manyp (endb/sql:interpret-sql-literal manyp)))
        (if (and (stringp sql) (fset:collection? parameters) (typep manyp 'boolean))
            (multiple-value-bind (result result-code)
                (endb/sql:execute-sql write-db sql parameters manyp)
              (cond
                ((or result (and (listp result-code)
                                 (not (null result-code))))
                 (funcall on-response +http-ok+ content-type
                          (%stream-response content-type result-code result)))
                (result-code (if (equal "POST" request-method)
                                 (bt:with-lock-held (*write-lock*)
                                   (if (eq original-md (endb/sql/expr:db-meta-data *db*))
                                       (progn
                                         (setf *db* (endb/sql:commit-write-tx *db* write-db))
                                         (funcall on-response +http-created+ content-type
                                                  (%stream-response content-type '("result") (list (list result-code)))))
                                       (funcall on-response +http-conflict+ "text/plain" "")))
                                 (funcall on-response +http-bad-request+ "text/plain" "")))
                (t (funcall on-response +http-conflict+ "text/plain" ""))))
            (funcall on-response +http-bad-request+ "text/plain" "")))
    (endb/lib/parser:sql-parse-error (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (endb/sql/expr:sql-runtime-error (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (local-time:invalid-timestring (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (com.inuoe.jzon:json-error (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (error (e)
      (let ((backtrace (rest (rest (ppcre:split "[\\n\\r]+" (trivial-backtrace:print-backtrace e :output nil))))))
        (endb/lib:log-error "~A~%~{~A~^~%~}" (string-trim " " (first backtrace)) (rest backtrace))
        (funcall on-response +http-internal-server-error+ "text/plain" (format nil "~A~%" e))))))

(defun main ()
  (handler-case
      (unwind-protect
           (endb/lib/server:start-server #'endb-init #'endb-query)
        (when *db*
          (endb/sql:close-db *db*)))
    (#+sbcl sb-sys:interactive-interrupt ()
      (uiop:quit 130))))

;; (test parameters
;;   (let* ((db (endb/sql:make-db))
;;          (write-db (endb/sql:begin-write-tx db))
;;          (app (make-api-handler write-db)))

;;     (is (equal (list +http-ok+
;;                      '(:content-type "application/json")
;;                      (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
;;                (%req app
;;                      :post "/sql"
;;                      :body "{\"q\":\"SELECT ?, ?\",\"p\":[{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"},{\"b\":1}]}"
;;                      :content-type "application/ld+json")))

;;     (is (equal (list +http-ok+
;;                      '(:content-type "application/json")
;;                      (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
;;                (%req app
;;                      :post "/sql"
;;                      :body (format nil "--12345~AContent-Disposition: form-data; name=\"q\"~A~ASELECT ?, ?~A--12345~AContent-Disposition: form-data; name=\"p\"~A~A[2001-01-01,{b:1}]~A--12345--"
;;                                    endb/http::+crlf+ endb/http::+crlf+ endb/http:)
;;                      :content-type "multipart/form-data; boundary=12345")))

;;     (is (equal (list +http-ok+
;;                      '(:content-type "application/json")
;;                      (format nil "[[3]]~%"))
;;                (%req app
;;                      :post "/sql"
;;                      :body "{\"q\":\"SELECT :a + :b\",\"p\":{\"a\":1,\"b\":2}}"
;;                      :content-type "application/json")))

;;     (is (equal (list +http-created+
;;                      '(:content-type "application/json")
;;                      (format nil "[[2]]~%"))
;;                (%req app
;;                      :post "/sql"
;;                      :body "{\"q\":\"INSERT INTO foo {:a, :b}\",\"p\":[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}],\"m\":true}"
;;                      :content-type "application/json")))

;;     (is (equal (list +http-ok+
;;                      '(:content-type "application/json")
;;                      (format nil "[[1,2],[3,4]]~%"))
;;                (%req app
;;                      :get "/sql"
;;                      :query "q=SELECT%20a%2Cb%20FROM%20foo%20ORDER%20BY%20a")))))

;; (test errors
;;   (let* ((db (endb/sql:make-db))
;;          (write-db (endb/sql:begin-write-tx db))
;;          (app (make-api-handler write-db)))

;;     (is (equal (list +http-created+
;;                      '(:content-type "application/json")
;;                      (format nil "[[1]]~%"))
;;                (%req app
;;                      :post "/sql"
;;                      :body "{\"q\":\"INSERT INTO foo {a: 1, b: 2}\"}"
;;                      :content-type "application/json")))

;;     (is (equal (list +http-bad-request+
;;                      '(:content-type "text/plain"
;;                        :content-length 0)
;;                      '(""))
;;                (%req app
;;                      :get "/sql"
;;                      :query "q=DELETE%20FROM%20foo")))

;;     (is (equal (list +http-bad-request+
;;                      '(:content-type "text/plain")
;;                      (list (format nil "Invalid argument types: SIN(\"foo\")~%")))
;;                (%req app
;;                      :get "/sql"
;;                      :query "q=SELECT%20SIN%28%27foo%27%29")))))
