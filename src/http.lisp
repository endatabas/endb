(defpackage :endb/http
  (:use :cl)
  (:export #:endb-query #:+http-ok+ #:+http-created+ #:+http-bad-request+ #:+http-conflict+ #:+http-internal-server-error+)
  (:import-from :bordeaux-threads)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :fset)
  (:import-from :endb/json)
  (:import-from :endb/lib/cst)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :endb/sql/db)
  (:import-from :endb/sql/expr)
  (:import-from :trivial-backtrace))
(in-package :endb/http)

(defconstant +http-ok+ 200)
(defconstant +http-created+ 201)
(defconstant +http-bad-request+ 400)
(defconstant +http-conflict+ 409)
(defconstant +http-internal-server-error+ 500)

(defparameter +crlf+ (coerce '(#\return #\linefeed) 'string))

(defun %format-csv (x)
  (ppcre:regex-replace-all "\\\\\"" (com.inuoe.jzon:stringify x) "\"\""))

(defun %row-to-json (column-names row)
  (with-output-to-string (out)
    (com.inuoe.jzon:with-writer (writer :stream out)
      (com.inuoe.jzon:with-object writer
        (loop for column across row
              for column-name in column-names
              do (com.inuoe.jzon:write-key writer column-name)
                 (com.inuoe.jzon:write-value writer column))))))

(defun %stream-response (on-response-send content-type column-names rows)
  (let ((endb/json:*json-ld-scalars* (equal "application/ld+json" content-type)))
    (cond
      ((equal "application/json" content-type)
       (funcall on-response-send "[")
       (loop for row in rows
             for idx from 0
             unless (zerop idx)
               do (funcall on-response-send ",")
             do (funcall on-response-send (com.inuoe.jzon:stringify row))
             finally (funcall on-response-send (format nil "]~%"))))
      ((equal "application/ld+json" content-type)
       (funcall on-response-send "{\"@context\":{\"xsd\":\"http://www.w3.org/2001/XMLSchema#\",\"@vocab\":\"http://endb.io/\"},\"@graph\":[")
       (loop for row in rows
             for idx from 0
             unless (zerop idx)
               do (funcall on-response-send ",")
             do (funcall on-response-send (%row-to-json column-names row))
             finally (funcall on-response-send (format nil "]}~%"))))
      ((equal "application/x-ndjson" content-type)
       (loop for row in rows
             do (funcall on-response-send (%row-to-json column-names row))
                (funcall on-response-send (format nil "~%"))))
      ((equal "text/csv" content-type)
       (loop for row in (cons column-names rows)
             do (funcall on-response-send (format nil "~{~A~^,~}~A" (map 'list #'%format-csv row) +crlf+)))))))

(defun endb-query (request-method content-type sql parameters manyp on-response-init on-response-send)
  (handler-bind ((endb/lib/server:sql-abort-query-error
                   (lambda (e)
                     (declare (ignore e))
                     (return-from endb-query)))
                 (error (lambda (e)
                          (let ((backtrace (rest (ppcre:split "[\\n\\r]+" (trivial-backtrace:print-backtrace e :output nil)))))
                            (endb/lib:log-error "~A~%~{~A~^~%~}" (string-trim " " (first backtrace)) (rest backtrace))
                            (funcall on-response-init +http-internal-server-error+ "text/plain")
                            (return-from endb-query
                              (funcall on-response-send (format nil "~A~%" e)))))))
    (handler-case
        (let* ((write-db (endb/sql:begin-write-tx endb/lib/server:*db*))
               (original-md (endb/sql/db:db-meta-data write-db))
               (original-parameters parameters)
               (parameters (endb/json:resolve-json-ld-xsd-scalars (endb/sql:interpret-sql-literal parameters)))
               (original-manyp manyp)
               (manyp (endb/sql:interpret-sql-literal manyp)))
          (cond
            ((not (fset:collection? parameters))
             (funcall on-response-init +http-bad-request+ "text/plain")
             (funcall on-response-send (format nil "Invalid parameters: ~A~%" original-parameters)))
            ((not (typep manyp 'boolean))
             (funcall on-response-init +http-bad-request+ "text/plain")
             (funcall on-response-send (format nil "Invalid many: ~A~%" original-manyp)))
            (t (multiple-value-bind (result result-code)
                   (endb/sql:execute-sql write-db sql parameters manyp)
                 (cond
                   ((or result (and (listp result-code)
                                    (not (null result-code))))
                    (progn
                      (funcall on-response-init +http-ok+ content-type)
                      (%stream-response on-response-send content-type result-code result)))
                   (result-code (if (equal "POST" request-method)
                                    (bt:with-lock-held ((endb/sql/db:db-write-lock endb/lib/server:*db*))
                                      (if (eq original-md (endb/sql/db:db-meta-data endb/lib/server:*db*))
                                          (progn
                                            (setf endb/lib/server:*db* (endb/sql:commit-write-tx endb/lib/server:*db* write-db))
                                            (funcall on-response-init +http-created+ content-type)
                                            (%stream-response on-response-send content-type '("result") (list (vector result-code))))
                                          (funcall on-response-init +http-conflict+ "")))
                                    (funcall on-response-init +http-bad-request+ "")))
                   (t (funcall on-response-init +http-conflict+ "")))))))
      (endb/lib/cst:sql-parse-error (e)
        (funcall on-response-init +http-bad-request+ "text/plain")
        (funcall on-response-send (format nil "~A~%" e)))
      (endb/sql/expr:sql-runtime-error (e)
        (funcall on-response-init +http-bad-request+ "text/plain")
        (funcall on-response-send (format nil "~A~%" e)))
      (cl-ppcre:ppcre-syntax-error (e)
        (funcall on-response-init +http-bad-request+ "text/plain")
        (funcall on-response-send (format nil "~A~%" e))))))
