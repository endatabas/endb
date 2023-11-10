(defpackage :endb/http
  (:use :cl)
  (:export #:endb-query #:+http-ok+ #:+http-created+ #:+http-bad-request+ #:+http-conflict+ #:+http-internal-server-error+)
  (:import-from :bordeaux-threads)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :fset)
  (:import-from :endb/json)
  (:import-from :endb/lib/parser)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :endb/sql/expr)
  (:import-from :trivial-backtrace))
(in-package :endb/http)

(defvar *write-lock* (bt:make-lock))

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
      (let* ((write-db (endb/sql:begin-write-tx endb/lib/server:*db*))
             (original-md (endb/sql/expr:db-meta-data write-db))
             (original-parameters parameters)
             (parameters (endb/json:resolve-json-ld-xsd-scalars (endb/sql:interpret-sql-literal parameters)))
             (original-manyp manyp)
             (manyp (endb/sql:interpret-sql-literal manyp)))
        (cond
          ((not (fset:collection? parameters))
           (funcall on-response +http-bad-request+ "text/plain" (format nil "Invalid parameters: ~A~%" original-parameters)))
          ((not (typep manyp 'boolean))
           (funcall on-response +http-bad-request+ "text/plain" (format nil "Invalid many: ~A~%" original-manyp)))
          (t (multiple-value-bind (result result-code)
                 (endb/sql:execute-sql write-db sql parameters manyp)
               (cond
                 ((or result (and (listp result-code)
                                  (not (null result-code))))
                  (funcall on-response +http-ok+ content-type
                           (%stream-response content-type result-code result)))
                 (result-code (if (equal "POST" request-method)
                                  (bt:with-lock-held (*write-lock*)
                                    (if (eq original-md (endb/sql/expr:db-meta-data endb/lib/server:*db*))
                                        (progn
                                          (setf endb/lib/server:*db* (endb/sql:commit-write-tx endb/lib/server:*db* write-db))
                                          (funcall on-response +http-created+ content-type
                                                   (%stream-response content-type '("result") (list (list result-code)))))
                                        (funcall on-response +http-conflict+ "" "")))
                                  (funcall on-response +http-bad-request+ "" "")))
                 (t (funcall on-response +http-conflict+ "" "")))))))
    (endb/lib/parser:sql-parse-error (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (endb/sql/expr:sql-runtime-error (e)
      (funcall on-response +http-bad-request+ "text/plain" (format nil "~A~%" e)))
    (error (e)
      (let ((backtrace (rest (rest (ppcre:split "[\\n\\r]+" (trivial-backtrace:print-backtrace e :output nil))))))
        (endb/lib:log-error "~A~%~{~A~^~%~}" (string-trim " " (first backtrace)) (rest backtrace))
        (funcall on-response +http-internal-server-error+ "text/plain" (format nil "~A~%" e))))))
