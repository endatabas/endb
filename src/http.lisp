(defpackage :endb/http
  (:use :cl)
  (:export #:endb-query #:endb-on-ws-message
           #:+http-ok+ #:+http-created+ #:+http-no-content+ #:+http-bad-request+ #:+http-conflict+ #:+http-internal-server-error+ #:+http-service-unavailable+
           #:+json-rpc-parse-error+ #:+json-rpc-invalid-request+ #:+json-rpc-method-not-found+ #:+json-rpc-invalid-params+ #:+json-rpc-internal-error+)
  (:import-from :bordeaux-threads)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :fset)
  (:import-from :endb/arrow)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/lib/cst)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :endb/sql/db)
  (:import-from :endb/sql/expr)
  (:import-from :trivial-backtrace))
(in-package :endb/http)

(defconstant +http-ok+ 200)
(defconstant +http-created+ 201)
(defconstant +http-no-content+ 204)
(defconstant +http-bad-request+ 400)
(defconstant +http-conflict+ 409)
(defconstant +http-internal-server-error+ 500)
(defconstant +http-service-unavailable+ 503)

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

(defun %rows-to-arrow (column-names rows)
  (endb/arrow:to-arrow
   (loop for row in rows
         collect (fset:convert 'fset:map (pairlis (loop for idx below (length column-names)
                                                        collect (format nil "~(~16,'0x~)" idx))
                                                  (coerce row 'list))))))

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
             do (funcall on-response-send (format nil "~{~A~^,~}~A" (map 'list #'%format-csv row) +crlf+))))
      ((equal "application/vnd.apache.arrow.file" content-type)
       (funcall on-response-send
                (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
                 (list (%rows-to-arrow column-names rows))
                 :projection column-names)))
      ((equal "application/vnd.apache.arrow.stream" content-type)
       (funcall on-response-send
                (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
                 (list (%rows-to-arrow column-names rows))
                 :ipc-stream-p t
                 :projection column-names))))))

(defun %rows-response-p (result result-code)
  (or result (and (listp result-code)
                  (not (null result-code)))))

(defun %maybe-rows-stream-response (on-response-send content-type result result-code)
  (if (%rows-response-p result result-code)
      (%stream-response on-response-send content-type result-code result)
      (%stream-response on-response-send content-type '("result") (list (vector result-code)))))

(defun endb-query (dbms request-method content-type sql parameters manyp on-response-init on-response-send &optional (make-boundary #'endb/lib:uuid-v4))
  (handler-bind ((endb/lib/server:sql-abort-query-server-error
                   (lambda (e)
                     (declare (ignore e))
                     (return-from endb-query)))
                 (error (lambda (e)
                          (endb/lib:log-error (endb/lib:format-backtrace (trivial-backtrace:print-backtrace e :output nil)))
                          (funcall on-response-init +http-internal-server-error+ "text/plain")
                          (return-from endb-query
                            (funcall on-response-send (format nil "~A~%" e))))))
    (let ((write-db (endb/sql:begin-write-tx (endb/sql/db:dbms-db dbms))))
      (handler-case
          (let* ((original-md (endb/sql/db:db-meta-data write-db))
                 (original-parameters parameters)
                 (parameters (endb/json:resolve-json-ld-xsd-scalars (endb/sql:interpret-sql-literal parameters)))
                 (original-manyp manyp)
                 (acc)
                 (multipart-mixed-response-p (equalp "multipart/mixed" content-type))
                 (boundary (when multipart-mixed-response-p
                             (funcall make-boundary)))
                 (manyp (endb/sql:interpret-sql-literal manyp))
                 (endb/sql/db:*savepoints* (endb/sql/db:dbms-savepoints dbms)))
            (labels ((send-response (status-code result result-code)
                       (if multipart-mixed-response-p
                           (progn
                             (funcall on-response-init status-code (format nil "~A; boundary=~A" content-type boundary))
                             (loop for (result . result-code) in (reverse acc)
                                   for idx from 0
                                   do (funcall on-response-send (format nil "--~A~AContent-Type: application/ld+json~A~A" boundary +crlf+ +crlf+ +crlf+))
                                      (%maybe-rows-stream-response on-response-send "application/ld+json" result result-code)
                                      (funcall on-response-send +crlf+)
                                   finally (funcall on-response-send (format nil "--~A--" boundary))))
                           (progn
                             (funcall on-response-init status-code content-type)
                             (%maybe-rows-stream-response on-response-send content-type result result-code)))))
              (cond
                ((not (or (fset:collection? parameters)
                          (typep parameters 'endb/arrow:arrow-binary)))
                 (funcall on-response-init +http-bad-request+ "text/plain")
                 (funcall on-response-send (format nil "Invalid parameters: ~A~%" original-parameters)))
                ((not (typep manyp 'boolean))
                 (funcall on-response-init +http-bad-request+ "text/plain")
                 (funcall on-response-send (format nil "Invalid many: ~A~%" original-manyp)))
                ((and (typep parameters 'endb/arrow:arrow-binary)
                      (not manyp))
                 (funcall on-response-init +http-bad-request+ "text/plain")
                 (funcall on-response-send (format nil "Arrow parameters requires many to be true: ~A~%" original-manyp)))
                (t (multiple-value-bind (result result-code)
                       (if boundary
                           (endb/sql:execute-sql write-db sql parameters manyp
                                                 (lambda (result &optional result-code)
                                                   (push (cons result result-code) acc)))
                           (endb/sql:execute-sql write-db sql parameters manyp))
                     (let* ((unchangedp (or (eq original-md (endb/sql/db:db-meta-data write-db))
                                            (loop for savepoint-db being the hash-value in endb/sql/db:*savepoints*
                                                  thereis (eq (endb/sql/db:db-meta-data (endb/sql/db:savepoint-db savepoint-db))
                                                              (endb/sql/db:db-meta-data write-db))))))
                       (cond
                         (unchangedp (send-response +http-ok+ result result-code))
                         ((equal "POST" request-method)
                          (if (endb/sql/db:db-savepoint write-db)
                              (send-response +http-ok+ result result-code)
                              (bt:with-lock-held ((endb/sql/db:dbms-write-lock dbms))
                                (endb/lib:metric-monotonic-counter "transactions_prepared_total" 1)
                                (if (eq original-md (endb/sql/db:db-meta-data (endb/sql/db:dbms-db dbms)))
                                    (let* ((new-db (endb/sql:commit-write-tx (endb/sql/db:dbms-db dbms) write-db)))
                                      (setf (endb/sql/db:dbms-db dbms) new-db)
                                      (send-response +http-created+ result result-code))
                                    (let* ((retry-original-md (endb/sql/db:db-meta-data (endb/sql/db:dbms-db dbms)))
                                           (retry-write-db (endb/sql:begin-write-tx (endb/sql/db:dbms-db dbms))))
                                      (unwind-protect
                                           (multiple-value-bind (result result-code)
                                               (endb/sql:execute-sql retry-write-db sql parameters manyp)
                                             (let* ((new-db (endb/sql:commit-write-tx (endb/sql/db:dbms-db dbms) retry-write-db)))
                                               (setf (endb/sql/db:dbms-db dbms) new-db)
                                               (send-response (if (eq retry-original-md (endb/sql/db:db-meta-data new-db))
                                                                  +http-ok+
                                                                  +http-created+)
                                                              result result-code)))
                                        (endb/lib:metric-monotonic-counter "transactions_retried_total" 1)))))))
                         (t (funcall on-response-init +http-bad-request+ "")))))))))
        (endb/lib/cst:sql-parse-error (e)
          (funcall on-response-init +http-bad-request+ "text/plain")
          (funcall on-response-send (format nil "~A~%" e)))
        (endb/sql/expr:sql-runtime-error (e)
          (funcall on-response-init +http-bad-request+ "text/plain")
          (funcall on-response-send (format nil "~A~%" e)))
        (endb/sql/db:sql-abort-query-error ()
          #+sbcl (sb-ext:gc :full t)
          (let ((savepoint (endb/sql/db:db-savepoint write-db)))
            (funcall on-response-init +http-service-unavailable+ "text/plain")
            (if savepoint
                (progn
                  (remhash savepoint (endb/sql/db:dbms-savepoints dbms))
                  (funcall on-response-send (format nil "Request aborted by server, savepoint released: ~A~%" savepoint)))
                (funcall on-response-send (format nil "Request aborted by server~%")))))
        (endb/sql/db:sql-begin-error ()
          (funcall on-response-init +http-bad-request+ "text/plain")
          (funcall on-response-send (format nil "Explicit transactions not supported~%")))
        (endb/sql/db:sql-commit-error ()
          (funcall on-response-init +http-bad-request+ "text/plain")
          (funcall on-response-send (format nil "Explicit transactions not supported~%")))
        (endb/sql/db:sql-rollback-error ()
          (if (equal "POST" request-method)
              (progn
                (funcall on-response-init +http-ok+ content-type)
                (%stream-response on-response-send content-type '("result") (list (vector t))))
              (funcall on-response-init +http-bad-request+ "")))
        (cl-ppcre:ppcre-syntax-error (e)
          (funcall on-response-init +http-bad-request+ "text/plain")
          (funcall on-response-send (format nil "~A~%" e)))))))

(defparameter +json-rpc-parse-error+ -32700)
(defparameter +json-rpc-invalid-request+ -32600)
(defparameter +json-rpc-method-not-found+ -32601)
(defparameter +json-rpc-invalid-params+ -32602)
(defparameter +json-rpc-internal-error+ -32603)

(defun %json-rpc-error (code message &optional (id :null))
  (endb/json:json-stringify (fset:map ("jsonrpc" "2.0")
                                      ("id" id)
                                      ("error" (fset:map ("code" code)
                                                         ("message" message))))))

(defun %json-rpc-result (result id)
  (format nil "{\"jsonrpc\":\"2.0\",\"id\":~A,\"result\":~A}" id (string-right-trim (list #\Newline) result)))

(defun endb-on-ws-message (dbms connection message on-ws-send)
  (handler-bind ((endb/lib/server:sql-abort-query-server-error
                   (lambda (e)
                     (declare (ignore e))
                     (return-from endb-on-ws-message)))
                 (com.inuoe.jzon:json-parse-error
                   (lambda (e)
                     (declare (ignore e))
                     (funcall on-ws-send (%json-rpc-error +json-rpc-parse-error+ "Parse error"))
                     (return-from endb-on-ws-message)))
                 (error (lambda (e)
                          (endb/lib:log-error (endb/lib:format-backtrace (trivial-backtrace:print-backtrace e :output nil)))
                          (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ (format nil "~A" e)))
                          (endb/lib:metric-monotonic-counter "websocket_message_internal_errors_total" 1)
                          (return-from endb-on-ws-message))))
    (unwind-protect
         (let ((json-rpc-message (endb/json:json-parse message)))
           (if (not (fset:map? json-rpc-message))
               (funcall on-ws-send (%json-rpc-error +json-rpc-invalid-request+ "Invalid Request"))
               (let* ((json-rpc-version (or (fset:lookup json-rpc-message "jsonrpc") :null))
                      (json-rpc-method (fset:lookup json-rpc-message "method"))
                      (json-rpc-params (fset:lookup json-rpc-message "params"))
                      (json-rpc-params (if (fset:seq? json-rpc-params)
                                           (fset:map ("q" (fset:lookup json-rpc-params 0))
                                                     ("p" (fset:lookup json-rpc-params 1))
                                                     ("m" (fset:lookup json-rpc-params 2)))
                                           json-rpc-params))
                      (json-rpc-id (or (fset:lookup json-rpc-message "id") :null))
                      (interactive-tx-p (not (null (endb/sql/db:db-connection-db connection))))
                      (acc ""))
                 (labels ((on-response-send (chunk)
                            (setf acc (concatenate 'string acc chunk))))
                   (handler-case
                       (cond
                         ((not (equal "2.0" json-rpc-version))
                          (funcall on-ws-send (%json-rpc-error +json-rpc-invalid-request+ "Invalid Request" json-rpc-id)))
                         ((not (or (integerp json-rpc-id) (stringp json-rpc-id)))
                          (funcall on-ws-send (%json-rpc-error +json-rpc-invalid-request+ "Invalid Request" json-rpc-id)))
                         ((not (equal "sql" json-rpc-method))
                          (funcall on-ws-send (%json-rpc-error +json-rpc-method-not-found+ "Method not found" json-rpc-id)))
                         ((not (fset:map? json-rpc-params))
                          (funcall on-ws-send (%json-rpc-error +json-rpc-invalid-params+ "Invalid params" json-rpc-id)))
                         (t (let* ((sql (fset:lookup json-rpc-params "q"))
                                   (parameters (or (fset:lookup json-rpc-params "p") (fset:empty-seq)))
                                   (manyp (fset:lookup json-rpc-params "m"))
                                   (write-db (or (endb/sql/db:db-connection-db connection)
                                                 (endb/sql:begin-write-tx (endb/sql/db:dbms-db dbms))))
                                   (original-md (or (endb/sql/db:db-connection-original-md connection)
                                                    (endb/sql/db:db-meta-data write-db)))
                                   (content-type "application/ld+json"))
                              (labels ((commit-tx (result result-code)
                                         (bt:with-lock-held ((endb/sql/db:dbms-write-lock dbms))
                                           (endb/lib:metric-monotonic-counter "transactions_prepared_total" 1)
                                           (cond
                                             ((eq original-md (endb/sql/db:db-meta-data (endb/sql/db:dbms-db dbms)))
                                              (let* ((new-db (endb/sql:commit-write-tx (endb/sql/db:dbms-db dbms) write-db)))
                                                (setf (endb/sql/db:dbms-db dbms) new-db)
                                                (%maybe-rows-stream-response #'on-response-send content-type result result-code)
                                                (funcall on-ws-send (%json-rpc-result acc json-rpc-id))))
                                             (interactive-tx-p
                                              (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ "Conflict" json-rpc-id))
                                              (endb/lib:metric-monotonic-counter "transactions_conflicted_total" 1))
                                             (t (unwind-protect
                                                     (let* ((retry-write-db (endb/sql:begin-write-tx (endb/sql/db:dbms-db dbms))))
                                                       (multiple-value-bind (result result-code)
                                                           (endb/sql:execute-sql retry-write-db sql parameters manyp)
                                                         (let* ((new-db (endb/sql:commit-write-tx (endb/sql/db:dbms-db dbms) retry-write-db)))
                                                           (setf (endb/sql/db:dbms-db dbms) new-db)
                                                           (%maybe-rows-stream-response #'on-response-send content-type result result-code)
                                                           (funcall on-ws-send (%json-rpc-result acc json-rpc-id)))))
                                                  (endb/lib:metric-monotonic-counter "transactions_retried_total" 1)))))))
                                (if (or (not (stringp sql))
                                        (not (fset:collection? parameters))
                                        (not (typep manyp 'boolean)))
                                    (funcall on-ws-send (%json-rpc-error +json-rpc-invalid-params+ "Invalid params" json-rpc-id))
                                    (handler-case
                                        (multiple-value-bind (result result-code)
                                            (endb/sql:execute-sql write-db sql parameters manyp)
                                          (let* ((unchangedp (eq original-md (endb/sql/db:db-meta-data write-db))))
                                            (if (or unchangedp interactive-tx-p)
                                                (progn
                                                  (%maybe-rows-stream-response #'on-response-send content-type result result-code)
                                                  (funcall on-ws-send (%json-rpc-result acc json-rpc-id)))
                                                (commit-tx result result-code))))
                                      (endb/sql/db:sql-begin-error ()
                                        (if interactive-tx-p
                                            (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ "Cannot nest transactions" json-rpc-id))
                                            (progn
                                              (setf (endb/sql/db:db-interactive-tx-id write-db) (endb/lib:uuid-v4)
                                                    (endb/sql/db:db-connection-db connection) write-db
                                                    (endb/sql/db:db-connection-original-md connection) original-md)
                                              (%stream-response #'on-response-send content-type '("result") (list (vector t)))
                                              (funcall on-ws-send (%json-rpc-result acc json-rpc-id))
                                              (endb/lib:metric-counter "interactive_transactions_active" 1))))
                                      (endb/sql/db:sql-commit-error ()
                                        (if interactive-tx-p
                                            (unwind-protect
                                                 (progn
                                                   (setf (endb/sql/db:db-connection-db connection) nil
                                                         (endb/sql/db:db-connection-original-md connection) nil)
                                                   (commit-tx nil t))
                                              (endb/lib:metric-counter "interactive_transactions_active" -1))
                                            (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ "No active transaction" json-rpc-id))))
                                      (endb/sql/db:sql-rollback-error ()
                                        (unwind-protect
                                             (progn
                                               (setf (endb/sql/db:db-connection-db connection) nil
                                                     (endb/sql/db:db-connection-original-md connection) nil)
                                               (%stream-response #'on-response-send content-type '("result") (list (vector t)))
                                               (funcall on-ws-send (%json-rpc-result acc json-rpc-id)))
                                          (when interactive-tx-p
                                            (endb/lib:metric-counter "interactive_transactions_active" -1))))))))))
                     (endb/lib/cst:sql-parse-error (e)
                       (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ (format nil "~A" e) json-rpc-id)))
                     (endb/sql/expr:sql-runtime-error (e)
                       (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ (format nil "~A" e) json-rpc-id)))
                     (endb/sql/db:sql-abort-query-error ()
                       #+sbcl (sb-ext:gc :full t)
                       (setf (endb/sql/db:db-connection-db connection) nil
                             (endb/sql/db:db-connection-original-md connection) nil)
                       (if interactive-tx-p
                           (unwind-protect
                                (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ "Request aborted by server, transaction was rolled back" json-rpc-id))
                             (endb/lib:metric-counter "interactive_transactions_active" -1))
                           (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ "Request aborted by server" json-rpc-id))))
                     (cl-ppcre:ppcre-syntax-error (e)
                       (funcall on-ws-send (%json-rpc-error +json-rpc-internal-error+ (format nil "~A" e) json-rpc-id)))))))))))
