(defpackage :endb-test/http
  (:use :cl :fiveam :endb/http)
  (:import-from :bordeaux-threads)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql))
(in-package :endb-test/http)

(in-suite* :http)

(defvar *current-response*)

(defun %on-response-init (status-code content-type)
  (setf *current-response* (list status-code
                                 (unless (equalp "" content-type)
                                   (list :content-type content-type))
                                 ""))
  nil)

(defun %on-response-send (body)
  (setf *current-response* (append (butlast *current-response*)
                                   (list (concatenate 'string (car (last *current-response*)) body))))
  nil)

(defun %do-query (request-method content-type sql parameters manyp &optional (on-response-init #'%on-response-init) (on-response-send #'%on-response-send))
  (let ((*current-response*))
    (endb-query request-method content-type sql parameters manyp on-response-init on-response-send)
    *current-response*))

(test parameters
  (let* ((endb/lib/server:*db* (endb/sql:make-db)))
    (is (equal
         (list +http-ok+
               '(:content-type "application/json")
               (format nil "[[\"2001-01-01\",{\"b\":1}]]~%"))
         (%do-query "POST" "application/json" "SELECT ?, ?" "[{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"},{\"b\":1}]" "false")))


    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[3]]~%"))
               (%do-query "POST" "application/json" "SELECT :a + :b" "{\"a\":1,\"b\":2}" "false")))

    (is (equal (list +http-created+
                     '(:content-type "application/json")
                     (format nil "[[2]]~%"))
               (%do-query "POST" "application/json" "INSERT INTO foo {:a, :b}" "[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}]" "true")))

    (is (equal (list +http-ok+
                     '(:content-type "application/json")
                     (format nil "[[1,2],[3,4]]~%"))
               (%do-query "GET" "application/json" "SELECT * FROM foo ORDER BY a" "[]" "false")))))

(test errors
  (let* ((endb/lib/server:*db* (endb/sql:make-db)))

    (is (equal (list +http-created+
                     '(:content-type "application/json")
                     (format nil "[[1]]~%"))
               (%do-query "POST" "application/json" "INSERT INTO foo {a: 1, b: 2}" "[]" "false")))

    (is (equal (list +http-bad-request+ () "")
               (%do-query "GET" "application/json" "DELETE FROM foo" "[]" "false")))

    (is (equal (list +http-bad-request+ '(:content-type "text/plain") (format nil "Invalid argument types: SIN(\"foo\")~%"))
               (%do-query "GET" "application/json" "SELECT SIN(\"foo\")" "[]" "false")))

    (is (equal (list +http-bad-request+ '(:content-type "text/plain") (format nil "Invalid parameters: 1~%"))
               (%do-query "GET" "application/json" "SELECT 1" "1" "false")))

    (is (equal (list +http-bad-request+ '(:content-type "text/plain") (format nil "Invalid many: 1~%"))
               (%do-query "GET" "application/json" "SELECT 1" "[]" "1")))


    (destructuring-bind (status-code headers body)
        (%do-query "GET" "application/json" "SELECT" "[]" "false")
      (declare (ignore body))
      (is (eq +http-bad-request+ status-code))
      (is (equal '(:content-type "text/plain") headers)))

    (let ((endb/lib:*log-level* (endb/lib:resolve-log-level :off))
          (calls 0))
      (is (equal (list +http-internal-server-error+ '(:content-type "text/plain")
                       (format nil "common lisp error~%"))
                 (%do-query "GET" "application/json" "SELECT 1" "[]" "false"
                            (lambda (status-code content-type)
                              (incf calls)
                              (if (= 1 calls)
                                  (error "common lisp error")
                                  (%on-response-init status-code content-type)))
                            #'%on-response-send))))

    (let ((endb/lib:*log-level* (endb/lib:resolve-log-level :off))
          (calls 0))
      (is (null (%do-query "GET" "application/json" "SELECT 1" "[]" "false"
                           (lambda (status-code content-type)
                             (incf calls)
                             (if (= 1 calls)
                                 (error 'endb/lib/server:sql-abort-query-error)
                                 (%on-response-init status-code content-type)))
                           #'%on-response-send))))))

(test conflict
  (setf endb/lib/server:*db* (endb/sql:make-db))
  (let ((write-db (endb/sql:begin-write-tx endb/lib/server:*db*)))

    (is (bt:acquire-lock endb/http::*write-lock*))
    (let ((thread (bt:make-thread
                   (lambda ()
                     (%do-query "POST" "application/json" "INSERT INTO foo {a: 1, b: 2}" "[]" "false")))))

      (multiple-value-bind (result result-code)
          (endb/sql:execute-sql write-db "INSERT INTO foo {a: 1, b: 2}")
        (is (null result))
        (is (= 1 result-code))
        (setf endb/lib/server:*db* (endb/sql:commit-write-tx endb/lib/server:*db* write-db)))

      (bt:release-lock endb/http::*write-lock*)

      (is (equal (list +http-conflict+ () "")
                 (bt:join-thread thread))))))
