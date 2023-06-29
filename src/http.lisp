(defpackage :endb/http
  (:use :cl)
  (:export #:make-api-handler)
  (:import-from :lack.request)
  (:import-from :endb/sql))
(in-package :endb/http)

(defun make-api-handler (db)
  (declare (ignore db))
  (lambda (env)
    (let ((req (lack.request:make-request env)))
      (declare (ignore req))
      '(404 nil nil))))
