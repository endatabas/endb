(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :fset)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :uiop))
(in-package :endb/core)

(defun %endb-init (config)
  (setf endb/lib/server:*db* (endb/sql:make-directory-db :directory (fset:lookup config "data_directory") :object-store-path nil)))

(defun main ()
  (let ((endb/sql:*use-cst-parser* (equal "1" (uiop:getenv "ENDB_USE_CST_PARSER"))))
    (uiop:quit
     (handler-case
         (unwind-protect
              (endb/lib/server:start-server #'%endb-init #'endb/http:endb-query)
           (when endb/lib/server:*db*
             (endb/sql:close-db endb/lib/server:*db*)))
       (#+sbcl sb-sys:interactive-interrupt ()
         130)))))
