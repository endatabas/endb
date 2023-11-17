(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :bordeaux-threads)
  (:import-from :fset)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :uiop))
(in-package :endb/core)

(defun %endb-init (config)
  (setf endb/lib/server:*db* (endb/sql:make-directory-db :directory (fset:lookup config "data_directory") :object-store-path nil)))

(defun %endb-close-db ()
  (when (boundp 'endb/lib/server:*db*)
    (endb/lib:log-info "shutting down")
    (let ((db endb/lib/server:*db*))
      (if (bt:acquire-lock (endb/sql/expr:db-write-lock db) nil)
          (unwind-protect
               (progn
                 (endb/sql:close-db db)
                 (makunbound 'endb/lib/server:*db*))
            (bt:release-lock (endb/sql/expr:db-write-lock db)))
          (endb/lib:log-warn "could not close the database cleanly")))))

(defun main ()
  (setf endb/sql:*use-cst-parser* (equal "1" (uiop:getenv "ENDB_USE_CST_PARSER"))
        endb/lib:*panic-hook* #'%endb-close-db)
  (uiop:quit
   (handler-case
       (unwind-protect
            (endb/lib/server:start-server #'%endb-init #'endb/http:endb-query)
         (%endb-close-db))
     (#+sbcl sb-sys:system-condition ()
       130))))
