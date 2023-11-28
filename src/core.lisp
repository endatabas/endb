(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :bordeaux-threads)
  (:import-from :fset)
  (:import-from :endb/sql/db)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :uiop)
  (:import-from :trivial-backtrace))
(in-package :endb/core)

(defun %endb-init (config)
  (setf endb/lib/server:*db* (endb/sql:make-directory-db :directory (fset:lookup config "data_directory"))))

(defun %endb-close-db ()
  (when (boundp 'endb/lib/server:*db*)
    (endb/lib:log-info "shutting down")
    (let ((db endb/lib/server:*db*))
      (if (bt:acquire-lock (endb/sql/db:db-write-lock db) nil)
          (unwind-protect
               (progn
                 (endb/sql:close-db db)
                 (makunbound 'endb/lib/server:*db*))
            (bt:release-lock (endb/sql/db:db-write-lock db)))
          (endb/lib:log-warn "could not close the database cleanly")))))

(defun %endb-main ()
  (setf endb/sql:*use-cst-parser* (equal "1" (uiop:getenv "ENDB_USE_CST_PARSER"))
        endb/lib:*panic-hook* #'%endb-close-db)
  (handler-bind ((#+sbcl sb-sys:interactive-interrupt (lambda (e)
                                                        (declare (ignore e))
                                                        (return-from %endb-main 130)))
                 (error (lambda (e)
                          (endb/lib:log-error (format nil "~A" e))
                          (let ((backtrace (rest (ppcre:split "[\\n\\r]+" (trivial-backtrace:print-backtrace e :output nil)))))
                            (endb/lib:log-debug "~A~%~{~A~^~%~}" (string-trim " " (first backtrace)) (rest backtrace))
                            (return-from %endb-main 1)))))
    (unwind-protect
         (progn
           (%endb-init (endb/lib/server:parse-command-line))
           (endb/lib/server:start-server #'endb/http:endb-query))
      (%endb-close-db))))

(defun main ()
  (uiop:quit (%endb-main)))
