(defpackage :endb/core
  (:use :cl)
  (:export #:main #:endb-startup #:endb-shutdown)
  #-wasm32 (:import-from :bordeaux-threads)
  (:import-from :fset)
  (:import-from :endb/sql/db)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/lib/server)
  (:import-from :endb/sql)
  (:import-from :uiop)
  (:import-from :trivial-backtrace))
(in-package :endb/core)

(defun endb-startup (data-directory)
  (endb/lib:with-trace-span "startup"
    (endb/lib:log-info "version ~A" (endb/lib:get-endb-version))
    (endb/lib:log-info "data directory ~A" data-directory)
    (endb/sql:install-interrupt-query-handler)
    (endb/sql:make-dbms :directory data-directory)))

(defun endb-shutdown (dbms)
  (endb/lib:with-trace-span "shutdown"
    (endb/lib:log-info "shutting down")
    (if #+thread-support (bt:acquire-lock (endb/sql/db:dbms-write-lock dbms) nil)
        #-thread-support t
        (unwind-protect
             (endb/sql:dbms-close dbms)
          #+thread-support (bt:release-lock (endb/sql/db:dbms-write-lock dbms)))
        (endb/lib:log-warn "could not close the database cleanly")))
  (endb/lib:shutdown-logger))

(defun %endb-main ()
  (handler-bind ((#+sbcl sb-sys:interactive-interrupt
                  #+ecl ext:interactive-interrupt
                  (lambda (e)
                    (declare (ignore e))
                    (return-from %endb-main 130)))
                 (error (lambda (e)
                          (endb/lib:log-error "~A" e)
                          (endb/lib:log-debug "~A" (endb/lib:format-backtrace (trivial-backtrace:print-backtrace e :output nil)))
                          (return-from %endb-main 1))))
    (unwind-protect
         (endb/lib/server:start-tokio
          (lambda ()
            (let* ((data-directory (fset:lookup (endb/lib/server:parse-command-line) "data_directory"))
                   (dbms (endb-startup data-directory)))
              (setf endb/lib:*panic-hook* (lambda ()
                                            (endb-shutdown dbms)))
              (endb/lib/server:start-server dbms #'endb/http:endb-query #'endb/http:endb-on-ws-message))))
      (when endb/lib:*panic-hook*
        (funcall endb/lib:*panic-hook*)))))

(defun main ()
  (uiop:quit (%endb-main)))
