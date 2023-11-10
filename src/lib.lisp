(defpackage :endb/lib
  (:use :cl)
  (:export #:init-lib #:log-error #:log-warn #:log-info #:log-debug #:resolve-log-level #:*log-level*)
  (:import-from :bordeaux-threads)
  (:import-from :cffi)
  (:import-from :asdf)
  (:import-from :uiop))
(in-package :endb/lib)

(cffi:define-foreign-library libendb
  (t (:default "libendb")))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +log-levels+ '(:off :error :warn :info :debug :trace)))

(defun resolve-log-level (&optional level)
  (or (position level +log-levels+)
      (position :info +log-levels+)))

(defvar *log-level* (resolve-log-level :info))

(defmacro log-error (control-string &rest format-arguments)
  `(when (<= ,(position :error +log-levels+) *log-level*)
     (endb-log-error
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-warn (control-string &rest format-arguments)
  `(when (<= ,(position :warn +log-levels+) *log-level*)
     (endb-log-warn
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-info (control-string &rest format-arguments)
  `(when (<= ,(position :info +log-levels+) *log-level*)
     (endb-log-info
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-debug (control-string &rest format-arguments)
  `(when (<= ,(position :debug +log-levels+) *log-level*)
     (endb-log-debug
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-trace (control-string &rest format-arguments)
  `(when (<= ,(position :trace +log-levels+) *log-level*)
     (endb-log-trace
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(cffi:defcfun "endb_log_error" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_warn" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_info" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_debug" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_trace" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_init_logger" :void)

(defvar *initialized* nil)

(defun init-lib ()
  (unless *initialized*
    (pushnew (or (uiop:pathname-directory-pathname (uiop:argv0))
                 (asdf:system-relative-pathname :endb "target/"))
             cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libendb)
    (endb-init-logger)
    (let ((log-level (uiop:getenv "ENDB_LOG_LEVEL")))
      (when log-level
        (setf *log-level* (resolve-log-level (intern (string-upcase log-level) :keyword)))))
    (setf *initialized* t)))
