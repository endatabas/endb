(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
  (:import-from :asdf/component)
  (:import-from :bordeaux-threads)
  (:import-from :clack)
  (:import-from :clack.handler.hunchentoot)
  (:import-from :clingon)
  (:import-from :log4cl)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/sql)
  (:import-from :uiop))
(in-package :endb/core)

(defun endb-handler (cmd)
  (setf (log4cl:logger-log-level log4cl:*root-logger*) (clingon:getopt cmd :log-level))
  (endb/lib:init-lib)
  (let* ((db (endb/sql:make-directory-db :directory (clingon:getopt cmd :data-directory) :object-store-path nil))
         (http-port (clingon:getopt cmd :http-port))
         (http-server (clack:clackup (endb/http:make-api-handler db
                                                                 :username (clingon:getopt cmd :username)
                                                                 :password (clingon:getopt cmd :password))
                                     :port http-port
                                     :address "0.0.0.0"
                                     :silent t)))
    (unwind-protect
         (progn
           (log:info "~A ~A" (clingon:command-full-name cmd) (clingon:command-version cmd))
           (log:info "Listening on port ~A" http-port)
           (bt:join-thread (clack.handler::handler-acceptor http-server)))
      (clack:stop http-server)
      (endb/sql:close-db db))))

(defun endb-options ()
  (list
   (clingon:make-option
    :string
    :description "data directory"
    :short-name #\d
    :long-name "data-directory"
    :initial-value "endb_data"
    :env-vars '("ENDB_DATA_DIRECTORY")
    :key :data-directory)
   (clingon:make-option
    :integer
    :description "HTTP port"
    :short-name #\p
    :long-name "http-port"
    :initial-value 3803
    :env-vars '("ENDB_HTTP_PORT")
    :key :http-port)
   (clingon:make-option
    :string
    :description "username"
    :long-name "username"
    :env-vars '("ENDB_USERNAME")
    :key :username)
   (clingon:make-option
    :string
    :description "password"
    :long-name "password"
    :env-vars '("ENDB_PASSWORD")
    :key :password)
   (clingon:make-option
    :choice
    :description "log level"
    :short-name #\l
    :long-name "log-level"
    :initial-value "info"
    :items '("info" "warn" "error" "debug")
    :env-vars '("ENDB_LOG_LEVEL")
    :key :log-level)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defvar *git-describe* (or (uiop:getenv "ENDB_GIT_DESCRIBE")
                             (handler-case
                                 (multiple-value-bind (out err exit-code)
                                     (uiop:run-program '("git" "describe" "--always" "--dirty") :output '(:string :stripped t) :ignore-error-status t)
                                   (declare (ignore err))
                                   (when (zerop exit-code)
                                     out))
                               (error ()))
                             "<unknown revision>")))

(defun endb-command ()
  (let ((endb-system (asdf:find-system :endb)))
    (clingon:make-command :name (asdf:component-name endb-system)
                          :description (asdf/component:component-description endb-system)
                          :version (format nil "~A ~A" (asdf:component-version endb-system) *git-describe*)
                          :license (asdf:system-license endb-system)
                          :usage "[OPTION]..."
                          :options (endb-options)
                          :handler #'endb-handler)))

(defun main ()
  (let ((app (endb-command))
        (endb/sql:*use-cst-parser* (equal "1" (uiop:getenv "ENDB_USE_CST_PARSER"))))
    (clingon:run app)))
