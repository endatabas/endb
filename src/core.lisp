(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
  (:import-from :asdf/component)
  (:import-from :bordeaux-threads)
  (:import-from :clack)
  (:import-from :clingon)
  (:import-from :com.inuoe.jzon)
  (:import-from :endb/http)
  (:import-from :endb/lib)
  (:import-from :endb/sql)
  (:import-from :endb/storage/meta-data))
(in-package :endb/core)

(defvar *table-column-pad* 2)

(defun %format-column (col)
  (com.inuoe.jzon:stringify col))

(defun %format-row (widths row &optional center)
  (format nil "~{~A~^|~}"
          (loop for col in row
                for width in widths
                collect (format nil (cond
                                      (center " ~V:@<~A~>")
                                      ((numberp col) "~V:@A ")
                                      (t " ~VA"))
                                (1- width)
                                (%format-column col)))))

(defun %print-table (columns rows &optional stream)
  (let* ((endb/storage/meta-data:*json-ld-scalars* nil)
         (widths (loop for idx below (length columns)
                       collect (loop for row in (cons columns rows)
                                     maximize (+ *table-column-pad* (length (%format-column (nth idx row))))))))
    (format stream "~A~%" (%format-row widths columns t))
    (format stream "~{~A~^+~}~%"
            (loop for width in widths
                  collect (format nil "~v@{~A~:*~}" width "-")))
    (loop for row in rows
          do (format stream "~A~%" (%format-row widths row)))
    (format stream "(~D row~:P)~%~%" (length rows))))

(defun %repl (db)
  (loop
    (finish-output)
    (when (interactive-stream-p *standard-input*)
      (format t "-> ")
      (finish-output))
    (handler-case
        (let* ((line (read-line))
               (trimmed-line (string-trim " " line)))
          (cond
            ((equal "" trimmed-line))
            ((equal "help" trimmed-line)
             (format t "~:{~8A ~A~%~}~%"
                     '(("\\q" "quits this session.")
                       ("help" "displays this help."))))
            ((equal "\\q" trimmed-line)
             (error 'clingon:exit-error :code 0))
            (t (let ((write-db (endb/sql:begin-write-tx db)))
                 (multiple-value-bind (result result-code)
                     (endb/sql:execute-sql write-db line)
                   (cond
                     ((or result (and (listp result-code)
                                      (not (null result-code))))
                      (%print-table result-code result t))
                     (result-code (progn
                                    (setf db (endb/sql:commit-write-tx db write-db))
                                    (format t "~A~%" result-code)))
                     (t (format *error-output* "error~%~%"))))))))
      (clingon:exit-error (e)
        (error e))
      (end-of-file (e)
        (declare (ignore e))
        (when (interactive-stream-p *standard-input*)
          (format *error-output* "~%"))
        (error 'clingon:exit-error :code 0))
      (error (e)
        (format *error-output* "~A~%~%" e)
        (unless (interactive-stream-p *standard-input*)
          (error 'clingon:exit-error :code 1))))))

(defun endb-handler (cmd)
  (endb/lib:init-lib)
  (let* ((db (endb/sql:make-directory-db :directory (clingon:getopt cmd :data-directory) :object-store-path nil))
         (http-port (clingon:getopt cmd :http-port))
         (http-server (unless (clingon:getopt cmd :interactive)
                        (clack:clackup (endb/http:make-api-handler db) :port http-port :silent t))))
    (unwind-protect
         (progn
           (when (interactive-stream-p *standard-input*)
             (format t "~A ~A~%" (clingon:command-full-name cmd) (clingon:command-version cmd)))
           (if http-server
               (progn
                 (format t "Listening on port ~A~%" http-port)
                 (bt:join-thread (clack.handler::handler-acceptor http-server)))
               (progn
                 (when (interactive-stream-p *standard-input*)
                   (format t "Type \"help\" for help.~%~%"))
                 (%repl db))))
      (when http-server
        (clack:stop http-server))
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
    :flag
    :description "interactive session"
    :short-name #\i
    :long-name "interactive"
    :key :interactive)))

(defun endb-command ()
  (let ((endb-system (asdf:find-system :endb)))
    (clingon:make-command :name (asdf:component-name endb-system)
                          :description (asdf/component:component-description endb-system)
                          :version (asdf:component-version endb-system)
                          :license (asdf:system-license endb-system)
                          :usage "[OPTION]..."
                          :options (endb-options)
                          :handler #'endb-handler)))

(defun main ()
  ;; clingon:exit-error has a guard against existing the REPL, but clack brings in swank.
  (let ((*features* (remove :swank *features*))
        (app (endb-command)))
    (clingon:run app)))
