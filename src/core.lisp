(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
  (:import-from :clingon)
  (:import-from :uiop)
  (:import-from :endb/lib)
  (:import-from :endb/sql))
(in-package :endb/core)

(defvar *table-column-pad* 2)

(defun %format-column (col)
  (format nil (if (floatp col)
                  "~,3f"
                  "~A")
          (cond
            ((eq :null col) "NULL")
            ((eq t col) "TRUE")
            ((null col) "FALSE")
            (t col))))

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
  (let* ((widths (loop for idx below (length columns)
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
            ((equal "\\q" trimmed-line) (return 0))
            (t (let ((write-db (endb/sql:begin-write-tx db)))
                 (multiple-value-bind (result result-code)
                     (endb/sql:execute-sql write-db line)
                   (cond
                     ((or result (listp result-code))
                      (%print-table result-code result t))
                     (result-code (progn
                                    (setf db (endb/sql:commit-write-tx db write-db))
                                    (format t "~A~%" result-code)))
                     (t (format *error-output* "error~%~%"))))))))
      (end-of-file (e)
        (declare (ignore e))
        (if (interactive-stream-p *standard-input*)
            (progn
              (format *error-output* "~%")
              (return 1))
            (return 0)))
      #+sbcl (sb-sys:interactive-interrupt (e)
               (declare (ignore e))
               (format *error-output* "~%")
               (return 1))
      (error (e)
        (format *error-output* "~A~%~%" e)
        (unless (interactive-stream-p *standard-input*)
          (return 1))))))

(defun endb-handler (cmd)
  (endb/lib:init-lib)
  (let ((endb-system (asdf:find-system :endb)))
    (let* ((db (endb/sql:make-directory-db :directory (clingon:getopt cmd :data-directory)))
           (exit-code (unwind-protect
                           (progn (when (interactive-stream-p *standard-input*)
                                    (format t
                                            "~A ~A~%Type \"help\" for help.~%~%"
                                            (asdf:component-name endb-system)
                                            (asdf:component-version endb-system)))
                                  (%repl db))
                        (endb/sql:close-db db))))
      (uiop:quit (or exit-code 0)))))

(defun endb-options ()
  (list
   (clingon:make-option
    :string
    :description "data directory"
    :short-name #\d
    :long-name :data-directory
    :initial-value "endb_data"
    :env-vars '("ENDB_DATA")
    :key :data-directory)))

(defun endb-command ()
  (let ((endb-system (asdf:find-system :endb)))
    (clingon:make-command :name (asdf:component-name endb-system)
                          :description "Endatabas is an immutable, cloud-first, dynamic SQL database."
                          :version (asdf:component-version endb-system)
                          :license (asdf:system-license endb-system)
                          :usage "[OPTION]..."
                          :options (endb-options)
                          :handler #'endb-handler)))

(defun main ()
  (let ((app (endb-command)))
    (clingon:run app)))
