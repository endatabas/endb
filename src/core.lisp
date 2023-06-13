(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
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

(defun %repl ()
  (let ((db (endb/sql:make-db)))
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
              ((equal "\\q" trimmed-line) (uiop:quit 0))
              (t (let ((write-db (endb/sql:begin-write-tx db)))
                   (multiple-value-bind (result result-code)
                       (endb/sql:execute-sql write-db line)
                     (cond
                       (result (progn
                                 (%print-table result-code result t)))
                       (result-code (progn
                                      (setf db (endb/sql:commit-write-tx db write-db))
                                      (format t "~A~%" result-code)))
                       (t (format *error-output* "error~%~%"))))))))
        (end-of-file (e)
          (declare (ignore e))
          (if (interactive-stream-p *standard-input*)
              (progn
                (format *error-output* "~%")
                (uiop:quit 1))
              (uiop:quit 0)))
        #+sbcl (sb-sys:interactive-interrupt (e)
                 (declare (ignore e))
                 (format *error-output* "~%")
                 (uiop:quit 1))
        (error (e)
          (format *error-output* "~A~%~%" e)
          (unless (interactive-stream-p *standard-input*)
            (uiop:quit 1)))))))

(defun %main (args)
  (declare (ignore args))
  (endb/lib:init-lib)
  (let ((endb-system (asdf:find-system :endb)))
    (when (interactive-stream-p *standard-input*)
      (format t
              "~A ~A~%Type \"help\" for help.~%~%"
              (asdf:component-name endb-system)
              (asdf:component-version endb-system)))
    (%repl)))

(defun main ()
  (%main (uiop:command-line-arguments)))
