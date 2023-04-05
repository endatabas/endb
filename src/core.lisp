(defpackage :endb/core
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
  (:import-from :uiop)
  (:import-from :endb/lib)
  (:import-from :endb/sql))
(in-package :endb/core)

(defvar *table-column-min-width* 8)
(defvar *table-column-pad* 2)

(defun %format-row (widths row &optional center)
  (format nil "|~{ ~A |~}"
          (loop for col in row
                for width in widths
                collect (format nil (cond
                                      (center "~V:@<~A~>")
                                      ((numberp col)
                                       "~V@S")
                                      (t "~VS"))
                                width col))))

(defun %print-table (columns rows &optional stream)
  (let* ((strs (loop for row in (cons columns rows)
                     collect (loop for col in row
                                   collect (princ-to-string col))))
         (widths (loop for idx below (length columns)
                       collect (apply #'max 0
                                      (loop for col in strs
                                            collect (+ *table-column-pad* (max *table-column-min-width* (length (nth idx col))))))))
         (header (%format-row widths columns t)))
    (format stream "~A~%" header)
    (format stream "~A~%" (make-string (length header) :initial-element #\-))
    (loop for row in rows
          do (format stream "~A~%" (%format-row widths row)))
    (format stream "(~A ~A)~%~%" (length rows) (if (= 1 (length rows))
                                                   "row"
                                                   "rows"))))

(defun %repl ()
  (let ((db (endb/sql:create-db)))
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
               (format t "\\q   - quits this session.~%help - displays this help.~%~%"))
              ((equal "\\q" trimmed-line) (uiop:quit 0))
              (t (multiple-value-bind (result result-code)
                     (endb/sql:execute-sql db line)
                   (cond
                     (result (progn
                               (%print-table result-code result t)))
                     (result-code (format t "~A~%" result-code))
                     (t (format *error-output* "error~%~%")))))))
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
  (let ((endb-system (asdf:find-system :endb)))
    (when (interactive-stream-p *standard-input*)
      (format t
              "~A ~A~%Type \"help\" for help.~%~%"
              (asdf:component-name endb-system)
              (asdf:component-version endb-system)))
    (%repl)))

(defun main ()
  (%main (uiop:command-line-arguments)))
