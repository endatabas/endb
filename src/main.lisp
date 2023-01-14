(defpackage :endb/main
  (:use :cl)
  (:export #:main)
  (:import-from :asdf)
  (:import-from :uiop))
(in-package :endb/main)

(defun %main (args)
  (declare (ignore args))
  (let ((endb-system (asdf:find-system :endb)))
    (format t
            "~A ~A~%"
            (asdf:component-name endb-system)
            (asdf:component-version endb-system))))

(defun main ()
  (%main (uiop:command-line-arguments)))
