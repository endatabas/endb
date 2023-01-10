(defpackage endb
  (:use cl)
  (:export get-engine-name main))
(in-package endb)

(defun get-engine-name ()
  (let ((endb-system (asdf:find-system :endb)))
    (asdf:component-name endb-system)))

(defun %main (args)
  (declare (ignore args))
  (let ((endb-system (asdf:find-system :endb)))
    (format t
            "~A ~A~%"
            (asdf:component-name endb-system)
            (asdf:component-version endb-system))))

(defun main ()
  (%main (uiop:command-line-arguments)))
