(defpackage :endb/lib
  (:use :cl)
  (:export #:init-lib)
  (:import-from :cffi)
  (:import-from :asdf)
  (:import-from :uiop))
(in-package :endb/lib)

(cffi:define-foreign-library libendb
  (t (:default "libendb")))

(defvar *initialized* nil)

(defun init-lib ()
  (unless *initialized*
    (pushnew (or (uiop:pathname-directory-pathname (uiop:argv0))
                 (asdf:system-relative-pathname :endb "target/"))
             cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libendb)
    (setf *initialized* t)))
