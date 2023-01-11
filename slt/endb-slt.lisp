(defpackage endb-slt
  (:use cl)
  (:export slt-main))
(in-package endb-slt)

(cffi:define-foreign-library libsqllogictest
  (t (:default "libsqllogictest")))

(cffi:defcfun "sqllogictest_main" :int
  (argc :int)
  (argv (:pointer (:pointer :char))))

(defun %slt-main (args)
  (let ((argc (length args)))
    (cffi:with-foreign-object (argv :pointer (1+ argc))
      (unwind-protect
           (progn
             (dotimes (n argc)
               (setf (cffi:mem-aref argv :pointer n)
                     (cffi:foreign-string-alloc (elt args n))))
             (sqllogictest-main argc argv))
        (dotimes (n argc)
          (cffi:foreign-string-free (cffi:mem-aref argv :pointer n)))))))

(defun slt-main ()
  (setq cffi:*foreign-library-directories* (list (uiop:getcwd)))
  (cffi:use-foreign-library libsqllogictest)
  (uiop:quit (%slt-main (cons (uiop:argv0) (uiop:command-line-arguments)))))
