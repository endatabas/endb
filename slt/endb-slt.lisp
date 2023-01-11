(defpackage endb-slt
  (:use cl sb-alien)
  (:export slt-main))
(in-package endb-slt)

(define-alien-routine "sltmain" int
  (argc int)
  (argv (* (* char))))

(defun %slt-main (args)
  (let ((argc (length args)))
    (with-alien ((argv (* (* char)) (make-alien (* char) (1+ argc))))
      (unwind-protect
           (progn (dotimes (n argc)
                    (setf (deref argv n) (make-alien-string (elt args n))))
                  (sltmain argc argv))
        (dotimes (n argc)
          (free-alien (deref argv n)))
        (free-alien argv)))))

(defun slt-main ()
  (load-shared-object "libsqllogictest.so")
  (%slt-main (cons "sqllogictest" (uiop:command-line-arguments))))
