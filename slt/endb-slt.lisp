(defpackage endb-slt
  (:use cl)
  (:export slt-main))
(in-package endb-slt)

(sb-alien:define-alien-routine "sltmain" sb-alien:int
  (argc sb-alien:int)
  (argv (* (* sb-alien:char))))

(defun slt-main ()
  (sb-alien:load-shared-object "libsqllogictest.so")
  (let* ((args (cons "sqllogictest" (uiop:command-line-arguments)))
         (argc (length args)))
    (sb-alien:with-alien ((argv (* (* sb-alien:char)) (sb-alien:make-alien (* sb-alien:char) (1+ argc))))
      (unwind-protect
           (progn (dotimes (n argc)
                    (setf (sb-alien:deref argv n) (sb-alien:make-alien-string (elt args n))))
                  (sltmain argc argv))
        (dotimes (n argc)
          (sb-alien:free-alien (sb-alien:deref argv n)))
        (sb-alien:free-alien argv)))))
