(defpackage endb-slt
  (:use cl)
  (:export slt-main))
(in-package endb-slt)

(cffi:defcstruct DbEngine
  (zName :pointer)
  (pAuxData :pointer)
  (xConnect :pointer)
  (xGetEngineName :pointer)
  (xStatement :pointer)
  (xQuery :pointer)
  (xFreeResults :pointer)
  (xDisconnect :pointer))

(cffi:defctype DbEngine (:struct DbEngine))

(cffi:defcallback sqliteConnect :int
    ((p :pointer)
     (zCon :pointer)
     (ppConn (:pointer :pointer))
     (zOpt :pointer))
  (declare (ignore p zCon ppConn zOpt))
  1)

(defvar *engine-name*)

(cffi:defcallback sqliteGetEngineName :int
    ((p :pointer)
     (zName :pointer))
  (declare (ignorable p zName))
  (when *engine-name*
    (setf zName *engine-name*))
  0)

(cffi:defcallback sqliteStatement :int
    ((p :pointer)
     (zSql :pointer)
     (bQuiet :int))
  (declare (ignore p zSql bQuiet))
  1)

(cffi:defcallback sqliteQuery :int
    ((p :pointer)
     (zSql (:pointer :pointer))
     (zTypes (:pointer :pointer))
     (pazResult (:pointer (:pointer :pointer)))
     (pnResult :int))
  (declare (ignore p zSql zTypes pazResult pnResult))
  1)

(cffi:defcallback sqliteFreeResult :int
    ((p :pointer)
     (azResult (:pointer :pointer))
     (nResult :int))
  (declare (ignore p azResult nResult))
  1)

(cffi:defcallback sqliteDisconnect :int
    ((p :pointer))
  (declare (ignore p))
  1)

(cffi:define-foreign-library libsqllogictest
  (t (:default "libsqllogictest")))

(cffi:defcfun "sqllogictest_main" :int
  (argc :int)
  (argv (:pointer (:pointer :char))))

(cffi:defcfun "sqllogictestRegisterEngine" :void
  (p (:pointer (:struct DbEngine))))

(defun %slt-main (args)
  (let ((argc (length args)))
    (cffi:with-foreign-object (argv :pointer (1+ argc))
      (unwind-protect
           (progn
             (dotimes (n argc)
               (setf (cffi:mem-aref argv :pointer n)
                     (cffi:foreign-string-alloc (elt args n))))
             (setf (cffi:mem-aref argv :pointer argc)
                   (cffi:null-pointer))
             (sqllogictest-main argc argv))
        (dotimes (n argc)
          (cffi:foreign-string-free (cffi:mem-aref argv :pointer n)))))))

(defun %register-cl-sqlite-engine (engine engine-name)
  (cffi:with-foreign-slots ((zName xConnect xGetEngineName xStatement xQuery xFreeResults xDisconnect) engine DBEngine)
    (setf zName engine-name
          xConnect (cffi:callback sqliteConnect)
          xGetEngineName (cffi:callback sqliteGetEngineName)
          xStatement (cffi:callback sqliteStatement)
          xQuery (cffi:callback sqliteQuery)
          xFreeResults (cffi:callback sqliteFreeResult)
          xDisconnect (cffi:callback sqliteDisconnect))
    (sqllogictestRegisterEngine engine)))

(defun slt-main ()
  (push (uiop:getcwd) cffi:*foreign-library-directories*)
  (cffi:use-foreign-library libsqllogictest)
  (cffi:with-foreign-string (engine-name "CL-SQLite")
    (setf *engine-name* engine-name)
    (cffi:with-foreign-object (engine 'DbEngine)
      (%register-cl-sqlite-engine engine engine-name)
      (uiop:quit (%slt-main (cons (uiop:argv0) (uiop:command-line-arguments)))))))
