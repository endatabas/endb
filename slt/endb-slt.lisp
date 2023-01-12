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

(defvar *connections* (make-hash-table))

(cffi:defcallback sqliteConnect :int
    ((NotUsed :pointer)
     (zCon :string)
     (ppConn (:pointer :pointer))
     (zOpt :string))
  (declare (ignorable NotUsed ppConn zOpt))
  (let* ((handle (sqlite:connect (or zCon ":memory:")))
         (pConn (sqlite::handle handle)))
    (setf (cffi:mem-ref ppConn :pointer) pConn)
    (setf (gethash (cffi:pointer-address pConn) *connections*) handle)
    0))

(defvar *engine-name*)

(cffi:defcallback sqliteGetEngineName :int
    ((pConn :pointer)
     (zName (:pointer :char)))
  (declare (ignorable pConn zName))
  (if *engine-name*
      (progn
        (setf zName *engine-name*)
        0)
      1))

(cffi:defcallback sqliteStatement :int
    ((pConn :pointer)
     (zSql :string)
     (bQuiet :int))
  (declare (ignore bQuiet))
  (let ((handle (gethash (cffi:pointer-address pConn) *connections*)))
    (if handle
        (progn
          (sqlite:execute-non-query handle zSql)
          0)
        1)))

(cffi:defcallback sqliteQuery :int
    ((pConn :pointer)
     (zSql :string)
     (zTypes :string)
     (pazResult (:pointer (:pointer (:pointer :char))))
     (pnResult (:pointer :int)))
  (declare (ignorable pazResult pnResult))
  (let ((handle (gethash (cffi:pointer-address pConn) *connections*)))
    (if handle
        (let* ((result (sqlite:execute-to-list handle zSql))
               (n-used (* (length result) (length zTypes)))
               (az-result (cffi:foreign-alloc :pointer :count n-used)))
          (loop for n from 0 to (length result)
                for row in result
                do (let ((offset (* n (length zTypes))))
                     (loop for m from 0 to (length zTypes)
                           for col in row
                           do (setf (cffi:mem-aref az-result :pointer (+ m offset))
                                    (cffi:foreign-string-alloc
                                     (if col
                                         (case (aref zTypes m)
                                           (#\T (if (equal "" col)
                                                    "(empty)"
                                                    (format nil "~A" col)))
                                           (#\I (format nil "~D" col))
                                           (#\R (format nil "~,3F" col)))
                                         "NULL"))))))
          (setf (cffi:mem-ref pnResult :int) n-used)
          (setf (cffi:mem-ref pazResult :pointer) az-result)
          0)
        1)))

(cffi:defcallback sqliteFreeResult :int
    ((pConn :pointer)
     (azResult (:pointer (:pointer :char)))
     (nResult :int))
  (declare (ignore pConn))
  (dotimes (n nResult)
    (cffi:foreign-free (cffi:mem-aref azResult :pointer n)))
  (cffi:foreign-free azResult)
  0)

(cffi:defcallback sqliteDisconnect :int
    ((pConn :pointer))
  (let ((handle (gethash (cffi:pointer-address pConn) *connections*)))
    (if handle
        (progn
          (remhash (cffi:pointer-address pConn) *connections*)
          (sqlite:disconnect handle)
          0)
        1)))

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
  (cffi:with-foreign-strings ((engine-name "CLSQLite")
                              (real-engine-name "SQLite"))
    (setf *engine-name* real-engine-name)
    (cffi:with-foreign-object (engine 'DbEngine)
      (%register-cl-sqlite-engine engine engine-name)
      (uiop:quit (%slt-main (cons (uiop:argv0) (uiop:command-line-arguments)))))))
