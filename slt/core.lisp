(defpackage :endb-slt/core
  (:use :cl)
  (:export #:main)
  (:import-from :cffi)
  (:import-from :sqlite)
  (:import-from :uiop))
(in-package :endb-slt/core)

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

(defvar *db-engine*)

(cffi:defcallback sqliteGetEngineName :int
    ((pConn :pointer)
     (zName (:pointer :char)))
  (declare (ignorable pConn zName))
  (if *db-engine*
      (progn
        (setf zName (cffi:foreign-slot-value *db-engine* 'DbEngine 'zName))
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

(defun %slt-format (value type)
  (if value
      (ecase type
        (#\T (if (equal "" value)
                 "(empty)"
                 (substitute-if #\@ (lambda (c)
                                      (or (char> c #\~)
                                          (char< c #\ )))
                                (princ-to-string value))))
        (#\I (format nil "~D" value))
        (#\R (format nil "~,3F" value)))
      "NULL"))

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
          (loop for row-offset from 0 by (length zTypes)
                for row in result
                do (loop for col-offset from row-offset
                         for col in row
                         for type across zTypes
                         do (setf (cffi:mem-aref az-result :pointer col-offset)
                                  (cffi:foreign-string-alloc (%slt-format col type)))))
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

(defun %register-cl-sqlite-engine ()
  (when (not (boundp '*db-engine*))
    (pushnew (uiop:getcwd) cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libsqllogictest)
    (let* ((engine (cffi:foreign-alloc 'DbEngine))
           (engine-name (cffi:foreign-string-alloc "CLSQLite")))
      (cffi:with-foreign-slots ((zName xConnect xGetEngineName xStatement xQuery xFreeResults xDisconnect) engine DBEngine)
        (setf zName engine-name
              xConnect (cffi:callback sqliteConnect)
              xGetEngineName (cffi:callback sqliteGetEngineName)
              xStatement (cffi:callback sqliteStatement)
              xQuery (cffi:callback sqliteQuery)
              xFreeResults (cffi:callback sqliteFreeResult)
              xDisconnect (cffi:callback sqliteDisconnect))
        (sqllogictestRegisterEngine engine)
        (setq *db-engine* engine)))))

(defun %slt-main (args)
  (%register-cl-sqlite-engine)
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

(defun slt-test (test &key (engine "CLSQLite"))
  (%slt-main (list "slt-runner" "-engine" engine "-verify" test)))

(defun main ()
  (unwind-protect
       (uiop:quit (%slt-main (cons (uiop:argv0) (uiop:command-line-arguments))))
    (cffi:foreign-free (cffi:foreign-slot-value *db-engine* 'DbEngine 'zName))
    (cffi:foreign-free *db-engine*)))
