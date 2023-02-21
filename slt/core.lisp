(defpackage :endb-slt/core
  (:use :cl)
  (:export #:main)
  (:import-from :cffi)
  (:import-from :sqlite)
  (:import-from :asdf)
  (:import-from :uiop)
  (:import-from :endb/sql)
  (:import-from :endb/sql/expr)
  (:import-from :endb/sql/compiler)
  #+sbcl (:import-from :sb-sprof))
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

(defvar *sqlite-db-engine*)
(defvar *endb-db-engine*)

(defvar *endb-db-engine-reported-name* "postgresql")

(cffi:defcfun "sqlite3_snprintf" :int
  (size :size)
  (str :pointer)
  (control :string)
  &rest)

(defun %slt-format (value type)
  (cond
    ((eq :null value) "NULL")
    ((eq t value) "1")
    ((null value) "0")
    (t (ecase type
         (#\T (if (equal "" value)
                  "(empty)"
                  (substitute-if #\@ (lambda (c)
                                       (or (char> c #\~)
                                           (char< c #\ )))
                                 (endb/sql/expr:sql-cast value :varchar))))
         (#\I (format nil "~D" (cond
                                 ((floatp value) (round value))
                                 ((numberp value) value)
                                 (t 0))))
         (#\R (cffi:with-foreign-pointer-as-string (s 32)
                (sqlite3-snprintf 32 s "%.3f" :double (coerce (if (numberp value)
                                                                  value
                                                                  0.0d0)
                                                              'double-float))))))))

(defun %slt-result (result zTypes pazResult pnResult &key nil-is-null)
  (let* ((n-used (* (length result) (length zTypes)))
         (az-result (cffi:foreign-alloc :pointer :count n-used)))
    (loop for row-offset from 0 by (length zTypes)
          for row in result
          do (loop for col-offset from row-offset
                   for col in row
                   for type across zTypes
                   do (setf (cffi:mem-aref az-result :pointer col-offset)
                            (cffi:foreign-string-alloc (%slt-format (if (and (null col) nil-is-null)
                                                                        :null
                                                                        col) type)))))
    (setf (cffi:mem-ref pnResult :int) n-used)
    (setf (cffi:mem-ref pazResult :pointer) az-result)))

(defun %slt-free-result (azResult nResult)
  (dotimes (n nResult)
    (cffi:foreign-free (cffi:mem-aref azResult :pointer n)))
  (cffi:foreign-free azResult))

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

(cffi:defcallback sqliteGetEngineName :int
    ((pConn :pointer)
     (zName (:pointer :char)))
  (declare (ignorable pConn zName))
  (if *sqlite-db-engine*
      (progn
        (setf zName (cffi:foreign-slot-value *sqlite-db-engine* 'DbEngine 'zName))
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
        (progn
          (%slt-result (sqlite:execute-to-list handle zSql) zTypes pazResult pnResult :nil-is-null t)
          0)
        1)))

(cffi:defcallback sqliteFreeResult :int
    ((pConn :pointer)
     (azResult (:pointer (:pointer :char)))
     (nResult :int))
  (declare (ignore pConn))
  (%slt-free-result azResult nResult)
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

(cffi:defcallback endbConnect :int
    ((NotUsed :pointer)
     (zCon :string)
     (ppConn (:pointer :pointer))
     (zOpt :string))
  (declare (ignorable NotUsed zCon ppConn zOpt))
  (let* ((endb (endb/sql:create-db)))
    (setf (cffi:mem-ref ppConn :pointer) (cffi:null-pointer))
    (setf (gethash (cffi:pointer-address (cffi:null-pointer)) *connections*) endb)
    0))

(cffi:defcallback endbGetEngineName :int
    ((pConn :pointer)
     (zName (:pointer :char)))
  (declare (ignorable pConn zName))
  (if *endb-db-engine*
      (progn
        (setf (cffi:mem-ref zName :pointer)
              (cffi:foreign-slot-value *endb-db-engine* 'DbEngine 'pAuxData))
        0)
      1))

(cffi:defcallback endbStatement :int
    ((pConn :pointer)
     (zSql :string)
     (bQuiet :int))
  (declare (ignore bQuiet))
  (let ((endb (gethash (cffi:pointer-address pConn) *connections*)))
    (if endb
        (handler-case
            (multiple-value-bind (result result-code)
                (endb/sql:execute-sql endb zSql)
              (declare (ignore result))
              (if result-code
                  0
                  1))
          (endb/sql/expr:sql-runtime-error (e)
            (declare (ignore e))
            1))
        1)))

(cffi:defcallback endbQuery :int
    ((pConn :pointer)
     (zSql :string)
     (zTypes :string)
     (pazResult (:pointer (:pointer (:pointer :char))))
     (pnResult (:pointer :int)))
  (declare (ignorable zTypes pazResult pnResult))
  (let ((endb (gethash (cffi:pointer-address pConn) *connections*)))
    (if endb
        (handler-case
            (multiple-value-bind (result result-code)
                (endb/sql:execute-sql endb zSql)
              (if result-code
                  (progn
                    (%slt-result result zTypes pazResult pnResult)
                    0)
                  1))
          (endb/sql/expr:sql-runtime-error (e)
            (declare (ignore e))
            1))
        1)))

(cffi:defcallback endbFreeResult :int
    ((pConn :pointer)
     (azResult (:pointer (:pointer :char)))
     (nResult :int))
  (declare (ignore pConn))
  (%slt-free-result azResult nResult)
  0)

(cffi:defcallback endbDisconnect :int
    ((pConn :pointer))
  (if (gethash (cffi:pointer-address pConn) *connections*)
      (progn
        (remhash (cffi:pointer-address pConn) *connections*)
        0)
      1))

(cffi:define-foreign-library libsqllogictest
  (t (:default "libsqllogictest")))

(cffi:defcfun "sqllogictest_main" :int
  (argc :int)
  (argv (:pointer (:pointer :char))))

(cffi:defcfun "sqllogictestRegisterEngine" :void
  (p (:pointer (:struct DbEngine))))

(defun %register-sqlite-engine ()
  (let* ((engine (cffi:foreign-alloc 'DbEngine))
         (engine-name (cffi:foreign-string-alloc "CLSQLite")))
    (cffi:with-foreign-slots ((zName pAuxData xConnect xGetEngineName xStatement xQuery xFreeResults xDisconnect) engine DBEngine)
      (setf zName engine-name
            pAuxData (cffi:null-pointer)
            xConnect (cffi:callback sqliteConnect)
            xGetEngineName (cffi:callback sqliteGetEngineName)
            xStatement (cffi:callback sqliteStatement)
            xQuery (cffi:callback sqliteQuery)
            xFreeResults (cffi:callback sqliteFreeResult)
            xDisconnect (cffi:callback sqliteDisconnect))
      (sqllogictestRegisterEngine engine)
      (setq *sqlite-db-engine* engine))))

(defun %register-endb-engine ()
  (let* ((engine (cffi:foreign-alloc 'DbEngine))
         (engine-name (cffi:foreign-string-alloc "endb"))
         (reported-engine-name (cffi:foreign-string-alloc *endb-db-engine-reported-name*)))
    (cffi:with-foreign-slots ((zName pAuxData xConnect xGetEngineName xStatement xQuery xFreeResults xDisconnect) engine DBEngine)
      (setf zName engine-name
            pAuxData reported-engine-name
            xConnect (cffi:callback endbConnect)
            xGetEngineName (cffi:callback endbGetEngineName)
            xStatement (cffi:callback endbStatement)
            xQuery (cffi:callback endbQuery)
            xFreeResults (cffi:callback endbFreeResult)
            xDisconnect (cffi:callback endbDisconnect))
      (sqllogictestRegisterEngine engine)
      (setq *endb-db-engine* engine))))

(defun %register-db-engines ()
  (when (not (boundp '*sqlite-db-engine*))
    (pushnew (or (uiop:pathname-directory-pathname (uiop:argv0))
                 (asdf:system-relative-pathname :endb-slt "target/"))
             cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libsqllogictest)
    (%register-sqlite-engine)
    (%register-endb-engine)))

(defun %slt-main (args)
  (%register-db-engines)
  (let ((argc (length args)))
    (cffi:with-foreign-object (argv :pointer (1+ argc))
      (unwind-protect
           (progn
             (dotimes (n argc)
               (setf (cffi:mem-aref argv :pointer n)
                     (cffi:foreign-string-alloc (elt args n))))
             (setf (cffi:mem-aref argv :pointer argc)
                   (cffi:null-pointer))
             #+sbcl (let ((sb-ext:*evaluator-mode* (if (equal "1" (uiop:getenv "SB_INTERPRET"))
                                                       :interpret
                                                       sb-ext:*evaluator-mode*)))
                      (if (equal "1" (uiop:getenv "SLT_TIMING"))
                          (time (sqllogictest-main argc argv))
                          (sqllogictest-main argc argv)))
             #-sbcl (if (uiop:getenv "SLT_TIMING")
                        (time (sqllogictest-main argc argv))
                        (sqllogictest-main argc argv)))
        (dotimes (n argc)
          (cffi:foreign-string-free (cffi:mem-aref argv :pointer n)))))))

(defun slt-test (test &key (engine "endb"))
  (%slt-main (list "slt-runner" "-engine" engine "-verify" test)))

(defun %free-db-engine (db-engine)
  (cffi:foreign-free (cffi:foreign-slot-value db-engine 'DbEngine 'zName))
  (cffi:with-foreign-slots ((pAuxData) db-engine DbEngine)
    (unless (cffi:null-pointer-p pAuxData)
      (cffi:foreign-free pAuxData)))
  (cffi:foreign-free db-engine))

(defun main ()
  (unwind-protect
       (let ((endb/sql/compiler:*verbose* (equal "1" (uiop:getenv "ENDB_VERBOSE")))
             (endb/sql:*query-timing* (equal "1" (uiop:getenv "ENDB_QUERY_TIMING")))
             (*endb-db-engine-reported-name* (or (uiop:getenv "ENDB_ENGINE_REPORTED_NAME")
                                                 *endb-db-engine-reported-name*)))
         (uiop:quit
          (let ((exit-code 0)
                (args (cons (uiop:argv0) (uiop:command-line-arguments))))
            #+sbcl (if (uiop:getenv "SB_SPROF")
                       (progn (sb-sprof:with-profiling (:max-samples 1000
                                                        :report :flat
                                                        :loop nil)
                                (setq exit-code (%slt-main args)))
                              exit-code)
                       (%slt-main args))
            #-sbcl (%slt-main args))))
    (%free-db-engine *sqlite-db-engine*)
    (%free-db-engine *endb-db-engine*)))

(defun slt-sanity (&key (engine "endb"))
  (loop for idx from 1 upto 5
        do (time (slt-test (format nil "sqllogictest/test/select~D.test" idx) :engine engine))))
