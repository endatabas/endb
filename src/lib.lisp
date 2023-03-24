(defpackage :endb/lib
  (:use :cl)
  (:export #:parse-sql)
  (:import-from :cffi)
  (:import-from :cl-ppcre)
  (:import-from :asdf)
  (:import-from :uiop))
(in-package :endb/lib)

(cffi:define-foreign-library libendb
  (t (:default "libendb")))

(defvar *initialized* nil)

(defun %init-lib ()
  (unless *initialized*
    (pushnew (or (uiop:pathname-directory-pathname (uiop:argv0))
                 (asdf:system-relative-pathname :endb "target/"))
             cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libendb)
    (setf *initialized* t)))

(cffi:defcenum Keyword
  :select
  :from
  :where
  :group-by
  :having
  :order-by
  :<
  :>
  :and
  :function
  :asc
  :desc
  :all
  :distinct
  :true
  :false
  :null
  :limit
  :offset
  :join
  :type
  :left
  :inner
  :on
  :except
  :intesect
  :union
  :union-all
  :values
  :insert
  :column-names
  :delete
  :update
  :create-index
  :drop-index
  :create-view
  :drop-view
  :if-exists
  :drop-table)

(cffi:defcenum Ast_Tag
  :List
  :KW
  :Integer
  :Float
  :Id
  :String
  :Binary)

(cffi:defcstruct Id_Union
  (start :uint64)
  (end :uint64))

(cffi:defcstruct String_Union
  (start :uint64)
  (end :uint64))

(cffi:defcstruct Binary_Union
  (start :uint64)
  (end :uint64))

(cffi:defcstruct Integer_Union
  (n :int64))

(cffi:defcstruct Float_Union
  (n :double))

(cffi:defcstruct KW_Union
  (kw :int))

(cffi:defcstruct List_Union
  (cap :uint64)
  (ptr :pointer)
  (len :uint64))

(cffi:defcunion Ast_Union
  (list (:struct List_Union))
  (kw (:struct KW_Union))
  (integer (:struct Integer_Union))
  (float (:struct float_Union))
  (id (:struct Id_Union))
  (string (:struct String_Union))
  (binary (:struct Binary_Union)))

(cffi:defcstruct Ast
  (tag :int)
  (value (:union Ast_Union)))

(cffi:defcfun "endb_ast_vec_len" :size
  (vec (:pointer (:struct List_Union))))

(cffi:defcfun "endb_ast_vec_ptr" :pointer
  (vec (:pointer (:struct List_Union))))

(cffi:defcfun "endb_ast_size" :size)

(cffi:defcfun "endb_ast_vec_element" (:pointer (:struct Ast))
  (vec (:pointer (:struct List_union)))
  (idx :size))

(cffi:defcfun "endb_parse_sql" :void
  (input (:pointer :char))
  (on_success :pointer)
  (on_error :pointer))

(defstruct ast-builder (acc (list nil)))

(defparameter kw-array (loop with kw-enum-hash = (cffi::value-keywords (cffi::parse-type 'Keyword))
                             with acc = (make-array (hash-table-count kw-enum-hash)
                                                    :element-type 'keyword
                                                    :initial-element :select)
                             for k being the hash-key
                               using (hash-value v)
                                 of kw-enum-hash
                             do (setf (aref acc k) v)
                             finally (return acc)))

(defun hex-to-binary (hex)
  (loop with acc = (make-array (/ (length hex) 2) :element-type '(unsigned-byte 8))
        with tmp = (make-string 2)
        for idx below (length hex) by 2
        for out-idx from 0
        do (setf (schar tmp 0) (aref hex idx))
           (setf (schar tmp 1) (aref hex (1+ idx)))
           (setf (aref acc out-idx) (parse-integer tmp :radix 16))
        finally (return acc)))

(defun visit-ast (input builder ast)
  (loop with queue = (list ast)
        with acc = (ast-builder-acc builder)
        with stride = (cffi:foreign-type-size '(:struct Ast))
        while queue
        for ast = (pop queue)
        do (case ast
             (:start-list (push () acc))
             (:end-list (push (pop acc) (first acc)))
             (t
              (cffi:with-foreign-slots ((tag value) ast (:struct Ast))
                (ecase tag
                  (0 (progn
                       (push :end-list queue)
                       (loop with ptr = (endb-ast-vec-ptr value)
                             for idx below (endb-ast-vec-len value)
                             for offset from 0 by stride
                             do (push (cffi:inc-pointer ptr offset) queue))
                       (push :start-list queue)))
                  (1 (cffi:with-foreign-slots ((kw) value (:struct KW_Union))
                       (push (aref kw-array kw) (first acc))))
                  (2 (cffi:with-foreign-slots ((n) value (:struct Integer_Union))
                       (push n (first acc))))
                  (3 (cffi:with-foreign-slots ((n) value (:struct Float_Union))
                       (push n (first acc))))
                  (4 (cffi:with-foreign-slots ((start end) value (:struct Id_Union))
                       (push (make-symbol (subseq input start end)) (first acc))))
                  (5 (cffi:with-foreign-slots ((start end) value (:struct String_Union))
                       (push (subseq input start end) (first acc))))
                  (6 (cffi:with-foreign-slots ((start end) value (:struct Binary_Union))
                       (push (hex-to-binary (subseq input start end)) (first acc))))))))))

(defvar *input*)
(defvar *ast-builder*)

(defun strip-ansi-escape-codes (s)
  (cl-ppcre:regex-replace-all "\\[3\\d(?:;\\d+;\\d+)?m(.+?)\\[0m" s "\\1"))

(define-condition sql-parse-error (error)
  ((message :initarg :message :reader sql-parse-error-message))
  (:report (lambda (condition stream)
             (write (strip-ansi-escape-codes (sql-parse-error-message condition)) :stream stream))))

(cffi:defcallback cl-on-success :void
    ((ast (:pointer (:struct Ast))))
  (visit-ast *input* *ast-builder* ast))

(cffi:defcallback cl-on-error :void
    ((err :string))
  (error 'sql-parse-error :message err))

(defvar on-success (cffi:callback cl-on-success))
(defvar on-error (cffi:callback cl-on-error))

(defun parse-sql (input)
  (%init-lib)
  (let ((*input* input)
        (*ast-builder* (make-ast-builder)))
    (if (typep input 'base-string)
        (cffi:with-pointer-to-vector-data (ptr input)
          (endb-parse-sql ptr on-success on-error))
        (cffi:with-foreign-string (ptr input)
          (endb-parse-sql ptr on-success on-error)))
    (caar (ast-builder-acc *ast-builder*))))

;; (time
;;  (let ((acc))
;;    (dotimes (n 100000)
;;      (setf acc (parse-sql "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b")))
;;    acc))
