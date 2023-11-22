(defpackage :endb/lib/parser
  (:use :cl)
  (:export  #:sql-parse-error #:parse-sql #:annotate-input-with-error #:strip-ansi-escape-codes #:sql-string-to-cl)
  (:import-from :endb/lib)
  (:import-from :cffi)
  (:import-from :cl-ppcre)
  (:import-from :trivial-utf-8))
(in-package :endb/lib/parser)

(cffi:defcenum Keyword
  :select
  :from
  :where
  :group-by
  :having
  :order-by
  :<
  :<=
  :>
  :>=
  :=
  :<>
  :is
  :in
  :in-query
  :between
  :like
  :case
  :exists
  :scalar-subquery
  :else
  :+
  :-
  :*
  :/
  :%
  :<<
  :>>
  :and
  :or
  :not
  :function
  :aggregate-function
  :count
  :count-star
  :avg
  :sum
  :min
  :max
  :total
  :group_concat
  :cast
  :asc
  :desc
  :distinct
  :all
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
  :intersect
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
  :create-table
  :drop-table
  :multiple-statments
  :date
  :time
  :timestamp
  :array
  :object
  :access
  :as-of
  :with
  :array_agg
  :object_agg
  :array-query
  :unnest
  :with-ordinality
  :objects
  :parameter
  :\|\|
  :shorthand-property
  :spread-property
  :computed-property
  :duration
  :current_date
  :current_time
  :current_timestamp
  :unset
  :recursive
  :overlaps
  :contains
  :precedes
  :succeeds
  :immediately_precedes
  :immediately_succeeds
  :year
  :month
  :day
  :hour
  :minute
  :second
  :interval
  :on-conflict
  :blob
  :glob
  :regexp
  :patch
  :match
  :~
  :&
  :\|
  :#
  :path
  :create-assertion
  :drop-assertion
  :extract
  :erase)

(cffi:defcenum Ast_Tag
  :List
  :KW
  :Integer
  :Float
  :Id
  :String)

(cffi:defcstruct Id_Union
  (start :int32)
  (end :int32))

(cffi:defcstruct String_Union
  (start :int32)
  (end :int32))

(cffi:defcstruct Integer_Union
  (lower :uint64)
  (upper :int64))

(cffi:defcstruct Float_Union
  (n :double))

(cffi:defcstruct KW_Union
  (kw :unsigned-int))

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
  (string (:struct String_Union)))

(cffi:defcstruct Ast
  (tag :unsigned-int)
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

(cffi:defcfun "endb_annotate_input_with_error" :void
  (input :string)
  (message :string)
  (start :size)
  (end :size)
  (on_success :pointer))

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

(defparameter +double-single-quote-scanner+ (ppcre:create-scanner "''"))
(defparameter +backslash-escape-scanner+ (ppcre:create-scanner "(?s)(\\\\u[0-9a-fA-F]{4}|\\\\.)"))

(defun sql-string-to-cl (single-quote-p s)
  (let* ((s (if (and single-quote-p (find #\' s))
                (ppcre:regex-replace-all +double-single-quote-scanner+ s "'")
                s)))
    (if (find #\\ s)
        (ppcre:regex-replace-all +backslash-escape-scanner+
                                 s
                                 (lambda (target-string start end match-start match-end reg-starts reg-ends)
                                   (declare (ignore start end match-end reg-starts reg-ends))
                                   (let ((c (char target-string (1+ match-start))))
                                     (string
                                      (case c
                                        ((#\" #\' #\\ #\/) c)
                                        ((#\Newline #\Return #\Line_Separator #\Paragraph_Separator) "")
                                        (#\0 #\Nul)
                                        (#\t #\Tab)
                                        (#\n #\Newline)
                                        (#\r #\Return)
                                        (#\f #\Page)
                                        (#\b #\Backspace)
                                        (#\v #\Vt)
                                        (#\u (code-char (parse-integer (subseq target-string (+ 2 match-start) (+ 6 match-start)) :radix 16))))))))
        s)))

(defun visit-ast (input input-bytes builder ast)
  (loop with queue = (list ast)
        with acc = (ast-builder-acc builder)
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
                       (loop for idx below (endb-ast-vec-len value)
                             do (push (endb-ast-vec-element value idx) queue))
                       (push :start-list queue)))
                  (1 (cffi:with-foreign-slots ((kw) value (:struct KW_Union))
                       (push (aref kw-array kw) (first acc))))
                  (2 (cffi:with-foreign-slots ((lower upper) value (:struct Integer_Union))
                       (push (logior (ash upper 64) lower) (first acc))))
                  (3 (cffi:with-foreign-slots ((n) value (:struct Float_Union))
                       (push n (first acc))))
                  (4 (cffi:with-foreign-slots ((start end) value (:struct Id_Union))
                       (let ((s (make-symbol (trivial-utf-8:utf-8-bytes-to-string input-bytes :start start :end end))))
                         (setf (get s :start) start (get s :end) end (get s :input) input)
                         (push s (first acc)))))
                  (5 (cffi:with-foreign-slots ((start end) value (:struct String_Union))
                       (let* ((s (trivial-utf-8:utf-8-bytes-to-string input-bytes :start start :end end))
                              (s (sql-string-to-cl (= (char-code #\') (aref input-bytes (1- start))) s)))
                         (push s (first acc)))))))))))

(defun strip-ansi-escape-codes (s)
  (ppcre:regex-replace-all "\\[3\\d(?:;\\d+;\\d+)?m(.+?)\\[0m" s "\\1"))

(define-condition sql-parse-error (error)
  ((message :initarg :message :reader sql-parse-error-message))
  (:report (lambda (condition stream)
             (write (strip-ansi-escape-codes (sql-parse-error-message condition)) :stream stream))))

(defvar *parse-sql-on-success*)

(cffi:defcallback parse-sql-on-success :void
    ((ast (:pointer (:struct Ast))))
  (funcall *parse-sql-on-success* ast))

(defvar *parse-sql-on-error*)

(cffi:defcallback parse-sql-on-error :void
    ((err :string))
  (funcall *parse-sql-on-error* err))

(defun parse-sql (input)
  (endb/lib:init-lib)
  (if (zerop (length input))
      (error 'sql-parse-error :message "Empty input")
      (let* ((ast-builder (make-ast-builder))
             (err)
             (input-bytes (trivial-utf-8:string-to-utf-8-bytes input :null-terminate t))
             (*parse-sql-on-success* (lambda (ast)
                                       (visit-ast input input-bytes ast-builder ast)))
             (*parse-sql-on-error* (lambda (e)
                                     (setf err e))))
        (cffi:with-pointer-to-vector-data (input-ptr #+sbcl (sb-ext:array-storage-vector input-bytes)
                                                     #-sbcl input-bytes)
          (endb-parse-sql input-ptr (cffi:callback parse-sql-on-success) (cffi:callback parse-sql-on-error)))
        (when err
          (error 'sql-parse-error :message err))
        (caar (ast-builder-acc ast-builder)))))

(defvar *annotate-input-with-error-on-success*)

(cffi:defcallback annotate-input-with-error-on-success :void
    ((err :string))
  (funcall *annotate-input-with-error-on-success* err))

(defun annotate-input-with-error (input message start end)
  (endb/lib:init-lib)
  (let* ((result)
         (*annotate-input-with-error-on-success* (lambda (report)
                                                   (setf result report))))
    (endb-annotate-input-with-error input
                                    message
                                    start
                                    end
                                    (cffi:callback annotate-input-with-error-on-success))
    (strip-ansi-escape-codes result)))
