(defpackage :endb/lib/cst
  (:use :cl)
  (:export  #:parse-sql-cst #:render-error-report #:cst->ast)
  (:import-from :endb/lib)
  (:import-from :endb/lib/parser)
  (:import-from :endb/json)
  (:import-from :cffi)
  (:import-from :trivial-utf-8)
  (:import-from :trivia))
(in-package :endb/lib/cst)

(cffi:defcfun "endb_parse_sql_cst" :void
  (filename (:pointer :char))
  (input (:pointer :char))
  (on_open :pointer)
  (on_close :pointer)
  (on_token :pointer)
  (on_error :pointer))

(cffi:defcfun "endb_render_json_error_report" :void
  (report_json (:pointer :char))
  (on_success :pointer)
  (on_error :pointer))

(cffi:defcallback parse-sql-cst-on-error :void
    ((err :string))
  (error 'endb/lib/parser:sql-parse-error :message err))

(defvar *parse-sql-cst-on-open*)

(cffi:defcallback parse-sql-cst-on-open :void
    ((label-ptr :pointer)
     (label-size :size))
  (funcall *parse-sql-cst-on-open* label-ptr label-size))

(defvar *parse-sql-cst-on-close*)

(cffi:defcallback parse-sql-cst-on-close :void
    ()
  (funcall *parse-sql-cst-on-close*))

(defvar *parse-sql-cst-on-token*)

(cffi:defcallback parse-sql-cst-on-token :void
    ((start :size)
     (end :size))
  (funcall *parse-sql-cst-on-token* start end))

(defparameter +kw-cache+ (make-hash-table))

(defun parse-sql-cst (input &key (filename ""))
  (endb/lib:init-lib)
  (if (zerop (length input))
      (error 'endb/lib/parser:sql-parse-error :message "Empty input")
      (let* ((result (list (list)))
             (input-bytes (trivial-utf-8:string-to-utf-8-bytes input))
             (*parse-sql-cst-on-open* (lambda (label-ptr label-size)
                                        (let* ((address (cffi:pointer-address label-ptr))
                                               (kw (or (gethash address +kw-cache+)
                                                       (let* ((kw-string (make-array label-size :element-type 'character)))
                                                         (dotimes (n label-size)
                                                           (setf (aref kw-string n)
                                                                 (code-char (cffi:mem-ref label-ptr :char n))))
                                                         (setf (gethash address +kw-cache+)
                                                               (intern kw-string :keyword))))))
                                          (push (list kw) result))))
             (*parse-sql-cst-on-close* (lambda ()
                                         (push (nreverse (pop result)) (first result))))
             (*parse-sql-cst-on-token* (lambda (start end)
                                         (let ((token (trivial-utf-8:utf-8-bytes-to-string input-bytes :start start :end end)))
                                           (push (list token start end) (first result))))))
        (if (and (typep filename 'base-string)
                 (typep input 'base-string))
            (cffi:with-pointer-to-vector-data (filename-ptr input)
              (cffi:with-pointer-to-vector-data (input-ptr input)
                (endb-parse-sql-cst filename-ptr
                                    input-ptr
                                    (cffi:callback parse-sql-cst-on-open)
                                    (cffi:callback parse-sql-cst-on-close)
                                    (cffi:callback parse-sql-cst-on-token)
                                    (cffi:callback parse-sql-cst-on-error))))
            (cffi:with-foreign-string (filename-ptr input)
              (cffi:with-foreign-string (input-ptr input)
                (endb-parse-sql-cst filename-ptr
                                    input-ptr
                                    (cffi:callback parse-sql-cst-on-open)
                                    (cffi:callback parse-sql-cst-on-close)
                                    (cffi:callback parse-sql-cst-on-token)
                                    (cffi:callback parse-sql-cst-on-error)))))
        (caar result))))

(defvar *render-json-error-report-on-success*)

(cffi:defcallback render-json-error-report-on-success :void
    ((report :string))
  (funcall *render-json-error-report-on-success* report))

(cffi:defcallback render-json-error-report-on-error :void
    ((err :string))
  (error err))

(defun render-error-report (report)
  (endb/lib:init-lib)
  (let* ((result)
         (*render-json-error-report-on-success* (lambda (report)
                                                  (setf result report)))
         (report-json (endb/json:json-stringify report)))
    (cffi:with-foreign-string (report-json-ptr report-json)
      (endb-render-json-error-report report-json-ptr
                                     (cffi:callback render-json-error-report-on-success)
                                     (cffi:callback render-json-error-report-on-error)))
    (endb/lib/parser:strip-ansi-escape-codes result)))

(defun cst->ast (input cst)
  (labels ((strip-delimiters (delimiter xs)
             (remove-if (lambda (x)
                          (and (listp x) (= 3 (length x)) (equal delimiter (first x))))
                        xs))
           (binary-op-tree (xs)
             (first
              (reduce
               (lambda (acc x)
                 (let ((acc (trivia:match x
                              ((trivia:guard (list op _ _)
                                             (stringp op))
                               (cons (intern op :keyword) acc))
                              (_ (cons (walk x) acc)))))
                   (trivia:match acc
                     ((list y op x)
                      (list (list op x y)))
                     (_ acc))))
               xs
               :initial-value ())))
           (walk (cst)
             (trivia:ematch cst
               ((list :|ident| (list id start end))
                (let ((s (make-symbol id)))
                  (setf (get s :start) start (get s :end) end (get s :input) input)
                  s))

               ((list :|sql_stmt_list| x)
                (first (walk x)))

               ((list* :|sql_stmt_list| xs)
                (mapcar #'walk (strip-delimiters ";" xs)))

               ((list :|sql_stmt| x)
                (walk x))

               ((list* :|select_stmt| xs)
                (list (mapcan #'walk xs)))

               ((list* :|select_core| _ xs)
                (cons :select (mapcan #'walk xs)))

               ((list* :|result_expr_list| xs)
                (list (mapcar #'walk (strip-delimiters "," xs))))

               ((list :|result_column| x)
                (list (walk x)))

               ((list* :|from_clause| _ xs)
                (cons :from (mapcar #'walk xs)))

               ((list* :|join_clause| xs)
                (mapcar #'walk xs))

               ((list :|table_or_subquery| x)
                (list (walk x)))

               ((list :|table_name| x)
                (walk x))

               ((list :|where_clause| _ expr)
                (list :where (walk expr)))

               ((list* :|order_by_clause| _ _ xs)
                (list :order-by (mapcar #'walk (strip-delimiters "," xs))))

               ((list :|ordering_term| x (list dir _ _))
                (list (walk x) (intern dir :keyword)))

               ((list :|ordering_term| x)
                (list (walk x) :asc))

               ((list :|literal| x)
                (walk x))

               ((list :|atom| x)
                (walk x))

               ((list :|access_expr| x)
                (walk x))

               ((list :|unary_expr| x)
                (walk x))

               ((list* :|unary_expr| (list op _ _) x)
                (list (intern op :keyword) (walk (list :|unary_expr| x))))

               ((list* :|concat_expr| xs)
                (binary-op-tree xs))

               ((list* :|mul_expr| xs)
                (binary-op-tree xs))

               ((list* :|add_expr| xs)
                (binary-op-tree xs))

               ((list* :|bit_expr| xs)
                (binary-op-tree xs))

               ((list* :|rel_expr| xs)
                (binary-op-tree xs))

               ((list* :|equal_expr| xs)
                (binary-op-tree xs))

               ((list :|not_expr| x)
                (walk x))

               ((list* :|not_expr| (list op _ _) x)
                (list (intern op :keyword)  (walk (list :|not_expr| x))))

               ((list* :|and_expr| xs)
                (binary-op-tree xs))

               ((list* :|or_expr| xs)
                (binary-op-tree xs))

               ((list :|expr| x)
                (walk x))

               ((list* :|function_call_expr| xs)
                (cons :function (mapcar #'walk (strip-delimiters ")" (strip-delimiters "(" xs)))))

               ((list* :|expr_list| xs)
                (mapcar #'walk xs))

               ((list :|column_reference| x)
                (walk x))

               ((list :|column_name| x)
                (walk x))

               ((list :|function_name| x)
                (walk x))

               ((list :|numeric_literal| (list x _ _))
                (read-from-string x)))))
    (let ((*read-eval* nil)
          (*read-default-float-format* 'double-float))
      (walk cst))))
