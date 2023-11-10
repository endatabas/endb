(defpackage :endb/lib/cst
  (:use :cl)
  (:export  #:parse-sql-cst #:render-error-report #:cst->ast)
  (:import-from :endb/lib)
  (:import-from :endb/lib/parser)
  (:import-from :endb/json)
  (:import-from :alexandria)
  (:import-from :cffi)
  (:import-from :trivial-utf-8)
  (:import-from :trivia))
(in-package :endb/lib/cst)

(cffi:defcfun "endb_parse_sql_cst" :void
  (filename (:pointer :char))
  (input (:pointer :char))
  (on_open :pointer)
  (on_close :pointer)
  (on_literal :pointer)
  (on_pattern :pointer)
  (on_error :pointer))

(cffi:defcfun "endb_render_json_error_report" :void
  (report_json :string)
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

(defvar *parse-sql-cst-on-literal*)

(cffi:defcallback parse-sql-cst-on-literal :void
    ((literal-ptr :pointer)
     (literal-size :size)
     (start :size)
     (end :size))
  (funcall *parse-sql-cst-on-literal* literal-ptr literal-size start end))

(defvar *parse-sql-cst-on-pattern*)

(cffi:defcallback parse-sql-cst-on-pattern :void
    ((start :size)
     (end :size))
  (funcall *parse-sql-cst-on-pattern* start end))

(defparameter +kw-cache+ (make-hash-table))
(defparameter +literal-cache+ (make-hash-table))

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
             (*parse-sql-cst-on-literal* (lambda (literal-ptr literal-size start end)
                                           (let* ((address (cffi:pointer-address literal-ptr))
                                                  (literal (or (gethash address +literal-cache+)
                                                               (let* ((literal-string (make-array literal-size :element-type 'character)))
                                                                 (dotimes (n literal-size)
                                                                   (setf (aref literal-string n)
                                                                         (code-char (cffi:mem-ref literal-ptr :char n))))
                                                                 (setf (gethash address +literal-cache+) literal-string)))))
                                             (push (list literal start end) (first result)))))
             (*parse-sql-cst-on-pattern* (lambda (start end)
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
                                    (cffi:callback parse-sql-cst-on-literal)
                                    (cffi:callback parse-sql-cst-on-pattern)
                                    (cffi:callback parse-sql-cst-on-error))))
            (cffi:with-foreign-string (filename-ptr input)
              (cffi:with-foreign-string (input-ptr input)
                (endb-parse-sql-cst filename-ptr
                                    input-ptr
                                    (cffi:callback parse-sql-cst-on-open)
                                    (cffi:callback parse-sql-cst-on-close)
                                    (cffi:callback parse-sql-cst-on-literal)
                                    (cffi:callback parse-sql-cst-on-pattern)
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
    (endb-render-json-error-report report-json
                                   (cffi:callback render-json-error-report-on-success)
                                   (cffi:callback render-json-error-report-on-error))
    (endb/lib/parser:strip-ansi-escape-codes result)))

(defun cst->ast (input cst)
  (labels ((strip-delimiters (delimiters xs)
             (remove-if (lambda (x)
                          (trivia:match x
                            ((trivia:guard (list x _ _)
                                           (member x delimiters :test 'equalp))
                             t)))
                        xs))
           (binary-equal-op-tree (acc xs)
             (trivia:ematch xs
               ((list* (list "IMMEDIATELY" _ _) (list "PRECEDES" _ _) x xs)
                (binary-equal-op-tree (list :immediately_precedes acc (walk x)) xs))
               ((list* (list "IMMEDIATELY" _ _) (list "SUCCEEDS" _ _) x xs)
                (binary-equal-op-tree (list :immediately_succeeds acc (walk x)) xs))
               ((list* (list "BETWEEN" _ _) x (list "AND" _ _) y xs)
                (binary-equal-op-tree (list :between acc (walk x) (walk y)) xs))
               ((list* (list "NOT" _ _) (list "BETWEEN" _ _) x (list "AND" _ _) y xs)
                (binary-equal-op-tree (list :not (list :between acc (walk x) (walk y))) xs))
               ((list* (list "LIKE" _ _) x (list "ESCAPE" _ _) y xs)
                (binary-equal-op-tree (list :like acc (walk x) (walk y)) xs))
               ((list* (list "NOT" _ _) (list "LIKE" _ _) x (list "ESCAPE" _ _) y xs)
                (binary-equal-op-tree (list :not (list :like acc (walk x) (walk y))) xs))
               ((list* (list "NOT" _ _) (list "IN" _ _) (and x (list* :|subquery| _)) xs)
                (binary-equal-op-tree (list :not (list :in-query acc (walk x))) xs))
               ((list* (list "IN" _ _) (and x (list* :|subquery| _)) xs)
                (binary-equal-op-tree (list :in-query acc (walk x)) xs))
               ((list* (list "NOT" _ _) (list "IN" _ _) (list :|table_name| x) xs)
                (binary-equal-op-tree (list :not (list :in-query acc (walk x))) xs))
               ((list* (list "IN" _ _) (list :|table_name| x) xs)
                (binary-equal-op-tree (list :in-query acc (walk x)) xs))
               ((list* (list "NOT" _ _) (list "NULL" _ _) xs)
                (binary-equal-op-tree (list :not (list :is acc :null)) xs))
               ((list* (list (and op (type string)) _ _) (list "NOT" _ _) x xs)
                (binary-equal-op-tree (list :not (list (intern op :keyword) acc (walk x))) xs))
               ((list* (list "NOT" _ _) (list (and op (type string)) _ _) x xs)
                (binary-equal-op-tree (list :not (list (intern op :keyword) acc (walk x))) xs))
               ((list* (list "==" _ _) x xs)
                (binary-equal-op-tree (list := acc (walk x)) xs))
               ((list* (list "!=" _ _) x xs)
                (binary-equal-op-tree (list :<> acc (walk x)) xs))
               ((list* (list "@>" _ _) x xs)
                (binary-equal-op-tree (list :match acc (walk x)) xs))
               ((list* (list (and op (type string)) _ _) x xs)
                (binary-equal-op-tree (list (intern op :keyword) acc (walk x)) xs))
               (() acc)))
           (binary-op-tree (acc xs)
             (trivia:ematch xs
               ((list* (list op _ _) x xs)
                (binary-op-tree (list (intern op :keyword) acc (walk x)) xs))
               (() acc)))
           (flatten-join (acc xs)
             (trivia:ematch xs
               ((list* (list* :|join_operator| (list op _ _) _) x (list :|join_constraint| _ expr) xs)
                (flatten-join (list (append (butlast acc)
                                            (list :join (first (last acc)) (walk x)
                                                  :on (walk expr)
                                                  :type (if (equal "LEFT" op)
                                                            :left
                                                            :inner))))
                              xs))
               ((list* (list* :|join_operator| (list "LEFT" _ _) _) x xs)
                (flatten-join (list (append (butlast acc)
                                            (list :join (first (last acc)) (walk x)
                                                  :on :true
                                                  :type :left)))
                              xs))
               ((list* (list* :|join_operator| _) x xs)
                (flatten-join (append acc (list (walk x))) xs))
               ((list x)
                (append acc (list (walk x))))
               (() acc)))
           (build-compound-select-stmt (acc xs)
             (trivia:ematch xs
               ((list* (list :|compound_operator| (list "UNION" _ _) (list "ALL" _ _)) x xs)
                (build-compound-select-stmt (list :union-all acc (walk x)) xs))
               ((list* (list :|compound_operator| (list op _ _)) x xs)
                (build-compound-select-stmt (list (intern op :keyword) acc (walk x)) xs))
               ((list* xs)
                (append acc (mapcan #'walk xs)))))
           (walk (cst)
             (trivia:ematch cst
               ((list :|ident| (list id start end))
                (let ((s (make-symbol id)))
                  (setf (get s :start) start (get s :end) end (get s :input) input)
                  s))

               ((list :|bind_parameter| (list "?" _ _))
                (list :parameter))

               ((list :|bind_parameter| (list parameter start end))
                (list :parameter (let ((s (make-symbol (subseq parameter 1))))
                                   (setf (get s :start) start (get s :end) end (get s :input) input)
                                   s)))

               ((list :|sql_stmt_list| x)
                (walk x))

               ((list* :|sql_stmt_list| xs)
                (list :multiple-statments (mapcar #'walk (strip-delimiters '(";") xs))))

               ((list* :|select_stmt| (and with (list* :|with_clause| _ (list "RECURSIVE" _ _) _)) xs)
                (append (walk with) (list (walk (cons :|select_stmt| xs))) (list :recursive :recursive)))

               ((list* :|select_stmt| (and with (list* :|with_clause| _)) xs)
                (append (walk with) (list (walk (cons :|select_stmt| xs)))))

               ((list* :|select_stmt| x xs)
                (build-compound-select-stmt (walk x) xs))

               ((list* :|create_table_stmt| _ _ table-name xs)
                (list :create-table (walk table-name) (remove nil (mapcar #'walk (strip-delimiters '("(" ")" ",") xs)))))

               ((list* :|column_def| column-name _)
                (walk column-name))

               ((list* :|table_constraint| _))

               ((list* :|create_index_stmt| _ (list "UNIQUE" _ _) _ index-name _ table-name _)
                (list :create-index (walk index-name) (walk table-name)))

               ((list* :|create_index_stmt| _ _ index-name _ table-name _)
                (list :create-index (walk index-name) (walk table-name)))

               ((list :|create_view_stmt| _ _ view-name _ query)
                (list :create-view (walk view-name) (walk query)))

               ((list :|create_view_stmt| _ (or (list "TEMP" _ _) (list "TEMPORARY" _ _)) _ view-name _ query)
                (list :create-view (walk view-name) (walk query)))

               ((list :|create_view_stmt| _ _ view-name column-name-list _ query)
                (list :create-view (walk view-name) (walk query) :column-names (walk column-name-list)))

               ((list :|create_assertion_stmt| _ _ assertion-name _ _ expr _)
                (list :create-assertion (walk assertion-name) (walk expr)))

               ((list :|upsert_clause| _ _ column-name-list _ _)
                (list :on-conflict (walk column-name-list)))

               ((list :|upsert_clause| _ _ column-name-list _ _ update-clause)
                (list :on-conflict (walk column-name-list) :update (walk update-clause)))

               ((list :|insert_stmt| _ _ table-name query (and upsert (list* :|upsert_clause| _)))
                (append (list :insert (walk table-name) (walk query)) (walk upsert)))

               ((list :|insert_stmt| _ (list* :|or_replace| _) _ table-name query (and upsert (list* :|upsert_clause| _)))
                (append (list :insert (walk table-name) (walk query)) (walk upsert)))

               ((list :|insert_stmt| _ _ table-name column-name-list query (and upsert (list* :|upsert_clause| _)))
                (append (list :insert (walk table-name) (walk query) :column-names (walk column-name-list)) (walk upsert)))

               ((list :|insert_stmt| _ _ table-name query)
                (list :insert (walk table-name) (walk query)))

               ((list :|insert_stmt| _ (list* :|or_replace| _) _ table-name query)
                (list :insert (walk table-name) (walk query)))

               ((list :|insert_stmt| _ _ table-name column-name-list query)
                (list :insert (walk table-name) (walk query) :column-names (walk column-name-list)))

               ((list :|delete_stmt| _ _ table-name _ expr)
                (list :delete (walk table-name) :where (walk expr)))

               ((list :|delete_stmt| _ _ table-name)
                (list :delete (walk table-name)))

               ((list :|erase_stmt| _ _ table-name _ expr)
                (list :erase (walk table-name) :where (walk expr)))

               ((list :|erase_stmt| _ _ table-name)
                (list :erase (walk table-name)))

               ((list :|update_stmt| _ table-name update-clause)
                (append (list :update (walk table-name)) (or (walk update-clause) (list nil))))

               ((list :|update_clause|))

               ((list* :|update_clause| (and set (list* :|update_set_clause| _)) xs)
                (mapcan #'walk (cons set xs)))

               ((list* :|update_clause| xs)
                (cons nil (mapcan #'walk xs)))

               ((list :|update_set_assignment| target _ expr)
                (list (walk target) (walk expr)))

               ((list* :|update_set_clause| _ xs)
                (list (mapcar #'walk (strip-delimiters '(",") xs))))

               ((list* :|update_remove_clause| _ xs)
                (cons :unset (list (mapcar #'walk (strip-delimiters '(",") xs)))))

               ((list :|update_patch_clause| _ expr)
                (list :patch (walk expr)))

               ((list :|update_patch_clause| expr)
                (list :patch (walk expr)))

               ((list :|update_where_clause| _ expr)
                (list :where (walk expr)))

               ((list :|drop_table_stmt| _ _ table-name)
                (list :drop-table (walk table-name)))

               ((list :|drop_table_stmt| _ _ _ table-name)
                (list :drop-table (walk table-name) :if-exists :if-exists))

               ((list :|drop_view_stmt| _ _ view-name)
                (list :drop-view (walk view-name)))

               ((list :|drop_view_stmt| _ _ _ view-name)
                (list :drop-view (walk view-name) :if-exists :if-exists))

               ((list :|drop_index_stmt| _ _ index-name)
                (list :drop-index (walk index-name)))

               ((list :|drop_index_stmt| _ _ _ index-name)
                (list :drop-index (walk index-name) :if-exists :if-exists))

               ((list :|drop_assertion_stmt| _ _ assertion-name)
                (list :drop-assertion (walk assertion-name)))

               ((list :|drop_assertion_stmt| _ _ _ assertion-name)
                (list :drop-assertion (walk assertion-name) :if-exists :if-exists))

               ((list* :|column_name_list| xs)
                (mapcar #'walk (strip-delimiters '("(" ")" ",") xs)))

               ((list :|all_distinct| (list "ALL" _ _))
                (list :distinct :all))

               ((list :|all_distinct| (list "DISTINCT" _ _))
                (list :distinct :distinct))

               ((list* :|select_core| (list "SELECT" _ _) (and all-distinct (list :|all_distinct| _)) result-expr-list xs)
                (append (cons :select (walk result-expr-list)) (walk all-distinct) (mapcan #'walk xs)))

               ((list* :|select_core| (list "SELECT" _ _) xs)
                (cons :select (mapcan #'walk xs)))

               ((list* :|values_clause| _ xs)
                (cons :values (list (mapcar #'walk (strip-delimiters '(",") xs)))))

               ((list* :|objects_clause| (list "OBJECTS" _ _) xs)
                (cons :objects (list (mapcar #'walk (strip-delimiters '(",") xs)))))

               ((list* :|objects_clause| xs)
                (cons :objects (list (mapcar #'walk (strip-delimiters '(",") xs)))))

               ((list* :|with_clause| _ (list "RECURSIVE" _ _) xs)
                (list :with (mapcar #'walk (strip-delimiters '(",") xs))))

               ((list* :|with_clause| _ xs)
                (list :with (mapcar #'walk (strip-delimiters '(",") xs))))

               ((list :|common_table_expression| table-name _ subquery)
                (list (walk table-name) (walk subquery)))

               ((list :|common_table_expression| table-name column-name-list _ subquery)
                (list (walk table-name) (walk subquery) (walk column-name-list)))

               ((list* :|result_expr_list| xs)
                (list (mapcar #'walk (strip-delimiters '(",") xs))))

               ((list :|result_column| (list :|star| _))
                (list :*))

               ((list :|result_column| (list :|qualified_star| table-name _  _))
                (list (list :* (walk table-name))))

               ((list :|result_column| expr)
                (list (walk expr)))

               ((list :|result_column| expr alias)
                (list (walk expr) (walk alias)))

               ((list :|result_column| expr _ alias)
                (list (walk expr) (walk alias)))

               ((list :|from_clause| _ join-clause)
                (list :from (walk join-clause)))

               ((list* :|join_clause| x xs)
                (flatten-join (list (walk x)) xs))

               ((list :|table_name| (list "INFORMATION_SCHEMA" start _) _ table-name)
                (let* ((table-name (walk table-name))
                       (s (make-symbol (concatenate 'string "information_schema." (symbol-name table-name)))))
                  (setf (get s :start) start (get s :end) (get table-name :end) (get s :input) input)
                  s))

               ((list :|system_time_clause| _ _ (list "ALL" _ _))
                (list (list :all)))

               ((list :|system_time_clause| _ _ _ from (list "TO" _ _) to)
                (list (list :from (walk from) (walk to))))

               ((list :|system_time_clause| _ _ _ from (list "AND" _ _) to)
                (list (list :between (walk from) (walk to))))

               ((list :|system_time_clause| _ _ _ _ as-of)
                (list (list :as-of (walk as-of))))

               ((list :|unnest_table_function| _ paren-expr-list)
                (list :unnest (walk paren-expr-list)))

               ((list :|unnest_table_function| _ paren-expr-list (list* :|with_ordinality| _))
                (list :unnest (walk paren-expr-list) :with-ordinality :with-ordinality))

               ((list :|table_or_subquery| (and unnest (list* :|unnest_table_function| _)) alias)
                (cons (walk unnest) (walk alias)))

               ((list :|table_or_subquery| (and unnest (list* :|unnest_table_function| _)) _ alias)
                (cons (walk unnest) (walk alias)))

               ((list :|table_or_subquery| (list "(" _ _) join-clause (list ")" _ _))
                (append (cons :join (walk join-clause)) (list :on :true :type :inner)))

               ((list :|table_or_subquery| table-name (and sys-time (list* :|system_time_clause| _)))
                (cons (walk table-name) (append (list (walk table-name) nil) (walk sys-time))))

               ((list :|table_or_subquery| table-name (and sys-time (list* :|system_time_clause| _)) alias)
                (cons (walk table-name) (append (walk alias) (walk sys-time))))

               ((list :|table_or_subquery| table-name  (and sys-time (list* :|system_time_clause| _)) _ alias)
                (cons (walk table-name) (append (walk alias) (walk sys-time))))

               ((list :|table_or_subquery| table-name (list* :|not_indexed| _))
                (list (walk table-name)))

               ((list :|table_or_subquery| table-name)
                (list (walk table-name)))

               ((list :|table_or_subquery| table-name alias)
                (cons (walk table-name) (walk alias)))

               ((list :|table_or_subquery| table-name _ alias)
                (cons (walk table-name) (walk alias)))

               ((list :|table_alias| alias)
                (list (walk alias)))

               ((list :|table_alias| alias column-name-list)
                (list (walk alias) (walk column-name-list)))

               ((list :|where_clause| _ expr)
                (list :where (walk expr)))

               ((list* :|group_by_clause| _ _ xs)
                (list :group-by (mapcan #'walk xs)))

               ((list :|having_clause| _ expr)
                (list :having (walk expr)))

               ((list* :|order_by_clause| _ _ xs)
                (list :order-by (mapcar #'walk (strip-delimiters '(",") xs))))

               ((list :|limit_offset_clause| _ limit)
                (list :limit (walk limit)))

               ((list :|limit_offset_clause| _ limit _ offset)
                (list :limit (walk limit) :offset (walk offset)))

               ((list :|ordering_term| expr (list dir _ _))
                (list (walk expr) (intern dir :keyword)))

               ((list :|ordering_term| expr)
                (list (walk expr) :asc))

               ((list :|column_reference| table-name _ column-name)
                (let* ((table-name (walk table-name))
                       (column-name (walk column-name))
                       (s (make-symbol (concatenate 'string (symbol-name table-name) "." (symbol-name column-name)))))
                  (setf (get s :start) (get table-name :start) (get s :end) (get column-name :end) (get s :input) input)
                  s))

               ((list :|property_bracket_wildcard_access| _ _ _)
                (list :*))

               ((list :|property_bracket_access| _ expr _)
                (list (walk expr)))

               ((list :|property_recursive_field_access| _ expr)
                (list (walk expr) :recursive :recursive))

               ((list :|property_recursive_bracket_access| _ expr)
                (append (walk expr) (list :recursive :recursive)))

               ((list :|property_field_access| _ expr)
                (list (walk expr)))

               ((list* :|access_expr| expr xs)
                (reduce
                 (lambda (acc x)
                   (append (list :access acc) x))
                 (mapcar #'walk xs)
                 :initial-value (walk expr)))

               ((list* :|unary_expr| (list (and op (type string)) _ _) xs)
                (list (intern op :keyword) (walk (cons :|unary_expr| xs))))

               ((list* :|concat_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|mul_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|add_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|bit_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|rel_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|equal_expr| x xs)
                (binary-equal-op-tree (walk x) xs))

               ((list* :|not_expr| (list "NOT" _ _) xs)
                (list :not (walk (cons :|not_expr| xs))))

               ((list* :|and_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list* :|or_expr| x xs)
                (binary-op-tree (walk x) xs))

               ((list :|filter_clause| _ _ _ expr _)
                (list :where (walk expr)))

               ((list :|simple_function_invocation| simple-func _ _)
                (list :function (walk simple-func) nil))

               ((list :|simple_function_invocation| simple-func _ expr-list _)
                (list :function (walk simple-func) (walk expr-list)))

               ((list :|aggregate_func| (list fn _ _))
                (intern (string-upcase fn) :keyword))

               ((list* :|aggregate_function_invocation| aggregate-func _ (and all-distinct (list :|all_distinct| _)) (list :|star| _) xs)
                (append (list :aggregate-function (intern (string-upcase (concatenate 'string (symbol-name (walk aggregate-func)) "-star")) :keyword))
                        (list nil)
                        (mapcan #'walk (strip-delimiters '(")") xs))
                        (walk all-distinct)))

               ((list* :|aggregate_function_invocation| aggregate-func _ (list :|star| _) xs)
                (append (list :aggregate-function (intern (string-upcase (concatenate 'string (symbol-name (walk aggregate-func)) "-star")) :keyword))
                        (list nil)
                        (mapcan #'walk (strip-delimiters '(")") xs))))

               ((list* :|aggregate_function_invocation| aggregate-func _ (and all-distinct (list :|all_distinct| _)) expr-list xs)
                (append (list :aggregate-function (walk aggregate-func) (walk expr-list))
                        (mapcan #'walk (strip-delimiters '(")") xs))
                        (walk all-distinct)))

               ((list* :|aggregate_function_invocation| aggregate-func _ expr-list xs)
                (append (list :aggregate-function (walk aggregate-func) (walk expr-list))
                        (mapcan #'walk (strip-delimiters '(")") xs))))

               ((list* :|case_expr| _ (list :|case_operand| case-operand) xs)
                (cons :case (cons (walk case-operand) (list (mapcar #'walk (strip-delimiters '("END") xs))))))

               ((list* :|case_expr| _ xs)
                (cons :case (list (mapcar #'walk (strip-delimiters '("END") xs)))))

               ((list :|case_when_then_expr| _ when-expr _ then-expr)
                (list (walk when-expr) (walk then-expr)))

               ((list :|case_else_expr| _ else-expr)
                (list :else (walk else-expr)))

               ((list :|paren_expr| _ expr _)
                (walk expr))

               ((list :|exists_expr| _ query)
                (list :exists (walk query)))

               ((list :|extract_expr| _ _ field _ expr _)
                (list :extract (walk field) (walk expr)))

               ((list :|cast_expr| _ _ expr _ type _)
                (list :cast (walk expr) (walk type)))

               ((list :|atom| (list :|subquery| _ query _))
                (list :scalar-subquery (walk query)))

               ((list :|subquery| _ query _)
                (walk query))

               ((list* :|expr_list| xs)
                (mapcar #'walk (strip-delimiters '(",") xs)))

               ((list :|paren_expr_list| _ expr-list _)
                (walk expr-list))

               ((list :|empty_list| _ _))

               ((list :|numeric_literal| (trivia:guard (list x _ _)
                                                       (or (alexandria:starts-with-subseq "0x" x)
                                                           (alexandria:starts-with-subseq "0X" x))))
                (parse-integer x :start 2 :radix 16))

               ((list :|numeric_literal| (list x _ _))
                (read-from-string x))

               ((list :|string_literal| (list x _ _))
                (endb/lib/parser:sql-string-to-cl (eql #\' (char x 0)) (subseq x 1 (1- (length x)))))

               ((list :|blob_literal| (list x _ _))
                (list :blob (subseq x 2 (1- (length x)))))

               ((list :|iso_date_literal| (list x _ _))
                (list :date x))

               ((list :|date_literal| _ (list x _ _))
                (list :date (subseq x 1 (1- (length x)))))

               ((list :|iso_time_literal| (list x _ _))
                (list :time x))

               ((list :|time_literal| _ (list x _ _))
                (list :time (subseq x 1 (1- (length x)))))

               ((list :|iso_timestamp_literal| (list x _ _))
                (list :timestamp x))

               ((list :|timestamp_literal| _ (list x _ _))
                (list :timestamp (subseq x 1 (1- (length x)))))

               ((list :|iso_duration_literal| (list x _ _))
                (list :duration x))

               ((list :|interval_literal| _ (list x _ _) from)
                (list :interval (subseq x 1 (1- (length x))) (walk from)))

               ((list :|interval_literal| _ (list x _ _) from _ to)
                (list :interval (subseq x 1 (1- (length x))) (walk from) (walk to)))

               ((list :|datetime_field| (list x _ _))
                (intern x :keyword))

               ((list :|object_expr| (list "OBJECT" _ _) _ object-key-value-list _)
                (list :object (walk object-key-value-list)))

               ((list :|object_expr| (list "OBJECT" _ _) _ _)
                (list :object nil))

               ((list :|object_expr| _ object-key-value-list _)
                (list :object (walk object-key-value-list)))

               ((list :|object_expr| _ _)
                (list :object nil))

               ((list* :|object_key_value_list| xs)
                (mapcar #'walk (strip-delimiters '(",") xs)))

               ((list :|object_key_value_pair| (list :|spread_expr| _ expr))
                (list :spread-property (walk expr)))

               ((list :|object_key_value_pair| (list :|object_key| (list :|computed_property| _ key _)) _ value)
                (list :computed-property (walk key) (walk value)))

               ((list :|object_key_value_pair| key _ value)
                (list (walk key) (walk value)))

               ((list :|object_key_value_pair| (list :|qualified_star| table-name _ _))
                (list :* (walk table-name)))

               ((list :|object_key_value_pair| expr)
                (list :shorthand-property (walk expr)))

               ((list :|array_element| (list :|spread_expr| _ expr))
                (list :spread-property (walk expr)))

               ((list :|array_expr| (list "ARRAY" _ _) (list :|subquery| _ query _))
                (list :array-query (walk query)))

               ((list* :|array_expr| (list "ARRAY" _ _) xs)
                (list :array (mapcar #'walk (strip-delimiters '("[" "]" ",") xs))))

               ((list* :|array_expr| xs)
                (list :array (mapcar #'walk (strip-delimiters '("[" "]" ",") xs))))

               ((list :|path_array_index| (list :|path_array_length| _) (list "-" _ _) expr)
                (list :- (walk expr)))

               ((list :|path_array_index| (list :|path_array_length| _))
                :#)

               ((list :|path_array_access| _ expr _)
                (walk expr))

               ((list :|path_object_label| _ property)
                (walk property))

               ((list* :|path_expr| _ xs)
                (list :path (mapcar #'walk xs)))

               ((list "NULL" _ _)
                :null)

               ((list "TRUE" _ _)
                :true)

               ((list "FALSE" _ _)
                :false)

               ((list "CURRENT_TIMESTAMP" _ _)
                :current_timestamp)

               ((list "CURRENT_TIME" _ _)
                :current_time)

               ((list "CURRENT_DATE" _ _)
                :current_date)

               ((list (type keyword) x)
                (walk x)))))
    (let ((*read-eval* nil)
          (*read-default-float-format* 'double-float))
      (walk cst))))
