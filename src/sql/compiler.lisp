(defpackage :endb/sql/compiler
  (:use :cl)
  (:export #:compile-sql)
  (:import-from :endb/sql/db)
  (:import-from :endb/sql/expr)
  (:import-from :endb/lib/cst)
  (:import-from :endb/lib)
  (:import-from :endb/arrow)
  (:import-from :endb/bloom)
  (:import-from :alexandria)
  (:import-from :fset)
  (:import-from :trivia))
(in-package :endb/sql/compiler)

(defgeneric sql->cl (ctx type &rest args))

(defun %anonymous-column-name (idx)
  (concatenate 'string "column" (princ-to-string idx)))

(defun %unqualified-column-name (column)
  (let* ((idx (position #\. column)))
    (if idx
        (subseq column (1+ idx))
        column)))

(defun %qualified-column-name (table-alias column)
  (concatenate 'string table-alias "." column))

(defun %qualified-asterisk-p (x)
  (and (listp x)
       (= 2 (length x))
       (eq :* (first x))
       (symbolp (second x))))

(defun %select-projection (ctx select-list select-star-projection table-projections)
  (loop for idx from 1
        for (expr alias) in select-list
        append (cond
                 ((eq :* expr) select-star-projection)
                 (alias (list (symbol-name alias)))
                 ((symbolp expr) (list (%unqualified-column-name (symbol-name expr))))
                 ((%qualified-asterisk-p expr)
                  (let ((projection (fset:lookup table-projections (symbol-name (second expr)))))
                    (if projection
                        (mapcar #'%unqualified-column-name projection)
                        (%annotated-error (fset:lookup ctx :sql) (second expr) (format nil "Unknown table: ~A" (second expr)) "Unknown table"))))
                 ((and (listp expr)
                       (eq :parameter (first expr))
                       (symbolp (second expr)))
                  (list (%unqualified-column-name (symbol-name (second expr)))))
                 ((and (listp expr)
                       (eq :object (first expr))
                       (= 1 (length (second expr)))
                       (eq :* (first (first (second expr)))))
                  (list (%unqualified-column-name (symbol-name (second (first (second expr)))))))
                 ((and (listp expr)
                       (eq :access (first expr))
                       (not (member (nth 2 expr) '(:* :#)))
                       (or (stringp (nth 2 expr))
                           (symbolp (nth 2 expr)))) (list (format nil "~A" (nth 2 expr))))
                 (t (list (%anonymous-column-name idx))))))

(defun %annotated-error-with-span (input message label-message start end)
  (let* ((filename endb/lib/cst:*default-filename*)
         (report (fset:map ("kind" "Error")
                           ("msg" message)
                           ("location" (fset:seq filename start))
                           ("source" input)
                           ("labels" (fset:seq (fset:map ("span" (fset:seq filename (fset:map ("start" start) ("end" end))))
                                                         ("msg" label-message)
                                                         ("color" "Red")
                                                         ("order" 0)
                                                         ("priority" 0)))))))

    (error 'endb/sql/expr:sql-runtime-error
           :message (endb/lib/cst:render-error-report report))))

(defun %annotated-error (input s message &optional (label-message message))
  (%annotated-error-with-span input message label-message (get s :start) (get s :end)))

(defun %base-table-or-view->cl (ctx table-name &key temporal (errorp t))
  (let* ((db (fset:lookup ctx :db))
         (ctes (or (fset:lookup ctx :ctes) (fset:empty-map)))
         (cte (fset:lookup ctes (symbol-name table-name)))
         (table-type (endb/sql/db:table-type db (symbol-name table-name))))
    (cond
      (cte
       (progn
         (dolist (cb (fset:lookup ctx :on-cte-access))
           (funcall cb (symbol-name table-name)))
         (values `(,(intern (symbol-name table-name))) (cte-projection cte) (cte-free-vars cte))))
      ((equal "BASE TABLE" table-type)
       (values (make-base-table :name (symbol-name table-name)
                                :temporal temporal
                                :size (endb/sql/db:base-table-size db (symbol-name table-name)))
               (endb/sql/db:table-columns db (symbol-name table-name))))
      ((equal "VIEW" table-type)
       (let ((view-sql (endb/sql/db:view-definition db (symbol-name table-name))))
         (multiple-value-bind (src projection free-vars)
             (%ast->cl-with-free-vars ctx (endb/lib/cst:parse-sql-ast view-sql))
           (declare (ignore projection))
           (values src (endb/sql/db:table-columns db (symbol-name table-name)) free-vars))))
      (errorp (%annotated-error (fset:lookup ctx :sql) table-name (format nil "Unknown table: ~A" table-name) "Unknown table")))))

(defun %wrap-with-order-by-and-limit (src order-by limit offset)
  (let* ((src (if order-by
                  `(endb/sql/expr:ra-order-by ,src ',order-by)
                  src))
         (src (if (and order-by limit)
                  `(endb/sql/expr:ra-limit ,src ',limit ',offset)
                  src)))
    src))

(defun %resolve-order-by (ctx order-by projection &key allow-expr-p)
  (loop with expr-idx = (length projection)
        for ordering-term in order-by
        for span = (nthcdr 2 ordering-term)
        for start = (getf span :start)
        for end = (getf span :end)
        for (expr direction) = ordering-term
        for projected-idx = (when (symbolp expr)
                              (position (symbol-name expr) projection :test 'equal))
        collect (list (cond
                        ((numberp expr)
                         (if (<= 1 expr (length projection))
                             expr
                             (%annotated-error-with-span (fset:lookup ctx :sql)
                                                         (format nil "ORDER BY index not in range: ~A" expr)
                                                         "ORDER BY index not in range"
                                                         start
                                                         end)))
                        (projected-idx (1+ projected-idx))
                        (allow-expr-p (incf expr-idx))
                        (t (if (symbolp expr)
                               (%annotated-error (fset:lookup ctx :sql) expr (format nil "Cannot resolve ORDER BY column: ~A" expr) "Cannot resolve ORDER BY column")
                               (%annotated-error-with-span (fset:lookup ctx :sql)
                                                           "Invalid ORDER BY expression"
                                                           "Invalid ORDER BY expression"
                                                           start
                                                           end))))
                      (or direction :asc))))

(defun %and-clauses (expr)
  (if (listp expr)
      (case (first expr)
        (:and (append (%and-clauses (second expr))
                      (%and-clauses (third expr))))
        (:or (append (list expr)
                     (intersection (%and-clauses (second expr))
                                   (%and-clauses (third expr))
                                   :key #'prin1-to-string
                                   :test 'equal)))
        (t (list expr)))
      (list expr)))

(defstruct base-table name temporal size)

(defstruct from-table src vars free-vars size projection alias)

(defstruct where-clause src free-vars ast)

(defstruct aggregate src init-src var where-src)

(defstruct cte src free-vars projection ast)

(defun %binary-predicate-p (x)
  (and (listp x)
       (= 3 (length x))))

(defun %equi-join-predicate-p (vars clause-src)
  (when (%binary-predicate-p clause-src)
    (destructuring-bind (op lhs rhs)
        clause-src
      (and (equal 'endb/sql/expr:sql-= op)
           (member lhs vars)
           (member rhs vars)))))

(defun %equi-join-participant-p (vars clause-src)
  (when (%binary-predicate-p clause-src)
    (destructuring-bind (op lhs rhs)
        clause-src
      (and (equal 'endb/sql/expr:sql-= op)
           (or (and (member lhs vars) (symbolp rhs))
               (and (member rhs vars) (symbolp lhs)))))))

(defun %maybe-equi-join-p (clause-src)
  (when (%binary-predicate-p clause-src)
    (destructuring-bind (op lhs rhs)
        clause-src
      (and (equal 'endb/sql/expr:sql-= op)
           (and (symbolp rhs)
                (symbolp lhs))))))

(defun %unique-vars (vars)
  (let ((seen ()))
    (loop for x in (reverse vars)
          if (not (member x seen))
            do (push x seen)
          else
            do (push nil seen)
          finally (return seen))))

(defun %table-alias (table-ast)
  (if (= 1 (length table-ast))
      (first table-ast)
      (second table-ast)))

(defun %using-to-on (table-1 table-2 using)
  (labels ((make-column-reference (table-name column-name)
             (let ((col-ref (make-symbol (concatenate 'string (symbol-name table-name) "." (symbol-name column-name)))))
               (setf (get col-ref :start) (get column-name :start) (get col-ref :end) (get column-name :end))
               col-ref)))
    (let* ((table-1-alias (%table-alias table-1))
           (table-2-alias (%table-alias table-2)))
      (reduce
       (lambda (acc column)
         (let ((expr (list := (make-column-reference table-1-alias column) (make-column-reference table-2-alias column))))
           (if acc
               (list :and acc expr)
               expr)))
       using
       :initial-value nil))))

(defun %flatten-from (from)
  (let ((from-element (first from)))
    (when from-element
      (append (if (eq :join (first from-element))
                  (destructuring-bind (table-1 table-2 &key on using type)
                      (rest from-element)
                    (if (eq :left type)
                        (let ((table-alias (%table-alias table-2)))
                          (append (%flatten-from (list table-1)) (list (list (list :left-join
                                                                                   table-2
                                                                                   (if using
                                                                                       (%using-to-on table-1 table-2 using)
                                                                                       on))
                                                                             table-alias))))
                        (%flatten-from (list table-1 table-2))))
                  (list from-element))
              (%flatten-from (rest from))))))

(defun %from-where-clauses (from)
  (let ((from-element (first from)))
    (when from-element
      (append
       (when (eq :join (first from-element))
         (destructuring-bind (table-1 table-2 &key on using type)
             (rest from-element)
           (if (eq :left type)
               (%from-where-clauses (list table-1))
               (append (if using
                           (%and-clauses (%using-to-on table-1 table-2 using))
                           (%and-clauses on))
                       (%from-where-clauses (list table-1 table-2))))))
       (%from-where-clauses (rest from))))))

(defun %replace-all (smap x)
  (cond
    ((symbolp x) (or (cdr (assoc x smap)) x))
    ((listp x) (cons (%replace-all smap (car x))
                     (%replace-all smap (cdr x))))
    (t x)))

(defun %numeric-bloom-check-src (x y stats-md-sym)
  (let* ((hashes (endb/sql/expr:ra-bloom-hashes y)))
    (if (= 1 (length hashes))
        `(endb/bloom:sbbf-check-p
          (fset:lookup
           (fset:lookup ,stats-md-sym ,(get x :column))
           "bloom")
          ,(first hashes))
        (alexandria:with-gensyms (bloom-sym lambda-sym)
          `(let ((,bloom-sym (fset:lookup
                              (fset:lookup ,stats-md-sym ,(get x :column))
                              "bloom")))
             (some (lambda (,lambda-sym)
                     (endb/bloom:sbbf-check-p ,bloom-sym ,lambda-sym))
                   ',hashes))))))

(defun %parameter-lookup-p (src param-sym)
  (and (listp src)
       (eq 'fset:lookup (first src))
       (eq param-sym (second src))))

(defun %hash-index-column-and-src (clause vars param-sym)
  (labels ((free-var-or-constant-p (y)
             (or (and (symbolp y)
                      (not (member y vars)))
                 (constantp y)
                 (%parameter-lookup-p y param-sym))))
    (trivia:match (where-clause-src clause)
      ((trivia:guard
         (list (or 'endb/sql/expr:sql-=
                   'endb/sql/expr:sql-is)
               x y)
         (and (member x vars) (free-var-or-constant-p y)))
       (values (get x :column) y))

      ((trivia:guard
         (list (or 'endb/sql/expr:sql-=
                   'endb/sql/expr:sql-is)
               y x)
         (and (member x vars) (free-var-or-constant-p y)))
       (values (get x :column) y)))))

(defun %where-clause-stats-src (clause vars stats-md-sym param-sym)
  (labels ((free-var-or-constant-p (y)
             (or (and (symbolp y)
                      (not (member y vars)))
                 (constantp y)
                 (%parameter-lookup-p y param-sym))))
    (trivia:match (where-clause-src clause)
      ((trivia:guard
        (list 'endb/sql/expr:sql-= x y)
        (and (member x vars) (numberp y)))
       (%numeric-bloom-check-src x y stats-md-sym))

      ((trivia:guard
        (list 'endb/sql/expr:sql-= y x)
        (and (member x vars) (numberp y)))
       (%numeric-bloom-check-src x y stats-md-sym))

      ((trivia:guard
        (list (or 'endb/sql/expr:sql-=
                  'endb/sql/expr:sql-is)
              x y)
        (and (member x vars) (free-var-or-constant-p y)))
       (alexandria:with-gensyms (bloom-sym lambda-sym)
         `(let ((,bloom-sym (fset:lookup
                             (fset:lookup ,stats-md-sym ,(get x :column))
                             "bloom")))
            (some (lambda (,lambda-sym)
                    (endb/bloom:sbbf-check-p ,bloom-sym ,lambda-sym))
                  ,(if (constantp y)
                       `',(endb/sql/expr:ra-bloom-hashes y)
                       `(endb/sql/expr:ra-bloom-hashes ,y))))))

      ((trivia:guard
        (list (or 'endb/sql/expr:sql-=
                  'endb/sql/expr:sql-is)
              y x)
        (and (member x vars) (free-var-or-constant-p y)))
       (alexandria:with-gensyms (bloom-sym lambda-sym)
         `(let ((,bloom-sym (fset:lookup
                             (fset:lookup ,stats-md-sym ,(get x :column))
                             "bloom")))
            (some (lambda (,lambda-sym)
                    (endb/bloom:sbbf-check-p ,bloom-sym ,lambda-sym))
                  ,(if (constantp y)
                       `',(endb/sql/expr:ra-bloom-hashes y)
                       `(endb/sql/expr:ra-bloom-hashes ,y))))))

      ((trivia:guard
        (list 'endb/sql/expr:ra-in x y)
        (and (member x vars) (listp y) (every #'free-var-or-constant-p y)))
       (alexandria:with-gensyms (bloom-sym lambda-sym)
         `(let ((,bloom-sym (fset:lookup
                             (fset:lookup ,stats-md-sym ,(get x :column))
                             "bloom")))
            (some (lambda (,lambda-sym)
                    (endb/bloom:sbbf-check-p ,bloom-sym ,lambda-sym))
                  (append ,@(loop for v in (if (eq 'list (first y))
                                               (rest y)
                                               y)
                                  collect (if (constantp v)
                                              `',(endb/sql/expr:ra-bloom-hashes v)
                                              `(endb/sql/expr:ra-bloom-hashes ,v))))))))

      ((trivia:guard
        (list 'endb/sql/expr:sql-between x y z)
        (and (member x vars) (free-var-or-constant-p y) (free-var-or-constant-p z)))
       `(and (eq t (endb/sql/expr:sql->=
                    (fset:lookup
                     (fset:lookup ,stats-md-sym ,(get x :column))
                     "max")
                    ,y))
             (eq t (endb/sql/expr:sql-<=
                    (fset:lookup
                     (fset:lookup ,stats-md-sym ,(get x :column))
                     "min")
                    ,z))))

      ((trivia:guard
        (list 'endb/sql/expr:sql-< x y)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql-<
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "min")
               ,y)))

      ((trivia:guard
        (list 'endb/sql/expr:sql-<= x y)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql-<=
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "min")
               ,y)))

      ((trivia:guard
        (list 'endb/sql/expr:sql-< y x)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql-<
               ,y
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "max"))))

      ((trivia:guard
        (list 'endb/sql/expr:sql-<= y x)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql-<=
               ,y
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "max"))))

      ((trivia:guard
        (list 'endb/sql/expr:sql-> x y)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql->
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "max")
               ,y)))

      ((trivia:guard
        (list 'endb/sql/expr:sql->= x y)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql->=
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "max")
               ,y)))

      ((trivia:guard
        (list 'endb/sql/expr:sql-> y x)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql->
               ,y
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "min"))))

      ((trivia:guard
        (list 'endb/sql/expr:sql->= y x)
        (and (member x vars) (free-var-or-constant-p y)))
       `(eq t (endb/sql/expr:sql->=
               ,y
               (fset:lookup
                (fset:lookup ,stats-md-sym ,(get x :column))
                "min")))))))

(defun %table-scan->cl (ctx vars projection from-src where-clauses nested-src &optional extra-stats-src)
  (let* ((where-src (loop for clause in where-clauses
                          collect `(eq t ,(where-clause-src clause)))))
    (if (base-table-p from-src)
        (alexandria:with-gensyms (table-md-sym
                                  arrow-file-md-sym
                                  deleted-md-sym
                                  erased-md-sym
                                  stats-md-sym
                                  sha1-md-sym
                                  batch-row-sym
                                  system-time-start-sym
                                  system-time-end-sym
                                  temporal-sym
                                  raw-deleted-row-ids-sym
                                  deleted-row-ids-sym
                                  erased-row-ids-sym
                                  lambda-sym
                                  batch-sym
                                  scan-row-id-sym
                                  scan-batch-idx-sym
                                  scan-batch-idx-string-sym
                                  scan-arrow-file-sym
                                  uncommitted-p-sym
                                  hash-index-rows-sym
                                  row-idx-sym)
          (let* ((table-name (base-table-name from-src))
                 (batch-sym (or (fset:lookup ctx :batch-sym) batch-sym))
                 (stats-md-sym (or (fset:lookup ctx :stats-md-sym) stats-md-sym))
                 (scan-row-id-sym (or (fset:lookup ctx :scan-row-id-sym) scan-row-id-sym))
                 (scan-batch-idx-sym (or (fset:lookup ctx :scan-batch-idx-sym) scan-batch-idx-sym))
                 (scan-arrow-file-sym (or (fset:lookup ctx :scan-arrow-file-sym) scan-arrow-file-sym))
                 (db-sym (fset:lookup ctx :db-sym))
                 (param-sym (fset:lookup ctx :param-sym))
                 (kw-projection (remove :|system_time| (loop for c in projection
                                                             collect (intern c :keyword))))
                 (array-vars (loop repeat (length kw-projection)
                                   collect (gensym)))
                 (stats-src (loop for clause in where-clauses
                                  for stats-src = (%where-clause-stats-src clause vars stats-md-sym param-sym)
                                  when stats-src
                                    collect stats-src))
                 (stats-src (append stats-src extra-stats-src)))
            (multiple-value-bind (hash-index-column hash-index-src)
                (loop for clause in where-clauses
                      for pair = (%hash-index-column-and-src clause vars param-sym)
                      when pair
                        do (return-from nil pair))
              (destructuring-bind (&optional (temporal-type :as-of temporal-type-p) (temporal-start :current_timestamp) (temporal-end temporal-start))
                  (base-table-temporal from-src)
                (let ((loop-body-src `(when (and ,@(when temporal-type-p
                                                     `((eq t (,(case temporal-type
                                                                 ((:as-of :between :all) 'endb/sql/expr:sql-<=)
                                                                 (:from 'endb/sql/expr:sql-<))
                                                              (endb/arrow:arrow-get ,temporal-sym ,scan-row-id-sym)
                                                              ,system-time-end-sym))))
                                                 (endb/sql/expr:ra-visible-row-p ,deleted-row-ids-sym ,erased-row-ids-sym ,scan-row-id-sym)
                                                 ,@where-src)
                                        ,nested-src)))
                  `(symbol-macrolet (,@(loop for v in vars
                                             for p in (append
                                                       (loop for p in kw-projection
                                                             for v in array-vars
                                                             collect `(if ,v
                                                                          (endb/arrow:arrow-get ,v ,scan-row-id-sym)
                                                                          (endb/arrow:arrow-struct-column-value ,batch-sym ,scan-row-id-sym ,p)))
                                                       (unless endb/sql/expr:*sqlite-mode*
                                                         (list `(let ((,scan-row-id-sym ,scan-row-id-sym))
                                                                  (lambda ()
                                                                    (endb/arrow:arrow-get ,batch-sym ,scan-row-id-sym)))
                                                               `(let ((,scan-row-id-sym ,scan-row-id-sym))
                                                                  (lambda ()
                                                                    (fset:map ("start" (endb/arrow:arrow-get ,temporal-sym ,scan-row-id-sym))
                                                                              ("end" (endb/sql/db:batch-row-system-time-end ,raw-deleted-row-ids-sym ,scan-row-id-sym))))))))
                                             collect `(,v ,p)))
                     (let ((,table-md-sym (endb/sql/db:base-table-meta ,db-sym ,table-name))
                           ,@(when temporal-type-p
                               `((,system-time-start-sym ,(ast->cl ctx (if (eq temporal-type :all)
                                                                           endb/sql/expr:+unix-epoch-time+
                                                                           temporal-start)))
                                 (,system-time-end-sym ,(ast->cl ctx (if (eq temporal-type :all)
                                                                         endb/sql/expr:+end-of-time+
                                                                         temporal-end))))))
                       (fset:do-map (,scan-arrow-file-sym ,arrow-file-md-sym ,table-md-sym)
                         (let ((,deleted-md-sym (fset:lookup ,arrow-file-md-sym "deleted"))
                               (,erased-md-sym (fset:lookup ,arrow-file-md-sym "erased"))
                               (,stats-md-sym (fset:lookup ,arrow-file-md-sym "stats"))
                               (,sha1-md-sym (fset:lookup ,arrow-file-md-sym "sha1"))
                               (,scan-batch-idx-sym -1))
                           (declare (ignorable ,stats-md-sym))
                           (when (and ,@stats-src)
                             (multiple-value-bind (,batch-row-sym ,uncommitted-p-sym)
                                 (endb/sql/db:base-table-arrow-batches ,db-sym ,table-name ,scan-arrow-file-sym :sha1 ,sha1-md-sym)
                               (declare (ignorable ,uncommitted-p-sym))
                               (dolist (,batch-row-sym ,batch-row-sym)
                                 (incf ,scan-batch-idx-sym)
                                 (let* ((,batch-sym (endb/arrow:arrow-struct-column-array ,batch-row-sym ,(intern table-name :keyword)))
                                        (,temporal-sym (endb/arrow:arrow-struct-column-array ,batch-row-sym :|system_time_start|))
                                        (,scan-batch-idx-string-sym (prin1-to-string ,scan-batch-idx-sym))
                                        (,raw-deleted-row-ids-sym (fset:lookup ,deleted-md-sym ,scan-batch-idx-string-sym))
                                        (,deleted-row-ids-sym ,(if temporal-type-p
                                                                   `(fset:filter
                                                                     (lambda (,lambda-sym)
                                                                       (eq t (endb/sql/expr:sql-<= (fset:lookup ,lambda-sym "system_time_end") ,system-time-start-sym)))
                                                                     ,raw-deleted-row-ids-sym)
                                                                   raw-deleted-row-ids-sym))
                                        (,erased-row-ids-sym (fset:lookup ,erased-md-sym ,scan-batch-idx-string-sym))
                                        ,@(loop for p in kw-projection
                                                for v in array-vars
                                                collect `(,v (endb/arrow:arrow-struct-column-array ,batch-sym ,p))))
                                   (declare (ignorable ,temporal-sym ,@array-vars))
                                   ,(if hash-index-column
                                        `(let ((,hash-index-rows-sym (unless ,uncommitted-p-sym
                                                                       (endb/sql/expr:ra-hash-index
                                                                        (endb/sql/db:db-hash-index-cache ,db-sym)
                                                                        (endb/sql/db:db-indexer-queue ,db-sym)
                                                                        (format nil "~A/~A/~A/~A" ,table-name ,scan-arrow-file-sym ,scan-batch-idx-sym ,hash-index-column)
                                                                        ,batch-sym
                                                                        ,(intern hash-index-column :keyword)
                                                                        ,hash-index-src))))
                                           (dotimes (,row-idx-sym (if ,hash-index-rows-sym
                                                                      (length ,hash-index-rows-sym)
                                                                      (endb/arrow:arrow-length ,batch-sym)))
                                             (let ((,scan-row-id-sym (if ,hash-index-rows-sym
                                                                         (aref ,hash-index-rows-sym ,row-idx-sym)
                                                                         ,row-idx-sym)))
                                               ,loop-body-src)))
                                        `(dotimes (,scan-row-id-sym (endb/arrow:arrow-length ,batch-sym))
                                           ,loop-body-src)))))))))))))))
        (alexandria:with-gensyms (row-sym)
          `(symbol-macrolet (,@(loop for v in (%unique-vars vars)
                                     for idx from 0
                                     when v
                                       collect `(,v (aref ,row-sym ,idx))))
             (dolist (,row-sym ,from-src)
               ,(if where-src
                    `(when (and ,@where-src)
                       ,nested-src)
                    nested-src)))))))

(defparameter +sip-hashes-limit+ 1024)

(defun %uncorrelated-index-key-form (index-key-form)
  (and (vectorp index-key-form)
       (= 1 (length index-key-form))))

(defun %join->cl (ctx from-table scan-where-clauses equi-join-clauses var-groups)
  (with-slots (src vars free-vars)
      from-table
    (multiple-value-bind (in-vars out-vars)
        (loop for (nil lhs rhs) in (mapcar #'where-clause-src equi-join-clauses)
              if (member lhs vars)
                collect lhs into out-vars
              else
                collect lhs into in-vars
              if (member rhs vars)
                collect rhs into out-vars
              else
                collect rhs into in-vars
              finally
                 (return (values in-vars out-vars)))
      (alexandria:with-gensyms (row-sym lambda-sym stats-md-sym)
        (let* ((index-sym (fset:lookup ctx :index-sym))
               (sip-init-src)
               (sip-probe-src)
               (sip-stats-src)
               (ctx (fset:with ctx :stats-md-sym stats-md-sym)))
          (fset:do-map (sip-index-key-form sip (or (fset:lookup ctx :index-sip) (fset:empty-map)))
            (let* ((source-from-table (fset:lookup sip :from-table))
                   (source-out-vars (fset:lookup sip :out-vars))
                   (source-vars (from-table-vars source-from-table))

                   (out-var-groups (remove nil (loop for v in source-out-vars
                                                     collect (find-if (lambda (xs)
                                                                        (member v xs))
                                                                      var-groups))))

                   (sip-in-vars (intersection in-vars source-vars))
                   (sip-can-reuse-index-p (and out-var-groups
                                               (loop for out-var-group in out-var-groups
                                                     always (intersection out-vars out-var-group))))

                   (sip-out-vars (if sip-can-reuse-index-p
                                     (loop for out-var-group in out-var-groups
                                           collect (first (intersection out-vars out-var-group)))
                                     (loop for v in sip-in-vars
                                           collect (nth (position v in-vars) out-vars)))))
              (when (and sip-out-vars (%uncorrelated-index-key-form sip-index-key-form))
                (alexandria:with-gensyms (sip-table-sym sip-hashes-sym)
                  (when (and (base-table-p (from-table-src from-table))
                             (= 1 (length sip-out-vars)))
                    (alexandria:with-gensyms (bloom-sym)
                      (push `(,sip-hashes-sym (when (< (hash-table-count ,sip-table-sym) +sip-hashes-limit+)
                                                (let ((,sip-hashes-sym (make-array 0
                                                                                   :element-type '(unsigned-byte 64)
                                                                                   :fill-pointer 0)))
                                                  (alexandria:maphash-keys
                                                   (lambda (,lambda-sym)
                                                     (dolist (,lambda-sym (endb/sql/expr:ra-bloom-hashes ,lambda-sym))
                                                       (vector-push-extend ,lambda-sym ,sip-hashes-sym)))
                                                   ,sip-table-sym)
                                                  ,sip-hashes-sym)))
                            sip-init-src)
                      (push `(or (null ,sip-hashes-sym)
                                 (let ((,bloom-sym (fset:lookup
                                                    (fset:lookup ,stats-md-sym ,(get (first sip-out-vars) :column))
                                                    "bloom")))
                                   (some (lambda (,lambda-sym)
                                           (endb/bloom:sbbf-check-p ,bloom-sym ,lambda-sym))
                                         ,sip-hashes-sym)))
                            sip-stats-src)))

                  (if sip-can-reuse-index-p
                      (push `(,sip-table-sym (let ((,sip-table-sym (gethash ,sip-index-key-form ,index-sym)))
                                               (endb/lib:log-debug "reuse sip: ~A ~A~%" ,(from-table-alias source-from-table) (hash-table-count ,sip-table-sym))
                                               ,sip-table-sym))
                            sip-init-src)
                      (let ((sip-in-key-form (if (= 1 (length sip-in-vars))
                                                 `(aref ,row-sym ,(position (first sip-in-vars) source-vars))
                                                 `(vector ,@(loop for v in sip-in-vars
                                                                  collect `(aref ,row-sym ,(position v source-vars)))))))
                        (push `(,sip-table-sym (let ((,sip-table-sym (make-hash-table :test endb/sql/expr:+hash-table-test-no-nulls+)))
                                                 (alexandria:maphash-values
                                                  (lambda (,lambda-sym)
                                                    (dolist (,row-sym ,lambda-sym)
                                                      (setf (gethash ,sip-in-key-form ,sip-table-sym) t)))
                                                  (gethash ,sip-index-key-form ,index-sym))
                                                 (endb/lib:log-debug "sip: ~A ~A~%" ,(from-table-alias source-from-table) (hash-table-count ,sip-table-sym))
                                                 ,sip-table-sym))
                              sip-init-src)))
                  (let ((sip-out-key-form (if (= 1 (length sip-out-vars))
                                              (first sip-out-vars)
                                              `(vector ,@sip-out-vars))))
                    (push `(gethash ,sip-out-key-form ,sip-table-sym) sip-probe-src))))))
          (alexandria:with-gensyms (index-table-sym index-key-form-sym)
            (let* ((new-free-vars (set-difference free-vars in-vars))
                   (index-key-form (if new-free-vars
                                       `(vector ',index-key-form-sym ,@new-free-vars)
                                       (vector index-key-form-sym))))
              (values
               `(gethash ,(if (= 1 (length in-vars))
                              (first in-vars)
                              `(vector ,@in-vars))
                         (endb/sql/expr:ra-compute-index-if-absent
                          ,index-sym
                          ,index-key-form
                          (lambda ()
                            (let* ((,index-table-sym (make-hash-table :test endb/sql/expr:+hash-table-test-no-nulls+))
                                   ,@sip-init-src)
                              ,(%table-scan->cl ctx
                                                vars
                                                (mapcar #'%unqualified-column-name (from-table-projection from-table))
                                                src
                                                scan-where-clauses
                                                `(when (and ,@sip-probe-src)
                                                   (push (vector ,@vars)
                                                         (gethash ,(if (= 1 (length out-vars))
                                                                       (first out-vars)
                                                                       `(vector ,@out-vars))
                                                                  ,index-table-sym)))
                                                sip-stats-src)
                              (endb/lib:log-debug "join table: ~A ~A~%"
                                                  ,(from-table-alias from-table)
                                                  (reduce #'+ (mapcar #'length
                                                                      (alexandria:hash-table-values ,index-table-sym))))

                              (maphash
                               (lambda (,lambda-sym ,row-sym)
                                 (setf (gethash ,lambda-sym ,index-table-sym) (reverse ,row-sym)))
                               ,index-table-sym)

                              ,index-table-sym))))
               (fset:with ctx :index-sip (fset:with (or (fset:lookup ctx :index-sip) (fset:empty-map))
                                                    index-key-form (fset:map (:from-table from-table) (:out-vars out-vars))))))))))))

(defun %selection-with-limit-offset->cl (ctx selected-src &optional limit offset)
  (let ((acc-sym (fset:lookup ctx :acc-sym)))
    (if (or limit offset)
        (let* ((rows-sym (fset:lookup ctx :rows-sym))
               (block-sym (fset:lookup ctx :block-sym))
               (limit (if offset
                          (+ offset limit)
                          limit))
               (offset (or offset 0)))
          `(progn
             (when (and (>= ,rows-sym ,offset)
                        (not (eql ,rows-sym ,limit)))
               (incf ,rows-sym)
               (push (vector ,@selected-src) ,acc-sym))
             (when (eql ,rows-sym ,limit)
               (return-from ,block-sym ,acc-sym))))
        `(push (vector ,@selected-src) ,acc-sym))))

(defun %scan-where-clause-p (ast)
  (if (listp ast)
      (if (eq :not (first ast))
          (%scan-where-clause-p (second ast))
          (not (eq :exists (first ast))))
      t))

(defun %build-var-groups (groups)
  (let ((new-groups (delete-duplicates
                     (loop for g1 in groups
                           collect (delete-duplicates
                                    (loop for g2 in groups
                                          when (intersection g1 g2)
                                            append g2)))
                     :test 'equal)))
    (if (equal new-groups groups)
        groups
        (%build-var-groups new-groups))))

(defun %from->cl (ctx from-tables where-clauses selected-src &optional vars (var-groups nil var-groups-p))
  (let* ((var-groups (if var-groups-p
                         var-groups
                         (let ((var-pairs (loop for clause in where-clauses
                                                when (%maybe-equi-join-p (where-clause-src clause))
                                                  collect (rest (where-clause-src clause)))))
                           (%build-var-groups var-pairs))))
         (candidate-tables (remove-if-not (lambda (x)
                                            (subsetp (from-table-free-vars x) vars))
                                          from-tables))
         (from-table (or (find-if (lambda (x)
                                    (loop with vars = (append (from-table-vars x) vars)
                                          for c in where-clauses
                                          thereis (%equi-join-predicate-p vars (where-clause-src c))))
                                  candidate-tables)
                         (first candidate-tables)))
         (from-tables (remove from-table from-tables))
         (equi-join-participant-p (loop for c in where-clauses
                                        thereis (%equi-join-participant-p (from-table-vars from-table)
                                                                          (where-clause-src c))))
         (new-vars (from-table-vars from-table))
         (vars (append new-vars vars)))
    (multiple-value-bind (scan-clauses equi-join-clauses pushdown-clauses)
        (loop for c in where-clauses
              if (and (subsetp (where-clause-free-vars c) new-vars)
                      (%scan-where-clause-p (where-clause-ast c)))
                collect c into scan-clauses
              else if (%equi-join-predicate-p vars (where-clause-src c))
                     collect c into equi-join-clauses
              else if (subsetp (where-clause-free-vars c) vars)
                     collect c into pushdown-clauses
              finally
                 (return (values scan-clauses equi-join-clauses pushdown-clauses)))
      (multiple-value-bind (join-src join-ctx)
          (when equi-join-participant-p
            (%join->cl ctx from-table scan-clauses equi-join-clauses var-groups))
        (let* ((new-where-clauses (append scan-clauses equi-join-clauses pushdown-clauses))
               (where-clauses (set-difference where-clauses new-where-clauses))
               (nested-src (cond
                             (from-tables
                              (%from->cl (or join-ctx ctx) from-tables where-clauses selected-src vars var-groups))
                             (where-clauses
                              `(when (and ,@(loop for clause in where-clauses collect `(eq t ,(where-clause-src clause))))
                                 ,selected-src))
                             (t selected-src)))
               (projection (mapcar #'%unqualified-column-name (from-table-projection from-table))))
          (if equi-join-participant-p
              (%table-scan->cl join-ctx
                               new-vars
                               projection
                               join-src
                               pushdown-clauses
                               nested-src)
              (progn
                (endb/lib:log-debug "table scan: ~A~%" (from-table-alias from-table))
                (%table-scan->cl ctx
                                 new-vars
                                 projection
                                 (from-table-src from-table)
                                 (append scan-clauses pushdown-clauses)
                                 nested-src))))))))

(defun %group-by->cl (ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars selected-non-aggregate-columns)
  (alexandria:with-gensyms (group-acc-sym group-key-sym group-sym)
    (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
           (group-by-projection (loop for g in group-by
                                      collect (ast->cl ctx g)))
           (group-by-exprs-projection (loop for k being the hash-key of aggregate-table
                                            collect k))
           (group-key-form `(vector ,@group-by-projection))
           (init-srcs (loop for v being the hash-value of aggregate-table
                            collect (aggregate-init-src v)))
           (group-by-selected-src `(symbol-macrolet (,@(loop for v in group-by-exprs-projection
                                                             for idx from 0
                                                             collect `(,v (aref ,group-sym ,idx))))
                                     (let* ((,group-key-sym ,group-key-form)
                                            (,group-sym  (gethash ,group-key-sym ,group-acc-sym)))
                                       (unless ,group-sym
                                         (setf ,group-sym (vector ,@init-srcs))
                                         (setf (gethash ,group-key-sym ,group-acc-sym) ,group-sym))
                                       ,@(loop for v being the hash-value of aggregate-table
                                               collect `(when (eq t ,(aggregate-where-src v))
                                                          (endb/sql/expr:agg-accumulate ,(aggregate-var v) ,@(aggregate-src v)))))))
           (empty-group-key-form (make-array (length group-by-projection) :initial-element :null))
           (group-by-src `(let ((,group-acc-sym (make-hash-table :test endb/sql/expr:+hash-table-test+)))
                            ,(%from->cl ctx from-tables where-clauses group-by-selected-src correlated-vars)
                            ,(unless group-by
                               `(when (zerop (hash-table-count ,group-acc-sym))
                                  (setf (gethash ,empty-group-key-form ,group-acc-sym)
                                        (vector ,@init-srcs))))
                            ,group-acc-sym))
           (non-group-selected-vars (loop for v in selected-non-aggregate-columns
                                          unless (find (fset:lookup ctx v) group-by-projection)
                                            collect v)))
      (when non-group-selected-vars
        (%annotated-error-with-span (fset:lookup ctx :sql)
                                    (format nil "Selecting columns: ~A that does not match group by: ~A"
                                            non-group-selected-vars
                                            (or group-by "()"))
                                    (format nil "Selecting columns: ~A that does not match group by"
                                            non-group-selected-vars)
                                    (get (first group-by) :start)
                                    (get (first (last group-by)) :end)))
      (alexandria:with-gensyms (key-sym val-sym)
        `(symbol-macrolet (,@(loop for v in (%unique-vars group-by-projection)
                                   for idx from 0
                                   when v
                                     collect `(,v (aref ,key-sym ,idx)))
                           ,@(loop for v in group-by-exprs-projection
                                   for idx from 0
                                   collect `(,v (aref ,val-sym ,idx))))
           (maphash
            (lambda (,key-sym ,val-sym)
              (declare (ignorable ,key-sym ,val-sym))
              (when (eq t ,having-src)
                ,(%selection-with-limit-offset->cl ctx selected-src limit offset)))
            ,group-by-src))))))

(defun %env-extension (table-alias projection &optional functions)
  (reduce
   (lambda (acc column)
     (let* ((qualified-column (%qualified-column-name table-alias column))
            (column-sym (gensym (concatenate 'string qualified-column "__")))
            (acc (fset:with acc column column-sym)))
       (setf (get column-sym :column) column)
       (when (member column functions :test 'equal)
         (setf (get column-sym :functionp) t))
       (fset:with acc qualified-column column-sym)))
   projection
   :initial-value (fset:empty-map)))

(defun %recursive-select-p (ctx table-or-subquery)
  (and (symbolp table-or-subquery)
       (equal (fset:lookup ctx :current-cte)
              (symbol-name table-or-subquery))))

(defun %pick-initial-join-order (from-tables where-clauses)
  (loop for from-table in from-tables
        if (loop for c in where-clauses
                 thereis (and (subsetp (where-clause-free-vars c) (from-table-vars from-table))
                              (%scan-where-clause-p (where-clause-ast c))))
          collect from-table into filtered-tables
        else
          collect from-table into unfiltered-tables
        finally (return (append (stable-sort filtered-tables #'< :key #'from-table-size)
                                (stable-sort unfiltered-tables #'< :key #'from-table-size)))))

(defmethod sql->cl (ctx (type (eql :select)) &rest args)
  (destructuring-bind (select-list &key distinct (from '(((:values ((:null))) #:dual))) (where :true)
                                     (group-by () group-by-p) (having :true havingp)
                                     order-by limit offset)
      args
    (labels ((select->cl (ctx from-ast from-tables-acc)
               (destructuring-bind (table-or-subquery &optional (table-alias table-or-subquery) column-names temporal)
                   (first from-ast)
                 (multiple-value-bind (table-src projection free-vars)
                     (cond
                       ((symbolp table-or-subquery)
                        (%base-table-or-view->cl ctx table-or-subquery :temporal temporal))
                       ((and (listp table-or-subquery)
                             (eq :objects (first table-or-subquery)))
                        (%ast->cl-with-free-vars (fset:with ctx :inside-from-p t) table-or-subquery))
                       (t (%ast->cl-with-free-vars ctx table-or-subquery)))
                   (when (and column-names (not (= (length projection) (length column-names))))
                     (%annotated-error (fset:lookup ctx :sql) table-alias "Number of column names does not match projection"))
                   (let* ((projection (or (mapcar #'symbol-name column-names) projection))
                          (table-alias (%unqualified-column-name (symbol-name table-alias)))
                          (qualified-projection (loop for column in projection
                                                      collect (%qualified-column-name table-alias column)))
                          (extra-projection (unless endb/sql/expr:*sqlite-mode*
                                              (cond
                                                ((base-table-p table-src)
                                                 (list "!doc" "system_time"))
                                                ((and (listp table-or-subquery)
                                                      (eq :objects (first table-or-subquery)))
                                                 (list "!doc")))))
                          (projection (append projection extra-projection))
                          (env-extension (%env-extension table-alias projection (when (base-table-p table-src)
                                                                                  extra-projection)))
                          (ctx (fset:map-union ctx env-extension))
                          (ctx (if (%recursive-select-p ctx table-or-subquery)
                                   (fset:with ctx :recursive-select t)
                                   ctx))
                          (ctx (fset:with ctx
                                          :table-projections
                                          (fset:with (or (fset:lookup ctx :table-projections) (fset:empty-map))
                                                     table-alias
                                                     qualified-projection)))
                          (from-table (make-from-table :src table-src
                                                       :vars (loop for p in projection
                                                                   collect (fset:lookup env-extension p))
                                                       :free-vars free-vars
                                                       :size (if (base-table-p table-src)
                                                                 (base-table-size table-src)
                                                                 most-positive-fixnum)
                                                       :projection qualified-projection
                                                       :alias table-alias))
                          (from-tables-acc (append from-tables-acc (list from-table))))
                     (if (rest from-ast)
                         (select->cl ctx (rest from-ast) from-tables-acc)
                         (let* ((aggregate-table (make-hash-table))
                                (ctx (fset:with ctx :aggregate-table aggregate-table))
                                (ctx (fset:less ctx :on-cte-access))
                                (full-projection (loop for from-table in from-tables-acc
                                                       append (from-table-projection from-table)))
                                (table-projections (fset:lookup ctx :table-projections))
                                (selected-non-aggregate-columns ())
                                (selected-ctx (fset:with
                                               ctx
                                               :on-var-access
                                               (cons (lambda (inner-ctx k v)
                                                       (when (and (eq v (fset:lookup ctx k))
                                                                  (not (fset:lookup inner-ctx :aggregate)))
                                                         (pushnew k selected-non-aggregate-columns)))
                                                     (fset:lookup ctx :on-var-access))))
                                (selected-src (loop for (expr) in select-list
                                                    append (cond
                                                             ((eq :* expr)
                                                              (loop for p in full-projection
                                                                    collect (ast->cl selected-ctx (make-symbol p))))
                                                             ((%qualified-asterisk-p expr)
                                                              (loop for p in (fset:lookup table-projections (symbol-name (second expr)))
                                                                    collect (ast->cl selected-ctx (make-symbol p))))
                                                             (t (list (ast->cl selected-ctx expr))))))
                                (select-star-projection (mapcar #'%unqualified-column-name full-projection))
                                (select-projection (%select-projection ctx select-list select-star-projection table-projections))
                                (order-by-selected-src (loop for (expr) in order-by
                                                             for projected-idx = (when (symbolp expr)
                                                                                   (position (symbol-name expr) select-projection :test 'equal))
                                                             unless (or projected-idx (numberp expr))
                                                               collect (ast->cl selected-ctx expr)))
                                (selected-src (append selected-src order-by-selected-src))
                                (where-clauses (loop for clause in (append (%from-where-clauses from)
                                                                           (%and-clauses where))
                                                     collect (multiple-value-bind (src projection free-vars)
                                                                 (%ast->cl-with-free-vars ctx clause)
                                                               (declare (ignore projection))
                                                               (make-where-clause :src src
                                                                                  :free-vars free-vars
                                                                                  :ast clause))))
                                (from-tables (%pick-initial-join-order from-tables-acc where-clauses))
                                (having-src (ast->cl selected-ctx having))
                                (limit (unless order-by
                                         limit))
                                (offset (unless order-by
                                          offset))
                                (group-by-needed-p (or group-by-p havingp (plusp (hash-table-count aggregate-table))))
                                (correlated-vars (append (set-difference (loop for clause in where-clauses
                                                                               append (where-clause-free-vars clause))
                                                                         (loop for from-table in from-tables
                                                                               append (from-table-vars from-table)))
                                                         (loop for (nil . cte) in (fset:convert 'list (fset:lookup ctx :ctes))
                                                               append (cte-free-vars cte)))))
                           (values
                            (if group-by-needed-p
                                (%group-by->cl ctx from-tables where-clauses selected-src limit offset group-by having-src correlated-vars selected-non-aggregate-columns)
                                (%from->cl ctx from-tables where-clauses (%selection-with-limit-offset->cl ctx selected-src limit offset) correlated-vars))
                            select-projection))))))))
      (alexandria:with-gensyms (block-sym acc-sym rows-sym row-sym)
        (let* ((ctx (fset:map-union ctx (fset:map (:rows-sym rows-sym)
                                                  (:acc-sym acc-sym)
                                                  (:block-sym block-sym))))
               (from (%flatten-from from)))
          (multiple-value-bind (src select-projection)
              (select->cl ctx from ())
            (let* ((src `(block ,block-sym
                           (let (,@(when (and limit (not order-by))
                                     (list `(,rows-sym 0)))
                                 (,acc-sym))
                             ,src
                             ,acc-sym)))
                   (src (if (eq :distinct distinct)
                            `(endb/sql/expr:ra-distinct ,src)
                            src))
                   (resolved-order-by (%resolve-order-by ctx order-by select-projection :allow-expr-p t))
                   (src (%wrap-with-order-by-and-limit src resolved-order-by limit offset)))
              (values (if (loop for (idx) in resolved-order-by
                                thereis (> idx (length select-projection)))
                          `(loop for ,row-sym in ,src
                                 collect (subseq ,row-sym 0 ,(length select-projection)))
                          src)
                      select-projection))))))))

(defun %values-projection (arity)
  (loop for idx from 1 upto arity
        collect (%anonymous-column-name idx)))

(defun %maybe-constant-list (asts)
  (if (every #'constantp asts)
      `',asts
      `(list ,@asts)))

(defun %maybe-constant-vector (asts)
  (if (every #'constantp asts)
      `#(,@asts)
      `(vector ,@asts)))

(defmethod sql->cl (ctx (type (eql :values)) &rest args)
  (destructuring-bind (values-list &key order-by limit offset start end)
      args
    (let* ((arity (length (first values-list)))
           (projection (%values-projection arity)))
      (unless (apply #'= (mapcar #'length values-list))
        (%annotated-error-with-span (fset:lookup ctx :sql)
                                    (format nil "All VALUES must have the same number of columns: ~A" arity)
                                    "All VALUES must have the same number of columns"
                                    start
                                    end))
      (values (%wrap-with-order-by-and-limit (%maybe-constant-list
                                              (loop for ast in values-list
                                                    collect (%maybe-constant-vector
                                                             (loop for ast in ast
                                                                   collect (ast->cl ctx ast)))))
                                             (%resolve-order-by ctx order-by projection) limit offset)
              projection))))

(defun %object-ast-keys (ctx object &key (require-literal-p t))
  (loop for (k v) in (second object)
        for x = (cond
                  ((and (symbolp k)
                        (not (keywordp k)))
                   (symbol-name k))
                  ((eq :shorthand-property k)
                   (symbol-name (second v)))
                  ((stringp k) k)
                  (require-literal-p
                   (%annotated-error-with-span (fset:lookup ctx :sql)
                                               "All OBJECTS must have literal keys"
                                               "All OBJECTS must have literal keys"
                                               (getf object :start) (getf object :end))))
        when x
          collect x))

(defmethod sql->cl (ctx (type (eql :objects)) &rest args)
  (destructuring-bind (objects-list &key order-by limit offset)
      args
    (let* ((projection (sort (delete-duplicates
                              (loop for object in objects-list
                                    append (%object-ast-keys ctx object))
                              :test 'equal)
                             #'string<)))
      (alexandria:with-gensyms (object-sym key-sym)
        (values (%wrap-with-order-by-and-limit
                 `(loop for ,object-sym in ,(let ((ctx (fset:less ctx :inside-from-p)))
                                              (%maybe-constant-list (loop for ast in objects-list
                                                                          collect (ast->cl ctx ast))))
                        collect (coerce (append (loop for ,key-sym in ',projection
                                                      collect (endb/sql/expr:syn-access-finish ,object-sym ,key-sym nil))
                                                ,(when (fset:lookup ctx :inside-from-p)
                                                   `(list ,object-sym)))
                                        'vector))
                 (%resolve-order-by ctx order-by projection) limit offset)
                projection)))))

(defmethod sql->cl (ctx (type (eql :exists)) &rest args)
  (destructuring-bind (query)
      args
    `(endb/sql/expr:ra-exists ,(ast->cl ctx (append query '(:limit 1))))))

(defmethod sql->cl (ctx (type (eql :in)) &rest args)
  (destructuring-bind (expr query)
      args
    `(endb/sql/expr:ra-in ,(ast->cl ctx expr) ,(%maybe-constant-list (loop for ast in query
                                                                           collect (ast->cl ctx ast))))))

(defmethod sql->cl (ctx (type (eql :left-join)) &rest args)
  (destructuring-bind (table on)
      args
    (multiple-value-bind (src projection free-vars)
        (%ast->cl-with-free-vars ctx (list :select (list (list :*)) :from (list table) :where on))
      (values `(or ,src '(,(make-array (length projection) :initial-element :null)))
              projection
              free-vars))))

(defmethod sql->cl (ctx (type (eql :scalar-subquery)) &rest args)
  (destructuring-bind (query)
      args
    (multiple-value-bind (src projection free-vars)
        (%ast->cl-with-free-vars ctx query)
      (declare (ignore projection))
      (alexandria:with-gensyms (index-key-form-sym)
        (let* ((index-sym (fset:lookup ctx :index-sym))
               (index-key-form `(vector ',index-key-form-sym ,@free-vars)))
          `(endb/sql/expr:ra-scalar-subquery
            (endb/sql/expr:ra-compute-index-if-absent ,index-sym ,index-key-form (lambda () ,src))))))))

(defmethod sql->cl (ctx (type (eql :unnest)) &rest args)
  (destructuring-bind (exprs &key with-ordinality)
      args
    (values `(endb/sql/expr:ra-unnest ,(%maybe-constant-list (loop for ast in exprs
                                                                   collect (ast->cl ctx ast)))
                                      :with-ordinality ,with-ordinality)
            (%values-projection (if with-ordinality
                                    (1+ (length exprs))
                                    (length exprs))))))

(defun %compound-select->cl (ctx fn-name fn args)
  (destructuring-bind (lhs rhs &key order-by limit offset start end)
      args
    (multiple-value-bind (lhs-src lhs-projection)
        (ast->cl ctx lhs)
      (multiple-value-bind (rhs-src rhs-projection)
          (ast->cl ctx rhs)
        (unless (= (length lhs-projection)
                   (length rhs-projection))
          (%annotated-error-with-span (fset:lookup ctx :sql)
                                      (format nil "Number of ~A left columns: ~A does not match right columns: ~A"
                                              fn-name
                                              (length lhs-projection)
                                              (length rhs-projection))
                                      "Number of left columns does not match right columns"
                                      start end))
        (values (%wrap-with-order-by-and-limit `(,fn ,lhs-src ,rhs-src)
                                               (%resolve-order-by ctx order-by lhs-projection) limit offset)
                lhs-projection)))))

(defmethod sql->cl (ctx (type (eql :union)) &rest args)
  (%compound-select->cl ctx "UNION" 'endb/sql/expr:ra-union args))

(defmethod sql->cl (ctx (type (eql :union-all)) &rest args)
  (%compound-select->cl ctx "UNION ALL" 'endb/sql/expr:ra-union-all args))

(defmethod sql->cl (ctx (type (eql :except)) &rest args)
  (%compound-select->cl ctx "EXCEPT" 'endb/sql/expr:ra-except args))

(defmethod sql->cl (ctx (type (eql :intersect)) &rest args)
  (%compound-select->cl ctx "INTERSECT" 'endb/sql/expr:ra-intersect args))

(defmethod sql->cl (ctx (type (eql :+)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-+ ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        (ast->cl ctx lhs))))

(defmethod sql->cl (ctx (type (eql :-)) &rest args)
  (destructuring-bind (lhs &optional (rhs nil rhsp))
      args
    (if rhsp
        `(endb/sql/expr:sql-- ,(ast->cl ctx lhs) ,(ast->cl ctx rhs))
        (if (numberp lhs)
            (- lhs)
            `(endb/sql/expr:sql-- 0 ,(ast->cl ctx lhs))))))

(defmethod sql->cl (ctx (type (eql :create-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/db:ddl-create-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :drop-index)) &rest args)
  (declare (ignore args))
  `(endb/sql/db:ddl-drop-index ,(fset:lookup ctx :db-sym)))

(defmethod sql->cl (ctx (type (eql :create-table)) &rest args)
  (destructuring-bind (table-name column-names)
      args
    `(endb/sql/db:ddl-create-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ',(mapcar #'symbol-name column-names))))

(defmethod sql->cl (ctx (type (eql :drop-table)) &rest args)
  (destructuring-bind (table-name &key if-exists)
      args
    `(endb/sql/db:ddl-drop-table ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) :if-exists ,(when if-exists
                                                                                                    t))))

(defparameter +create-assertion-scanner+ (ppcre:create-scanner "(?is).+?\\s+CHECK\\s*((?:'.*?'|\"(?:[^\"\\\\]|\\\\.)*\"|[^;]*?)*)\\s*(?:;|$)"))

(defmethod sql->cl (ctx (type (eql :create-assertion)) &rest args)
  (destructuring-bind (constraint-name check-clause)
      args
    (when (find :parameter (alexandria:flatten check-clause))
      (%annotated-error (fset:lookup ctx :sql) constraint-name "Assertions do not support parameters"))
    (let ((assertion-matches (nth-value 1 (ppcre:scan-to-strings +create-assertion-scanner+
                                                                 (fset:lookup ctx :sql)
                                                                 :start (get constraint-name :start)))))
      (unless (and (vectorp assertion-matches) (= 1 (length assertion-matches)))
        (%annotated-error (fset:lookup ctx :sql) constraint-name "Invalid assertion definition"))
      `(endb/sql/db:ddl-create-assertion ,(fset:lookup ctx :db-sym) ,(symbol-name constraint-name) ,(aref assertion-matches 0)))))

(defmethod sql->cl (ctx (type (eql :drop-assertion)) &rest args)
  (destructuring-bind (constraint-name &key if-exists)
      args
    `(endb/sql/db:ddl-drop-assertion ,(fset:lookup ctx :db-sym) ,(symbol-name constraint-name) :if-exists ,(when if-exists
                                                                                                             t))))

(defparameter +create-view-scanner+ (ppcre:create-scanner "(?is).+?\\s+AS\\s+((?:'.*?'|\"(?:[^\"\\\\]|\\\\.)*\"|[^;]*?)*)\\s*(?:;|$)"))

(defmethod sql->cl (ctx (type (eql :create-view)) &rest args)
  (destructuring-bind (table-name query &key column-names)
      args
    (multiple-value-bind (view-src projection)
        (ast->cl (fset:with ctx :no-parameters "Views do not support parameters") query)
      (declare (ignore view-src))
      (when (and column-names (not (= (length projection) (length column-names))))
        (%annotated-error (fset:lookup ctx :sql) table-name "Number of column names does not match projection"))
      (let ((query-matches (nth-value 1 (ppcre:scan-to-strings +create-view-scanner+
                                                               (fset:lookup ctx :sql)
                                                               :start (get table-name :start)))))
        (unless (and (vectorp query-matches) (= 1 (length query-matches)))
          (%annotated-error (fset:lookup ctx :sql) table-name "Invalid view definition"))
        `(endb/sql/db:ddl-create-view ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,(aref query-matches 0) ',(or (mapcar #'symbol-name column-names) projection))))))

(defmethod sql->cl (ctx (type (eql :drop-view)) &rest args)
  (destructuring-bind (view-name &key if-exists)
      args
    `(endb/sql/db:ddl-drop-view ,(fset:lookup ctx :db-sym) ,(symbol-name view-name)  :if-exists ,(when if-exists
                                                                                                   t))))

(defun %insert-on-conflict (ctx table-name on-conflict update &key (values nil upsertp) column-names)
  (when upsertp
    (assert (or (and (eq :objects (first values)) (null column-names))
                column-names)
            nil))
  (destructuring-bind (update-cols &key (where :true) unset patch)
      (or update '(nil))
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name :errorp (not upsertp))
      (when (or (base-table-p from-src) (null from-src))
        (alexandria:with-gensyms (scan-row-id-sym
                                  scan-batch-idx-sym
                                  scan-arrow-file-sym
                                  batch-sym
                                  object-sym
                                  updated-rows-sym
                                  deleted-row-ids-sym
                                  value-sym
                                  key-sym
                                  insertp-sym)
          (let* ((objectsp (eq :objects (first values)))
                 (on-conflict (delete-duplicates (mapcar #'symbol-name on-conflict) :test 'equal))
                 (excluded-projection (when upsertp
                                        (if objectsp
                                            (sort (delete-duplicates (loop for object in (second values)
                                                                           for keys = (%object-ast-keys ctx object :require-literal-p nil)
                                                                           unless (subsetp on-conflict keys :test 'equal)
                                                                             do (%annotated-error (fset:lookup ctx :sql)
                                                                                                  table-name
                                                                                                  "All inserted values needs to provide the on conflict columns")
                                                                           append keys)
                                                                     :test 'equal)
                                                  #'string<)
                                            (if (subsetp on-conflict column-names :test 'equal)
                                                column-names
                                                (%annotated-error (fset:lookup ctx :sql) table-name "Column names needs to contain the on conflict columns")))))
                 (extra-projection (unless endb/sql/expr:*sqlite-mode*
                                     (list "!doc" "system_time")))
                 (projection (append (delete-duplicates (append projection on-conflict) :test 'equal) extra-projection))
                 (env-extension (%env-extension (symbol-name table-name) projection extra-projection))
                 (vars (loop for p in projection
                             collect (fset:lookup env-extension p)))
                 (excluded-env-extension (%env-extension "excluded" excluded-projection))
                 (excluded-env-extension (fset:with excluded-env-extension "excluded.!doc" object-sym))
                 (ctx (fset:map-union (fset:map-union ctx (fset:map-union excluded-env-extension env-extension))
                                      (fset:map (:scan-row-id-sym scan-row-id-sym)
                                                (:scan-arrow-file-sym scan-arrow-file-sym)
                                                (:scan-batch-idx-sym scan-batch-idx-sym)
                                                (:batch-sym batch-sym))))
                 (conflict-clauses (when upsertp
                                     (loop for v in on-conflict
                                           collect (list :is
                                                         (make-symbol (format nil "~A.~A" table-name v))
                                                         (make-symbol (format nil "excluded.~A" v))))))
                 (where-clauses (loop for clause in (if upsertp
                                                        conflict-clauses
                                                        (%and-clauses where))
                                      collect (multiple-value-bind (src projection free-vars)
                                                  (%ast->cl-with-free-vars ctx clause)
                                                (declare (ignore projection))
                                                (make-where-clause :src src
                                                                   :free-vars free-vars
                                                                   :ast clause))))
                 (update-paths (loop for (update-col expr) in update-cols
                                     append (list (if (symbolp update-col)
                                                      `(fset:seq ,(symbol-name update-col))
                                                      (ast->cl ctx update-col))
                                                  (ast->cl ctx expr))))
                 (unset-paths (loop for unset-col in unset
                                    collect (if (symbolp unset-col)
                                                `(fset:seq ,(symbol-name unset-col))
                                                (ast->cl ctx unset-col))))
                 (updated-columns (delete-duplicates
                                   (append (loop for col in (append (mapcar #'car update-cols) unset)
                                                 collect (if (symbolp col)
                                                             (symbol-name col)
                                                             (second col))))
                                   :test 'equal))
                 (update-src (when update
                               `((push (endb/sql/expr:sql-patch
                                        (endb/sql/expr:sql-path_remove
                                         (endb/sql/expr:sql-path_set
                                          (endb/arrow:arrow-get ,batch-sym ,scan-row-id-sym)
                                          ,@update-paths)
                                         ,@unset-paths)
                                        ,(if patch
                                             (ast->cl ctx patch)
                                             (fset:empty-map)))
                                       ,updated-rows-sym)
                                 (push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym)))))
            (when (and update (null updated-columns) (null patch))
              (%annotated-error (fset:lookup ctx :sql) table-name "Update requires at least one set or unset column or patch"))
            (when (and upsertp (intersection on-conflict updated-columns :test 'equal))
              (%annotated-error (fset:lookup ctx :sql) table-name "Cannot update the on conflict columns"))
            `(let ((,updated-rows-sym)
                   (,deleted-row-ids-sym))
               ,(if upsertp
                    `(symbol-macrolet (,@(loop for v in excluded-projection
                                               collect `(,(fset:lookup excluded-env-extension v)
                                                         (endb/sql/expr:syn-access-finish ,object-sym ,v nil))))
                       (dolist (,object-sym (let ((,object-sym ,(if objectsp
                                                                    (%maybe-constant-list (loop for ast in (second values)
                                                                                                collect (ast->cl ctx ast)))
                                                                    `(mapcar (lambda (,value-sym)
                                                                               (fset:convert 'fset:map (pairlis ',excluded-projection (coerce ,value-sym 'list))))
                                                                             ,(ast->cl ctx values)))))
                                              (unless (= (length ,object-sym)
                                                         (length (delete-duplicates
                                                                  ,object-sym
                                                                  :test endb/sql/expr:+hash-table-test+
                                                                  :key (lambda (,object-sym)
                                                                         (loop for ,key-sym in ',on-conflict
                                                                               collect (endb/sql/expr:syn-access-finish ,object-sym ,key-sym nil))))))
                                                (%annotated-error ',(fset:lookup ctx :sql)',table-name "Inserted values cannot contain duplicated on conflict columns"))
                                              ,object-sym))
                         (let ((,insertp-sym t))
                           ,(when from-src
                              (%table-scan->cl ctx vars projection from-src where-clauses
                                               `(if (and ,@(loop for clause in (%and-clauses where)
                                                                 collect `(eq t ,(ast->cl ctx clause))))
                                                    (progn
                                                      (setf ,insertp-sym nil)
                                                      ,@update-src)
                                                    (setf ,insertp-sym nil))))
                           (when ,insertp-sym
                             (push ,object-sym ,updated-rows-sym)))))
                    (%table-scan->cl ctx vars projection from-src where-clauses `(progn ,@update-src)))
               (endb/sql/db:dml-insert-objects ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,updated-rows-sym)
               (endb/sql/db:dml-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym)
               (values nil (length ,updated-rows-sym)))))))))

(defmethod sql->cl (ctx (type (eql :insert)) &rest args)
  (destructuring-bind (table-name values &key column-names on-conflict update)
      args
    (when (and endb/sql/expr:*sqlite-mode* on-conflict)
      (%annotated-error (fset:lookup ctx :sql) table-name "Insert on conflict not supported in SQLite mode"))
    (when (and (not endb/sql/expr:*sqlite-mode*) (null column-names) (eq :values (first values)))
      (%annotated-error (fset:lookup ctx :sql) table-name "Column names are required for values"))
    (if (eq :objects (first values))
        (if on-conflict
            (%insert-on-conflict ctx table-name on-conflict update :values values)
            `(endb/sql/db:dml-insert-objects ,(fset:lookup ctx :db-sym)
                                             ,(symbol-name table-name)
                                             ,(%maybe-constant-list (loop for ast in (second values)
                                                                          collect (ast->cl ctx ast)))))
        (multiple-value-bind (src projection)
            (ast->cl ctx values)
          (let ((column-names (if (or column-names endb/sql/expr:*sqlite-mode*)
                                  (mapcar #'symbol-name column-names)
                                  projection)))
            (if on-conflict
                (%insert-on-conflict ctx table-name on-conflict update :values values :column-names column-names)
                `(endb/sql/db:dml-insert ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,src
                                         :column-names ',column-names)))))))

(defmethod sql->cl (ctx (type (eql :delete)) &rest args)
  (destructuring-bind (table-name &key (where :true))
      args
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name)
      (when (base-table-p from-src)
        (alexandria:with-gensyms (scan-row-id-sym scan-batch-idx-sym scan-arrow-file-sym deleted-row-ids-sym)
          (let* ((env-extension (%env-extension (symbol-name table-name) projection))
                 (ctx (fset:map-union (fset:map-union ctx env-extension)
                                      (fset:map (:scan-row-id-sym scan-row-id-sym)
                                                (:scan-arrow-file-sym scan-arrow-file-sym)
                                                (:scan-batch-idx-sym scan-batch-idx-sym))))
                 (vars (loop for p in projection
                             collect (fset:lookup env-extension p)))
                 (where-clauses (loop for clause in (%and-clauses where)
                                      collect (multiple-value-bind (src projection free-vars)
                                                  (%ast->cl-with-free-vars ctx clause)
                                                (declare (ignore projection))
                                                (make-where-clause :src src
                                                                   :free-vars free-vars
                                                                   :ast clause)))))
            `(let ((,deleted-row-ids-sym))
               ,(%table-scan->cl ctx vars projection from-src where-clauses
                                 `(push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,deleted-row-ids-sym))
               (endb/sql/db:dml-delete ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,deleted-row-ids-sym))))))))

(defmethod sql->cl (ctx (type (eql :erase)) &rest args)
  (destructuring-bind (table-name &key (where :true))
      args
    (multiple-value-bind (from-src projection)
        (%base-table-or-view->cl ctx table-name)
      (when (base-table-p from-src)
        (alexandria:with-gensyms (scan-row-id-sym scan-batch-idx-sym scan-arrow-file-sym erased-row-ids-sym)
          (setf (base-table-temporal from-src) (list :all))
          (let* ((env-extension (%env-extension (symbol-name table-name) projection))
                 (ctx (fset:map-union (fset:map-union ctx env-extension)
                                      (fset:map (:scan-row-id-sym scan-row-id-sym)
                                                (:scan-arrow-file-sym scan-arrow-file-sym)
                                                (:scan-batch-idx-sym scan-batch-idx-sym))))
                 (vars (loop for p in projection
                             collect (fset:lookup env-extension p)))
                 (where-clauses (loop for clause in (%and-clauses where)
                                      collect (multiple-value-bind (src projection free-vars)
                                                  (%ast->cl-with-free-vars ctx clause)
                                                (declare (ignore projection))
                                                (make-where-clause :src src
                                                                   :free-vars free-vars
                                                                   :ast clause)))))
            `(let ((,erased-row-ids-sym))
               ,(%table-scan->cl ctx vars projection from-src where-clauses
                                 `(push (list ,scan-arrow-file-sym ,scan-batch-idx-sym ,scan-row-id-sym) ,erased-row-ids-sym))
               (endb/sql/db:dml-erase ,(fset:lookup ctx :db-sym) ,(symbol-name table-name) ,erased-row-ids-sym))))))))

(defmethod sql->cl (ctx (type (eql :update)) &rest args)
  (destructuring-bind (table-name update-cols &key (where :true) unset patch)
      args
    (%insert-on-conflict ctx table-name nil (list update-cols :where where :unset unset :patch patch))))

(defmethod sql->cl (ctx (type (eql :in-query)) &rest args)
  (destructuring-bind (expr query &key start end)
      args
    (multiple-value-bind (src projection free-vars)
        (if (symbolp query)
            (ast->cl ctx (list :select (list (list :*)) :from (list (list query))))
            (%ast->cl-with-free-vars ctx query))
      (unless (= 1 (length projection))
        (%annotated-error-with-span (fset:lookup ctx :sql)
                                    (format nil "IN query must return single column, got: ~A" (length projection))
                                    "IN query must return single column"
                                    start end))
      (alexandria:with-gensyms (index-key-form-sym)
        (let* ((index-sym (fset:lookup ctx :index-sym))
               (index-key-form `(vector ',index-key-form-sym ,@free-vars)))
          `(endb/sql/expr:ra-in-query
            (endb/sql/expr:ra-compute-index-if-absent
             ,index-sym
             ,index-key-form
             (lambda ()
               (endb/sql/expr:ra-in-query-index ,src)))
            ,(ast->cl ctx expr)))))))

(defmethod sql->cl (ctx (type (eql :subquery)) &rest args)
  (destructuring-bind (query)
      args
    (ast->cl ctx query)))

(defmethod sql->cl (ctx (type (eql :cast)) &rest args)
  (destructuring-bind (x sql-type)
      args
    `(endb/sql/expr:syn-cast ,(ast->cl ctx x) ,(intern (string-upcase (symbol-name sql-type)) :keyword))))

(defmethod sql->cl (ctx (type (eql :extract)) &rest args)
  (destructuring-bind (field x)
      args
    `(endb/sql/expr:syn-extract ,(intern (string-upcase (symbol-name field)) :keyword) ,(ast->cl ctx x))))

(defmethod sql->cl (ctx (type (eql :interval)) &rest args)
  (destructuring-bind (x from &optional to)
      args
    `(endb/sql/expr:syn-interval ,(ast->cl ctx x) ,from ,to)))

(defmethod sql->cl (ctx (type (eql :date)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-date x)))

(defmethod sql->cl (ctx (type (eql :time)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-time x)))

(defmethod sql->cl (ctx (type (eql :timestamp)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-timestamp x)))

(defmethod sql->cl (ctx (type (eql :blob)) &rest args)
  (destructuring-bind (x)
      args
    (endb/sql/expr:sql-unhex x)))

(defmethod sql->cl (ctx (type (eql :like)) &rest args)
  (destructuring-bind (x y &optional (z nil zp))
      args
    (let ((x (ast->cl ctx x))
          (y (ast->cl ctx y))
          (z (when zp
               (ast->cl ctx z))))
      (cond
        ((and (not zp) (stringp y))
         (alexandria:with-gensyms (x-sym)
           `(let ((,x-sym ,x))
              (if (stringp ,x-sym)
                  (integerp (ppcre:scan ,(endb/sql/expr:build-like-regex y) ,x-sym))
                  (endb/sql/expr:sql-like ,y ,x-sym)))))
        ((and zp (stringp y) (stringp z))
         (alexandria:with-gensyms (x-sym)
           `(let ((,x-sym ,x))
              (if (stringp ,x-sym)
                  (integerp (ppcre:scan ,(endb/sql/expr:build-like-regex y z) ,x-sym))
                  (endb/sql/expr:sql-like ,y ,x-sym ,z)))))
        (zp `(endb/sql/expr:sql-like ,y ,x ,z))
        (t `(endb/sql/expr:sql-like ,y ,x))))))

(defmethod sql->cl (ctx (type (eql :glob)) &rest args)
  (destructuring-bind (x y)
      args
    (let ((x (ast->cl ctx x))
          (y (ast->cl ctx y)))
      (if (stringp y)
          (alexandria:with-gensyms (x-sym)
            `(let ((,x-sym ,x))
               (if (stringp ,x-sym)
                   (integerp (ppcre:scan ,(endb/sql/expr:build-glob-regex y) ,x-sym))
                   (endb/sql/expr:sql-glob ,y ,x-sym))))
          `(endb/sql/expr:sql-glob ,y ,x)))))

(defmethod sql->cl (ctx (type (eql :regexp)) &rest args)
  (destructuring-bind (x y)
      args
    (let ((x (ast->cl ctx x))
          (y (ast->cl ctx y)))
      (if (stringp y)
          (alexandria:with-gensyms (x-sym)
            `(let ((,x-sym ,x))
               (if (stringp ,x-sym)
                   (integerp (ppcre:scan ,y ,x-sym))
                   (endb/sql/expr:sql-regexp ,y ,x-sym))))
          `(endb/sql/expr:sql-regexp ,y ,x)))))

(defmethod sql->cl (ctx (type (eql :match)) &rest args)
  (destructuring-bind (x y)
      args
    `(endb/sql/expr:sql-match ,(ast->cl ctx y) ,(ast->cl ctx x))))

(defmethod sql->cl (ctx (type (eql :array)) &rest args)
  (destructuring-bind (args)
      args
    (alexandria:with-gensyms (acc-sym spread-sym)
      `(let ((,acc-sym (make-array 0 :fill-pointer 0)))
         ,@(loop for ast in args
                 collect (if (and (listp ast)
                                  (eq :spread-property (first ast)))
                             `(let* ((,spread-sym ,(ast->cl ctx (second ast)))
                                     (,spread-sym (if (fset:seq? ,spread-sym)
                                                      (fset:convert 'vector ,spread-sym)
                                                      ,spread-sym)))
                                (when (vectorp ,spread-sym)
                                  (loop for ,spread-sym across ,spread-sym
                                        do (vector-push-extend (if (characterp ,spread-sym)
                                                                   (princ-to-string ,spread-sym)
                                                                   ,spread-sym)
                                                               ,acc-sym))))
                             `(vector-push-extend ,(ast->cl ctx ast) ,acc-sym)))
         (fset:convert 'fset:seq ,acc-sym)))))

(defmethod sql->cl (ctx (type (eql :array-query)) &rest args)
  (destructuring-bind (query &key start end)
      args
    (multiple-value-bind (src projection)
        (ast->cl ctx query)
      (unless (= 1 (length projection))
        (%annotated-error-with-span (fset:lookup ctx :sql)
                                    (format nil "ARRAY query must return single column, got: ~A" (length projection))
                                    "ARRAY query must return single column"
                                    start end))
      (alexandria:with-gensyms (row-sym)
        `(fset:convert 'fset:seq (mapcar (lambda (,row-sym)
                                           (aref ,row-sym 0))
                                         ,src))))))

(defmethod sql->cl (ctx (type (eql :object)) &rest args)
  (destructuring-bind (args &key start end)
      args
    (declare (ignore start end))
    `(fset:convert
      'fset:map
      (append ,@(loop for kv in args
                      collect (case (first kv)
                                (:shorthand-property
                                 (let* ((k (second kv))
                                        (k (if (and (listp k)
                                                    (eq :parameter (first k)))
                                               (second k)
                                               k)))
                                   `(list (cons ,(%unqualified-column-name (symbol-name k))
                                                ,(ast->cl ctx (second kv))))))
                                (:computed-property
                                 `(list (cons (endb/sql/expr:syn-cast ,(ast->cl ctx (second kv)) :varchar)
                                              ,(ast->cl ctx (nth 2 kv)))))
                                (:*
                                 (let* ((k (second kv))
                                        (doc-column (%qualified-column-name (symbol-name k) "!doc"))
                                        (doc (fset:lookup ctx doc-column))
                                        (projection (fset:lookup (fset:lookup ctx :table-projections) (symbol-name k))))
                                   (cond
                                     (doc `(fset:convert 'list ,(ast->cl ctx (make-symbol doc-column))))
                                     (projection `(list ,@(loop for p in projection
                                                                collect `(cons ,(%unqualified-column-name p)
                                                                               ,(ast->cl ctx (make-symbol p))))))
                                     (t (%annotated-error (fset:lookup ctx :sql) k (format nil "Unknown table: ~A" k) "Unknown table")))))
                                (:spread-property
                                 (alexandria:with-gensyms (spread-sym idx-sym)
                                   `(let* ((,spread-sym ,(ast->cl ctx (second kv)))
                                           (,spread-sym (if (fset:seq? ,spread-sym)
                                                            (fset:convert 'vector ,spread-sym)
                                                            ,spread-sym)))
                                      (cond
                                        ((typep ,spread-sym 'endb/arrow:arrow-struct)
                                         (fset:convert 'list ,spread-sym))
                                        ((vectorp ,spread-sym)
                                         (loop for ,spread-sym across ,spread-sym
                                               for ,idx-sym from 0
                                               collect (cons (format nil "~A" ,idx-sym) (if (characterp ,spread-sym)
                                                                                            (princ-to-string ,spread-sym)
                                                                                            ,spread-sym))))))))
                                (t `(list (cons ,(if (symbolp (first kv))
                                                     (symbol-name (first kv))
                                                     (first kv))
                                                ,(ast->cl ctx (second kv)))))))))))

(defmethod sql->cl (ctx (type (eql :access)) &rest args)
  (destructuring-bind (base path &key recursive)
      args
    `(,(if (eq :seq (fset:lookup ctx :access))
           'endb/sql/expr:syn-access
           'endb/sql/expr:syn-access-finish)
      ,(ast->cl (fset:with ctx :access :seq) base)
      ,(cond
         ((member path '(:* :#)) path)
         ((symbolp path) (symbol-name path))
         (t (ast->cl (fset:less ctx :access) path)))
      ,(eq :recursive recursive))))

(defun %find-recursive-cte (ast)
  (let ((init-operator)
        (init-ast))
    (labels ((walk (ast)
               (cond
                 ((and (listp ast)
                       (member (first ast) '(:union :union-all)))
                  (if (and (listp (second ast))
                           (member (first (second ast))
                                   '(:union :union-all :intersect :except)))
                      (append (list (first ast))
                              (list (walk (second ast)))
                              (nthcdr 2 ast))
                      (progn
                        (setf init-operator (first ast))
                        (setf init-ast (second ast))
                        (let ((rest-ast (nthcdr 3 ast)))
                          (remf rest-ast :start)
                          (remf rest-ast :end)
                          (append (nth 2 ast) rest-ast)))))
                 ((and (listp ast)
                       (member (first ast) '(:intersect :except)))
                  (append (list (first ast))
                          (list (walk (second ast)))
                          (nthcdr 2 ast)))
                 (t ast))))
      (values (walk ast) init-operator init-ast))))

(defun %with-recursive->cl (ctx ctes query)
  (let* ((new-ctes (reduce
                    (lambda (acc cte)
                      (destructuring-bind (cte-name cte-ast &optional cte-columns)
                          cte
                        (unless cte-columns
                          (%annotated-error (fset:lookup ctx :sql) cte-name "WITH RECURSIVE requires named columns"))
                        (alexandria:with-gensyms (cte-varying-free-var)
                          (let ((cte (make-cte :src nil
                                               :free-vars (list cte-varying-free-var)
                                               :projection (mapcar #'symbol-name cte-columns)
                                               :ast cte-ast)))
                            (fset:with acc (symbol-name cte-name) cte)))))
                    ctes
                    :initial-value (fset:empty-map)))
         (ctx (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) new-ctes)))
         (cte-free-vars (loop for (cte-name) in ctes
                              for cte = (fset:lookup new-ctes (symbol-name cte-name))
                              collect (first (cte-free-vars cte)))))
    (multiple-value-bind (src projection)
        (ast->cl ctx query)
      (values `(let ,(loop for var in cte-free-vars
                           collect (list var 0))
                 (declare (ignorable ,@cte-free-vars))
                 (labels ,(loop for (cte-name) in ctes
                                for cte = (fset:lookup new-ctes (symbol-name cte-name))
                                collect
                                (let* ((ctx (fset:with ctx :current-cte (symbol-name cte-name)))
                                       (src (multiple-value-bind (cte-ast init-operator init-ast)
                                                (%find-recursive-cte (cte-ast cte))
                                              (if init-operator
                                                  (multiple-value-bind (src projection)
                                                      (let* ((cte-accessed)
                                                             (ctx (fset:with
                                                                   ctx
                                                                   :on-cte-access
                                                                   (cons (lambda (k)
                                                                           (when (equal k (symbol-name cte-name))
                                                                             (if cte-accessed
                                                                                 (%annotated-error (fset:lookup ctx :sql) cte-name "Non-linear recursion not supported")
                                                                                 (setf cte-accessed t))))
                                                                         (fset:lookup ctx :on-cte-access)))))
                                                        (ast->cl ctx cte-ast))
                                                    (multiple-value-bind (init-src init-projection)
                                                        (let* ((ctx (fset:with
                                                                     ctx
                                                                     :on-cte-access
                                                                     (cons (lambda (k)
                                                                             (when (equal k (symbol-name cte-name))
                                                                               (%annotated-error (fset:lookup ctx :sql) cte-name "Left recursion not supported")))
                                                                           (fset:lookup ctx :on-cte-access)))))
                                                          (ast->cl ctx init-ast))
                                                      (unless (= (length projection) (length init-projection) (length (cte-projection cte)))
                                                        (%annotated-error (fset:lookup ctx :sql) cte-name "Number of column names does not match projection"))
                                                      (alexandria:with-gensyms (acc-sym last-acc-sym cte-sym)
                                                        (let ((distinct (when (eq :union init-operator)
                                                                          :distinct)))
                                                          `(block ,cte-sym
                                                             (let* ((,last-acc-sym (endb/sql/expr:ra-distinct ,init-src ,distinct))
                                                                    (,acc-sym ,last-acc-sym))
                                                               (flet ((,(intern (symbol-name cte-name)) ()
                                                                        (incf ,(first (cte-free-vars cte)))
                                                                        ,last-acc-sym))
                                                                 (loop
                                                                   (if ,last-acc-sym
                                                                       (progn
                                                                         (setf ,last-acc-sym (endb/sql/expr:ra-distinct ,src ,distinct))
                                                                         (setf ,acc-sym (append ,acc-sym ,last-acc-sym)))
                                                                       (return-from ,cte-sym (endb/sql/expr:ra-distinct ,acc-sym ,distinct)))))))))))
                                                  (multiple-value-bind (src projection)
                                                      (let* ((ctx (fset:with
                                                                   ctx
                                                                   :on-cte-access
                                                                   (cons (lambda (k)
                                                                           (when (equal k (symbol-name cte-name))
                                                                             (%annotated-error (fset:lookup ctx :sql) cte-name "Recursion not supported without UNION / UNION ALL")))
                                                                         (fset:lookup ctx :on-cte-access)))))
                                                        (ast->cl ctx cte-ast))
                                                    (unless (= (length projection) (length (cte-projection cte)))
                                                      (%annotated-error (fset:lookup ctx :sql) cte-name "Number of column names does not match projection"))
                                                    src)))))
                                  `(,(intern (symbol-name cte-name)) () ,src)))
                   ,src))
              projection))))

(defmethod sql->cl (ctx (type (eql :with)) &rest args)
  (destructuring-bind (ctes query &key recursive)
      args
    (if recursive
        (%with-recursive->cl ctx ctes query)
        (let* ((new-ctes (reduce
                          (lambda (acc cte)
                            (destructuring-bind (cte-name cte-ast &optional cte-columns)
                                cte
                              (multiple-value-bind (src projection)
                                  (ast->cl (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) acc)) cte-ast)
                                (when (and cte-columns (not (= (length projection) (length cte-columns))))
                                  (%annotated-error (fset:lookup ctx :sql) cte-name "Number of column names does not match projection"))
                                (let ((cte (make-cte :src src
                                                     :projection (or (mapcar #'symbol-name cte-columns) projection))))
                                  (fset:with acc (symbol-name cte-name) cte)))))
                          ctes
                          :initial-value (fset:empty-map)))
               (ctx (fset:with ctx :ctes (fset:map-union (or (fset:lookup ctx :ctes) (fset:empty-map)) new-ctes))))
          (multiple-value-bind (src projection)
              (ast->cl ctx query)
            (values `(flet ,(loop for (cte-name) in ctes
                                  for cte = (fset:lookup new-ctes (symbol-name cte-name))
                                  collect `(,(intern (symbol-name cte-name)) () ,(cte-src cte)))
                       ,src)
                    projection))))))

(defun %find-expr-symbol (fn prefix)
  (find-symbol (string-upcase (concatenate 'string prefix (symbol-name fn))) :endb/sql/expr))

(defun %valid-sql-fn-call-p (ctx fn-sym fn args start end)
  (handler-case
      (let ((sql-fn (symbol-function fn-sym)))
        #+sbcl (let* ((arg-list (if (typep sql-fn 'standard-generic-function)
                                    (sb-pcl::generic-function-pretty-arglist sql-fn)
                                    (sb-impl::%fun-lambda-list sql-fn)))
                      (optional-idx (position '&optional arg-list))
                      (rest-idx (position '&rest arg-list))
                      (optional-args (if optional-idx
                                         (- (or rest-idx (length arg-list)) (1+ optional-idx))
                                         0))
                      (rest-idx (if (and optional-idx rest-idx)
                                    (1- rest-idx)
                                    rest-idx))
                      (min-args (or optional-idx rest-idx (length arg-list)))
                      (max-args (if rest-idx
                                    (max (+ min-args optional-args) (length args))
                                    (+ min-args optional-args))))
                 (unless (<= min-args (length args) max-args)
                   (if (and start end)
                       (%annotated-error-with-span (fset:lookup ctx :sql)
                                                   (format nil "Invalid number of arguments: ~A to: ~A min: ~A max: ~A"
                                                           (length args)
                                                           (string-upcase (symbol-name fn))
                                                           min-args
                                                           max-args)
                                                   "Invalid number of arguments" start end)
                       (error 'endb/sql/expr:sql-runtime-error
                              :message (format nil "Invalid number of arguments: ~A to: ~A min: ~A max: ~A"
                                               (length args)
                                               (string-upcase (symbol-name fn))
                                               min-args
                                               max-args)))))
        t)
    (undefined-function (e)
      (declare (ignore e)))))

(defmethod sql->cl (ctx (type (eql :path)) &rest args)
  (destructuring-bind (args)
      args
    `(fset:seq ,@(loop for ast in args
                       collect (cond
                                 ((and (symbolp ast)
                                       (not (keywordp ast)))
                                  (symbol-name ast))
                                 ((eq :# ast)
                                  (symbol-name ast))
                                 (t (ast->cl ctx ast)))))))

(defmethod sql->cl (ctx (type (eql :function)) &rest args)
  (destructuring-bind (fn args &key start end)
      args
    (let ((fn-sym (%find-expr-symbol fn "sql-")))
      (unless (and fn-sym (%valid-sql-fn-call-p ctx fn-sym fn args start end))
        (if (and start end)
            (%annotated-error-with-span (fset:lookup ctx :sql) (format nil "Unknown built-in function: ~A" fn)
                                        "Unknown built-in function" start (+ start (length (symbol-name fn))))
            (error 'endb/sql/expr:sql-runtime-error :message (format nil "Unknown built-in function: ~A" fn))))
      (let ((args (loop for ast in args
                        collect (ast->cl ctx ast))))
        (if (and (not (member fn-sym endb/sql/expr:+impure-functions+))
                 (not (macro-function fn-sym))
                 (every #'constantp args))
            (apply fn-sym args)
            `(,fn-sym ,@args))))))

(defmethod sql->cl (ctx (type (eql :aggregate-function)) &rest args)
  (destructuring-bind (fn args &key distinct (where :true) order-by start end)
      args
    (if (and (member fn '(:min :max))
             (> (length args) 1))
        (ast->cl ctx (cons fn args))
        (progn
          (when (fset:lookup ctx :aggregate)
            (%annotated-error-with-span (fset:lookup ctx :sql) (format nil "Cannot nest aggregate functions: ~A" (string-upcase (symbol-name fn)))
                                        "Cannot nest aggregate functions" start (+ start (length (symbol-name fn)))))
          (when (fset:lookup ctx :recursive-select)
            (%annotated-error-with-span (fset:lookup ctx :sql) (format nil "Cannot use aggregate functions in recursion: ~A" (string-upcase (symbol-name fn)))
                                        "Cannot use aggregate functions in recursion" start (+ start (length (symbol-name fn)))))
          (let ((min-args (if (eq :object_agg fn)
                              2
                              1))
                (max-args (if (member fn '(:group_concat :object_agg))
                              2
                              1))
                (args (if (eq :count-star fn)
                          (list :null)
                          args)))
            (unless (<= min-args (length args) max-args)
              (%annotated-error-with-span (fset:lookup ctx :sql)
                                          (format nil "Invalid number of arguments: ~A to: ~A min: ~A max: ~A"
                                                  (length args)
                                                  (string-upcase (symbol-name fn))
                                                  min-args
                                                  max-args)
                                          "Invalid number of arguments" start end))
            (alexandria:with-gensyms (aggregate-sym)
              (let* ((aggregate-table (fset:lookup ctx :aggregate-table))
                     (ctx (fset:with ctx :aggregate t))
                     (fn-sym (%find-expr-symbol fn "agg-"))
                     (init-src (if order-by
                                   `(endb/sql/expr:make-agg ,fn :order-by ',(loop for (nil dir) in order-by
                                                                                  for idx from (1+ (length args))
                                                                                  collect (list idx dir)))
                                   `(endb/sql/expr:make-agg ,fn :distinct ,distinct)))
                     (src (loop for ast in (append args (mapcar #'first order-by))
                                collect (ast->cl ctx ast)))
                     (where-src (ast->cl ctx where))
                     (agg (make-aggregate :src src :init-src init-src :var aggregate-sym :where-src where-src)))
                (assert fn-sym nil (format nil "Unknown aggregate function: ~A" fn))
                (setf (gethash aggregate-sym aggregate-table) agg)
                `(endb/sql/expr:agg-finish ,aggregate-sym))))))))

(defmethod sql->cl (ctx (type (eql :case)) &rest args)
  (destructuring-bind (cases-or-expr &optional cases)
      args
    (alexandria:with-gensyms (expr-sym)
      `(let ((,expr-sym ,(if cases
                             (ast->cl ctx cases-or-expr)
                             t)))
         (cond
           ,@(append (loop for (test then) in (or cases cases-or-expr)
                           collect (list (if (eq :else test)
                                             t
                                             `(eq t (endb/sql/expr:sql-= ,expr-sym ,(ast->cl ctx test))))
                                         (ast->cl ctx then)))
                     (list (list t :null))))))))

(defmethod sql->cl (ctx (type (eql :parameter)) &rest args)
  (when (fset:lookup ctx :no-parameters)
    (error 'endb/sql/expr:sql-runtime-error :message (fset:lookup ctx :no-parameters)))
  (destructuring-bind (parameter)
      args
    `(fset:lookup ,(fset:lookup ctx :param-sym)
                  ,(if (symbolp parameter)
                       (symbol-name parameter)
                       parameter))))

(defmethod sql->cl (ctx (type (eql :begin)) &rest args)
  (declare (ignore args))
  (append `(endb/sql/db:tx-begin ,(fset:lookup ctx :db-sym))))

(defun %savepoint-src (ctx savepoint)
  (list :savepoint (cond
                     ((and (symbolp savepoint)
                           (not (null savepoint)))
                      (symbol-name savepoint))
                     (savepoint (ast->cl ctx savepoint))
                     (t `(endb/sql/expr:sql-uuid)))))

(defmethod sql->cl (ctx (type (eql :savepoint)) &rest args)
  (append `(endb/sql/db:tx-begin ,(fset:lookup ctx :db-sym))
          (%savepoint-src ctx (first args))))

(defmethod sql->cl (ctx (type (eql :release)) &rest args)
  (destructuring-bind (savepoint)
      args
    (append `(endb/sql/db:tx-commit ,(fset:lookup ctx :db-sym))
            (%savepoint-src ctx savepoint))))

(defmethod sql->cl (ctx (type (eql :commit)) &rest args)
  (declare (ignore args))
  (append `(endb/sql/db:tx-commit ,(fset:lookup ctx :db-sym))))

(defmethod sql->cl (ctx (type (eql :rollback)) &rest args)
  (append `(endb/sql/db:tx-rollback ,(fset:lookup ctx :db-sym))
          (when args
            (%savepoint-src ctx (first args)))))

(defmethod sql->cl (ctx fn &rest args)
  (sql->cl ctx :function fn args))

(defun %ast-function-call-p (ast)
  (and (listp ast)
       (keywordp (first ast))
       (not (member (first ast) '(:null :true :false)))))

(defun ast->cl (ctx ast)
  (cond
    ((eq :true ast) t)
    ((eq :false ast) nil)
    ((eq :current_date ast)
     `(endb/sql/db:syn-current_date ,(fset:lookup ctx :db-sym)))
    ((eq :current_time ast)
     `(endb/sql/db:syn-current_time ,(fset:lookup ctx :db-sym)))
    ((eq :current_timestamp ast)
     `(endb/sql/db:syn-current_timestamp ,(fset:lookup ctx :db-sym)))
    ((%ast-function-call-p ast)
     (apply #'sql->cl ctx ast))
    ((and (symbolp ast)
          (not (keywordp ast)))
     (let* ((k (symbol-name ast))
            (v (fset:lookup ctx k)))
       (if v
           (progn
             (dolist (cb (fset:lookup ctx :on-var-access))
               (funcall cb ctx k v))
             (if (get v :functionp)
                 `(funcall ,v)
                 v))
           (let* ((idx (position #\. k)))
             (if idx
                 (let ((column (subseq k 0 idx))
                       (path (subseq k (1+ idx))))
                   (if (fset:lookup ctx column)
                       (ast->cl ctx (list :access (make-symbol column) path))
                       (%annotated-error (fset:lookup ctx :sql) ast (format nil "Unknown column: ~A" ast) "Unknown column")))
                 (%annotated-error (fset:lookup ctx :sql) ast (format nil "Unknown column: ~A" ast) "Unknown column"))))))
    (t (progn
         (assert (not (listp ast)))
         ast))))

(defun %ast->cl-with-free-vars (ctx ast)
  (let* ((vars ())
         (ctx (fset:with
               ctx
               :on-var-access
               (cons (lambda (inner-ctx k v)
                       (declare (ignore inner-ctx))
                       (when (eq v (fset:lookup ctx k))
                         (pushnew v vars)))
                     (fset:lookup ctx :on-var-access)))))
    (multiple-value-bind (src projection)
        (ast->cl ctx ast)
      (values src projection vars))))

(defparameter +interpreter-from-limit+ 10)

(defun %interpretp (ast)
  (when (listp ast)
    (case (first ast)
      ((:create-table :create-index :drop-table :drop-view :create-assertion :drop-assertion :begin :commit :rollback) t)
      (:insert (member (first (third ast)) '(:values :objects)))
      (:select (let ((from (getf ast :from)))
                 (or (> (length from)
                        +interpreter-from-limit+)
                     (null from)))))))

(defun compile-sql (ctx ast &optional parameters)
  (endb/lib:log-debug "~A" ast)
  (alexandria:with-gensyms (db-sym index-sym param-sym)
    (let* ((ctx (fset:map-union ctx (fset:map (:db-sym db-sym)
                                              (:index-sym index-sym)
                                              (:param-sym param-sym)))))
      (multiple-value-bind (src projection)
          (ast->cl ctx ast)
        (endb/lib:log-debug "~A" src)
        (let* ((src (if projection
                        `(values ,src ',projection)
                        src))
               (src `(lambda (,db-sym &optional ,param-sym)
                       (declare (optimize (speed 3) (safety 0) (debug 0) (compilation-speed 3)))
                       (declare (ignorable ,db-sym ,param-sym))
                       (unless (fset:equal? (fset:convert 'fset:set ',parameters)
                                            (fset:domain ,param-sym))
                         (error 'endb/sql/expr:sql-runtime-error :message (format nil "Required parameters: ~A does not match given: ~A"
                                                                                  (fset:convert 'list (fset:convert 'fset:set ',parameters))
                                                                                  (or (fset:convert 'list (fset:domain ,param-sym)) "()"))))
                       (let ((,index-sym (make-hash-table :test endb/sql/expr:+hash-table-test+)))
                         (declare (ignorable ,index-sym))
                         ,src)))
               (cachep (not (%interpretp ast))))
          (values
           #+sbcl (let ((sb-ext:*evaluator-mode* (if (%interpretp ast)
                                                     :interpret
                                                     sb-ext:*evaluator-mode*)))
                    (eval src))
           #-sbcl (eval src)
           cachep))))))
