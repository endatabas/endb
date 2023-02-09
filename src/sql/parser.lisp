(defpackage :endb/sql/parser
  (:use :cl :yacc)
  (:export #:parse-sql)
  (:import-from :yacc)
  (:import-from :cl-ppcre))
(in-package :endb/sql/parser)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun %i2p (a b c)
    (list b a c))

  (defun %k-2-2 (a b)
    (declare (ignore a))
    b)

  (defun %k-2-3 (a b c)
    (declare (ignore a c))
    b)

  (defun %rcons2 (a b)
    (nconc a (list b)))

  (defun %rcons3 (a b c)
    (declare (ignore b))
    (nconc a (list c)))

  (defun %list-1 (&rest list)
    (first list))

  (defun %extract (&rest items)
    (lambda (&rest list)
      (loop for item in items
            if (integerp item)
              collect (nth item list)
            else
              collect item))))

(yacc:define-parser *sql-parser*
  (:start-symbol sql-stmt)
  (:terminals (id float integer string binary
                  :>> :<< :* :+ :- :/ :|| :% :< :> :<= :>= := :<> :|,| :|(| :|)| :|.|
                  :select :all :distinct :as :from :where :values
                  :order :by :asc :desc :group :having :limit :offset
                  :null :true :false :cross :join
                  :create :table :index :on :insert :into :unique :delete :drop :view :if
                  :temp :temporary :replace :update :set :indexed
                  :case :when :then :else :end
                  :and :or :not :exists :between :is :in :cast
                  :union :except :intersect
                  :count :avg :sum :min :max :total :group_concat))
  (:precedence ((:left :||)
                (:left :* :/ :%)
                (:left :+ :-)
                (:left :<< :>>)
                (:left :< :<= :> :>=)
                (:left :is :between :in :<> :=)
                (:right :not)
                (:left :and)
                (:left :or)))

  (expr-not
   (expr :not))

  (subquery-or-table
   id
   subquery)

  (in-expr
   (expr :in subquery-or-table (%extract :in-query 0 2))
   (expr-not :in subquery-or-table (lambda (expr in subquery)
                                     (declare (ignore in))
                                     (list :not (list :in-query (first expr) subquery))))
   (expr :in :|(| opt-expr-list :|)| (%extract :in 0 3))
   (expr-not :in :|(| opt-expr-list :|)| (lambda (expr in lp expr-list rp)
                                           (declare (ignore in lp rp))
                                           (list :not (list :in (first expr) expr-list)))))

  (is-expr
   (expr :is expr (lambda (expr-1 is expr-2)
                    (declare (ignore is))
                    (if (and (listp expr-2)
                             (eq :not (first expr-2)))
                        (list :not (list :is expr-1 (second  expr-2)))
                        (list :is expr-1 expr-2)))))

  (between-term
   (between-term :+ between-term #'%i2p)
   (between-term :- between-term #'%i2p)
   (between-term :* between-term #'%i2p)
   (between-term :/ between-term #'%i2p)
   (between-term :% between-term #'%i2p)
   cast-expr
   function-expr
   case-expr
   id
   integer
   float
   :null
   (:- between-term)
   (:+ between-term)
   (:|(| between-term :|)| #'%k-2-3))

  (between-and-expr
   (between-term :and between-term #'%i2p))

  (between-expr
   (expr :between between-and-expr (lambda (expr between between-and-expr)
                                     (declare (ignore between))
                                     (list :between expr (second between-and-expr) (third between-and-expr))))
   (expr-not :between between-and-expr (lambda (expr between between-and-expr)
                                         (declare (ignore between))
                                         (list :not (list :between (first expr) (second between-and-expr) (third between-and-expr))))))

  (exists-expr
   (:exists subquery))

  (aggregate-fn
   :avg
   :sum
   :min
   :max
   :total
   :group_concat)

  (function-expr
   (:count :|(| all-distinct :* :|)| (lambda (count lp all-distinct star rp)
                                       (declare (ignore count lp star rp))
                                       (nconc (list :aggregate-function :count-star ())
                                              (when (eq :distinct all-distinct)
                                                (list :distinct t)))))
   (:count :|(| all-distinct expr :|)| (lambda (count lp all-distinct expr rp)
                                         (declare (ignore count lp rp))
                                         (nconc (list :aggregate-function :count (list expr))
                                                (when (eq :distinct all-distinct)
                                                  (list :distinct t)))))
   (aggregate-fn :|(| all-distinct expr-list :|)| (lambda (id lp all-distinct expr-list rp)
                                                    (declare (ignore lp rp))
                                                    (nconc (list :aggregate-function id expr-list)
                                                           (when (eq :distinct all-distinct)
                                                             (list :distinct t)))))
   (id :|(| expr-list :|)| (%extract :function 0 2)))

  (case-when-list-element
   (:when expr :then expr (%extract 1 3)))

  (case-when-list
   (case-when-list-element)
   (case-when-list case-when-list-element #'%rcons2))

  (case-else-expr
   (:else expr)
   ())

  (case-base-expr
   (expr)
   ())

  (case-expr
   (:case case-base-expr case-when-list case-else-expr :end
          (lambda (case base-expr case-when-list case-else-expr end)
            (declare (ignore case end))
            (nconc (list :case)
                   (when base-expr
                     base-expr)
                   (list (nconc case-when-list
                                (when case-else-expr
                                  (list case-else-expr))))))))

  (scalar-subquery
   (subquery (%extract :scalar-subquery 0)))

  (cast-expr
   (:cast :|(| expr :as id :|)| (%extract :cast 2 4)))

  (expr-not
   (expr :not))

  (not-null-expr
   (expr-not :null (lambda (expr-not null)
                     (declare (ignore null))
                     (list :not (list :is :null (first expr-not))))))

  (expr (expr :* expr #'%i2p)
        (expr :/ expr #'%i2p)
        (expr :% expr #'%i2p)
        (expr :+ expr #'%i2p)
        (expr :- expr #'%i2p)
        (expr :<< expr #'%i2p)
        (expr :>> expr #'%i2p)
        (expr :< expr #'%i2p)
        (expr :<= expr #'%i2p)
        (expr :> expr #'%i2p)
        (expr :>= expr #'%i2p)
        (expr := expr #'%i2p)
        (expr :<> expr #'%i2p)
        (:not expr)
        (expr :and expr #'%i2p)
        (expr :or expr #'%i2p)
        not-null-expr
        cast-expr
        function-expr
        between-expr
        is-expr
        in-expr
        exists-expr
        case-expr
        scalar-subquery
        (:- expr)
        (:+ expr)
        term)

  (term id
        integer
        float
        string
        binary
        :null
        :true
        :false
        (:|(| expr :|)| #'%k-2-3))

  (id-list (id)
           (id-list :|,| id #'%rcons3)
           ())

  (opt-expr-list
   expr-list
   ())

  (expr-list (expr)
             (expr-list :|,| expr #'%rcons3))

  (select-list-element
   (:*)
   (expr)
   (expr :as id (%extract 0 2))
   (expr id))

  (select-list (select-list-element)
               (select-list :|,| select-list-element #'%rcons3))

  (opt-not-indexed
   (:not :indexed)
   ())

  (table-or-subquery
   (id opt-not-indexed #'%list-1)
   subquery)

  (table-list-element
   (table-or-subquery)
   (table-or-subquery :as id (%extract 0 2))
   (table-or-subquery id))

  (table-list-separator
   (:|,|)
   (:cross :join))

  (table-list
   (table-list-element)
   (:|(| table-list-element :cross :join table-list-element :|)| (%extract 1 4))
   (table-list-element :join table-list-element :on expr (lambda (table-1 join table-2 on expr)
                                                           (declare (ignore join on))
                                                           (list (list :join table-1 table-2 :on expr))))
   (table-list table-list-separator table-list-element #'%rcons3))

  (from
   (:from table-list)
   ())

  (where
   (:where expr)
   ())

  (group-by
   (:group :by id-list (%extract :group-by 2))
   ())

  (having
   (:having expr)
   ())

  (order-by-direction
   :asc
   :desc
   ())

  (order-by-element
   (id order-by-direction)
   (integer order-by-direction))

  (order-by-list
   (order-by-element)
   (order-by-list :|,| order-by-element #'%rcons3))

  (order-by
   (:order :by order-by-list (%extract :order-by 2))
   ())

  (offset
   (:offset integer)
   (:|,| integer (%extract :offset 1))
   ())

  (limit
   (:limit integer offset (lambda (limit integer offset)
                            (declare (ignore limit))
                            (nconc (list :limit integer) offset)))
   ())

  (all-distinct
   :distinct
   :all
   ())

  (values-row
   (:|(| expr-list :|)| #'%k-2-3))

  (values-row-list
   (values-row)
   (values-row-list :|,| values-row #'%rcons3))

  (select-core
   (:values values-row-list (%extract :values 1))

   (:select all-distinct select-list from where group-by having
            (lambda (select all-distinct select-list from where group-by having)
              (declare (ignore select))
              (nconc (list :select select-list)
                     (when (eq :distinct all-distinct)
                       (list :distinct t))
                     from where group-by having))))

  (compound-select-stmt
   (compound-select-stmt :union :all select-core (%extract :union-all 0 3))
   (compound-select-stmt :union select-core #'%i2p)
   (compound-select-stmt :intersect select-core #'%i2p)
   (compound-select-stmt :except select-core #'%i2p)
   select-core)

  (select-stmt (compound-select-stmt order-by limit #'nconc))

  (subquery (:|(| select-stmt :|)| #'%k-2-3))

  (opt-or-replace
   (:or :replace)
   ())

  (insert-stmt
   (:insert opt-or-replace :into id :|(| id-list :|)| select-stmt (%extract :insert 3 7 :column-names 5))

   (:insert opt-or-replace :into id select-stmt (%extract :insert 3 4)))

  (delete-stmt
   (:delete :from id :where expr (%extract :delete 2 4)))

  (update-col
   (id := expr (%extract 0 2)))

  (update-col-list (update-col)
                   (update-col-list :|,| update-col #'%rcons3))

  (update-stmt
   (:update id :set update-col-list where (lambda (update id set update-col-list where)
                                            (declare (ignore update set))
                                            (append (list :update id update-col-list) where))))

  (opt-primary-key
   (id id)
   ())

  (col-type
   (id)
   (id :|(| integer :|)|))

  (col-def (id col-type opt-primary-key opt-unique #'%list-1))

  (col-def-list (col-def)
                (col-def-list :|,| col-def #'%rcons3))

  (opt-unique
   (:unique)
   ())

  (create-index-stmt (:create opt-unique :index id :on id :|(| order-by-list :|)| (%extract :create-index)))

  (drop-index-stmt (:drop :index id (%extract :drop-index)))

  (create-table-stmt (:create :table id :|(| col-def-list :|)| (%extract :create-table 2 4)))

  (opt-if-exists
   (:if :exists)
   ())

  (drop-table-stmt (:drop :table opt-if-exists id (%extract :drop-table 3 :if-exists 2)))

  (drop-view-stmt (:drop :view opt-if-exists id (%extract :drop-view 3 :if-exists 2)))

  (opt-temp
   (:temp)
   (:temporary)
   ())

  (create-view-stmt (:create opt-temp :view id :as select-stmt (%extract :create-view 3 5)))

  (sql-stmt insert-stmt delete-stmt update-stmt select-stmt create-table-stmt drop-table-stmt create-index-stmt drop-index-stmt create-view-stmt drop-view-stmt))

(defparameter *kw-table* (make-hash-table :test 'equalp))

(dolist (kw '("SELECT" "ALL" "DISTINCT" "AS" "FROM" "WHERE" "VALUES"
              "ORDER" "BY" "ASC" "DESC" "GROUP" "HAVING" "LIMIT" "OFFSET"
              "NULL" "TRUE" "FALSE" "CROSS" "JOIN"
              "CREATE" "TABLE" "INDEX" "ON" "INSERT" "INTO" "UNIQUE" "DELETE" "DROP" "VIEW" "IF"
              "TEMP" "TEMPORARY" "REPLACE" "UPDATE" "SET" "INDEXED"
              "CASE" "WHEN" "THEN" "ELSE" "END"
              "AND" "OR" "NOT" "EXISTS" "BETWEEN" "IS" "IN" "CAST"
              "UNION" "EXCEPT" "INTERSECT"
              "COUNT" "AVG" "SUM" "MIN" "MAX" "TOTAL" "GROUP_CONCAT"))
  (setf (gethash kw *kw-table*) (intern kw :keyword)))

(defvar *comma* (intern "," :keyword))
(defvar *left-brace* (intern "(" :keyword))
(defvar *right-brace* (intern ")" :keyword))
(defvar *period* (intern "." :keyword))

(defvar *plus* (intern "+" :keyword))
(defvar *minus* (intern "-" :keyword))
(defvar *div* (intern "/" :keyword))
(defvar *mul* (intern "*" :keyword))
(defvar *mod* (intern "%" :keyword))

(defvar *eq* (intern "=" :keyword))
(defvar *lt* (intern "<" :keyword))
(defvar *gt* (intern ">" :keyword))

(defvar *ls* (intern "<<" :keyword))
(defvar *rs* (intern ">>" :keyword))

(defvar *ne* (intern "<>" :keyword))
(defvar *lte* (intern "<=" :keyword))
(defvar *gte* (intern ">=" :keyword))
(defvar *concat* (intern "||" :keyword))

(defvar *string-scanner* (ppcre:create-scanner "^'([^']|\\')*?'"))
(defvar *hex-scanner* (ppcre:create-scanner "^'[0-9a-fA-F]*?'"))
(defvar *number-scanner* (ppcre:create-scanner "^\\d+(\\.\\d+)?"))
(defvar *id-scanner* (ppcre:create-scanner "^(?i)[a-z_]([a-z_.]|\\d)*"))

(defun make-sql-lexer (in)
  (let ((idx 0))
    (labels ((parse-string (start-idx)
               (multiple-value-bind (start end)
                   (ppcre:scan *string-scanner* in :start start-idx)
                 (when start
                   (setf idx end)
                   (let* ((token (ppcre:regex-replace-all "\\'" (subseq in (1+ start) (1- end)) "'")))
                     (values 'string token)))))
             (parse-hex-string (start-idx)
               (multiple-value-bind (start end)
                   (ppcre:scan *hex-scanner* in :start start-idx)
                 (when start
                   (setf idx end)
                   (values 'binary (subseq in (1+ start) (1- end))))))
             (parse-number (start-idx)
               (multiple-value-bind (start end groups)
                   (ppcre:scan *number-scanner* in :start start-idx)
                 (when start
                   (setf idx end)
                   (if (aref groups 0)
                       (let ((*read-eval* nil)
                             (*read-default-float-format* 'double-float))
                         (values 'float (read-from-string (subseq in start end))))
                       (values 'integer (parse-integer (subseq in start end)))))))
             (parse-id-or-keyword (start-idx)
               (multiple-value-bind (start end)
                   (ppcre:scan *id-scanner* in :start start-idx)
                 (when start
                   (setf idx end)
                   (let* ((token (subseq in start end))
                          (kw (gethash token *kw-table*)))
                     (if kw
                         (values kw kw)
                         (values 'id (make-symbol token))))))))
      (lambda ()
        (loop
          while (< idx (length in))
          for start-idx = idx
          for c = (char in start-idx)
          do (incf idx)
          unless (member c '(#\Space #\Tab #\Newline))
            return (case c
                     (#\, *comma*)
                     (#\( *left-brace*)
                     (#\) *right-brace*)
                     (#\. *period*)
                     (#\- (values *minus* *minus*))
                     (#\+ (values *plus* *plus*))
                     (#\* (values *mul* *mul*))
                     (#\/ (values *div* *div*))
                     (#\% (values *mod* *mod*))
                     (#\= (values *eq* *eq*))
                     (#\< (if (< idx (length in))
                              (case (char in idx)
                                (#\= (incf idx) (values *lte* *lte*))
                                (#\> (incf idx) (values *ne* *ne*))
                                (#\< (incf idx) (values *ls* *ls*))
                                (t (values *lt* *lt*)))
                              (values *lt* *lt*)))
                     (#\> (if (< idx (length in))
                              (case (char in idx)
                                (#\= (incf idx) (values *gte* *gte*))
                                (#\> (incf idx) (values *rs* *rs*))
                                (t (values *gt* *gt*)))
                              (values *gt* *gt*)))
                     (#\| (when (and (< idx (length in))
                                     (eq #\| (char in idx)))
                            (incf idx)
                            (values *concat* *concat*)))
                     (#\' (parse-string start-idx))
                     ((#\0 #\1 #\2 #\3 #\4 #\5 #\6 #\7 #\8 #\9)
                      (parse-number start-idx))
                     (t (if (and (char-equal #\X c)
                                 (< idx (length in))
                                 (eq #\' (char in idx)))
                            (parse-hex-string idx)
                            (parse-id-or-keyword start-idx)))))))))

(defun parse-sql (in)
  (yacc:parse-with-lexer (make-sql-lexer in) *sql-parser*))
