(defpackage :endb/sql/parser
  (:use :cl :yacc)
  (:export #:parse-sql)
  (:import-from :yacc)
  (:import-from :cl-ppcre))
(in-package :endb/sql/parser)

(defvar *tokens*)
(setf *tokens* (list (cons nil (ppcre:create-scanner "^\\s+"))
                     (cons nil (ppcre:create-scanner "^--[^\n\r]*\r?(\n|$)"))
                     (cons 'id (ppcre:create-scanner "^(?i)[a-z_]([a-z_.]|\\d)*"))
                     (cons 'op (ppcre:create-scanner "^[-*/%+<>=|]+"))
                     (cons 'sep (ppcre:create-scanner "^[,().;]"))
                     (cons 'flt (ppcre:create-scanner "^\\d+\\.\\d+"))
                     (cons 'int (ppcre:create-scanner "^\\d+"))
                     (cons 'str (ppcre:create-scanner "^'([^']|\\')+'"))))

(defvar *kw-table* (make-hash-table :test 'equal))
(clrhash *kw-table*)

(dolist (kw '("SELECT" "ALL" "DISTINCT" "AS" "FROM" "WHERE" "VALUES"
              "ORDER" "BY" "ASC" "DESC" "GROUP" "HAVING" "LIMIT" "OFFSET"
              "NULL" "TRUE" "FALSE"
              "CREATE" "TABLE" "INDEX" "ON" "INSERT" "INTO"
              "CASE" "WHEN" "THEN" "ELSE" "END"
              "AND" "OR" "NOT" "EXISTS" "BETWEEN" "IS" "IN"
              "UNION" "EXCEPT" "INTERSECT"
              "COUNT" "AVG" "SUM" "MIN" "MAX"))
  (setf (gethash kw *kw-table*) (intern kw :keyword)))

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
    (append a (list b)))

  (defun %rcons3 (a b c)
    (declare (ignore b))
    (append a (list c)))

  (defun %extract (&rest items)
    (lambda (&rest list)
      (loop for item in items
            if (integerp item)
              collect (nth item list)
            else
              collect item))))

(yacc:define-parser *sql-parser*
  (:start-symbol sql-stmt)
  (:terminals (id flt int str :* :+ :- :/ :|| :% :< :> :<= :>= := :<> :|,| :|(| :|)| :|.|
                                 :select :all :distinct :as :from :where :values
                              :order :by :asc :desc :group :having :limit :offset
                              :null :true :false
                                 :create :table :index :on :insert :into
                                 :case :when :then :else :end
                              :and :or :not :exists :between :is :in
                                 :union :except :intersect
                              :count :avg :sum :min :max))
  ;;  (:muffle-conflicts (1 0))
  (:precedence ((:left :||) (:left :* :/ :%) (:left :+ :-)
                (:left :in)
                (:left :< :<= :> :>=)
                (:left := :<> :is)
                (:right :between :not)
                (:left :and)
                (:left :or)
                (:right :exists)))

  (expr-not
   (expr :not))

  (in-expr
   (expr :in subquery #'%i2p)
   (expr-not :in subquery (lambda (expr subquery)
                            (declare (ignore))
                            (list :not (list :in (first expr) subquery))))
   (expr :in :|(| expr-list :|)| (%extract :in 0 3))
   (expr-not :in :|(| expr-list :|)| (lambda (expr lb expr-list rb)
                                       (declare (ignore lb rb))
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
   id
   int
   flt
   (:- between-term))

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
   :max)

  (function-expr
   (:count :|(| :* :|)| (%extract :aggregate-function :count-star ()))
   (:count :|(| all-distinct expr :|)| (lambda (count lb all-distinct expr rb)
                                         (declare (ignore count lb rb))
                                         (append (list :aggregate-function :count (list expr))
                                                 (when (eq :distinct all-distinct)
                                                   (list :distinct t)))))
   (aggregate-fn :|(| all-distinct expr-list :|)| (lambda (id lb all-distinct expr-list rb)
                                                    (declare (ignore lb rb))
                                                    (append (list :aggregate-function id expr-list)
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
            (append (list :case)
                    (when base-expr
                      base-expr)
                    (list (append case-when-list (list case-else-expr)))))))

  (scalar-subquery
   (subquery (%extract :scalar-subquery 0)))

  (expr (expr :+ expr #'%i2p)
        (expr :- expr #'%i2p)
        (expr :* expr #'%i2p)
        (expr :/ expr #'%i2p)
        (expr :% expr #'%i2p)
        (expr :< expr #'%i2p)
        (expr :<= expr #'%i2p)
        (expr :> expr #'%i2p)
        (expr :>= expr #'%i2p)
        (expr := expr #'%i2p)
        (expr :<> expr #'%i2p)
        (expr :and expr #'%i2p)
        (expr :or expr #'%i2p)
        (:not expr)
        function-expr
        between-expr
        is-expr
        in-expr
        exists-expr
        case-expr
        scalar-subquery
        term)

  (term id
        int
        flt
        str
        :null
        :true
        :false
        (:- term)
        (:+ term)
        (:|(| expr :|)| #'%k-2-3))

  (id-list (id)
           (id-list :|,| id #'%rcons3)
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

  (table-list-element
   (id)
   (id :as id (%extract 0 2))
   (id id))

  (table-list
   (table-list-element)
   (table-list :|,| table-list-element #'%rcons3))

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
   (int order-by-direction))

  (order-by-list
   (order-by-element)
   (order-by-list :|,| order-by-element #'%rcons3))

  (order-by
   (:order :by order-by-list (%extract :order-by 2))
   ())

  (offset
   (:offset int)
   (:|,| int (%extract :offset 1))
   ())

  (limit
   (:limit int offset (lambda (limit int offset)
                        (declare (ignore limit))
                        (append (list :limit int) offset)))
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
              (append (list :select select-list)
                      (when (eq :distinct all-distinct)
                        (list :distinct t))
                      from where group-by having))))

  (compound-select-stmt
   (compound-select-stmt :union :all select-core (%extract :union-all 0 3))
   (compound-select-stmt :union select-core #'%i2p)
   (compound-select-stmt :intersect select-core #'%i2p)
   (compound-select-stmt :except select-core #'%i2p)
   select-core)

  (select-stmt (compound-select-stmt order-by limit #'append))

  (subquery (:|(| select-stmt :|)| #'%k-2-3))

  (insert-stmt
   (:insert :into id :|(| id-list :|)| select-stmt (%extract :insert 2 6 :column-names 4))

   (:insert :into id select-stmt (%extract :insert 2 3)))

  (opt-primary-key
   (id id)
   ())

  (col-type
   (id)
   (id :|(| int :|)|))

  (col-def (id col-type opt-primary-key (lambda (&rest list)
                                          (first list))))

  (col-def-list (col-def)
                (col-def-list :|,| col-def #'%rcons3))

  (create-index-stmt (:create :index id :on id :|(| order-by-list :|)| (%extract :create-index)))

  (create-table-stmt (:create :table id :|(| col-def-list :|)| (%extract :create-table 2 4)))

  (sql-stmt insert-stmt select-stmt create-table-stmt create-index-stmt))

(defun make-lexer (in)
  (let ((idx 0))
    (lambda ()
      (when (< idx (length in))
        (loop for (token-name . token-scanner) in *tokens*
              do (multiple-value-bind (start end)
                     (ppcre:scan token-scanner in :start idx)
                   (when start
                     (setf idx end)
                     (when token-name
                       (let* ((token (subseq in start end))
                              (token (if (eq 'id token-name)
                                         (string-downcase token)
                                         token))
                              (kw (gethash (string-upcase token) *kw-table*)))
                         (return (if kw
                                     (values kw kw)
                                     (let ((token (case token-name
                                                    (str (ppcre:regex-replace-all "\\'" (subseq token 1 (1- (length token))) "'"))
                                                    (id (make-symbol token))
                                                    ((op sep) (intern token :keyword))
                                                    (int (parse-integer token))
                                                    (flt (let ((*read-eval* nil))
                                                           (read-from-string token)))
                                                    (t token))))
                                       (values (if (keywordp token)
                                                   token
                                                   token-name)
                                               (unless (eq 'sep token-name)
                                                 token))))))))))))))

(defun parse-sql (in)
  (yacc:parse-with-lexer (make-lexer in) *sql-parser*))
