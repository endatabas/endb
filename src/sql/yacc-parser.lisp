(defpackage :endb/sql/yacc-parser
  (:use :cl :yacc)
  (:export #:parse-sql)
  (:import-from :yacc)
  (:import-from :ppcre))
(in-package :endb/sql/yacc-parser)

(defvar *tokens*)
(setf *tokens* (list (cons nil (ppcre:create-scanner "^\\s+"))
                     (cons nil (ppcre:create-scanner "^--[^\n\r]*\r?(\n|$)"))
                     (cons 'id (ppcre:create-scanner "^(?i)[a-z_]([a-z_]|\\d)*"))
                     (cons 'op (ppcre:create-scanner "^[-*/%+<>=|]+"))
                     (cons 'sep (ppcre:create-scanner "^[,().;]"))
                     (cons 'flt (ppcre:create-scanner "^\\d+\\.\\d+"))
                     (cons 'int (ppcre:create-scanner "^\\d+"))
                     (cons 'str (ppcre:create-scanner "^'([^']|\\')+'"))))

(defvar *kw-table* (make-hash-table :test 'equal))

(dolist (kw '("SELECT" "ALL" "DISTINCT" "AS" "FROM" "WHERE" "VALUES"
              "ORDER" "BY" "ASC" "DESC" "GROUP" "HAVING"
              "NULL" "TRUE" "FALSE"
              "CREATE" "TABLE" "INSERT" "INTO"
              "CASE" "WHEN" "THEN" "ELSE" "END"
              "AND" "OR" "NOT" "EXISTS" "BETWEEN" "IS"
              "UNION" "EXCEPT" "INTERSECT"))
  (setf (gethash kw *kw-table*) (intern kw :keyword)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun %i2p (a b c)
    (list b a c))

  (defun %k-2-3 (a b c)
    (declare (ignore a c))
    b)

  (defun %rcons3 (a b c)
    (declare (ignore b))
    (append a (list c)))

  (defun %del-2 (a b c)
    (declare (ignore b))
    (list a c)))

(yacc:define-parser *sql-parser*
  (:start-symbol sql-stmt)
  (:terminals (id flt int str :* :+ :- :/ :% :< :> :<= :>= := :<> :|,| :|(| :|)|
                                 :select :all :distinct :as :from :where :values
                              :order :by :asc :desc :group :having
                              :null :true :false
                                 :create :table :insert :into
                                 :case :when :then :else :end
                              :and :or :not :exists :between :is
                              :union :except :intersect))
  (:precedence ((:left :* :/ :%) (:left :+ :-)))

  (expr (expr :+ expr #'%i2p)
        (expr :- expr #'%i2p)
        (expr :* expr #'%i2p)
        (expr :/ expr #'%i2p)
        (expr :% expr #'%i2p)
        term)

  (term id int flt str
        :null :true :false
        (:- term)
        (:|(| expr :|)| #'%k-2-3))

  (id-list (id)
           (id-list :|,| id #'%rcons3)
           ())

  (expr-list (expr)
             (expr-list :|,| expr #'%rcons3))

  (select-list-element
   (:*)
   (expr)
   (expr :as id #'%del-2)
   (expr id))

  (select-list (select-list-element)
               (select-list :|,| select-list-element  #'%rcons3))

  (table-list-element
   (id)
   (id :as id #'%del-2)
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
   (:group :by id-list (lambda (group by id-list)
                         (declare (ignore group by))
                         (list :group-by id-list)))
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
   (:order :by order-by-list (lambda (order by order-by-list)
                               (declare (ignore order by))
                               (list :order-by order-by-list)))
   ())

  (all-distinct
   :distinct
   :all
   ())

  (select-stmt
   (:values :|(| expr-list :|)|
            (lambda (values lb expr-list rb)
              (declare (ignore values lb rb))
              (list :values expr-list)))

   (:select all-distinct select-list from where group-by having order-by
            (lambda (select all-distinct select-list from where group-by having order-by)
              (declare (ignore select))
              (append (list :select select-list :distinct (eq :distinct all-distinct))
                      from where group-by having order-by))))
  (insert-stmt
   (:insert :into id :|(| id-list :|)| select-stmt
            (lambda (insert into id lb id-list rb select-stmt)
              (declare (ignore insert into lb rb))
              (list :insert id select-stmt :column-names id-list)))

   (:insert :into id select-stmt
            (lambda (insert into id select-stmt)
              (declare (ignore insert into))
              (list :insert id select-stmt))))

  (col-def (id id))

  (col-def-list (col-def)
                (col-def-list :|,| col-def #'%rcons3))

  (create-table-stmt (:create :table id :|(| col-def-list :|)|
                              (lambda (create table id lb col-def-list rb)
                                (declare (ignore create table lb rb))
                                (list :create-table id col-def-list))))

  (create-index-stmt (:create :index id :on id :|(| order-by-list :|)|
                              (lambda (create index id1 on id2 lb order-by-list rb)
                                (declare (ignore create index on lb rb))
                                (list :create-index id1 id2 order-by-list))))

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
                                         (string-upcase token)
                                         token))
                              (kw (gethash token *kw-table*)))
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

;; (time (dotimes (n 10000)
;;         (let ((lex (make-lexer "SELECT a+b*2+c*3+d*4+e*5,
;;        (a+b+c+d+e)/5
;;   FROM t1
;;  ORDER BY 1,2")))
;;           (yacc:parse-with-lexer lex *sql-parser*))))
