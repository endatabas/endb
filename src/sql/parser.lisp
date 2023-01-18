(defpackage :endb/sql/parser
  (:use :cl :esrap)
  (:export #:parse-sql)
  (:import-from :esrap))
(in-package :endb/sql/parser)

(defun %flatten-ast (name items)
  (if (and (listp items) (listp (rest items)))
      (let ((items (remove-if (lambda (x)
                                (or (null x)
                                    (case x
                                      ("(" x)
                                      (")" x)
                                      ("," x)
                                      ("." x))))
                              (mapcar (lambda (item)
                                        (%flatten-ast nil item))
                                      items))))
        (cond
          ((= 1 (length items))
           (first items))
          ((and name (not (keywordp (first items))))
           (cons name items))
          (t items)))
      items))

(defun %flatten-list (name items)
  (list name (if (= 1 (length items))
                 items
                 (cons (first items)
                       (remove-if (lambda (x)
                                    (or (null x) (eq "," x)))
                                  (apply #'concatenate 'list (second items)))))))

(defrule ws
    (+ (or #\Space #\Tab #\Newline))
  (:constant nil))

(defrule identifier
    (and (alpha-char-p character) (* (alphanumericp character)))
  (:text t)
  (:lambda (items)
    (make-symbol items)))

(defrule numeric-literal
    (+ (digit-char-p character))
  (:text t)
  (:lambda (items)
    (parse-integer items)))

(defrule string-literal
    (and "'" (* (not "'")) "'")
  (:text t)
  (:lambda (items)
    (subseq items 1 (1- (length items)))))

(defrule null-literal
    (~ "NULL")
  (:constant :sql/null))

(defrule literal-value
    (or numeric-literal
        string-literal
        null-literal))

(defrule %expr-or
    (and expr-or ws (~ "OR") ws expr-and)
  (:destructure (expr-1 ws1 or ws2 expr-2)
    (declare (ignore ws1 or ws2))
    (list :expr-or expr-1 expr-2)))

(defrule expr-or
    (or %expr-or expr-and))

(defrule %expr-and
    (and expr-and ws (~ "AND") ws expr-not)
  (:destructure (expr-1 ws1 and ws2 expr-2)
    (declare (ignore ws1 and ws2))
    (list :expr-and expr-1 expr-2)))

(defrule expr-and
    (or %expr-and expr-not))

(defrule %expr-not
    (and (~ "NOT") ws expr-boolean-primary)
  (:destructure (not ws expr)
    (declare (ignore not ws))
    (list :expr-not expr)))

(defrule expr-not
    (or %expr-not expr-boolean-primary))

(defrule expr-compare
    (and expr-boolean-primary (? ws) (or "<>" "<=" ">=" "<"  ">" "=" ) (? ws) expr-add)
  (:destructure (expr-1 ws1 op ws2 expr-2)
    (declare (ignore ws1 ws2))
    (list :expr-compare expr-1 (make-symbol op) expr-2)))

(defrule %expr-is
    (and expr-boolean-primary ws (~ "IS") ws expr-add)
  (:destructure (expr-1 ws1 is ws2 expr-2)
    (declare (ignore ws1 is ws2))
    (list :expr-is expr-1 expr-2)))

(defrule %expr-is-not
    (and expr-boolean-primary ws (~ "IS") ws (~ "NOT") ws expr-add)
  (:destructure (expr-1 ws1 is ws2 not ws3 expr-2)
    (declare (ignore ws1 is ws2 not ws3))
    (list :expr-is-not expr-1 expr-2)))

(defrule expr-is
    (or %expr-is-not %expr-is))

(defrule %expr-between
    (and expr-boolean-primary ws (~ "BETWEEN") ws expr-add ws (~ "AND") ws expr-add)
  (:destructure (expr-1 ws1 between ws2 expr-2 ws3 and ws4 expr-3)
    (declare (ignore ws1 between  ws2 and ws3 ws4))
    (list :expr-between expr-1 expr-2 expr-3)))

(defrule %expr-not-between
    (and expr-boolean-primary ws (~ "NOT") ws (~ "BETWEEN") ws expr-add ws (~ "AND") ws expr-add)
  (:destructure (expr-1 ws1 not ws2 between ws3 expr-2 ws4 and ws5 expr-3)
    (declare (ignore ws1 not ws2 between ws3 and ws4 ws5))
    (list :expr-not-between expr-1 expr-2 expr-3)))

(defrule expr-between
    (or %expr-not-between %expr-between))

(defrule %expr-in
    (and expr-boolean-primary ws (~ "IN") (? ws) "(" (? ws) expr-list (? ws) ")")
  (:destructure (expr ws1 in ws2 open-brace ws3 expr-list ws4 close-brace)
    (declare (ignore ws1 in ws2 open-brace ws3 ws4 close-brace))
    (list :expr-in expr expr-list)))

(defrule %expr-not-in
    (and expr-boolean-primary ws (~ "NOT") ws (~ "IN") (? ws) "(" (? ws) expr-list (? ws) ")")
  (:destructure (expr ws1 not ws2 in ws3 open-brace ws4 expr-list ws5 close-brace)
    (declare (ignore ws1 not ws2 in ws3 open-brace ws4 ws5 close-brace))
    (list :expr-in expr expr-list)))

(defrule expr-in
    (or %expr-not-in %expr-in))

(defrule %expr-exists
    (and (~ "EXISTS") (? ws) subquery)
  (:destructure (exists ws subquery)
    (declare (ignore exists ws))
    (list :expr-exists subquery)))

(defrule %expr-not-exists
    (and (~ "NOT") ws (~ "EXISTS") (? ws) subquery)
  (:destructure (not ws1 exists ws2 subquery)
    (declare (ignore not ws1 exists ws2))
    (list :expr-not-exists subquery)))

(defrule expr-exists
    (or %expr-not-exists %expr-exists))

(defrule expr-boolean-primary
    (or expr-compare
        expr-is
        expr-between
        expr-in
        expr-exists
        expr-add))

(defrule %expr-add
    (and expr-add (? ws) (or "+" "-") (? ws) expr-mult)
  (:destructure (expr-1 ws1 op ws2 expr-2)
    (declare (ignore ws1 ws2))
    (list :expr-binary expr-1 (make-symbol op) expr-2)))

(defrule expr-add
    (or %expr-add expr-mult))

(defrule %expr-mult
    (and expr-mult (? ws) (or "*" "/" "%") (? ws) expr-unary)
  (:destructure (expr-1 ws1 op ws2 expr-2)
    (declare (ignore ws1 ws2))
    (list :expr-binary expr-1 (make-symbol op) expr-2)))

(defrule expr-mult
    (or %expr-mult expr-unary))

(defrule %expr-unary
    (and (or "+" "-") (? ws) expr-primary)
  (:destructure (op ws expr)
    (declare (ignore ws))
    (list :expr-unary (make-symbol op) expr)))

(defrule expr-unary
    (or %expr-unary expr-primary))

(defrule expr-case
    (and (~ "CASE") ws
         (? (and (! (~ "WHEN")) expr ws))
         (+ (and (~ "WHEN") ws expr ws (~ "THEN") ws expr ws))
         (? (and (~ "ELSE") ws expr ws))
         (~ "END"))
  (:lambda (items)
    (%flatten-ast :expr-case items)))

(defrule %expr-set-star
    (and identifier (? ws) "(" (? ws) star (? ws) ")")
  (:destructure (identifier &rest rest)
    (declare (ignore rest))
    (list :expr-set-star identifier)))

(defrule %expr-set
    (and identifier (? ws) "(" (? ws) expr-list (? ws) ")")
  (:destructure (identifier ws1 open-brace ws2 expr-list ws3 close-brace)
    (declare (ignore  ws1 open-brace ws2 ws3 close-brace))
    (list :expr-set identifier expr-list)))

(defrule %expr-set-distinct
    (and identifier (? ws) "(" (? ws) (~ "DISTINCT") ws expr-list (? ws) ")")
  (:destructure (identifier ws1 open-brace ws2 distinct ws3 expr-list ws4 close-brace)
    (declare (ignore ws1 open-brace ws2 distinct ws3 ws4 close-brace))
    (list :expr-set-distinct identifier expr-list)))

(defrule expr-set
    (or %expr-set-star %expr-set-distinct %expr-set))

(defrule %expr-column
    (and identifier "." identifier)
  (:destructure (table dot column)
    (declare (ignore dot))
    (make-symbol (format nil "~A.~A"
                         (symbol-name table)
                         (symbol-name column)))))

(defrule expr-column
    (or %expr-column identifier))

(defrule subquery
    (and "(" (? ws) select-stmt (? ws) ")")
  (:destructure (open-brace ws1 select-stmt ws2 close-brace)
    (declare (ignore open-brace ws1 ws2 close-brace))
    (list :subquery select-stmt)))

(defrule expr-paren
    (and "(" (? ws) expr (? ws) ")")
  (:destructure (open-brace ws1 expr ws2 close-brace)
    (declare (ignore open-brace ws1 ws2 close-brace))
    expr))

(defrule expr-primary
    (or subquery
        expr-paren
        expr-case
        expr-set
        literal-value
        expr-column))

(defrule expr-list
    (and expr (* (and (? ws) "," (? ws) expr)))
  (:lambda (items)
    (%flatten-list :expr-list items)))

(defrule expr expr-or)

(defrule column-def
    (and identifier (? (and ws identifier)) (? (and "("  numeric-literal ")")) (? (and ws (~ "PRIMARY") ws (~ "KEY"))))
  (:destructure (identifier &rest type-def)
    (declare (ignore type-def))
    identifier))

(defrule column-def-list
    (and column-def (* (and (? ws) "," (? ws) column-def)))
  (:lambda (items)
    (%flatten-list :column-def-list items)))

(defrule create-table-stmt
    (and (~ "CREATE") ws (~ "TABLE") ws identifier (? ws)
         "(" (? ws) column-def-list (? ws) ")")
  (:destructure (create ws1 table ws2 identifier ws3 open-brace ws4 column-def-list ws5 close-brace)
    (declare (ignore create ws1 table ws2 ws3 open-brace ws4 ws5 close-brace))
    (list :create-table-stmt identifier column-def-list)))

(defrule indexed-column
    (and identifier (? (and ws (or (~ "ASC") (~ "DESC")))))
  (:constant nil))

(defrule indexed-column-list
    (and indexed-column (* (and (? ws) "," (? ws) indexed-column)))
  (:constant nil))

(defrule create-index-stmt
    (and (~ "CREATE") ws (~ "INDEX") ws identifier ws (~ "ON") ws identifier (? ws)
         "(" (? ws) indexed-column-list (? ws) ")")
  (:constant nil))

(defrule values-stmt
    (and (~ "VALUES") (? ws) "(" (? ws) expr-list (? ws) ")")
  (:destructure (values ws1 open-brace ws2 expr-list ws3 close-brace)
    (declare (ignore values ws1 open-brace ws2 ws3 close-brace))
    (list :values-stmt expr-list)))

(defrule identifier-list
    (and identifier (* (and (? ws) "," (? ws) identifier)))
  (:lambda (items)
    (%flatten-list :identifier-list items)))

(defrule %insert-stmt
    (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws) select-core)
  (:destructure (insert ws1 into ws2 identifier ws3 select)
    (declare (ignore insert ws1 into ws2 ws3))
    (list :insert-stmt identifier select)))

(defrule %insert-stmt-identifier-list
    (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws)
         "(" (? ws) identifier-list (? ws) ")" (? ws)
         select-core)
  (:destructure (insert ws1 into ws2 identifier ws3 open-brace ws4 identifier-list ws5 close-brace ws6 select)
    (declare (ignore insert ws1 into ws2 ws3 open-brace ws4 ws5 close-brace ws6))
    (list :insert-stmt identifier select identifier-list)))

(defrule insert-stmt
    (or %insert-stmt %insert-stmt-identifier-list))

(defrule star
    "*"
  (:lambda (items)
    (list :star items)))

(defrule %table-or-subquery-identifier
    identifier
  (:lambda (identifier)
    (cons identifier identifier)))

(defrule %table-or-subquery-as
    (and identifier ws (~ "AS") ws identifier)
  (:destructure (identifier ws1 as ws2 as-identifier)
    (declare (ignore ws1 as ws2))
    (cons identifier as-identifier)))

(defrule %table-or-subquery-alias
    (and identifier ws (! (or (~ "ORDER") (~ "WHERE"))) identifier)
  (:destructure (identifier ws not-order-or-where as-identifier)
    (declare (ignore ws not-order-or-where))
    (cons identifier as-identifier)))

(defrule table-or-subquery
    (or %table-or-subquery-as %table-or-subquery-alias %table-or-subquery-identifier))

(defrule table-subquery-list
    (and table-or-subquery (* (and (? ws) "," (? ws) table-or-subquery)))
  (:lambda (items)
    (%flatten-list :table-subquery-list items)))

(defrule from-clause
    (and (~ "FROM") ws table-subquery-list)
  (:destructure (from ws table-or-subquery-list)
    (declare (ignore from ws))
    (list :from-clause table-or-subquery-list)))

(defrule where-clause
    (and (~ "WHERE") ws expr)
  (:destructure (where ws expr)
    (declare (ignore where ws))
    (list :where-clause expr)))

(defrule %result-column-expr
    expr
  (:lambda (expr)
    (cons expr :sql/unassigned)))

(defrule %result-column-as
    (and expr ws (~ "AS") ws identifier)
  (:destructure (expr ws1 as ws2 as-identifier)
    (declare (ignore ws1 as ws2))
    (cons expr as-identifier)))

(defrule %result-column-alias
    (and expr ws (! (~ "FROM")) identifier)
  (:destructure (expr ws not-from as-identifier)
    (declare (ignore ws not-from))
    (cons expr as-identifier)))

(defrule result-column
    (or star %result-column-as %result-column-alias %result-column-expr))

(defrule result-column-list
    (and result-column (* (and (? ws) "," (? ws) result-column)))
  (:lambda (items)
    (%flatten-list :result-column-list items)))

(defrule %select-core
    (and (~ "SELECT") ws result-column-list
         (? (and ws from-clause))
         (? (and ws where-clause)))
  (:destructure (select ws1 result-column-list (&optional ws2 from-clause) (&optional ws3 where-clause))
    (declare (ignore select ws1 ws2 ws3))
    (concatenate 'list
                 (list :select-core result-column-list)
                 (when from-clause
                   (list from-clause))
                 (when where-clause
                   (list where-clause)))))

(defrule select-core
    (or %select-core values-stmt))

(defrule %union-all-stmt
    (and select-core ws (~ "UNION") ws (~ "ALL") ws compound-select-stmt)
  (:destructure (select-1 ws1 union ws2 all ws3 select-2)
    (declare (ignore ws1 union ws2 all ws3))
    (list :union-all-stmt select-1 select-2)))

(defrule %union-stmt
    (and select-core ws (~ "UNION") ws compound-select-stmt)
  (:destructure (select-1 ws1 union ws2 select-2)
    (declare (ignore ws1 union ws2))
    (list :union-stmt select-1 select-2)))

(defrule %intersect-stmt
    (and select-core ws (~ "INTERSECT") ws compound-select-stmt)
  (:destructure (select-1 ws1 intersect ws2 select-2)
    (declare (ignore ws1 intersect ws2))
    (list :intersect-stmt select-1 select-2)))

(defrule %except-stmt
    (and select-core ws (~ "EXCEPT") ws compound-select-stmt)
  (:destructure (select-1 ws1 except ws2 select-2)
    (declare (ignore ws1 except ws2))
    (list :except-stmt select-1 select-2)))

(defrule compound-select-stmt
    (or %union-all-stmt %union-stmt %intersect-stmt %except-stmt select-core))

(defrule ordering-term
    (and expr (? (and ws (or (~ "ASC") (~ "DESC")))))
  (:destructure (expr &optional ws (direction "ASC"))
    (declare (ignore ws))
    (cons expr direction)))

(defrule ordering-term-list
    (and ordering-term (* (and (? ws) "," (? ws) ordering-term)))
  (:lambda (items)
    (%flatten-list :ordering-term-list items)))

(defrule order-by-clause
    (and (~ "ORDER") ws (~ "BY") ws ordering-term-list)
  (:destructure (order ws1 by ws2 ordering-term-list)
    (declare (ignore order ws1 by ws2))
    (list :order-by-clause ordering-term-list)))

(defrule select-stmt
    (and compound-select-stmt (? (and ws order-by-clause)))
  (:destructure (select (&optional ws order-by))
    (declare (ignore ws))
    (concatenate 'list
                 (list :select-stmt select)
                 (when order-by
                   (list order-by)))))

(defrule sql-stmt
    (and (? ws)
         (or create-table-stmt
             create-index-stmt
             insert-stmt
             select-stmt)
         (? ws))
  (:destructure (ws1 stmt ws2)
    (declare (ignore ws1 ws2))
    stmt))

(defun parse-sql (in)
  (esrap:parse 'sql-stmt in))
