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
  (cons name (if (= 1 (length items))
                 items
                 (cons (first items) (second items)))))

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

(defrule expr-set
    (and identifier (? ws) "(" (? ws) (or (and (? (and (~ "DISTINCT") ws)) expr-list)
                                          star) (? ws) ")")
  (:lambda (items)
    (%flatten-ast :expr-set items)))

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
  (:lambda (items)
    (%flatten-ast :create-table-stmt items)))

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

(defrule insert-stmt
    (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws)
         (? (and "(" (? ws) identifier-list (? ws) ")" (? ws)))
         values-stmt)
  (:lambda (items)
    (%flatten-ast :insert-stmt items)))

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

(defrule select-core
    (or (and (~ "SELECT") ws result-column-list
             (? (and ws from-clause))
             (? (and ws where-clause)))
        values-stmt)
  (:lambda (items)
    (%flatten-ast :select-core items)))

(defrule compound-select-stmt
    (and select-core (? (and ws (or (and (~ "UNION") (? (and ws (~ "ALL")))) (~ "INTERSECT") (~ "EXCEPT")) ws compound-select-stmt)))
  (:lambda (items)
    (%flatten-ast :compound-select-stmt items)))

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
  (:lambda (items)
    (%flatten-ast :select-stmt items)))

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
