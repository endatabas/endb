(defpackage :endb/sql/parser
  (:use :cl :esrap)
  (:export #:parse-sql)
  (:import-from :esrap))
(in-package :endb/sql/parser)

(defun %flatten-ast (name items)
  (if (listp items)
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
  (:lambda (items)
    :sql/null))

(defrule literal-value
    (or numeric-literal
        string-literal
        null-literal))

(defrule expr-or
    (or (and expr-or ws (~ "OR") ws expr-and)
        expr-and)
  (:lambda (items)
    (%flatten-ast :expr-or items)))

(defrule expr-and
    (or (and expr-and ws (~ "AND") ws expr-not)
        expr-not)
  (:lambda (items)
    (%flatten-ast :expr-and items)))

(defrule expr-not
    (and (? (and (~ "NOT") ws)) expr-boolean-primary)
  (:lambda (items)
    (%flatten-ast :expr-not items)))

(defrule expr-compare
    (and expr-boolean-primary (? ws) (or "<>" "<=" ">=" "<"  ">" "=" ) (? ws) expr-add)
  (:lambda (items)
    (%flatten-ast :expr-compare items)))

(defrule expr-is
    (and expr-boolean-primary ws (~ "IS") (? (and ws (~ "NOT"))) ws expr-add)
  (:lambda (items)
    (%flatten-ast :expr-is items)))

(defrule expr-between
    (and expr-boolean-primary ws (? (and (~ "NOT") ws)) (~ "BETWEEN") ws (and expr-add ws (~ "AND") ws expr-add))
  (:lambda (items)
    (%flatten-ast :expr-between items)))

(defrule expr-in
    (and expr-boolean-primary ws (? (and (~ "NOT") ws)) (~ "IN") (? ws) "(" (? ws) expr-list (? ws) ")")
  (:lambda (items)
    (%flatten-ast :expr-in items)))

(defrule expr-exists
    (and (? (and (~ "NOT") ws)) (~ "EXISTS") (? ws) "(" (? ws) select-stmt (? ws) ")")
  (:lambda (items)
    (%flatten-ast :expr-exists items)))

(defrule expr-boolean-primary
    (or expr-compare
        expr-is
        expr-between
        expr-in
        expr-exists
        expr-add)
  (:lambda (items)
    (%flatten-ast :expr-boolean-primary items)))

(defrule expr-add
    (or (and expr-add (? ws) (or "+" "-") (? ws) expr-mult)
        expr-mult)
  (:lambda (items)
    (%flatten-ast :expr-binary items)))

(defrule expr-mult
    (or (and expr-mult (? ws) (or "*" "/" "%") (? ws) expr-unary)
        expr-unary)
  (:lambda (items)
    (%flatten-ast :expr-binary items)))

(defrule expr-unary
    (and (? (and (or "+" "-") (? ws))) expr-primary)
  (:lambda (items)
    (%flatten-ast :expr-unary items)))

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
                                          "*") (? ws) ")")
  (:lambda (items)
    (%flatten-ast :expr-set items)))

(defrule expr-column
    (and (? (and identifier ".")) identifier)
  (:lambda (items)
    (cons :expr-column items)))

(defrule expr-primary
    (or (and "(" (? ws) (or select-stmt expr) (? ws) ")")
        expr-case
        expr-set
        literal-value
        expr-column)
  (:lambda (items)
    (%flatten-ast :expr-primary items)))

(defrule expr-list
    (and expr (* (and (? ws) "," (? ws) expr)))
  (:lambda (items)
    (%flatten-list :expr-list items)))

(defrule expr expr-or)

(defrule column-def
    (and identifier (? (and ws identifier)) (? (and "("  numeric-literal ")")) (? (and ws (~ "PRIMARY") ws (~ "KEY"))))
  (:lambda (items)
    (cons :column-def items)))

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
  (:lambda (items)
    (cons :indexed-column items)))

(defrule indexed-column-list
    (and indexed-column (* (and (? ws) "," (? ws) indexed-column)))
  (:lambda (items)
    (%flatten-list :indexed-column-list items)))

(defrule create-index-stmt
    (and (~ "CREATE") ws (~ "INDEX") ws identifier ws (~ "ON") ws identifier (? ws)
         "(" (? ws) indexed-column-list (? ws) ")")
  (:lambda (items)
    (%flatten-ast :create-index-stmt items)))

(defrule values-stmt
    (and (~ "VALUES") (? ws) "(" (? ws) expr-list (? ws) ")")
  (:lambda (items)
    (%flatten-ast :values-stmt items)))

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

(defrule result-column
    (or (and expr (? (or (and ws (~ "AS") ws identifier)
                         (and ws (! (~ "FROM")) identifier))))
        star)
  (:lambda (items)
    (cons :result-column items)))

(defrule table-or-subquery
    (and identifier (? (and ws (or (and (~ "AS") ws identifier)
                                   (and (! (or (~ "ORDER") (~ "WHERE"))) identifier)))))
  (:lambda (items)
    (cons :table-or-subquery items)))

(defrule table-subquery-list
    (and table-or-subquery (* (and (? ws) "," (? ws) table-or-subquery)))
  (:lambda (items)
    (%flatten-list :table-subquery-list items)))

(defrule from-clause
    (and (~ "FROM") ws table-subquery-list)
  (:lambda (items)
    (%flatten-ast :from-stmt items)))

(defrule where-clause
    (and (~ "WHERE") ws expr)
  (:lambda (items)
    (%flatten-ast :where-stmt items)))

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
  (:lambda (items)
    (cons :ordering-term items)))

(defrule ordering-term-list
    (and ordering-term (* (and (? ws) "," (? ws) ordering-term)))
  (:lambda (items)
    (%flatten-list :ordering-term-list items)))

(defrule order-by-clause
    (and (~ "ORDER") ws (~ "BY") ws ordering-term-list)
  (:lambda (items)
    (%flatten-ast :order-by-clause items)))

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
  (:lambda (items)
    (%flatten-ast :sql-stmt items)))

(defun parse-sql (in)
  (esrap:parse 'sql-stmt in))
