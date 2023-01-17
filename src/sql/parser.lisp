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
          ((and name (not (symbolp (first items))))
           (cons name items))
          (t items)))
      items))

(defrule ws
    (+ (or #\Space #\Tab #\Newline))
  (:constant nil))

(defrule identifier
    (and (alpha-char-p character) (* (alphanumericp character)))
  (:text t))

(defrule numeric-literal
    (+ (digit-char-p character))
  (:text t))

(defrule string-literal
    (and "'" (* (not "'")) "'")
  (:text t))

(defrule literal-value
    (or numeric-literal
        string-literal
        (~ "NULL")))

(defrule expr-boolean
    (or (and expr-boolean ws (~ "OR") ws expr-boolean-term)
        expr-boolean-term)
  (:lambda (items)
    (%flatten-ast :expr-boolean items)))

(defrule expr-boolean-term
    (or (and expr-boolean-term ws (~ "AND") ws expr-boolean-factor)
        expr-boolean-factor)
  (:lambda (items)
    (%flatten-ast :expr-boolean-term items)))

(defrule expr-boolean-factor
    (and (? (and (~ "NOT") ws)) expr-boolean-primary)
  (:lambda (items)
    (%flatten-ast :expr-boolean-factor items)))

(defrule expr-compare
    (and expr-boolean-primary (? ws) (or "<>" "<=" ">=" "<"  ">" "=" ) (? ws) expr-numeric)
  (:lambda (items)
    (%flatten-ast :expr-compare items)))

(defrule expr-is
    (and expr-boolean-primary ws (~ "IS") (? (and ws (~ "NOT"))) ws expr-numeric)
  (:lambda (items)
    (%flatten-ast :expr-is items)))

(defrule expr-between
    (and expr-boolean-primary ws (? (and (~ "NOT") ws)) (~ "BETWEEN") ws (and expr-numeric ws (~ "AND") ws expr-numeric))
  (:lambda (items)
    (%flatten-ast :expr-between items)))

(defrule expr-in
    (and expr-boolean-primary ws (? (and (~ "NOT") ws)) (~ "IN") (? ws) "(" (? ws) expr (* (and (? ws) "," (? ws) expr)) (? ws) ")")
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
        expr-numeric)
  (:lambda (items)
    (%flatten-ast :expr-boolean-primary items)))

(defrule expr-numeric
    (or (and expr-numeric (? ws) (or "+" "-") (? ws) expr-numeric-term)
        expr-numeric-term)
  (:lambda (items)
    (%flatten-ast :expr-numeric items)))

(defrule expr-numeric-term
    (or (and expr-numeric-term (? ws) (or "*" "/" "%") (? ws) expr-numeric-factor)
        expr-numeric-factor)
  (:lambda (items)
    (%flatten-ast :expr-term items)))

(defrule expr-numeric-factor
    (and (? (and (or "+" "-") (? ws))) expr-primary)
  (:lambda (items)
    (%flatten-ast :expr-factor items)))

(defrule expr-case
    (and (~ "CASE") ws
         (? (and (! (~ "WHEN")) expr ws))
         (+ (and (~ "WHEN") ws expr ws (~ "THEN") ws expr ws))
         (? (and (~ "ELSE") ws expr ws))
         (~ "END"))
  (:lambda (items)
    (%flatten-ast :expr-case items)))

(defrule expr-set
    (and identifier (? ws) "(" (? ws) (or (and (? (and (~ "DISTINCT") ws)) expr (* (and (? ws) "," (? ws) expr)))
                                          "*") (? ws) ")")
  (:lambda (items)
    (%flatten-ast :expr-set items)))

(defrule column-ref
    (and (? (and identifier ".")) identifier)
  (:lambda (items)
    (%flatten-ast :column-ref items)))

(defrule expr-primary
    (or (and "(" (? ws) (or select-stmt expr) (? ws) ")")
        expr-case
        expr-set
        literal-value
        column-ref)
  (:lambda (items)
    (%flatten-ast :expr-primary items)))

(defrule expr expr-boolean)

(defrule column-def
    (and identifier (? (and ws identifier)) (? (and "("  numeric-literal ")")) (? (and ws (~ "PRIMARY") ws (~ "KEY"))))
  (:lambda (items)
    (%flatten-ast :column-def items)))

(defrule create-table-stmt
    (and (~ "CREATE") ws (~ "TABLE") ws identifier (? ws)
         "(" (? ws) column-def (* (and (? ws) "," (? ws) column-def)) (? ws) ")")
  (:lambda (items)
    (%flatten-ast :create-table-stmt items)))

(defrule indexed-column
    (and identifier (? (and ws (or (~ "ASC") (~ "DESC")))))
  (:lambda (items)
    (%flatten-ast :indexed-column items)))

(defrule create-index-stmt
    (and (~ "CREATE") ws (~ "INDEX") ws identifier ws (~ "ON") ws identifier (? ws)
         "(" (? ws) indexed-column (* (and (? ws) "," (? ws) indexed-column)) (? ws) ")")
  (:lambda (items)
    (%flatten-ast :create-index-stmt items)))

(defrule values-stmt
    (and (~ "VALUES") (? ws) "(" (? ws) expr (* (and (? ws) "," (? ws) expr)) (? ws) ")")
  (:lambda (items)
    (%flatten-ast :values-stmt items)))

(defrule insert-stmt
    (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws)
         (? (and "(" (? ws) identifier (* (and (? ws) "," (? ws) identifier)) (? ws) ")" (? ws)))
         values-stmt)
  (:lambda (items)
    (%flatten-ast :insert-stmt items)))

(defrule result-column
    (or (and expr (? (or (and ws (~ "AS") ws identifier)
                         (and ws (! (~ "FROM")) identifier))))
        "*")
  (:lambda (items)
    (%flatten-ast :result-column items)))

(defrule table-or-subquery
    (and identifier (? (and ws (or (and (~ "AS") ws identifier)
                                   (and (! (or (~ "ORDER") (~ "WHERE"))) identifier)))))
  (:lambda (items)
    (%flatten-ast :table-or-subquery items)))

(defrule ordering-term
    (and expr (? (and ws (or (~ "ASC") (~ "DESC")))))
  (:lambda (items)
    (%flatten-ast :ordering-term items)))

(defrule from-clause
    (and (~ "FROM") ws table-or-subquery (* (and (? ws) "," (? ws) table-or-subquery)))
  (:lambda (items)
    (%flatten-ast :from-stmt items)))

(defrule where-clause
    (and (~ "WHERE") ws expr)
  (:lambda (items)
    (%flatten-ast :where-stmt items)))

(defrule select-core
    (or (and (~ "SELECT") ws (and result-column (* (and (? ws) "," (? ws) result-column)))
             (? (and ws from-clause))
             (? (and ws where-clause)))
        values-stmt)
  (:lambda (items)
    (%flatten-ast :select-core items)))

(defrule compound-select-stmt
    (and select-core (? (and ws (or (and (~ "UNION") (? (and ws (~ "ALL")))) (~ "INTERSECT") (~ "EXCEPT")) ws compound-select-stmt)))
  (:lambda (items)
    (%flatten-ast :compound-select-stmt items)))

(defrule order-by-clause
    (and (~ "ORDER") ws (~ "BY") ws ordering-term (* (and (? ws) "," (? ws) ordering-term)))
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
