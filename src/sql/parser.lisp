(defpackage :endb/sql/parser
  (:use :cl :esrap)
  (:export #:parse-sql)
  (:import-from :esrap))
(in-package :endb/sql/parser)

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

(defrule expr
    (or (and expr (? ws) (or "*" "/" "%") (? ws) expr)
        (and expr (? ws) (or "+" "-") (? ws) expr)
        (and expr (? ws) (or "<>" "<=" ">=" "<"  ">" "=" ) (? ws) expr)
        (and expr ws (~ "AND") ws expr)
        (and expr ws (~ "OR") ws expr)
        (and "(" (? ws) (or select-stmt expr) (? ws) ")")
        (and (~ "CASE") ws
             (? (and (! (~ "WHEN")) expr ws))
             (+ (and (~ "WHEN") ws expr ws (~ "THEN") ws expr ws))
             (? (and (~ "ELSE") ws expr ws))
             (~ "END"))
        (and expr ws (~ "IS") (? (and ws (~ "NOT"))) ws expr)
        ;; ws (~ "AND") ws expr
        (and expr ws (? (and (~ "NOT") ws)) (~ "BETWEEN") ws expr)
        (and expr ws (? (and (~ "NOT") ws)) (~ "IN") (? ws) "(" (? ws) expr (* (and (? ws) "," (? ws) expr)) (? ws) ")")
        (and (? (and (~ "NOT") ws)) (~ "EXISTS") (? ws) "(" (? ws) select-stmt (? ws) ")")
        (and identifier (? ws) "(" (? ws) (or (and (? (and (~ "DISTINCT") ws)) expr (* (and (? ws) "," (? ws) expr)))
                                              "*") (? ws) ")")
        literal-value
        (and (? (and identifier ".")) identifier)))

(defrule column-def
    (and identifier (? (and ws identifier)) (? (and "("  numeric-literal ")")) (? (and ws (~ "PRIMARY") ws (~ "KEY")))))

(defrule create-table-stmt
    (and (~ "CREATE") ws (~ "TABLE") ws identifier (? ws)
         "(" (? ws) column-def (* (and (? ws) "," (? ws) column-def)) (? ws) ")"))

(defrule indexed-column
    (and identifier (? (and ws (or (~ "ASC") (~ "DESC"))))))

(defrule create-index-stmt
    (and (~ "CREATE") ws (~ "INDEX") ws identifier ws (~ "ON") ws identifier (? ws)
         "(" (? ws) indexed-column (* (and (? ws) "," (? ws) indexed-column)) (? ws) ")"))

(defrule insert-stmt
    (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws)
         (? (and "(" (? ws) identifier (* (and (? ws) "," (? ws) identifier)) (? ws) ")" (? ws)))
         (~ "VALUES") (? ws) "(" (? ws) expr (* (and (? ws) "," (? ws) expr)) (? ws) ")"))

(defrule result-column
    (or (and expr (? (or (and ws (~ "AS") ws identifier)
                         (and ws (! (~ "FROM")) identifier))))
        "*"))

(defrule table-or-subquery
    (and identifier (? (and ws (or (and (~ "AS") ws identifier)
                                   (and (! (or (~ "ORDER") (~ "WHERE"))) identifier))))))

(defrule ordering-term
    (and expr (? (and ws (or (~ "ASC") (~ "DESC"))))))

(defrule select-core
    (and (~ "SELECT") ws (and result-column (* (and (? ws) "," (? ws) result-column)))
         (? (and ws (~ "FROM") ws table-or-subquery (* (and (? ws) "," (? ws) table-or-subquery))))
         (? (and ws (~ "WHERE") ws expr))))

(defrule select-stmt
    (and select-core
         (* (and ws (or (and (~ "UNION") (? (and ws (~ "ALL")))) (~ "INTERSECT") (~ "EXCEPT")) ws select-core))
         (? (and ws (~ "ORDER") ws (~ "BY") ws ordering-term (* (and (? ws) "," (? ws) ordering-term))))))

(defrule sql-stmt
    (and (? ws)
         (or create-table-stmt
             create-index-stmt
             insert-stmt
             select-stmt)
         (? ws)))

(defun parse-sql (in)
  (esrap:parse 'sql-stmt in))
