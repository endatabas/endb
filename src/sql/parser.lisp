(defpackage :endb/sql/parser
  (:use :cl :esrap)
  (:export #:parse-sql)
  (:import-from :esrap))
(in-package :endb/sql/parser)

(defrule ws (+ (or #\Space #\Tab #\Newline))
  (:constant nil))

(defrule identifier (and (alpha-char-p character) (* (alphanumericp character)))
  (:text t))

(defrule numeric-literal (+ (digit-char-p character))
  (:text t))

(defrule literal-value (or numeric-literal))

(defrule expr (or (and expr (? ws) (or #\* #\/) (? ws) expr)
                  (and expr (? ws) (or #\+ #\-) (? ws) expr)
                  (and #\( (? ws) expr (? ws) #\))
                  literal-value
                  identifier))

(defrule column-def (and identifier (? (and ws identifier))))

(defrule create-table-stmt (and (~ "CREATE") ws (~ "TABLE") ws identifier (? ws)
                                #\( (? ws) column-def (* (and (? ws) "," (? ws) column-def)) (? ws) #\)))

(defrule insert-stmt (and (~ "INSERT") ws (~ "INTO") ws identifier (? ws)
                          #\( (? ws) identifier (* (and (? ws) "," (? ws) identifier)) (? ws) #\) (? ws)
                          (~ "VALUES") (? ws) #\( (? ws) expr (* (and (? ws) "," (? ws) expr)) (? ws) #\)))

(defrule result-column (or (and expr (? (or (and ws (~ "AS") ws identifier)
                                            (and ws (not (~ "FROM")) identifier))))
                          #\*))

(defrule table-or-subquery (and identifier (? (or (and ws (~ "AS") ws identifier)
                                                  (and ws (not (or (~ "ORDER")
                                                                   (~ "WHERE"))) identifier)))))

(defrule ordering-term (and expr (? (or (~ "ASC") (~ "DESC")))))

(defrule select-stmt (and (~ "SELECT") ws (and result-column (* (and (? ws) "," (? ws) result-column))) ws
                          (~ "FROM") (? ws) table-or-subquery (* (and (? ws) "," (? ws) table-or-subquery)) ws
                          (and (~ "ORDER") ws (~ "BY")) (? ws) ordering-term (* (and (? ws) "," (? ws) ordering-term))))

(defrule sql-stmt (or create-table-stmt insert-stmt select-stmt))

(defun parse-sql (in)
  (esrap:parse 'sql-stmt in))
