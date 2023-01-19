(defpackage :endb-test/sql/expr
  (:use :cl :fiveam :endb/sql/expr))
(in-package :endb-test/sql/expr)

(in-suite* :all-tests)

(test test-three-valued-logic-=
  (is (eq t (sql-= t t)))
  (is (eq :sql/null (sql-= t :sql/null)))
  (is (eq nil (sql-= t nil)))
  (is (eq :sql/null (sql-= :sql/null t)))
  (is (eq :sql/null (sql-= :sql/null :sql/null)))
  (is (eq :sql/null (sql-= :sql/null nil)))
  (is (eq nil (sql-= nil t)))
  (is (eq :sql/null (sql-= nil :sql/null)))
  (is (eq t (sql-= nil nil))))

(test test-three-valued-logic-is
  (is (eq t (sql-is t t)))
  (is (eq nil (sql-is t :sql/null)))
  (is (eq nil (sql-is t nil)))
  (is (eq nil (sql-is :sql/null t)))
  (is (eq t (sql-is :sql/null :sql/null)))
  (is (eq nil (sql-is :sql/null nil)))
  (is (eq nil (sql-is nil t)))
  (is (eq nil (sql-is nil :sql/null)))
  (is (eq t (sql-= nil nil))))

(test test-three-valued-logic-not
  (is (eq nil (sql-not t)))
  (is (eq :sql/null (sql-not :sql/null)))
  (is (eq t (sql-not nil))))

(test test-three-valued-logic-and
  (is (eq t (sql-and t t)))
  (is (eq :sql/null (sql-and t :sql/null)))
  (is (eq nil (sql-and t nil)))
  (is (eq :sql/null (sql-and :sql/null t)))
  (is (eq :sql/null (sql-and :sql/null :sql/null)))
  (is (eq nil (sql-and :sql/null nil)))
  (is (eq nil (sql-and nil t)))
  (is (eq nil (sql-and nil :sql/null)))
  (is (eq nil (sql-and nil nil))))

(test test-three-valued-logic-or
  (is (eq t (sql-or t t)))
  (is (eq t (sql-or t :sql/null)))
  (is (eq t (sql-or t nil)))
  (is (eq t (sql-or :sql/null t)))
  (is (eq :sql/null (sql-or :sql/null :sql/null)))
  (is (eq :sql/null (sql-or :sql/null nil)))
  (is (eq t (sql-or nil t)))
  (is (eq :sql/null (sql-or nil :sql/null)))
  (is (eq nil (sql-or nil nil))))
