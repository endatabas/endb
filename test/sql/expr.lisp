(defpackage :endb-test/sql/expr
  (:use :cl :fiveam :endb/sql/expr))
(in-package :endb-test/sql/expr)

(in-suite* :sql)

(test three-valued-logic-=
  (is (eq t (sql-= t t)))
  (is (eq :null (sql-= t :null)))
  (is (eq nil (sql-= t nil)))
  (is (eq :null (sql-= :null t)))
  (is (eq :null (sql-= :null :null)))
  (is (eq :null (sql-= :null nil)))
  (is (eq nil (sql-= nil t)))
  (is (eq :null (sql-= nil :null)))
  (is (eq t (sql-= nil nil))))

(test three-valued-logic-is
  (is (eq t (sql-is t t)))
  (is (eq nil (sql-is t :null)))
  (is (eq nil (sql-is t nil)))
  (is (eq nil (sql-is :null t)))
  (is (eq t (sql-is :null :null)))
  (is (eq nil (sql-is :null nil)))
  (is (eq nil (sql-is nil t)))
  (is (eq nil (sql-is nil :null)))
  (is (eq t (sql-= nil nil))))

(test three-valued-logic-not
  (is (eq nil (sql-not t)))
  (is (eq :null (sql-not :null)))
  (is (eq t (sql-not nil))))

(test three-valued-logic-and
  (is (eq t (sql-and t t)))
  (is (eq :null (sql-and t :null)))
  (is (eq nil (sql-and t nil)))
  (is (eq :null (sql-and :null t)))
  (is (eq :null (sql-and :null :null)))
  (is (eq nil (sql-and :null nil)))
  (is (eq nil (sql-and nil t)))
  (is (eq nil (sql-and nil :null)))
  (is (eq nil (sql-and nil nil)))

  (is (eq nil (sql-and (sql-= :null 1) (sql-= 0 1)))))

(test three-valued-logic-or
  (is (eq t (sql-or t t)))
  (is (eq t (sql-or t :null)))
  (is (eq t (sql-or t nil)))
  (is (eq t (sql-or :null t)))
  (is (eq :null (sql-or :null :null)))
  (is (eq :null (sql-or :null nil)))
  (is (eq t (sql-or nil t)))
  (is (eq :null (sql-or nil :null)))
  (is (eq nil (sql-or nil nil)))

  (is (eq t (sql-or (sql-= :null 1) (sql-= 1 1)))))

(test three-valued-logic-compare
  (is (eq :null (sql-< 2 :null)))
  (is (eq :null (sql-<= :null 2)))
  (is (eq :null (sql-> 1 :null)))
  (is (eq :null (sql->= :null 3)))

  (is (eq t (sql-between 2 1 3)))
  (is (eq nil (sql-between 4 1 3)))
  (is (eq :null (sql-between 1 1 :null)))
  (is (eq :null (sql-between :null 1 2))))

(test three-valued-logic-in
  (is (eq :null (sql-not (ra-in 1 '(:null)))))
  (is (eq t (sql-not (ra-in 1 '(0)))))
  (is (eq :null (sql-not (ra-in 1 '(:null 2)))))
  (is (eq nil (sql-not (ra-in 1 '(:null 1)))))

  (is (eq t (ra-exists '(1))))
  (is (eq t (ra-exists '(:null))))
  (is (eq nil (ra-exists '()))))

(test three-valued-logic-coalesce
  (is (eq :null (sql-coalesce :null :null)))
  (is (eq 1 (sql-coalesce :null 1)))
  (is (eq 1 (sql-coalesce 1 :null)))
  (is (eq 1 (sql-coalesce 1 :null 2)))
  (is (eq 2 (sql-coalesce :null :null 2 3)))
  (is (eq nil (sql-coalesce nil 2))))

(test arithmetic
  (is (eq :null (sql-+ 2 :null)))
  (is (eq :null (sql-- 0 :null)))
  (is (eq :null (sql-* :null 3)))
  (is (eq :null (sql-/ :null 3)))
  (is (= 0 (sql-/ 2 3)))
  (is (= 0.5 (sql-/ 1 2.0)))
  (is (= 2 (sql-abs -2)))
  (is (eq :null (sql-abs :null)))
  (is (= -2 (sql-- 0 2))))

(test sorting
  (is (equal '((:null) (1) (2)) (endb/sql/expr:ra-order-by (copy-list '((2) (:null) (1))) '((1 :ASC)))))
  (is (equal '((2) (1) (:null)) (endb/sql/expr:ra-order-by (copy-list '((1) (2) (:null))) '((1 :DESC)))))
  (is (equal '((2 1) (2 2) (1 1)) (endb/sql/expr:ra-order-by (copy-list '((1 1) (2 2) (2 1))) '((1 :DESC) (2 :ASC)))))
  (is (equal '((2 2) (1 1) (2 1)) (endb/sql/expr:ra-order-by (copy-list '((1 1) (2 2) (2 1))) '((2 :DESC))))))

(test like
  (is (eq t (sql-like "foo" "foo")))
  (is (eq nil (sql-like "foo" "fo")))
  (is (eq nil (sql-like "fo" "foo")))
  (is (eq t (sql-like "fo%" "foo")))
  (is (eq t (sql-like "%oo" "foo")))
  (is (eq t (sql-like "%o%" "foo")))
  (is (eq :null (sql-like "%o%" :null)))
  (is (eq :null (sql-like :null "foo"))))

(test substring
  (is (equal "foo" (sql-substring "foo" 1)))
  (is (equal "oo" (sql-substring "foo" 2)))
  (is (equal "fo" (sql-substring "foo" 1 2)))
  (is (eq :null (sql-substring "foo" 4)))
  (is (equal "foo" (sql-substring "foo" 1 5))))

(defun %aggregate (type xs &key distinct)
  (agg-finish (reduce #'agg-accumulate xs :initial-value (make-agg type :distinct distinct))))

(test aggregates
  (is (= 6 (%aggregate :sum '(1 2 3))))
  (is (eq :null (%aggregate :sum '())))
  (is (eq :null (%aggregate :sum '(:null))))
  (is (= 1 (%aggregate :sum '(:null 1))))
  (is (= 6 (%aggregate :sum '(1 2 3 3) :distinct :distinct)))

  (is (= 3 (%aggregate :count '(1 2 3))))
  (is (= 3 (%aggregate :count '(1 2 3 3) :distinct :distinct)))
  (is (= 2 (%aggregate :count '(1 2 :null))))
  (is (= 3 (%aggregate :count-star '(1 2 :null))))

  (is (= 1 (%aggregate :avg '(:null 1))))
  (is (eq :null (%aggregate :avg '(:null))))
  (is (= 2.5 (%aggregate :avg '(:null 2 3))))

  (is (= 2 (%aggregate :min '(:null 2 3))))
  (is (eq :null (%aggregate :min '())))
  (is (= 3 (%aggregate :max '(:null 2 3))))
  (is (eq :null (%aggregate :max '(:null))))

  (is (equal "1,0" (%aggregate :group_concat '(1 0 :null) :distinct t)))
  (is (equal "1:0" (agg-finish (reduce (lambda (acc x)
                                         (agg-accumulate acc x ":"))
                                       '(1 0 :null)
                                       :initial-value (make-agg :group_concat)))))
  (signals endb/sql/expr:sql-runtime-error
    (agg-finish (reduce (lambda (acc x)
                          (agg-accumulate acc x ":"))
                        '(1 0 :null)
                        :initial-value (make-agg :group_concat :distinct :distinct)))))
