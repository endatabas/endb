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

(test sorting
  (is (equalp '(#(:null) #(1) #(2)) (endb/sql/expr:ra-order-by (copy-list '(#(2) #(:null) #(1))) '((1 :ASC)))))
  (is (equalp '(#(2) #(1) #(:null)) (endb/sql/expr:ra-order-by (copy-list '(#(1) #(2) #(:null))) '((1 :DESC)))))
  (is (equalp '(#(2 1) #(2 2) #(1 1)) (endb/sql/expr:ra-order-by (copy-list '(#(1 1) #(2 2) #(2 1))) '((1 :DESC) (2 :ASC)))))
  (is (equalp '(#(2 2) #(1 1) #(2 1)) (endb/sql/expr:ra-order-by (copy-list '(#(1 1) #(2 2) #(2 1))) '((2 :DESC))))))

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
