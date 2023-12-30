(defpackage :endb-test/sql/expr
  (:use :cl :fiveam :endb/sql/expr)
  (:import-from :endb/arrow)
  (:import-from :fset)
  (:import-from :trivial-utf-8))
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

(test equality-and-hashing
  (is (equalp-case-sensitive (endb/arrow:parse-arrow-date-millis "2001-01-01")
                             (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00")))

  (is (equalp-case-sensitive (vector (endb/arrow:parse-arrow-date-millis "2001-01-01"))
                             (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))
  (is (equalp-case-sensitive (vector (endb/arrow:parse-arrow-date-millis "2001-01-01") 2)
                             (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00") 2.0d0)))
  (is (not (equalp-case-sensitive #(1) #(2))))

  (is (not (equalp-case-sensitive (fset:seq (endb/arrow:parse-arrow-date-millis "2001-01-01"))
                                  (fset:seq (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00")))))
  (is (equal (ra-bloom-hashes (endb/arrow:parse-arrow-date-millis "2001-01-01"))
             (ra-bloom-hashes (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))
  (is (not (equal (ra-bloom-hashes (fset:seq (endb/arrow:parse-arrow-date-millis "2001-01-01")))
                  (ra-bloom-hashes (fset:seq (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))))
  (is (not (sql-is (endb/arrow:parse-arrow-date-millis "2001-01-01")
                   (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))

  (is (= (equalp-case-sensitive-hash-fn (endb/arrow:parse-arrow-date-millis "2001-01-01"))
         (equalp-case-sensitive-hash-fn (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))

  (is (= (equalp-case-sensitive-hash-fn (vector (endb/arrow:parse-arrow-date-millis "2001-01-01")))
         (equalp-case-sensitive-hash-fn (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00")))))

  (is (not (= (equalp-case-sensitive-hash-fn (fset:seq (endb/arrow:parse-arrow-date-millis "2001-01-01")))
              (equalp-case-sensitive-hash-fn (fset:seq (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00"))))))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (endb/arrow:parse-arrow-date-millis "2001-01-01") ht) t)
    (is (gethash (endb/arrow:parse-arrow-date-millis "2001-01-01") ht))
    (is (gethash (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00") ht)))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01")) ht) t)
    (is (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01")) ht))
    (is (not (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-02")) ht)))
    (is (gethash (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00")) ht)))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01") 2) ht) t)
    (is (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01") 2) ht))
    (is (not (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01") 3) ht)))
    (is (gethash (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00") 2) ht))
    (is (gethash (vector (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00") 2.0d0) ht))
    (is (gethash (vector (endb/arrow:parse-arrow-date-millis "2001-01-01") 2.0d0) ht)))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (fset:seq (endb/arrow:parse-arrow-date-millis "2001-01-01")) ht) t)
    (is (gethash (fset:seq (endb/arrow:parse-arrow-date-millis "2001-01-01")) ht))
    (is (not (gethash (fset:seq (endb/arrow:parse-arrow-timestamp-micros "2001-01-01T00:00:00")) ht))))

  (is (equalp-case-sensitive 2 2.0d0))
  (is (not (equalp-case-sensitive (fset:seq 2) (fset:seq 2.0d0))))
  (is (equal (ra-bloom-hashes 2) (ra-bloom-hashes 2.0d0)))
  (is (not (equal (ra-bloom-hashes (fset:seq 2)) (ra-bloom-hashes (fset:seq 2.0d0)))))
  (is (not (sql-is 2 2.0d0)))

  (is (= (equalp-case-sensitive-hash-fn 2) (equalp-case-sensitive-hash-fn 2.0d0)))
  (is (= (equalp-case-sensitive-hash-fn (fset:seq 2)) (equalp-case-sensitive-hash-fn (fset:seq 2.0d0))))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash 2 ht) t)
    (is (gethash 2 ht))
    (is (gethash 2.0d0 ht)))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (fset:seq 2) ht) t)
    (is (gethash (fset:seq 2) ht))
    (is (not (gethash (fset:seq 2.0d0) ht))))

  (is (equalp-case-sensitive "foo" "foo"))
  (is (not (equalp-case-sensitive "foo" "FOO")))
  (is (= (equalp-case-sensitive-hash-fn "foo") (equalp-case-sensitive-hash-fn "foo")))
  (is (= (equalp-case-sensitive-hash-fn "foo") (equalp-case-sensitive-hash-fn "FOO")))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash "foo" ht) t)
    (is (gethash "foo" ht))
    (is (not (gethash "FOO" ht))))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash (fset:seq "foo") ht) t)
    (is (gethash (fset:seq "foo") ht))
    (is (not (gethash (fset:seq "FOO") ht))))

  (let ((x (trivial-utf-8:string-to-utf-8-bytes "foo"))
        (y (trivial-utf-8:string-to-utf-8-bytes "FOO")))
    (is (equalp-case-sensitive x x))
    (is (not (equalp-case-sensitive x y)))
    (is (equalp-case-sensitive (fset:seq x) (fset:seq x)))

    (let ((ht (make-hash-table :test +hash-table-test+)))
      (setf (gethash x ht) t)
      (is (gethash x ht))
      (is (not (gethash y ht)))))

  (is (eq t (equalp-case-sensitive :null :null)))
  (is (equalp-case-sensitive (fset:seq :null) (fset:seq :null)))

  (let ((ht (make-hash-table :test +hash-table-test+)))
    (setf (gethash :null ht) t)
    (is (gethash :null ht)))

  (is (not (equalp-case-sensitive-no-nulls :null :null)))
  (is (equalp-case-sensitive-no-nulls (fset:seq :null) (fset:seq :null)))

  (let ((ht (make-hash-table :test +hash-table-test-no-nulls+)))
    (setf (gethash :null ht) t)
    (is (not (gethash :null ht)))))
