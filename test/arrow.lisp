(defpackage :endb-test/arrow
  (:use :cl :fiveam :endb/arrow))
(in-package :endb-test/arrow)

(in-suite* :all-tests)

;; https://arrow.apache.org/docs/format/Columnar.html

(test fixed-sized-primitive-layout
  (let* ((expected '(1 :null 2 4 8))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::int64-array))
    (is (= 5 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*10111
               (slot-value array 'endb/arrow::validity)))
    (is (equalp #(1 0 2 4 8)
                (slot-value array 'endb/arrow::values)))
    (is (equal expected (coerce array 'list))))

  (let* ((expected '(1 2 3 4 8))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::int64-array))
    (is (= 5 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (null (slot-value array 'endb/arrow::validity)))
    (is (equalp #(1 2 3 4 8)
                (slot-value array 'endb/arrow::values)))
    (is (equal expected (coerce array 'list)))))

(test variable-size-list-layout
  (let* ((expected '((12 -7 25) :null (0 -127 127 50) ()))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::list-array))
    (is (= 4 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*1011
               (slot-value array 'endb/arrow::validity)))
    (is (equalp #(0 3 3 7 7)
                (slot-value array 'endb/arrow::offsets)))
    (is (equal expected (coerce array 'list)))
    (let ((values (slot-value array 'endb/arrow::values)))
      (is (typep values 'endb/arrow::int64-array))
      (is (equal '(12 -7 25 0 -127 127 50) (coerce values 'list)))))

  (let* ((expected '(((1 2) (3 4)) ((5 6 7) :null (8)) ((9 10))))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::list-array))
    (is (= 3 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (equalp #(0 2 5 6)
                (slot-value array 'endb/arrow::offsets)))
    (is (equal expected (coerce array 'list)))

    (let ((values (slot-value array 'endb/arrow::values)))
      (is (typep values 'endb/arrow::list-array))
      (is (= 6 (arrow-length values)))
      (is (= 1 (arrow-null-count values)))
      (is (equal #*111011
                 (slot-value values 'endb/arrow::validity)))
      (is (equalp #(0 2 4 7 7 8 10)
                  (slot-value values 'endb/arrow::offsets)))
      (let ((values (slot-value values 'endb/arrow::values)))
        (is (typep values 'endb/arrow::int64-array))
        (is (= 10 (arrow-length values)))
        (is (zerop (arrow-null-count values)))
        (is (equal '(1 2 3 4 5 6 7 8 9 10) (coerce values 'list)))))))

(test struct-layout
  (let* ((expected '(((:name . "joe") (:id . 1)) ((:name . :null) (:id . 2)) :null ((:name . "mark") (:id . 4))))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::struct-array))
    (is (= 4 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*1101
               (slot-value array 'endb/arrow::validity)))
    (is (equal expected (coerce array 'list)))

    (let* ((values (slot-value array 'endb/arrow::values))
           (names (cdr (elt values 0)))
           (ids (cdr (elt values 1))))

      (is (typep names 'endb/arrow::utf8-array))
      (is (= 4 (arrow-length names)))
      (is (= 2 (arrow-null-count names)))
      (is (equal #*1001
                 (slot-value names 'endb/arrow::validity)))
      (is (equalp #(0 3 3 3 7)
                  (slot-value names 'endb/arrow::offsets)))
      (is (equalp "joemark"
                  (map 'string #'code-char
                       (slot-value names 'endb/arrow::data))))

      (is (typep ids 'endb/arrow::int64-array))
      (is (= 4 (arrow-length ids)))
      (is (= 1 (arrow-null-count ids)))
      (is (equal #*1101
                 (slot-value ids 'endb/arrow::validity)))
      (is (equalp #(1 2 0 4) (slot-value ids 'endb/arrow::values))))))

(test union-layout
  (let* ((expected '(1.2d0 :null 3.4d0 5))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::dense-union-array))
    (is (= 4 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal expected (coerce array 'list)))
    (is (equalp #(0 0 0 1) (slot-value array 'endb/arrow::type-ids)))
    (is (equalp #(0 1 2 0) (slot-value array 'endb/arrow::offsets)))

    (let* ((children (slot-value array 'endb/arrow::children))
           (f (elt children 0))
           (i (elt children 1)))

      (is (typep f 'endb/arrow::float64-array))
      (is (= 3 (arrow-length f)))
      (is (= 1 (arrow-null-count f)))
      (is (equal #*101
                 (slot-value f 'endb/arrow::validity)))
      (is (equal '(1.2d0 :null 3.4d0) (coerce f 'list)))

      (is (typep i 'endb/arrow::int64-array))
      (is (= 1 (arrow-length i)))
      (is (zerop (arrow-null-count i)))
      (is (null (slot-value i 'endb/arrow::validity)))
      (is (equal '(5) (coerce i 'list))))))

(test extensible-sequence
  (let ((array (to-arrow '(1 2 3))))
    (is (= 3 (length array)))
    (is (= 3 (elt array 2)))
    (is (equal '(1 2 3)
               (loop for x being the element in array
                     collect x)))
    (is (equal '(1 2) (coerce (subseq array 0 2) 'list)))
    (is (equal '(3) (coerce (subseq array 2) 'list)))
    (is (equal '(1 2 3) (coerce (copy-seq array) 'list)))))
