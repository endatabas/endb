(defpackage :endb-test/arrow
  (:use :cl :fiveam :endb/arrow)
  (:import-from :fset)
  (:import-from :trivial-utf-8))
(in-package :endb-test/arrow)

(in-suite* :arrow)

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
    (is (equal expected (coerce array 'list)))
    (is (null (arrow-children array)))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected '(1 2 3 4 8))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::int64-array))
    (is (= 5 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (null (slot-value array 'endb/arrow::validity)))
    (is (equalp #(1 2 3 4 8)
                (slot-value array 'endb/arrow::values)))
    (is (equal expected (coerce array 'list)))
    (is (null (arrow-children array)))
    (is (= 2 (length (arrow-buffers array))))))

(test variable-size-list-layout
  (let* ((expected (list (fset:seq 12 -7 25) :null (fset:seq 0 -127 127 50) (fset:seq)))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::list-array))
    (is (= 4 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*1011
               (slot-value array 'endb/arrow::validity)))
    (is (equalp #(0 3 3 7 7)
                (slot-value array 'endb/arrow::offsets)))
    (is (equalp expected (coerce array 'list)))
    (is (= 1 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array))))
    (let ((values (slot-value array 'endb/arrow::values)))
      (is (typep values 'endb/arrow::int64-array))
      (is (equal '(12 -7 25 0 -127 127 50) (coerce values 'list)))))

  (let* ((expected (list (fset:seq (fset:seq 1 2) (fset:seq 3 4)) (fset:seq (fset:seq 5 6 7) :null (fset:seq 8)) (fset:seq (fset:seq 9 10))))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::list-array))
    (is (= 3 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (equalp #(0 2 5 6)
                (slot-value array 'endb/arrow::offsets)))
    (is (equalp expected (coerce array 'list)))
    (is (= 1 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array))))

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
  (let* ((expected (list (fset:map ("name" "joe") ("id" 1))
                         (fset:map ("name" :null) ("id" 2))
                         :null
                         (fset:map ("name" "mark") ("id" 4))))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::struct-array))
    (is (= 4 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*1101
               (slot-value array 'endb/arrow::validity)))
    (is (equalp expected (coerce array 'list)))
    (is (= 2 (length (arrow-children array))))
    (is (= 1 (length (arrow-buffers array))))

    (let* ((children (slot-value array 'endb/arrow::children))
           (ids (gethash :|id| children))
           (names (gethash :|name| children)))

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
    (is (= 2 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array))))

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

(test empty-structs
  (let* ((expected (list (fset:map ("x" 1) ("y" "foo"))
                         (fset:map ("x" 2) ("z" "bar"))
                         (fset:empty-map)))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::dense-union-array))
    (is (= 3 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (= 3 (length (slot-value array 'endb/arrow::children))))
    (is (equalp expected (coerce array 'list)))
    (is (= 3 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected (list (fset:empty-map) (fset:map ("x" 1) ("y" "foo"))))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::dense-union-array))
    (is (= 2 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (= 2 (length (slot-value array 'endb/arrow::children))))
    (is (equalp expected (coerce array 'list)))
    (is (= 2 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected (list (fset:empty-map) (fset:empty-map)))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::map-array))
    (is (= 2 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (null (slot-value array 'endb/arrow::validity)))
    (is (equalp expected (coerce array 'list)))
    (is (= 1 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected (list :null (fset:empty-map)))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::map-array))
    (is (= 2 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*01
               (slot-value array 'endb/arrow::validity)))
    (is (equalp expected (coerce array 'list)))
    (is (= 1 (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array))))))

(test boolean-arrays
  (let* ((expected '(t nil :null))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::boolean-array))
    (is (= 3 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*110
               (slot-value array 'endb/arrow::validity)))
    (is (equal expected (coerce array 'list)))
    (is (zerop (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected '(nil :null t))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::boolean-array))
    (is (= 3 (arrow-length array)))
    (is (= 1 (arrow-null-count array)))
    (is (equal #*101
               (slot-value array 'endb/arrow::validity)))
    (is (equal expected (coerce array 'list)))
    (is (zerop (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array)))))

  (let* ((expected '(nil))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::boolean-array))
    (is (= 1 (arrow-length array)))
    (is (zerop (arrow-null-count array)))
    (is (null (slot-value array 'endb/arrow::validity)))
    (is (equal expected (coerce array 'list)))
    (is (zerop (length (arrow-children array))))
    (is (= 2 (length (arrow-buffers array))))))

(test lisp-vector-like-types
  (let ((list (fset:empty-seq)))
    (is (not (typep list 'endb/arrow::arrow-binary)))
    (is (typep list 'endb/arrow::arrow-list))
    (is (not (typep list 'endb/arrow::arrow-struct)))
    (is (not (stringp list))))

  (let ((binary (make-array 0 :element-type '(unsigned-byte 8))))
    (is (typep binary 'endb/arrow::arrow-binary))
    (is (not (typep binary 'endb/arrow::arrow-list)))
    (is (not (typep binary 'endb/arrow::arrow-struct)))
    (is (not (stringp binary))))

  (let ((string ""))
    (is (not (typep string 'endb/arrow::arrow-binary)))
    (is (not (typep string 'endb/arrow::arrow-list)))
    (is (not (typep string 'endb/arrow::arrow-struct)))
    (is (stringp string))))

(test extensible-sequence
  (let ((array (to-arrow '(1 2 3))))
    (is (= 3 (length array)))
    (is (= 3 (elt array 2)))
    (is (equal '(1 2 3)
               (loop for x being the element in array
                     collect x)))
    (is (equal '(1 2) (coerce (subseq array 0 2) 'list)))
    (is (equal '(3) (coerce (subseq array 2) 'list)))
    (is (equal '(1 2 3) (coerce (copy-seq array) 'list)))
    (is (equal '(2 3 4) (map 'list #'1+ array)))
    (setf (elt array 1) 4)
    (setf (elt array 2) :null)
    (is (equal '(1 4 :null) (coerce array 'list)))
    (is (equal '(4 :null) (coerce (remove 1 array) 'list)))))

(test in128-overflow
  (let ((array (to-arrow '(9223372036854775807 9223372036854775808))))
    (is (typep array 'endb/arrow::dense-union-array))
    (is (equal '(9223372036854775807 9223372036854775808) (coerce array 'list)))
    (let* ((children (slot-value array 'endb/arrow::children))
           (i (elt children 0))
           (d (elt children 1)))
      (is (typep i 'endb/arrow::int64-array))
      (is (typep d 'endb/arrow::decimal-array))))

  (let ((array (to-arrow '(170141183460469231731687303715884105728 -170141183460469231731687303715884105729))))
    (is (typep array 'endb/arrow::float64-array))
    (is (equal '(1.7014118346046923d38 -1.7014118346046923d38) (coerce array 'list))))

  (let* ((expected '(170141183460469231731687303715884105727 -170141183460469231731687303715884105728 -1 -9223372036854775808 :null 0 2))
         (array (to-arrow expected)))
    (is (typep array 'endb/arrow::decimal-array))
    (is (= 16 (slot-value array 'endb/arrow::element-size)))
    (is (equal expected (coerce array 'list))))

  (let* ((array (to-arrow '(170141183460469231731687303715884105727))))
    (is (equalp #(255 255 255 255 255 255 255 255 255 255 255 255 255 255 255 127)
                (slot-value array 'endb/arrow::values))))

  (let* ((array (to-arrow '(-170141183460469231731687303715884105728))))
    (is (equalp #(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 128)
                (slot-value array 'endb/arrow::values)))))

(test temporal-arrays
  (let* ((expected (list (endb/arrow:parse-arrow-date-millis "2001-01-01")
                         (endb/arrow:parse-arrow-time-micros "12:01:20")
                         (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z")
                         (endb/arrow:parse-arrow-interval-month-day-nanos "P3Y6M")
                         (endb/arrow:parse-arrow-interval-month-day-nanos "PT12H30M5S")))
         (array (to-arrow expected)))
    (is (equalp expected (coerce array 'list))))

  (let* ((array (to-arrow (list (endb/arrow:parse-arrow-interval-month-day-nanos "P5M3DT0.000000006S")))))
    (is (equalp #(5 0 0 0 3 0 0 0 6 0 0 0 0 0 0 0)
                (slot-value array 'endb/arrow::values)))
    (is (equalp (list (endb/arrow::make-arrow-interval-month-day-nanos :month 5 :day 3 :ns 6))
                (coerce array 'list)))))

;; https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-2/

(test row-format
  (is (equalp #(0) (to-arrow-row-format :null)))

  (is (equalp #(1 0) (to-arrow-row-format nil)))
  (is (equalp #(1 1) (to-arrow-row-format t)))

  (let ((array (make-arrow-array-for 1)))
    (arrow-push array :null)
    (is (equalp #(0 0 0 0 0 0 0 0 0)
                (arrow-row-format array 0))))

  (is (equalp #(1 128 0 0 0 0 0 0 5)
              (to-arrow-row-format 5)))
  (is (equalp #(1 127 255 255 255 255 255 255 251)
              (to-arrow-row-format -5)))
  (is (equalp #(1 128 0 0 0 0 0 0 0)
              (to-arrow-row-format 0)))

  (is (equalp #(1 191 244 204 204 204 204 204 205)
              (to-arrow-row-format 1.3d0)))
  (is (equalp #(1 63 239 255 255 255 255 255 255)
              (to-arrow-row-format -4.0d0)))
  (is (equalp #(1 127 255 255 255 255 255 255 255)
              (to-arrow-row-format -0.0d0)))

  (is (equalp (vector 1) (to-arrow-row-format "")))

  (let ((array (make-arrow-array-for "")))
    (arrow-push array :null)
    (is (equalp #(0) (arrow-row-format array 0))))

  (is (equalp (vector 2 (char-int #\M) (char-int #\E) (char-int #\E) (char-int #\P) 4)
              (endb/arrow::%block-encoded-row (trivial-utf-8:string-to-utf-8-bytes "MEEP")
                                              :small-block-size 4)))

  (is (equalp (vector 2
                      (char-int #\D) (char-int #\e) (char-int #\f) (char-int #\e) 255
                      (char-int #\n) (char-int #\e) (char-int #\s) (char-int #\t) 255
                      (char-int #\r) (char-int #\a) (char-int #\t) (char-int #\i) 255
                      (char-int #\o) (char-int #\n) 0 0 2)
              (endb/arrow::%block-encoded-row (trivial-utf-8:string-to-utf-8-bytes "Defenestration")
                                              :number-of-small-blocks 4
                                              :small-block-size 4)))

  (is (equalp (vector 2
                      (char-int #\D) (char-int #\e) (char-int #\f) (char-int #\e) 255
                      (char-int #\n) (char-int #\e) (char-int #\s) (char-int #\t) (char-int #\r) (char-int #\a) (char-int #\t) (char-int #\i) 255
                      (char-int #\o) (char-int #\n) 0 0 0 0 0 0 2)
              (endb/arrow::%block-encoded-row (trivial-utf-8:string-to-utf-8-bytes "Defenestration")
                                              :number-of-small-blocks 1
                                              :small-block-size 4
                                              :large-block-size 8)))

  (is (equalp (concatenate 'vector #(1) (to-arrow-row-format 3.14) (to-arrow-row-format 3))
              (to-arrow-row-format (fset:map ("int" 3) ("float" 3.14)))))

  (is (equalp #(1) (to-arrow-row-format (fset:empty-map))))
  (is (equalp #(1) (to-arrow-row-format (fset:empty-seq))))

  (let ((array (make-arrow-array-for (fset:empty-seq))))
    (arrow-push array :null)
    (is (equalp #(0) (arrow-row-format array 0))))

  (is (equalp (endb/arrow::%block-encoded-row
               #(1 0 1 1 0 0 0 2 0 0 0 2 0 0 0 2))
              (to-arrow-row-format (fset:seq nil t))))

  (is (equalp (endb/arrow::%block-encoded-row
               #(1 1 0 0 0 0 0 2 0 0 0 2 0 0 0 2))
              (to-arrow-row-format (fset:seq t :null)))))
