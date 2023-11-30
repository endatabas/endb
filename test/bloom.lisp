(defpackage :endb-test/bloom
  (:use :cl :fiveam :endb/bloom)
  (:import-from :endb/arrow)
  (:import-from :endb/lib)
  (:import-from :endb/sql/db)
  (:import-from :trivial-utf-8))
(in-package :endb-test/bloom)

(in-suite* :bloom)

(test stats
  (let* ((expected (list (fset:map ("name" "joe") ("id" 1))
                         (fset:map ("name" :null) ("id" 2))
                         :null
                         (fset:map ("name" "mark") ("id" 4))))
         (array (endb/arrow:to-arrow expected)))
    (is (fset:equal? (fset:map ("id" (fset:map ("count" 3) ("count_star" 3) ("min" 1) ("max" 4)
                                               ("bloom" (coerce #(0 2 36 0 0 4 8 64 32 128 0 16 0 16 0 18 32 1 0 8 4 0 36 0 4 0 0 72 0 0 8 5)
                                                                '(vector (unsigned-byte 8))))))
                               ("name" (fset:map ("count" 2) ("count_star" 3) ("min" "joe") ("max" "mark")
                                                 ("bloom" (coerce #(4 160 0 0 0 129 0 8 16 4 0 64 8 0 8 32 0 0 33 8 0 0 0 9 64 64 2 0 0 0 1 12)
                                                                  '(vector (unsigned-byte 8)))))))
                     (endb/sql/db:calculate-stats (list array)))))

  (let ((batches (list (list (fset:map ("x" 1))
                             (fset:map ("x" 2))
                             (fset:map ("x" 3))
                             (fset:map ("x" 4)))

                       (list (fset:map ("x" 5))
                             (fset:map ("x" 6))
                             (fset:map ("x" 7))
                             (fset:map ("x" 8))))))
    (is (fset:equal?
         (fset:map ("x" (fset:map ("count" 8) ("count_star" 8) ("min" 1) ("max" 8)
                                  ("bloom" (coerce #(64 2 52 17 48 68 72 64 160 128 34 24 16 18 128 22 160 81 32 8 68 104 36 64 4 40 12 72 0 1 72 53)
                                                   '(vector (unsigned-byte 8)))))))
         (endb/sql/db:calculate-stats (mapcar #'endb/arrow:to-arrow batches))))))

(test bloom-filter
  (let* ((bloom (make-sbbf 3)))
    (sbbf-insert bloom (endb/lib:xxh64 (endb/arrow:to-arrow-row-format "mark")))
    (sbbf-insert bloom (endb/lib:xxh64 (endb/arrow:to-arrow-row-format "joe")))
    (is (sbbf-check-p bloom (endb/lib:xxh64 (endb/arrow:to-arrow-row-format "mark"))))
    (is (sbbf-check-p bloom (endb/lib:xxh64 (endb/arrow:to-arrow-row-format "joe"))))
    (is (not (sbbf-check-p bloom (endb/lib:xxh64 (endb/arrow:to-arrow-row-format "mike"))))))

  (let* ((bloom (make-array 32 :element-type '(unsigned-byte 8))))
    (dotimes (a 10)
      (endb/bloom:sbbf-insert bloom (endb/lib:xxh64 (trivial-utf-8:string-to-utf-8-bytes (format nil "a~A" a)))))

    (is (equalp #(200 1 80 20 64 68 8 109 6 37 4 67 144 80 96 32 8 132 43 33 0 5 99 65 2 0 224 44 64 78 96 4)
                bloom))

    (dotimes (a 10)
      (is (endb/bloom:sbbf-check-p bloom (endb/lib:xxh64 (trivial-utf-8:string-to-utf-8-bytes (format nil "a~A" a))))))

    (is (not (endb/bloom:sbbf-check-p bloom (endb/lib:xxh64 (trivial-utf-8:string-to-utf-8-bytes (format nil "a~A" 10))))))))
