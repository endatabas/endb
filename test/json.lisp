(defpackage :endb-test/json
  (:use :cl :fiveam :endb/json)
  (:import-from :endb/arrow)
  (:import-from :cl-bloom))
(in-package :endb-test/json)

(in-suite* :json)

(test random-uuid
  (let ((uuid (random-uuid #+sbcl (sb-ext:seed-random-state 0)
                           #-sbcl *random-state*)))
    #+sbcl (is (equal "8c7f0aac-97c4-4a2f-b716-a675d821ccc0" uuid))
    (is (random-uuid-p uuid))
    (is (random-uuid-p "00000000-0000-4000-8000-000000000000"))
    (is (not (random-uuid-p "00000000-0000-1000-8000-000000000000")))
    (is (not (random-uuid-p "foobar")))
    (is (not (random-uuid-p 42)))))

(test json
  (is (equal "{}" (json-stringify (fset:map))))
  (is (fset:equal? (json-parse "{}") (fset:map)))

  (is (equal "{\"a\":1}" (json-stringify (fset:map ("a" 1)))))
  (is (fset:equal? (json-parse "{\"a\":1}") (fset:map ("a" 1))))

  (is (equal "{\"a\":[1]}" (json-stringify (fset:map ("a" (fset:seq 1))))))
  (is (fset:equal? (json-parse "{\"a\":[1]}")  (fset:map ("a" (fset:seq 1)))))

  (is (equal "{\"a\":[1,{\"b\":\"foo\"}]}" (json-stringify (fset:map ("a" (fset:seq 1 (fset:map ("b" "foo"))))))))
  (is (fset:equal? (json-parse "{\"a\":[1,{\"b\":\"foo\"}]}")
                   (fset:map ("a" (fset:seq 1 (fset:map ("b" "foo"))))))))

(test json-merge-patch
  (is (fset:equal?
       (json-parse "{\"a\":\"z\",\"c\":{\"d\":\"e\"}}")
       (json-merge-patch
        (json-parse "{\"a\":\"b\",\"c\":{\"d\":\"e\",\"f\":\"g\"}}")
        (json-parse "{\"a\":\"z\",\"c\":{\"f\":null}}"))))

  (is (fset:equal?
       (json-parse
        "{\"title\":\"Hello!\",\"author\":{\"givenName\":\"John\"},\"tags\":[ \"example\"],\"content\":\"This will be unchanged\"}")
       (json-merge-patch
        (json-parse
         "{\"title\":\"Goodbye!\",\"author\":{\"givenName\":\"John\",\"familyName\":\"Doe\"},\"tags\":[ \"example\",\"sample\"],\"content\":\"This will be unchanged\"}")
        (json-parse
         "{\"title\":\"Hello!\",\"author\":{\"familyName\":null},\"tags\":[ \"example\"]}")))))

(test json-diff
  (is (fset:equal?
       (json-parse "{\"a\":\"z\",\"c\":{\"f\":null}}")
       (json-diff
        (json-parse "{\"a\":\"b\",\"c\":{\"d\":\"e\",\"f\":\"g\"}}")
        (json-parse "{\"a\":\"z\",\"c\":{\"d\":\"e\"}}"))))

  (is (fset:equal?
       (json-parse
        "{\"title\":\"Hello!\",\"author\":{\"familyName\":null},\"tags\":[ \"example\"]}")
       (json-diff
        (json-parse
         "{\"title\":\"Goodbye!\",\"author\":{\"givenName\":\"John\",\"familyName\":\"Doe\"},\"tags\":[ \"example\",\"sample\"],\"content\":\"This will be unchanged\"}")
        (json-parse
         "{\"title\":\"Hello!\",\"author\":{\"givenName\":\"John\"},\"tags\":[ \"example\"],\"content\":\"This will be unchanged\"}")))))

(test xsd-json-ld-scalars
  (let* ((date (endb/arrow:parse-arrow-date-days "2001-01-01"))
         (json "{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"}"))
    (is (equalp date (json-parse json)))
    (is (equal json (json-stringify date)))
    (is (fset:equal? date (json-parse json))))

  (let* ((date-time (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z"))
         (json "{\"@value\":\"2023-05-16T14:43:39.970062Z\",\"@type\":\"xsd:dateTime\"}"))
    (is (equalp date-time (json-parse json)))
    (is (equal json (json-stringify date-time)))
    (is (fset:equal? date-time (json-parse json))))

  (let* ((time (endb/arrow:parse-arrow-time-micros "14:43:39.970062"))
         (json "{\"@value\":\"14:43:39.970062\",\"@type\":\"xsd:time\"}"))
    (is (equalp time (json-parse json)))
    (is (equal json (json-stringify time)))
    (is (fset:equal? time (json-parse json))))

  (let* ((time (endb/arrow:parse-arrow-interval-month-day-nanos "PT12H30M5S"))
         (json "{\"@value\":\"PT12H30M5S\",\"@type\":\"xsd:duration\"}"))
    (is (equalp time (json-parse json)))
    (is (equal json (json-stringify time)))
    (is (fset:equal? time (json-parse json))))

  (let* ((binary (trivial-utf-8:string-to-utf-8-bytes "hello world"))
         (json "{\"@value\":\"aGVsbG8gd29ybGQ=\",\"@type\":\"xsd:base64Binary\"}"))
    (is (equalp binary (json-parse json)))
    (is (equal json (json-stringify binary)))
    (is (fset:equal? binary (json-parse json)))))

(test xsd-json-scalars
  (let ((*json-ld-scalars* nil))
    (let* ((date (endb/arrow:parse-arrow-date-days "2001-01-01"))
           (json "\"2001-01-01\""))
      (is (equal json (json-stringify date))))

    (let* ((date-time (endb/arrow:parse-arrow-timestamp-micros "2023-05-16T14:43:39.970062Z"))
           (json "\"2023-05-16T14:43:39.970062Z\""))
      (is (equal json (json-stringify date-time))))

    (let* ((time (endb/arrow:parse-arrow-time-micros "14:43:39.970062"))
           (json "\"14:43:39.970062\""))
      (is (equal json (json-stringify time))))

    (let* ((binary (trivial-utf-8:string-to-utf-8-bytes "hello world"))
           (json "\"aGVsbG8gd29ybGQ=\""))
      (is (equal json (json-stringify binary))))

    (let* ((time (endb/arrow:parse-arrow-interval-month-day-nanos "PT12H30M5S"))
           (json "\"PT12H30M5S\""))
      (is (equal json (json-stringify time))))

    (let* ((sql-null :null)
           (json "null"))
      (is (eql 'null (json-parse json)))
      (is (equal json (json-stringify sql-null))))))

(test json-int64
  (is (= (1- (ash 1 63)) (json-parse (json-stringify (1- (ash 1 63))))))
  (is (= (- (ash 1 63)) (json-parse (json-stringify (- (ash 1 63)))))))

(test json-arrow
  (is (equal "[1,2]" (json-stringify (vector 1 2))))
  (is (equal "{\"foo\":\"bar\",\"baz\":2}" (json-stringify (list (cons "foo" "bar") (cons "baz" 2)))))
  (is (equal "{}" (json-stringify :empty-struct))))

(test stats
  (let* ((expected '((("name" . "joe") ("id" . 1)) (("name" . :null) ("id" . 2)) :null (("name" . "mark") ("id" . 4))))
         (array (endb/arrow:to-arrow expected)))
    (is (fset:equal? (fset:map ("id" (fset:map ("count" 3) ("count_star" 3) ("min" 1) ("max" 4)
                                               ("bloom" (coerce #(20 195 165 82 10 165 210 104) '(vector (unsigned-byte 8))))))
                               ("name" (fset:map ("count" 2) ("count_star" 3) ("min" "joe") ("max" "mark")
                                                 ("bloom" (coerce #(10 192 38 170 10 26 185 168) '(vector (unsigned-byte 8)))))))
                     (calculate-stats (list array)))))

  (let ((batches '(((("x" . 1))
                    (("x" . 2))
                    (("x" . 3))
                    (("x" . 4)))

                   ((("x" . 5))
                    (("x" . 6))
                    (("x" . 7))
                    (("x" . 8))))))
    (is (fset:equal?
         (fset:map ("x" (fset:map ("count" 8) ("count_star" 8) ("min" 1) ("max" 8)
                                  ("bloom" (coerce #(221 83 121 73 211 117 15 215 69 92 198 129 97 148 5) '(vector (unsigned-byte 8)))))))
         (calculate-stats (mapcar #'endb/arrow:to-arrow batches))))))

(test binary-to-bloom
  (let* ((binary (coerce #(10 192 38 170 10 26 185 168) '(vector (unsigned-byte 8))))
         (bloom (binary-to-bloom binary)))
    (is (cl-bloom:memberp bloom "mark"))
    (is (cl-bloom:memberp bloom "joe"))
    (is (not (cl-bloom:memberp bloom "mike")))

    (is (binary-bloom-member-p binary "mark"))
    (is (binary-bloom-member-p binary "joe"))
    (is (not (binary-bloom-member-p binary "mike")))))
