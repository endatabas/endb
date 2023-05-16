(defpackage :endb-test/storage
  (:use :cl :fiveam :endb/storage/wal :endb/storage/object-store :endb/storage/buffer-pool :endb/storage/meta-data)
  (:import-from :endb/arrow)
  (:import-from :endb/lib/arrow)
  (:import-from :archive)
  (:import-from :fast-io)
  (:import-from :fset)
  (:import-from :trivial-utf-8)
  (:import-from :uiop))
(in-package :endb-test/storage)

(in-suite* :all-tests)

(test tar-wal-and-object-store
  (let* ((out (make-instance 'fast-io:fast-output-stream))
         (wal (open-tar-wal :stream out)))

    (wal-append-entry wal "foo.txt" (trivial-utf-8:string-to-utf-8-bytes "foo"))
    (wal-append-entry wal "bar.txt" (trivial-utf-8:string-to-utf-8-bytes "bar"))
    (wal-fsync wal)
    (wal-close wal)

    (let* ((in (make-instance 'fast-io:fast-input-stream :vector (fast-io:finish-output-stream out)))
           (wal (open-tar-wal :stream in :direction :input))
           (skip-pred (lambda (path)
                        (equal "foo.txt" path))))

      (is (archive::skippable-p wal))

      (multiple-value-bind (buffer name pos)
          (wal-read-next-entry wal :skip-if skip-pred)
        (is (null buffer))
        (is (equal "foo.txt" name))
        (is (= 1024 pos)))

      (multiple-value-bind (buffer name pos)
          (wal-read-next-entry wal :skip-if skip-pred)
        (is (equalp (trivial-utf-8:string-to-utf-8-bytes "bar") buffer))
        (is (equal "bar.txt" name))
        (is (= 2048 pos)))

      (multiple-value-bind (buffer name pos)
          (wal-read-next-entry wal :skip-if skip-pred)
        (is (null buffer))
        (is (null name))
        (is (= 2560 pos))))

    (let* ((in (make-instance 'fast-io:fast-input-stream :vector (fast-io:finish-output-stream out)))
           (wal (open-tar-object-store :stream in)))

      (is (archive::skippable-p wal))

      (is (equalp (trivial-utf-8:string-to-utf-8-bytes "bar")
                  (object-store-get wal "bar.txt")))

      (is (equalp (trivial-utf-8:string-to-utf-8-bytes "foo")
                  (object-store-get wal "foo.txt")))

      (is (null (object-store-get wal "baz.txt")))

      (is (equal '("bar.txt" "foo.txt") (object-store-list wal)))
      (is (equal '("foo.txt") (object-store-list wal :prefix "foo")))
      (is (equal '("foo.txt") (object-store-list wal :start-after "bar.txt"))))))

(test tar-wal-reopen-and-append
  (let* ((target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-log (merge-pathnames "example.log" target-dir)))
    (ensure-directories-exist target-dir)
    (unwind-protect
         (progn
           (with-open-file (in test-log :direction :io
                                        :element-type '(unsigned-byte 8)
                                        :if-exists :overwrite
                                        :if-does-not-exist :create)
             (tar-wal-position-stream-at-end in)
             (let* ((wal (open-tar-wal :stream in :direction :output)))

               (wal-append-entry wal "foo.txt" (trivial-utf-8:string-to-utf-8-bytes "foo"))
               (wal-append-entry wal "bar.txt" (trivial-utf-8:string-to-utf-8-bytes "bar"))
               (wal-close wal)))

           (with-open-file (in test-log :direction :io
                                        :element-type '(unsigned-byte 8)
                                        :if-exists :overwrite
                                        :if-does-not-exist :create)
             (tar-wal-position-stream-at-end in)
             (let* ((wal (open-tar-wal :stream in :direction :output)))

               (wal-append-entry wal "baz.txt" (trivial-utf-8:string-to-utf-8-bytes "baz"))
               (wal-close wal)))

           (with-open-file (in test-log :element-type '(unsigned-byte 8))
             (let* ((wal (open-tar-wal :stream in :direction :input)))

               (multiple-value-bind (buffer name pos)
                   (wal-read-next-entry wal)
                 (is (equalp (trivial-utf-8:string-to-utf-8-bytes "foo") buffer))
                 (is (equal "foo.txt" name))
                 (is (= 1024 pos)))

               (multiple-value-bind (buffer name pos)
                   (wal-read-next-entry wal)
                 (is (equalp (trivial-utf-8:string-to-utf-8-bytes "bar") buffer))
                 (is (equal "bar.txt" name))
                 (is (= 2048 pos)))

               (multiple-value-bind (buffer name pos)
                   (wal-read-next-entry wal)
                 (is (equalp (trivial-utf-8:string-to-utf-8-bytes "baz") buffer))
                 (is (equal "baz.txt" name))
                 (is (= 3072 pos)))

               (multiple-value-bind (buffer name pos)
                   (wal-read-next-entry wal)
                 (is (null buffer))
                 (is (null name))
                 (is (= 3584 pos))))))

      (when (probe-file test-log)
        (delete-file test-log)))))

(test directory-object-store
  (let* ((target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-dir (merge-pathnames "object-store/" target-dir)))
    (unwind-protect
         (let* ((os (make-directory-object-store :path test-dir)))

           (object-store-put os "foo.txt" (trivial-utf-8:string-to-utf-8-bytes "foo"))
           (object-store-put os "baz/bar.txt" (trivial-utf-8:string-to-utf-8-bytes "baz/bar"))

           (is (equalp (trivial-utf-8:string-to-utf-8-bytes "foo")
                       (object-store-get os "foo.txt")))

           (is (equalp (trivial-utf-8:string-to-utf-8-bytes "baz/bar")
                       (object-store-get os "baz/bar.txt")))

           (is (null (object-store-get os "baz.txt")))
           (is (null (object-store-get os "baz/foo.txt")))

           (is (equal '("baz/bar.txt" "foo.txt") (object-store-list os)))
           (is (equal '("baz/bar.txt") (object-store-list os :prefix "baz/")))
           (is (equal '("foo.txt") (object-store-list os :start-after "baz/bar.txt"))))

      (when (probe-file test-dir)
        (uiop:delete-directory-tree test-dir :validate t)))))

(test buffer-pool
  (let* ((out (make-instance 'fast-io:fast-output-stream))
         (wal (open-tar-wal :stream out))
         (batches '(((("x" . 1))
                     (("x" . 2))
                     (("x" . 3))
                     (("x" . 4)))

                    ((("x" . 5))
                     (("x" . 6))
                     (("x" . 7))
                     (("x" . 8))))))

    (wal-append-entry wal
                      "foo.arrow"
                      (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
                       (mapcar #'endb/arrow:to-arrow batches)))
    (wal-close wal)

    (let* ((in (make-instance 'fast-io:fast-input-stream :vector (fast-io:finish-output-stream out)))
           (os (open-tar-object-store :stream in))
           (bp (make-buffer-pool :object-store os))
           (actual (buffer-pool-get bp "foo.arrow")))

      (is (equal batches (loop for x in actual
                               collect (coerce x 'list))))
      (is (eq actual (buffer-pool-get bp "foo.arrow")))

      (is (null (buffer-pool-get bp "bar.arrow"))))))

(test writable-buffer-pool
  (let* ((out (make-instance 'fast-io:fast-output-stream))
         (batches '(((("x" . 1))
                     (("x" . 2)))))
         (in (make-instance 'fast-io:fast-input-stream :vector (fast-io:finish-output-stream out)))
         (os (open-tar-object-store :stream in))
         (bp (make-buffer-pool :object-store os))
         (wbp (make-writeable-buffer-pool :parent-pool bp)))

    (buffer-pool-put wbp "foo.arrow" (mapcar #'endb/arrow:to-arrow batches))

    (is (null (buffer-pool-get bp "foo.arrow")))

    (is (equal batches (loop for x in (buffer-pool-get wbp "foo.arrow")
                             collect (coerce x 'list))))

    (is (null (buffer-pool-get wbp "bar.arrow")))))

(test random-uuid
  (let ((uuid (random-uuid #+sbcl (sb-ext:seed-random-state 0)
                           #-sbcl *random-state*)))
    #+sbcl (is (equal "8c7f0aac-97c4-4a2f-b716-a675d821ccc0" uuid))
    (is (random-uuid-p uuid))
    (is (random-uuid-p "00000000-0000-4000-8000-000000000000"))
    (is (not (random-uuid-p "00000000-0000-1000-8000-000000000000")))
    (is (not (random-uuid-p "foobar")))
    (is (not (random-uuid-p 42)))))

(test meta-data-json
  (is (equal "{}" (meta-data->json (fset:map))))
  (is (fset:equal? (json->meta-data "{}") (fset:map)))

  (is (equal "{\"a\":1}" (meta-data->json (fset:map ("a" 1)))))
  (is (fset:equal? (json->meta-data "{\"a\":1}") (fset:map ("a" 1))))

  (is (equal "{\"a\":[1]}" (meta-data->json (fset:map ("a" (fset:seq 1))))))
  (is (fset:equal? (json->meta-data "{\"a\":[1]}")  (fset:map ("a" (fset:seq 1)))))

  (is (equal "{\"a\":[1,{\"b\":\"foo\"}]}" (meta-data->json (fset:map ("a" (fset:seq 1 (fset:map ("b" "foo"))))))))
  (is (fset:equal? (json->meta-data "{\"a\":[1,{\"b\":\"foo\"}]}")
                   (fset:map ("a" (fset:seq 1 (fset:map ("b" "foo"))))))))

(test meta-data-merge-patch
  (is (fset:equal?
       (json->meta-data "{\"a\":\"z\",\"c\":{\"d\":\"e\"}}")
       (meta-data-merge-patch
        (json->meta-data "{\"a\":\"b\",\"c\":{\"d\":\"e\",\"f\":\"g\"}}")
        (json->meta-data "{\"a\":\"z\",\"c\":{\"f\":null}}"))))

  (is (fset:equal?
       (json->meta-data
        "{\"title\":\"Hello!\",\"author\":{\"givenName\":\"John\"},\"tags\":[ \"example\"],\"content\":\"This will be unchanged\"}")
       (meta-data-merge-patch
        (json->meta-data
         "{\"title\":\"Goodbye!\",\"author\":{\"givenName\":\"John\",\"familyName\":\"Doe\"},\"tags\":[ \"example\",\"sample\"],\"content\":\"This will be unchanged\"}")
        (json->meta-data
         "{\"title\":\"Hello!\",\"author\":{\"familyName\":null},\"tags\":[ \"example\"]}")))))

(test meta-data-xsd-json-ld-scalars
  (let* ((date (local-time:parse-timestring "2001-01-01" :allow-missing-time-part t))
         (date (make-instance 'endb/arrow:arrow-date :day (local-time:day-of date)))
         (json "{\"@value\":\"2001-01-01\",\"@type\":\"xsd:date\"}"))
    (is (local-time:timestamp= date (json->meta-data json)))
    (is (equal json (meta-data->json date))))

  (let* ((date-time (local-time:parse-timestring "2023-05-16T14:43:39.970062Z"))
         (json "{\"@value\":\"2023-05-16T14:43:39.970062Z\",\"@type\":\"xsd:dateTime\"}"))
    (is (local-time:timestamp= date-time (json->meta-data json)))
    (is (equal json (meta-data->json date-time))))

  (let* ((time (local-time:parse-timestring "14:43:39.970062" :allow-missing-date-part t))
         (time (make-instance 'endb/arrow:arrow-time :sec (local-time:sec-of time) :nsec (local-time:nsec-of time)))
         (json "{\"@value\":\"14:43:39.970062\",\"@type\":\"xsd:time\"}"))
    (is (local-time:timestamp= time (json->meta-data json)))
    (is (equal json (meta-data->json time))))

  (let* ((binary (trivial-utf-8:string-to-utf-8-bytes "hello world"))
         (json "{\"@value\":\"aGVsbG8gd29ybGQ=\",\"@type\":\"xsd:base64Binary\"}"))
    (is (equalp binary (json->meta-data json)))
    (is (equal json (meta-data->json binary)))))

(test meta-data-json-int64
  (is (= (ash 1 63) (json->meta-data (meta-data->json (ash 1 63)))))
  (is (= (- 1 (ash 1 63)) (json->meta-data (meta-data->json (- 1 (ash 1 63)))))))
