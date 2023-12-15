(defpackage :endb-test/storage
  (:use :cl :fiveam :endb/storage/wal :endb/storage/object-store :endb/storage/buffer-pool)
  (:import-from :endb/arrow)
  (:import-from :endb/lib/arrow)
  (:import-from :archive)
  (:import-from :flexi-streams)
  (:import-from :fset)
  (:import-from :trivial-utf-8)
  (:import-from :uiop))
(in-package :endb-test/storage)

(in-suite* :storage)

(test tar-wal-and-object-store
  (let* ((out (flex:make-in-memory-output-stream))
         (wal (open-tar-wal :stream out)))

    (wal-append-entry wal "foo.txt" (trivial-utf-8:string-to-utf-8-bytes "foo"))
    (wal-append-entry wal "bar.txt" (trivial-utf-8:string-to-utf-8-bytes "bar"))
    (wal-fsync wal)

    (is (= 2048 (wal-size wal)))
    (wal-close wal)
    (is (= 10240 (wal-size wal)))

    (let ((out-buffer (flex:get-output-stream-sequence out)))
      (let* ((in (flex:make-in-memory-input-stream out-buffer))
             (wal (open-tar-wal :stream in :direction :input))
             (skip-pred (lambda (path)
                          (equal "foo.txt" path))))

        (is (archive::skippable-p wal))
        (is (= 10240 (wal-size wal)))

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

      (let* ((in (flex:make-in-memory-input-stream out-buffer))
             (wal (open-tar-object-store :stream in)))

        (is (archive::skippable-p wal))
        (is (= 10240 (wal-size wal)))

        (is (equalp (trivial-utf-8:string-to-utf-8-bytes "bar")
                    (object-store-get wal "bar.txt")))

        (is (equalp (trivial-utf-8:string-to-utf-8-bytes "foo")
                    (object-store-get wal "foo.txt")))

        (is (null (object-store-get wal "baz.txt")))

        (is (equal '("bar.txt" "foo.txt") (object-store-list wal)))
        (is (equal '("foo.txt") (object-store-list wal :prefix "foo")))
        (is (equal '("foo.txt") (object-store-list wal :start-after "bar.txt")))))))

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
             (is (= 2048 (file-position in)))
             (let* ((wal (open-tar-wal :stream in :direction :output)))

               (wal-append-entry wal "baz.txt" (trivial-utf-8:string-to-utf-8-bytes "baz"))
               (is (= 10240 (wal-size wal)))
               (wal-close wal)))

           (with-open-file (in test-log :element-type '(unsigned-byte 8))
             (let* ((wal (open-tar-wal :stream in :direction :input)))
               (is (= (+ 12288) (wal-size wal)))

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
  (let* ((out (flex:make-in-memory-output-stream))
         (wal (open-tar-wal :stream out))
         (batches (list (list (fset:map ("x" 1))
                              (fset:map ("x" 2))
                              (fset:map ("x" 3))
                              (fset:map ("x" 4)))

                        (list (fset:map ("x" 5))
                              (fset:map ("x" 6))
                              (fset:map ("x" 7))
                              (fset:map ("x" 8))))))

    (wal-append-entry wal
                      "foo.arrow"
                      (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
                       (mapcar #'endb/arrow:to-arrow batches)))
    (wal-close wal)

    (let* ((in (flex:make-in-memory-input-stream (flex:get-output-stream-sequence out)))
           (os (open-tar-object-store :stream in))
           (bp (make-buffer-pool :get-object-fn (lambda (path)
                                                  (object-store-get os path))))
           (actual (buffer-pool-get bp "foo.arrow")))

      (is (equalp batches (loop for x in actual
                                collect (coerce x 'list))))
      (is (eq actual (buffer-pool-get bp "foo.arrow")))

      (is (null (buffer-pool-get bp "bar.arrow"))))))

(test writable-buffer-pool
  (let* ((out (flex:make-in-memory-output-stream))
         (batches (list (list (fset:map ("x" 1))
                              (fset:map ("x" 2))
                              (fset:map ("x" 3)))))
         (in (flex:make-in-memory-input-stream (flex:get-output-stream-sequence out)))
         (os (open-tar-object-store :stream in))
         (bp (make-buffer-pool :get-object-fn (lambda (path)
                                                (object-store-get os path))))
         (wbp (make-writeable-buffer-pool :parent-pool bp)))

    (buffer-pool-put wbp "foo.arrow" (mapcar #'endb/arrow:to-arrow batches))

    (is (null (buffer-pool-get bp "foo.arrow")))

    (is (equalp batches (loop for x in (buffer-pool-get wbp "foo.arrow")
                              collect (coerce x 'list))))

    (is (null (buffer-pool-get wbp "bar.arrow")))))
