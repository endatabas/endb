(defpackage :endb-test/wal
  (:use :cl :fiveam :endb/wal)
  (:import-from :fast-io)
  (:import-from :trivial-utf-8))
(in-package :endb-test/wal)

(in-suite* :all-tests)

(test tar-wal
  (let* ((out (make-instance 'fast-io:fast-output-stream))
         (wal (make-wal :stream out)))

    (wal-append-entry wal "foo.txt" (trivial-utf-8:string-to-utf-8-bytes "foo"))
    (wal-append-entry wal "bar.txt" (trivial-utf-8:string-to-utf-8-bytes "bar"))
    (wal-close wal)

    (let* ((in (make-instance 'fast-io:fast-input-stream :vector (fast-io:finish-output-stream out)))
           (wal (make-wal :stream in :direction :input)))

      (multiple-value-bind (buffer name)
          (wal-read-next-entry wal)
        (is (equalp (trivial-utf-8:string-to-utf-8-bytes "foo") buffer))
        (is (equal "foo.txt" name)))

      (multiple-value-bind (buffer name)
          (wal-read-next-entry wal)
        (is (equalp (trivial-utf-8:string-to-utf-8-bytes "bar") buffer))
        (is (equal "bar.txt" name)))

      (is (null (wal-read-next-entry wal))))))
