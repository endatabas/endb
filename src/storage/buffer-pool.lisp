(defpackage :endb/storage/buffer-pool
  (:use :cl)
  (:export #:make-buffer-pool #:buffer-pool-get #:buffer-pool-close)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/lib/arrow))
(in-package :endb/storage/buffer-pool)

(defgeneric buffer-pool-get (bp path))
(defgeneric buffer-pool-close (bp))

(defstruct buffer-pool object-store (pool (make-hash-table :weakness :value :synchronized t)))

(defmethod buffer-pool-get ((bp buffer-pool) path)
  (or (gethash path (buffer-pool-pool bp))
      (setf (gethash path (buffer-pool-pool bp))
            (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer (endb/storage/object-store:object-store-get (buffer-pool-object-store bp) path)))))

(defmethod buffer-pool-close ((bp buffer-pool))
  (clrhash (buffer-pool-pool bp)))
