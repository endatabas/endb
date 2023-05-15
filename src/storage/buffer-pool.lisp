(defpackage :endb/storage/buffer-pool
  (:use :cl)
  (:export #:make-buffer-pool #:buffer-pool-get #:buffer-pool-put #:buffer-pool-close #:make-writeable-buffer-pool)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/lib/arrow))
(in-package :endb/storage/buffer-pool)

(defgeneric buffer-pool-get (bp path))
(defgeneric buffer-pool-put (bp path arrays))
(defgeneric buffer-pool-close (bp))

(defstruct buffer-pool object-store (pool (make-hash-table :weakness :value :synchronized t)))

(defmethod buffer-pool-get ((bp buffer-pool) path)
  (with-slots (object-store pool) bp
    (or (gethash path pool)
        (setf (gethash path pool)
              (let ((buffer (endb/storage/object-store:object-store-get object-store path)))
                (when buffer
                  (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer buffer)))))))

(defmethod buffer-pool-close ((bp buffer-pool))
  (clrhash (buffer-pool-pool bp)))

(defstruct writeable-buffer-pool parent-pool (pool (make-hash-table)))

(defmethod buffer-pool-get ((bp writeable-buffer-pool) path)
  (with-slots (parent-pool pool) bp
    (or (gethash path pool)
        (buffer-pool-get parent-pool path))))

(defmethod buffer-pool-put ((bp writeable-buffer-pool) path arrays)
  (with-slots (pool) bp
    (setf (gethash path pool) arrays)
    bp))

(defmethod buffer-pool-close ((bp writeable-buffer-pool))
  (clrhash (writeable-buffer-pool-pool bp)))
