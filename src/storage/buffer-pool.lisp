(defpackage :endb/storage/buffer-pool
  (:use :cl)
  (:export #:make-buffer-pool #:buffer-pool-get #:buffer-pool-put #:buffer-pool-close #:make-writeable-buffer-pool #:writeable-buffer-pool-pool)
  (:import-from :endb/lib/arrow))
(in-package :endb/storage/buffer-pool)

(defgeneric buffer-pool-get (bp path))
(defgeneric buffer-pool-put (bp path arrays))
(defgeneric buffer-pool-close (bp))

(defstruct buffer-pool get-object-fn (pool (make-hash-table :weakness #+sbcl nil #-sbcl :value :synchronized t :test 'equal)) (max-size 4096))

(defun %evict-buffer-pool (bp)
  #+sbcl (with-slots (pool max-size evict-lock) bp
           (sb-ext:with-locked-hash-table #+sbcl (pool)
             (maphash (lambda (k v)
                        (declare (ignore v))
                        (when (<= (hash-table-count pool) max-size)
                          (return-from %evict-buffer-pool))
                        (remhash k pool))
                      pool))))

(defmethod buffer-pool-get ((bp buffer-pool) path)
  (with-slots (get-object-fn pool max-size) bp
    (or (gethash path pool)
        (progn
          (when (> (hash-table-count pool) max-size)
            (%evict-buffer-pool bp))
          (setf (gethash path pool)
                (let ((buffer (funcall get-object-fn path)))
                  (when buffer
                    (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer buffer))))))))

(defmethod buffer-pool-put ((bp buffer-pool) path arrays)
  (error "DML not allowed in read only transaction"))

(defmethod buffer-pool-close ((bp buffer-pool))
  (clrhash (buffer-pool-pool bp)))

(defstruct writeable-buffer-pool parent-pool (pool (make-hash-table :test 'equal)))

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
