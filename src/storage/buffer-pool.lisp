(defpackage :endb/storage/buffer-pool
  (:use :cl)
  (:export #:make-buffer-pool #:buffer-pool-get #:buffer-pool-put #:buffer-pool-evict #:buffer-pool-close #:make-writeable-buffer-pool #:writeable-buffer-pool-pool)
  (:import-from :bordeaux-threads)
  (:import-from :endb/lib)
  (:import-from :endb/lib/arrow))
(in-package :endb/storage/buffer-pool)

(defgeneric buffer-pool-get (bp path &key sha1))
(defgeneric buffer-pool-put (bp path arrays))
(defgeneric buffer-pool-evict (bp path))
(defgeneric buffer-pool-close (bp))

(defstruct buffer-pool
  get-object-fn
  (pool (make-hash-table :weakness #+sbcl nil #-sbcl :value :synchronized t :test 'equal))
  (evict-lock (bt:make-lock))
  (max-size (* 512 1024 1024))
  (current-size 0)
  (evict-ratio 0.8d0))

(defstruct buffer-pool-entry arrays size)

(defun %evict-buffer-pool (bp)
  #+sbcl (with-slots (pool max-size evict-lock evict-ratio current-size) bp
           (bt:with-lock-held (evict-lock)
             (maphash (lambda (k v)
                        (when (<= current-size (* evict-ratio max-size))
                          (return-from %evict-buffer-pool))
                        (decf current-size (buffer-pool-entry-size v))
                        (remhash k pool))
                      pool))))

(defmethod buffer-pool-get ((bp buffer-pool) path &key sha1)
  (with-slots (get-object-fn pool max-size current-size evict-lock) bp
    (let ((entry (or (gethash path pool)
                     (progn
                       (when (> current-size max-size)
                         (%evict-buffer-pool bp))
                       (let ((buffer (funcall get-object-fn path)))
                         (when buffer
                           (when sha1
                             (let ((buffer-sha1 (endb/lib:sha1 buffer)))
                               (assert (equal sha1 buffer-sha1)
                                       nil
                                       (format nil "Arrow SHA1 mismatch: ~A does not match stored: ~A" sha1 buffer-sha1))))
                           (let ((entry (make-buffer-pool-entry :arrays (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer buffer)
                                                                :size (length buffer))))
                             (bt:with-lock-held (evict-lock)
                               (incf current-size (buffer-pool-entry-size entry))
                               (setf (gethash path pool) entry)))))))))
      (when entry
        (buffer-pool-entry-arrays entry)))))

(defmethod buffer-pool-put ((bp buffer-pool) path arrays)
  (error "DML not allowed in read only transaction"))

(defmethod buffer-pool-evict ((bp buffer-pool) path)
  (with-slots (pool current-size evict-lock) bp
    (bt:with-lock-held (evict-lock)
      (let ((entry (gethash path pool)))
        (when entry
          (decf current-size (buffer-pool-entry-size entry))
          (remhash path pool))
        bp))))

(defmethod buffer-pool-close ((bp buffer-pool))
  (clrhash (buffer-pool-pool bp)))

(defstruct writeable-buffer-pool parent-pool (pool (make-hash-table :test 'equal)))

(defmethod buffer-pool-get ((bp writeable-buffer-pool) path &key sha1)
  (declare (ignore sha1))
  (with-slots (parent-pool pool) bp
    (or (gethash path pool)
        (buffer-pool-get parent-pool path))))

(defmethod buffer-pool-put ((bp writeable-buffer-pool) path arrays)
  (with-slots (pool) bp
    (setf (gethash path pool) arrays)
    bp))

(defmethod buffer-pool-evict ((bp writeable-buffer-pool) path)
  (with-slots (parent-pool pool) bp
    (if (gethash path pool)
        (remhash path pool)
        (buffer-pool-evict parent-pool path))
    bp))

(defmethod buffer-pool-close ((bp writeable-buffer-pool))
  (clrhash (writeable-buffer-pool-pool bp)))
