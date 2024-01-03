(defpackage :endb/storage/buffer-pool
  (:use :cl)
  (:export #:make-buffer-pool #:buffer-pool-get #:buffer-pool-put #:buffer-pool-evict #:buffer-pool-close
           #:make-writeable-buffer-pool #:writeable-buffer-pool-pool #:deep-copy-writeable-buffer-pool)
  (:import-from :alexandria)
  (:import-from :bordeaux-threads)
  (:import-from :endb/lib)
  (:import-from :endb/lib/arrow))
(in-package :endb/storage/buffer-pool)

(defgeneric buffer-pool-get (bp path &key sha1 read-through-p))
(defgeneric buffer-pool-put (bp path arrays))
(defgeneric buffer-pool-evict (bp path))
(defgeneric buffer-pool-close (bp))

(defstruct buffer-pool
  get-object-fn
  (pool (make-hash-table :synchronized t :test 'equal))
  (evict-lock (bt:make-lock))
  (max-size (* 512 1024 1024))
  (current-size 0)
  (evict-ratio 0.8d0))

(defstruct buffer-pool-entry arrays size)

(defun %evict-buffer-pool (bp)
  (with-slots (pool max-size evict-lock evict-ratio current-size) bp
    (bt:with-lock-held (evict-lock)
      (let ((target-size (* evict-ratio max-size)))
        (loop until (<= current-size target-size)
              do (maphash (lambda (k v)
                            (when (<= current-size target-size)
                              (return-from %evict-buffer-pool))
                            (when (<= evict-ratio (random 1.0d0))
                              (decf current-size (buffer-pool-entry-size v))
                              (remhash k pool)))
                          pool))))))

(defmethod buffer-pool-get ((bp buffer-pool) path &key sha1 read-through-p)
  (with-slots (get-object-fn pool max-size current-size evict-lock) bp
    (let ((entry (or (gethash path pool)
                     (let ((buffer (funcall get-object-fn path)))
                       (when buffer
                         (when sha1
                           (let ((buffer-sha1 (endb/lib:sha1 buffer)))
                             (assert (equal sha1 buffer-sha1)
                                     nil
                                     (format nil "Arrow SHA1 mismatch: ~A does not match stored: ~A" sha1 buffer-sha1))))
                         (if read-through-p
                             (return-from buffer-pool-get (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer buffer))
                             (progn
                               (when (> current-size max-size)
                                 (%evict-buffer-pool bp))
                               (let ((entry (make-buffer-pool-entry :arrays (endb/lib/arrow:read-arrow-arrays-from-ipc-buffer buffer)
                                                                    :size (length buffer))))
                                 (bt:with-lock-held (evict-lock)
                                   (incf current-size (buffer-pool-entry-size entry))
                                   (setf (gethash path pool) entry))))))))))
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

(defmethod buffer-pool-get ((bp writeable-buffer-pool) path &key sha1 read-through-p)
  (with-slots (parent-pool pool) bp
    (let ((buffer (gethash path pool)))
      (if buffer
          (values buffer t)
          (buffer-pool-get parent-pool path :sha1 sha1 :read-through-p read-through-p)))))

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

(defun deep-copy-writeable-buffer-pool (bp)
  (let* ((new-pool (alexandria:copy-hash-table (writeable-buffer-pool-pool bp)))
         (bp (copy-writeable-buffer-pool bp)))
    (setf (writeable-buffer-pool-pool bp) new-pool)
    (maphash (lambda (k v)
               (setf (gethash k new-pool)
                     (loop for buffer in v
                           collect (endb/arrow:to-arrow (coerce buffer 'list)))))
             new-pool)
    bp))
