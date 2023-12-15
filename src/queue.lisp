(defpackage :endb/queue
  (:use :cl)
  (:export #:make-queue #:queue-pop #:queue-push #:queue-close)
  (:import-from :bordeaux-threads))
(in-package :endb/queue)

(defstruct queue (lock (bt:make-lock)) (cv (bt:make-condition-variable)) data)

(defun queue-pop (queue &key timeout)
  (with-slots (lock cv data) queue
    (bt:with-lock-held (lock)
      (loop until data
            do (or (bt:condition-wait cv lock :timeout timeout)
                   (return-from queue-pop (values nil t))))
      (let ((x (car (last data))))
        (unless (eq 'close x)
          (setf data (butlast data))
          x)))))

(defun queue-push (queue x)
  (with-slots (lock cv data) queue
    (bt:with-lock-held (lock)
      (unless (eq 'close (car (last data)))
        (push x data)
        (bt:condition-notify cv)))))

(defun queue-close (queue)
  (queue-push queue 'close))
