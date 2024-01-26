(defpackage :endb/queue
  (:use :cl)
  (:export #:make-queue #:queue-pop #:queue-push #:queue-close #:make-queue-consumer-worker #:make-queue-timer-worker)
  (:import-from :endb/lib)
  (:import-from :bordeaux-threads)
  (:import-from :trivial-backtrace))
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
  (when queue
    (queue-push queue 'close)))

(defun make-queue-consumer-worker (job-queue)
  (lambda ()
    (loop for job = (endb/queue:queue-pop job-queue)
          if (null job)
            do (return-from nil)
          else
            do (block job-block
                 (handler-bind ((error (lambda (e)
                                         (endb/lib:log-error "~A: ~A" (bt:thread-name (bt:current-thread)) e)
                                         (endb/lib:log-debug "~A" (endb/lib:format-backtrace (trivial-backtrace:print-backtrace e :output nil)))
                                         (return-from job-block))))
                   (funcall job))))))

(defun make-queue-timer-worker (close-queue job interval)
  (lambda ()
    (loop
      (multiple-value-bind (event timeoutp)
          (endb/queue:queue-pop close-queue :timeout interval)
        (declare (ignore event))
        (unless timeoutp
          (return-from nil))
        (block job-block
          (handler-bind ((error (lambda (e)
                                  (endb/lib:log-error "~A: ~A" (bt:thread-name (bt:current-thread)) e)
                                  (endb/lib:log-debug "~A" (endb/lib:format-backtrace (trivial-backtrace:print-backtrace e :output nil)))
                                  (return-from job-block))))
            (funcall job)))))))
