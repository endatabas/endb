(defpackage :endb/lib
  (:use :cl)
  (:export #:init-lib #:*panic-hook* #:format-backtrace #:shutdown-logger
           #:log-error #:log-warn #:log-info #:log-debug #:log-trace #:resolve-log-level #:log-level-active-p #:*log-level*
           #:trace-span #:with-trace-span #:with-trace-kvs-span
           #:sha1 #:uuid-v4 #:uuid-str #:base64-decode #:base64-encode #:xxh64
           #:vector-byte-size #:buffer-to-vector)
  (:import-from :cffi)
  (:import-from :asdf)
  (:import-from :com.inuoe.jzon)
  (:import-from :uiop))
(in-package :endb/lib)

(cffi:define-foreign-library libendb
  (t (:default "libendb")))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter +log-levels+ '(:off :error :warn :info :debug :trace)))

(defun resolve-log-level (&optional level)
  (or (position level +log-levels+)
      (position :info +log-levels+)))

(defvar *log-level* (resolve-log-level :info))

(defun log-level-active-p (level)
  (<= (position level +log-levels+) *log-level*))

(defun format-backtrace (printed-backtrace)
  (let ((backtrace (rest (ppcre:split "[\\n\\r]+" printed-backtrace))))
    (format nil "~A~%~{~A~^~%~}" (string-trim " " (first backtrace)) (rest backtrace))))

(defmacro log-error (control-string &rest format-arguments)
  `(when (<= ,(position :error +log-levels+) *log-level*)
     (init-lib)
     (endb-log-error
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-warn (control-string &rest format-arguments)
  `(when (<= ,(position :warn +log-levels+) *log-level*)
     (init-lib)
     (endb-log-warn
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-info (control-string &rest format-arguments)
  `(when (<= ,(position :info +log-levels+) *log-level*)
     (init-lib)
     (endb-log-info
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-debug (control-string &rest format-arguments)
  `(when (<= ,(position :debug +log-levels+) *log-level*)
     (init-lib)
     (endb-log-debug
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(defmacro log-trace (control-string &rest format-arguments)
  `(when (<= ,(position :trace +log-levels+) *log-level*)
     (init-lib)
     (endb-log-trace
      ,(string-downcase (package-name *package*))
      (format nil ,control-string ,@format-arguments))))

(cffi:defcfun "endb_log_error" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_warn" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_info" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_debug" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_log_trace" :void
  (target :string)
  (message :string))

(cffi:defcfun "endb_trace_span" :void
  (span :string)
  (kvs-json :string)
  (in-scope :pointer))

(cffi:defcfun "endb_init_logger" :void
  (on-success :pointer)
  (on-error :pointer))

(cffi:defcfun "endb_shutdown_logger" :void)

(cffi:defcfun "endb_set_panic_hook" :void
  (on-panic :pointer))

(defvar *initialized* nil)

(defvar *init-logger-on-success*)

(cffi:defcallback init-logger-on-success :void
    ((level :string))
  (funcall *init-logger-on-success* level))

(defvar *init-logger-on-error*)

(cffi:defcallback init-logger-on-error :void
    ((err :string))
  (funcall *init-logger-on-error* err))

(defvar *trace-span-in-scope*)

(cffi:defcallback trace-span-in-scope :void
    ()
  (funcall *trace-span-in-scope*))

(defun trace-span (span in-scope &optional kvs)
  (let* ((result)
         (err)
         (*trace-span-in-scope* (lambda ()
                                  (block error-block
                                    (handler-bind ((error (lambda (e)
                                                            (setf err e)
                                                            (return-from error-block))))
                                      (setf result (multiple-value-list (funcall in-scope))))))))
    (init-lib)
    (endb-trace-span span
                     (if kvs
                         (com.inuoe.jzon:stringify kvs)
                         (cffi:null-pointer))
                     (cffi:callback trace-span-in-scope))
    (when err
      (error err))
    (values-list result)))

(defmacro with-trace-span (span &body body)
  `(trace-span ,span (lambda () ,@body)))

(defmacro with-trace-kvs-span (span kvs &body body)
  `(trace-span ,span (lambda () ,@body) ,kvs))

(defvar *panic-hook* nil)

(cffi:defcallback on-panic-hook :void
    ((err :string))
  (when *initialized*
    (log-error err))
  (when *panic-hook*
    (funcall *panic-hook*)))

(defun shutdown-logger ()
  (endb-shutdown-logger))

(defun init-logger ()
  (let* ((result)
         (*init-logger-on-success* (lambda (level)
                                     (setf result level)))
         (err)
         (*init-logger-on-error* (lambda (e)
                                   (setf err e))))
    (endb-init-logger (cffi:callback init-logger-on-success)
                      (cffi:callback init-logger-on-error))
    (when err
      (error err))
    result))

(cffi:defcfun "endb_base64_encode" :void
  (buffer-ptr :pointer)
  (buffers-size :size)
  (on-success :pointer))

(defvar *endb-base64-encode-on-success*)

(cffi:defcallback endb-base64-encode-on-success :void
    ((base64 :string))
  (funcall *endb-base64-encode-on-success* base64))

(defun base64-encode (buffer)
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-base64-encode-on-success* (lambda (base64)
                                            (setf result base64))))
    (cffi:with-pointer-to-vector-data (buffer-ptr #+sbcl (sb-ext:array-storage-vector buffer)
                                                  #-sbcl buffer)
      (endb-base64-encode buffer-ptr (length buffer) (cffi:callback endb-base64-encode-on-success)))
    result))

(cffi:defcfun "endb_base64_decode" :void
  (string :string)
  (on-success :pointer)
  (on-error :pointer))

(defvar *endb-base64-decode-on-success*)

(cffi:defcallback endb-base64-decode-on-success :void
    ((buffer-ptr :pointer)
     (buffer-size :size))
  (funcall *endb-base64-decode-on-success* buffer-ptr buffer-size))

(defvar *endb-base64-decode-on-error*)

(cffi:defcallback endb-base64-decode-on-error :void
    ((err :string))
  (funcall *endb-base64-decode-on-error* err))

(defun base64-decode (string)
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-base64-decode-on-success* (lambda (buffer-ptr buffer-size)
                                            (setf result (buffer-to-vector buffer-ptr buffer-size))))
         (*endb-base64-decode-on-error* (lambda (err)
                                          (declare (ignore err))
                                          (setf result nil))))
    (endb-base64-decode string (cffi:callback endb-base64-decode-on-success) (cffi:callback endb-base64-decode-on-error))
    result))

(cffi:defcfun "endb_sha1" :void
  (buffer-ptr :pointer)
  (buffers-size :size)
  (on-success :pointer))

(defvar *endb-sha1-on-success*)

(cffi:defcallback endb-sha1-on-success :void
    ((sha1 :string))
  (funcall *endb-sha1-on-success* sha1))

(defun sha1 (buffer)
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-sha1-on-success* (lambda (sha1)
                                   (setf result sha1))))
    (cffi:with-pointer-to-vector-data (buffer-ptr #+sbcl (sb-ext:array-storage-vector buffer)
                                                  #-sbcl buffer)
      (endb-sha1 buffer-ptr (length buffer) (cffi:callback endb-sha1-on-success)))
    result))

(cffi:defcfun "endb_uuid_v4" :void
  (on-success :pointer))

(defvar *endb-uuid-v4-on-success*)

(cffi:defcallback endb-uuid-v4-on-success :void
    ((uuid :string))
  (funcall *endb-uuid-v4-on-success* uuid))

(defun uuid-v4 ()
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-uuid-v4-on-success* (lambda (uuid)
                                      (setf result uuid))))
    (endb-uuid-v4 (cffi:callback endb-uuid-v4-on-success))
    result))

(cffi:defcfun "endb_uuid_str" :void
  (buffer-ptr :pointer)
  (buffers-size :size)
  (on-success :pointer)
  (on-error :pointer))

(defvar *endb-uuid-str-on-success*)

(cffi:defcallback endb-uuid-str-on-success :void
    ((uuid :string))
  (funcall *endb-uuid-str-on-success* uuid))

(defvar *endb-uuid-str-on-error*)

(cffi:defcallback endb-uuid-str-on-error :void
    ((err :string))
  (funcall *endb-uuid-str-on-error* err))

(defun uuid-str (buffer)
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-uuid-str-on-success* (lambda (uuid)
                                       (setf result uuid)))
         (*endb-uuid-str-on-error* (lambda (err)
                                     (declare (ignore err))
                                     (setf result nil))))
    (cffi:with-pointer-to-vector-data (buffer-ptr #+sbcl (sb-ext:array-storage-vector buffer)
                                                  #-sbcl buffer)
      (endb-uuid-str buffer-ptr (length buffer) (cffi:callback endb-uuid-str-on-success) (cffi:callback endb-uuid-str-on-error)))
    result))

(cffi:defcfun "endb_xxh64" :size
  (buffer-ptr :pointer)
  (buffers-size :size)
  (seed :size))

(defun xxh64 (buffer &key (seed 0))
  (endb/lib:init-lib)
  (cffi:with-pointer-to-vector-data (buffer-ptr #+sbcl (sb-ext:array-storage-vector buffer)
                                                #-sbcl buffer)
    (endb-xxh64 buffer-ptr (length buffer) seed)))

(cffi:defcfun "endb_memcpy" :pointer
  (dest :pointer)
  (src :pointer)
  (n :size))

(defun vector-byte-size (b &optional (buffer-size (length b)))
  (etypecase b
    ((vector bit) (truncate (+ 7 buffer-size) 8))
    ((vector (unsigned-byte 8)) buffer-size)
    ((vector (signed-byte 8)) buffer-size)
    ((vector (signed-byte 32)) (* 4 buffer-size))
    ((vector (signed-byte 64)) (* 8 buffer-size))
    ((vector double-float) (* 8 buffer-size))))

(defun buffer-to-vector (buffer-ptr buffer-size &optional out)
  (let ((out (or out (make-array buffer-size :element-type '(unsigned-byte 8)))))
    (assert (<= buffer-size (vector-byte-size out)))
    (cffi:with-pointer-to-vector-data (out-ptr #+sbcl (sb-ext:array-storage-vector out)
                                               #-sbcl out)
      (endb-memcpy out-ptr buffer-ptr buffer-size))
    out))

(defun init-lib ()
  (unless *initialized*
    (pushnew (or (uiop:pathname-directory-pathname (uiop:argv0))
                 (asdf:system-relative-pathname :endb "target/"))
             cffi:*foreign-library-directories*)
    (cffi:use-foreign-library libendb)

    (endb-set-panic-hook (cffi:callback on-panic-hook))
    (let ((log-level (init-logger)))
      (setf *log-level* (resolve-log-level (intern (string-upcase log-level) :keyword))))
    (setf *initialized* t)))
