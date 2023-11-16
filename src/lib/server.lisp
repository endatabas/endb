(defpackage :endb/lib/server
  (:use :cl)
  (:export #:start-server #:*db* #:sql-abort-query-error)
  (:import-from :cffi)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :uiop))
(in-package :endb/lib/server)

(defvar *db*)

(cffi:defcfun "endb_start_server" :void
  (on-init :pointer)
  (on-query :pointer)
  (on-error :pointer))

(defvar *start-server-on-init*)

(cffi:defcallback start-server-on-init :void
    ((config-json :string))
  (funcall *start-server-on-init* (endb/json:json-parse config-json)))

(defvar *start-server-on-query*)

(define-condition sql-abort-query-error (error) ())

(cffi:defcallback start-server-on-query-abort :void
    ()
  (error 'sql-abort-query-error))

(cffi:defcallback start-server-on-query :void
  ((response :pointer)
   (sender :pointer)
   (tx :pointer)
   (method :string)
   (media-type :string)
   (q :string)
   (p :string)
   (m :string)
   (on-response-init :pointer)
   (on-response-send :pointer))
  (funcall *start-server-on-query* method media-type q p m
           (lambda (status-code content-type)
             (cffi:foreign-funcall-pointer on-response-init ()
                                           :pointer response
                                           :pointer tx
                                           :short status-code
                                           :string content-type
                                           :pointer (cffi:callback start-server-on-query-abort)
                                           :void))
           (lambda (body)
             (cffi:foreign-funcall-pointer on-response-send ()
                                           :pointer sender
                                           :string body
                                           :pointer (cffi:callback start-server-on-query-abort)
                                           :void))))

(defvar *start-server-on-error*)

(cffi:defcallback start-server-on-error :void
    ((err :string))
  (funcall *start-server-on-error* err))

(defun start-server (on-init on-query)
  (endb/lib:init-lib)
  (let* ((errorp nil)
         (*start-server-on-init* on-init)
         (*start-server-on-error* (lambda (err)
                                    (setf errorp t)
                                    (endb/lib:log-error err))))
    (setf *start-server-on-query* on-query)
    (endb-start-server (cffi:callback start-server-on-init)
                       (cffi:callback start-server-on-query)
                       (cffi:callback start-server-on-error))
    (if errorp
        1
        0)))
