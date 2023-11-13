(defpackage :endb/lib/server
  (:use :cl)
  (:export #:start-server #:*db*)
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

(cffi:defcallback start-server-on-query :void
    ((response :pointer)
     (method :string)
     (media-type :string)
     (q :string)
     (p :string)
     (m :string)
     (on-response :pointer))
  (funcall *start-server-on-query* method media-type q p m (lambda (status-code content-type body)
                                                             (cffi:foreign-funcall-pointer on-response () :pointer response :short status-code :string content-type :string body :void))))

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
