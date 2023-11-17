(defpackage :endb/lib/server
  (:use :cl)
  (:export #:start-server #:*db* #:sql-abort-query-error #:parse-command-line)
  (:import-from :cffi)
  (:import-from :endb/json)
  (:import-from :endb/lib))
(in-package :endb/lib/server)

(defvar *db*)

(cffi:defcfun "endb_start_server" :void
  (on-query :pointer)
  (on-error :pointer))

(cffi:defcfun "endb_parse_command_line_to_json" :void
  (on-success :pointer))

(defvar *parse-command-line-on-success*)

(cffi:defcallback parse-command-line-to-json-on-success :void
    ((config-json :string))
  (funcall *parse-command-line-on-success* config-json))

(defun parse-command-line ()
  (endb/lib:init-lib)
  (let* ((result)
         (*parse-command-line-on-success* (lambda (config-json)
                                            (setf result (endb/json:json-parse config-json)))))
    (endb-parse-command-line-to-json (cffi:callback parse-command-line-to-json-on-success))
    result))

(defvar *start-server-on-query*)

(define-condition sql-abort-query-error (error) ())

(defvar *start-server-on-query-on-abort*)

(cffi:defcallback start-server-on-query-abort :void
    ()
  (funcall *start-server-on-query-on-abort*))

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
             (let* ((abortp)
                    (*start-server-on-query-on-abort* (lambda ()
                                                        (setf abortp t))))
               (cffi:foreign-funcall-pointer on-response-init ()
                                             :pointer response
                                             :pointer tx
                                             :short status-code
                                             :string content-type
                                             :pointer (cffi:callback start-server-on-query-abort)
                                             :void)
               (when abortp
                 (error 'sql-abort-query-error))))
           (lambda (body)
             (let* ((abortp)
                    (*start-server-on-query-on-abort* (lambda ()
                                                        (setf abortp t))))
               (cffi:foreign-funcall-pointer on-response-send ()
                                             :pointer sender
                                             :string body
                                             :pointer (cffi:callback start-server-on-query-abort)
                                             :void)
               (when abortp
                 (error 'sql-abort-query-error))))))

(defvar *start-server-on-error*)

(cffi:defcallback start-server-on-error :void
    ((err :string))
  (funcall *start-server-on-error* err))

(defun start-server (on-query)
  (endb/lib:init-lib)
  (let* ((errorp nil)
         (*start-server-on-error* (lambda (err)
                                    (setf errorp t)
                                    (endb/lib:log-error err))))
    (setf *start-server-on-query* on-query)
    (endb-start-server (cffi:callback start-server-on-query)
                       (cffi:callback start-server-on-error))
    (if errorp
        1
        0)))
