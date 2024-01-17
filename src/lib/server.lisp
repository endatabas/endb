(defpackage :endb/lib/server
  (:use :cl)
  (:export #:start-server #:sql-abort-query-error #:parse-command-line #:get-endb-version #:start-tokio)
  (:import-from :alexandria)
  (:import-from :cffi)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :endb/sql/db)
  (:import-from :trivial-utf-8))
(in-package :endb/lib/server)

(cffi:defcfun "endb_start_server" :void
  (on-query :pointer)
  (on-error :pointer)
  (on-ws-init :pointer)
  (on-ws-close :pointer)
  (on-ws-message :pointer))

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

(cffi:defcfun "endb_version" :void
  (on-success :pointer))

(defvar *endb-version-on-success*)

(cffi:defcallback endb-version-on-success :void
    ((version :string))
  (funcall *endb-version-on-success* version))

(defun get-endb-version ()
  (endb/lib:init-lib)
  (let* ((result)
         (*endb-version-on-success* (lambda (version)
                                      (setf result version))))
    (endb-version (cffi:callback endb-version-on-success))
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
                                                        (setf abortp t)))
                    (body (if (or (typep body 'base-string)
                                  (typep body '(vector (unsigned-byte 8))))
                              body
                              (trivial-utf-8:string-to-utf-8-bytes body))))
               (cffi:with-pointer-to-vector-data (body-ptr  #+sbcl (sb-ext:array-storage-vector body)
                                                            #-sbcl body)
                 (cffi:foreign-funcall-pointer on-response-send ()
                                               :pointer sender
                                               :pointer body-ptr
                                               :size (length body)
                                               :pointer (cffi:callback start-server-on-query-abort)
                                               :void))
               (when abortp
                 (error 'sql-abort-query-error))))))

(defvar *start-server-on-error*)

(cffi:defcallback start-server-on-error :void
    ((err :string))
  (funcall *start-server-on-error* err))

(defvar *start-server-on-websocket-init*)

(cffi:defcallback start-server-on-websocket-init :void
    ((remote-addr :string))
  (funcall *start-server-on-websocket-init* remote-addr))

(defvar *start-server-on-websocket-close*)

(cffi:defcallback start-server-on-websocket-close :void
    ((remote-addr :string))
  (funcall *start-server-on-websocket-close* remote-addr))

(defvar *start-server-on-websocket-message-on-abort*)

(cffi:defcallback start-server-on-websocket-message-on-abort :void
    ()
  (funcall *start-server-on-websocket-message-on-abort*))

(defvar *start-server-on-websocket-message*)

(cffi:defcallback start-server-on-websocket-message :void
    ((remote-addr :string)
     (ws-stream :pointer)
     (message-ptr :pointer)
     (message-size :size)
     (on-ws-send :pointer))
  (funcall *start-server-on-websocket-message*
           remote-addr
           (cffi:foreign-string-to-lisp message-ptr :count message-size)
           (lambda (body)
             (let* ((abortp)
                    (*start-server-on-websocket-message-on-abort* (lambda ()
                                                                    (setf abortp t)))
                    (body (if (or (typep body 'base-string)
                                  (typep body '(vector (unsigned-byte 8))))
                              body
                              (trivial-utf-8:string-to-utf-8-bytes body))))
               (cffi:with-pointer-to-vector-data (body-ptr  #+sbcl (sb-ext:array-storage-vector body)
                                                            #-sbcl body)
                 (cffi:foreign-funcall-pointer on-ws-send ()
                                               :pointer ws-stream
                                               :pointer body-ptr
                                               :size (length body)
                                               :pointer (cffi:callback start-server-on-websocket-message-on-abort)
                                               :void))
               (when abortp
                 (error 'sql-abort-query-error))))))

(defun start-server (dbms on-query &optional on-ws-message)
  (endb/lib:init-lib)
  (let* ((errorp nil)
         (*start-server-on-error* (lambda (err)
                                    (setf errorp t)
                                    (endb/lib:log-error err)))
         (active-ws-connections (endb/sql/db:dbms-connections dbms)))
    (setf *start-server-on-query* (alexandria:curry on-query dbms)
          *start-server-on-websocket-init* (lambda (remote-addr)
                                             (setf (gethash remote-addr active-ws-connections) (endb/sql/db:make-db-connection :remote-addr remote-addr)))
          *start-server-on-websocket-close* (lambda (remote-addr)
                                              (remhash remote-addr active-ws-connections))
          *start-server-on-websocket-message* (lambda (remote-addr message on-ws-send)
                                                (funcall on-ws-message dbms (gethash remote-addr active-ws-connections) message on-ws-send)))
    (endb-start-server (cffi:callback start-server-on-query)
                       (cffi:callback start-server-on-error)
                       (cffi:callback start-server-on-websocket-init)
                       (cffi:callback start-server-on-websocket-close)
                       (cffi:callback start-server-on-websocket-message))
    (if errorp
        1
        0)))

(defvar *start-tokio-on-init*)

(cffi:defcallback start-tokio-on-init :void
    ()
  (funcall *start-tokio-on-init*))

(defvar *start-tokio-on-error*)

(cffi:defcallback start-tokio-on-error :void
    ((err :string))
  (funcall *start-tokio-on-error* err))

(cffi:defcfun "endb_start_tokio" :void
  (f :pointer)
  (on-error :pointer))

(defun start-tokio (on-init)
  (endb/lib:init-lib :init-logger-p nil)
  (let* ((result)
         (e nil)
         (*start-tokio-on-error* (lambda (err)
                                   (setf e err)
                                   (endb/lib:log-error err)))
         (*start-tokio-on-init* (lambda ()
                                  (endb/lib:init-logger)
                                  (setf result (funcall on-init)))))
    (endb-start-tokio (cffi:callback start-tokio-on-init)
                      (cffi:callback start-tokio-on-error))
    (when e
      (error e))
    result))
