(defpackage :endb/lib/cst
  (:use :cl)
  (:export  #:parse-sql-cst #:render-error-report)
  (:import-from :endb/lib)
  (:import-from :endb/lib/parser)
  (:import-from :endb/json)
  (:import-from :cffi)
  (:import-from :trivial-utf-8))
(in-package :endb/lib/cst)

(cffi:defcfun "endb_parse_sql_cst" :void
  (filename (:pointer :char))
  (input (:pointer :char))
  (on_open :pointer)
  (on_close :pointer)
  (on_token :pointer)
  (on_error :pointer))

(cffi:defcfun "endb_render_json_error_report" :void
  (report_json (:pointer :char))
  (on_success :pointer)
  (on_error :pointer))

(cffi:defcallback parse-sql-cst-on-error :void
    ((err :string))
  (error 'endb/lib/parser:sql-parse-error :message err))

(defvar *parse-sql-cst-on-open*)

(cffi:defcallback parse-sql-cst-on-open :void
    ((label-ptr :pointer)
     (label-size :size))
  (funcall *parse-sql-cst-on-open* label-ptr label-size))

(defvar *parse-sql-cst-on-close*)

(cffi:defcallback parse-sql-cst-on-close :void
    ()
  (funcall *parse-sql-cst-on-close*))

(defvar *parse-sql-cst-on-token*)

(cffi:defcallback parse-sql-cst-on-token :void
    ((start :size)
     (end :size))
  (funcall *parse-sql-cst-on-token* start end))

(defparameter +kw-cache+ (make-hash-table))

(defun parse-sql-cst (input &key (filename ""))
  (endb/lib:init-lib)
  (if (zerop (length input))
      (error 'endb/lib/parser:sql-parse-error :message "Empty input")
      (let* ((result (list (list)))
             (input-bytes (trivial-utf-8:string-to-utf-8-bytes input))
             (*parse-sql-cst-on-open* (lambda (label-ptr label-size)
                                        (let* ((address (cffi:pointer-address label-ptr))
                                               (kw (or (gethash address +kw-cache+)
                                                       (let* ((kw-string (make-array label-size :element-type 'character)))
                                                         (dotimes (n label-size)
                                                           (setf (aref kw-string n)
                                                                 (code-char (cffi:mem-ref label-ptr :char n))))
                                                         (setf (gethash address +kw-cache+)
                                                               (intern kw-string :keyword))))))
                                          (push (list kw) result))))
             (*parse-sql-cst-on-close* (lambda ()
                                         (push (nreverse (pop result)) (first result))))
             (*parse-sql-cst-on-token* (lambda (start end)
                                         (let ((token (trivial-utf-8:utf-8-bytes-to-string input-bytes :start start :end end)))
                                           (push (list token start end) (first result))))))
        (if (and (typep filename 'base-string)
                 (typep input 'base-string))
            (cffi:with-pointer-to-vector-data (filename-ptr input)
              (cffi:with-pointer-to-vector-data (input-ptr input)
                (endb-parse-sql-cst filename-ptr
                                    input-ptr
                                    (cffi:callback parse-sql-cst-on-open)
                                    (cffi:callback parse-sql-cst-on-close)
                                    (cffi:callback parse-sql-cst-on-token)
                                    (cffi:callback parse-sql-cst-on-error))))
            (cffi:with-foreign-string (filename-ptr input)
              (cffi:with-foreign-string (input-ptr input)
                (endb-parse-sql-cst filename-ptr
                                    input-ptr
                                    (cffi:callback parse-sql-cst-on-open)
                                    (cffi:callback parse-sql-cst-on-close)
                                    (cffi:callback parse-sql-cst-on-token)
                                    (cffi:callback parse-sql-cst-on-error)))))
        (caar result))))

(defvar *render-json-error-report-on-success*)

(cffi:defcallback render-json-error-report-on-success :void
    ((report :string))
  (funcall *render-json-error-report-on-success* report))

(cffi:defcallback render-json-error-report-on-error :void
    ((err :string))
  (error err))

(defun render-error-report (report)
  (endb/lib:init-lib)
  (let* ((result)
         (*render-json-error-report-on-success* (lambda (report)
                                                  (setf result report)))
         (report-json (endb/json:json-stringify report)))
    (cffi:with-foreign-string (report-json-ptr report-json)
      (endb-render-json-error-report report-json-ptr
                                     (cffi:callback render-json-error-report-on-success)
                                     (cffi:callback render-json-error-report-on-error)))
    (endb/lib/parser:strip-ansi-escape-codes result)))
