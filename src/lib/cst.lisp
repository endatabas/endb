(defpackage :endb/lib/cst
  (:use :cl)
  (:export  #:parse-sql-cst #:render-error-report)
  (:import-from :endb/lib)
  (:import-from :endb/lib/parser)
  (:import-from :endb/json)
  (:import-from :cffi))
(in-package :endb/lib/cst)

(cffi:defcfun "endb_parse_sql_cst" :void
  (filename (:pointer :char))
  (input (:pointer :char))
  (on_success :pointer)
  (on_error :pointer))

(cffi:defcfun "endb_render_json_error_report" :void
  (report_json (:pointer :char))
  (on_success :pointer)
  (on_error :pointer))

(defvar *parse-sql-cst-on-success*)

(cffi:defcallback parse-sql-cst-on-success :void
    ((cst :string))
  (funcall *parse-sql-cst-on-success* cst))

(cffi:defcallback parse-sql-cst-on-error :void
    ((err :string))
  (error 'endb/lib/parser:sql-parse-error :message err))

(defun parse-sql-cst (input &key (filename ""))
  (endb/lib:init-lib)
  (if (zerop (length input))
      (error 'sql-parse-error :message "Empty input")
      (let* ((result)
             (*parse-sql-cst-on-success* (lambda (cst)
                                           (let ((*read-eval* nil)
                                                 (*read-default-float-format* 'double-float))
                                             (setf result (read-from-string cst))))))
        (if (and (typep filename 'base-string)
                 (typep input 'base-string))
            (cffi:with-pointer-to-vector-data (filename-ptr input)
              (cffi:with-pointer-to-vector-data (input-ptr input)
                (endb-parse-sql-cst filename-ptr input-ptr (cffi:callback parse-sql-cst-on-success) (cffi:callback parse-sql-cst-on-error))))
            (cffi:with-foreign-string (filename-ptr input)
              (cffi:with-foreign-string (input-ptr input)
                (endb-parse-sql-cst filename-ptr input-ptr (cffi:callback parse-sql-cst-on-success) (cffi:callback parse-sql-cst-on-error)))))
        result)))

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
