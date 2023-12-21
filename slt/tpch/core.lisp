(defpackage :endb-test/tpch/core
  (:use :cl)
  (:export #:main)
  (:import-from :ppcre)
  (:import-from :uiop))

;; https://github.com/gregrahn/tpch-kit

;; export TPCH_SF=1
;; ./dbgen -vf -s $TPCH_SF
;; DSS_QUERY=queries ./qgen -s $TPCH_SF -v

;; New queries currently needs manual editing.

(defvar *date-scanner* (ppcre:create-scanner "^\\d\\d\\d\\d-\\d\\d-\\d\\d$"))
(defvar *number-scanner* (ppcre:create-scanner "^-?\\d+(.\\d+)?$"))
(defvar *pipe-scanner* (ppcre:create-scanner "\\|"))

(defvar *chunk-size* 1024)

(defun tpch-pipe-delimited-to-slt (file)
  (with-open-file (in file)
    (with-open-file (out (merge-pathnames (ppcre:regex-replace ".tbl$" (file-namestring file) "_tbl.test")
                                          (uiop:pathname-directory-pathname file))
                         :direction :output
                         :if-exists :supersede
                         :if-does-not-exist :create)
      (let* ((table-name (ppcre:regex-replace ".tbl$" (file-namestring file) "")))
        (loop for row-chunk = (loop for idx below *chunk-size*
                                    for line = (read-line in nil)
                                    while line
                                    collect line)
              while row-chunk
              do (format out "~%statement ok~%")
                 (format out "INSERT INTO ~a VALUES " table-name)
                 (loop for row in row-chunk
                       for idx from 0
                       for cols = (ppcre:split *pipe-scanner* row)
                       do (terpri out)
                          (princ "  (" out)
                          (loop for col in cols
                                for idx from 0
                                do (cond
                                     ((ppcre:scan *number-scanner* col) (princ col out))
                                     ((ppcre:scan *date-scanner* col) (format out "date('~a')" col))
                                     (t (format out "'~a'" col)))
                                if (< idx (1- (length cols)))
                                  do (princ "," out)
                                else
                                  do (princ ")" out))
                       if (< idx (1- (length row-chunk)))
                         do (princ "," out)
                       else
                         do (terpri out)))
        (format out "~%~%")))))

(defun main ()
  (let ((tbl-dir (or (uiop:getenv "ENDB_TPCH_TBL_DIR") "test/tpch/1/")))
    (dolist (file (uiop:directory-files tbl-dir "*.tbl"))
      (tpch-pipe-delimited-to-slt file))))
