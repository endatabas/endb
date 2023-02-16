(defpackage :endb-test/tpch/core
  (:import-from :ppcre)
  (:import-from :uiop))

(defvar *date-scanner* (ppcre:create-scanner "^\\d\\d\\d\\d-\\d\\d-\\d\\d$"))
(defvar *number-scanner* (ppcre:create-scanner "^\\d+(.\\d+)?$"))
(defvar *pipe-scanner* (ppcre:create-scanner "\\|"))

(defun tpch-pipe-delimited-to-slt (file)
  (with-open-file (in file)
    (with-open-file (out (merge-pathnames (ppcre:regex-replace ".tbl$" (file-namestring file) "_tbl.test")
                                          (uiop:pathname-directory-pathname file))
                         :direction :output
                         :if-exists :supersede
                         :if-does-not-exist :create)
      (let ((table-name (ppcre:regex-replace ".tbl$" (file-namestring file) ""))
            (rows (loop for line = (read-line in nil)
                        while line
                        collect line)))
        (format out "~%statement ok~%")
        (format out "INSERT INTO ~a VALUES " table-name)
        (loop for row in rows
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
              when (< idx (1- (length rows)))
                do (princ "," out))
        (format out "~%~%")))))

(defun main ()
  (let ((tbl-dir (or (uiop:getenv "ENDB_TPCH_TBL_DIR") "test/tpch/001/")))
    (dolist (file (uiop:directory-files tbl-dir "*.tbl"))
      (tpch-pipe-delimited-to-slt file))))
