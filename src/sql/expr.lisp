(defpackage :endb/sql/expr
  (:use :cl)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :sqlite)
  (:import-from :endb/sql/parser)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-% #:sql-<<  #:sql->> #:sql-unary+ #:sql-unary-
           #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-union-all #:sql-union #:sql-except #:sql-intersect #:sql-scalar-subquery
           #:sql-cast #:sql-nullif #:sql-abs #:sql-date #:sql-like #:sql-substring #:sql-strftime
           #:sql-count-star #:sql-count #:sql-sum #:sql-avg #:sql-min #:sql-max #:sql-total #:sql-group_concat
           #:sql-create-table #:sql-drop-table #:sql-create-view #:sql-drop-view #:sql-create-index #:sql-drop-index #:sql-insert #:sql-delete
           #:base-table-rows #:base-table-columns
           #:sql-runtime-error))
(in-package :endb/sql/expr)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (local-time:enable-read-macros))

(define-condition sql-runtime-error (error)
  ())

(defmethod sql-= ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-= ((x (eql :null)) y)
  :null)

(defmethod sql-= (x (y (eql :null)))
  :null)

(defmethod sql-= ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp= x y))

(defmethod sql-= ((x number) (y number))
  (= x y))

(defmethod sql-= (x y)
  (equal x y))

(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(defmethod sql-is ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp= x y))

(defmethod sql-is ((x number) (y number))
  (= x y))

(defmethod sql-is (x y)
  (equal x y))

(defmethod sql-< ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-< ((x (eql :null)) y)
  :null)

(defmethod sql-< (x (y (eql :null)))
  :null)

(defmethod sql-< ((x string) (y string))
  (not (null (string< x y))))

(defmethod sql-< ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp< x y))

(defmethod sql-< ((x number) (y number))
  (< x y))

(defmethod sql-< ((x number) (y string))
  t)

(defmethod sql-< ((x string) (y number))
  nil)

(defmethod sql-<= ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-<= ((x (eql :null)) y)
  :null)

(defmethod sql-<= (x (y (eql :null)))
  :null)

(defmethod sql-<= ((x string) (y string))
  (not (null (string<= x y))))

(defmethod sql-<= ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp<= x y))

(defmethod sql-<= ((x number) (y number))
  (<= x y))

(defmethod sql-<= ((x number) (y string))
  t)

(defmethod sql-<= ((x string) (y number))
  nil)

(defmethod sql-> ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-> ((x (eql :null)) y)
  :null)

(defmethod sql-> (x (y (eql :null)))
  :null)

(defmethod sql-> ((x string) (y string))
  (not (null (string> x y))))

(defmethod sql-> ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp> x y))

(defmethod sql-> ((x number) (y number))
  (> x y))

(defmethod sql-> ((x number) (y string))
  nil)

(defmethod sql-> ((x string) (y number))
  t)

(defmethod sql->= ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql->= ((x (eql :null)) y)
  :null)

(defmethod sql->= (x (y (eql :null)))
  :null)

(defmethod sql->= ((x string) (y string))
  (not (null (string>= x y))))

(defmethod sql->= ((x local-time:timestamp) (y local-time:timestamp))
  (local-time:timestamp>= x y))

(defmethod sql->= ((x number) (y number))
  (>= x y))

(defmethod sql->= ((x number) (y string))
  nil)

(defmethod sql->= ((x string) (y number))
  t)

(defmethod sql-<< ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-<< ((x (eql :null)) y)
  :null)

(defmethod sql-<< (x (y (eql :null)))
  :null)

(defmethod sql-<< ((x number) (y number))
  (ash x y))

(defmethod sql->> ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql->> ((x (eql :null)) y)
  :null)

(defmethod sql->> (x (y (eql :null)))
  :null)

(defmethod sql->> ((x number) (y number))
  (ash x (- y)))

(defmethod sql-not ((x (eql :null)))
  :null)

(defmethod sql-not (x)
  (not x))

(defmacro sql-and (x y)
  (let ((x-sym (gensym)))
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (and ,y :null)
           (and ,x-sym ,y)))))

(defmacro sql-or (x y)
  (let ((x-sym (gensym)))
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (or ,y :null)
           (or ,x-sym ,y)))))

(defun sql-coalesce (x y &rest args)
  (let ((tail (member-if-not (lambda (x)
                               (eq :null x))
                             (cons x (cons y args)))))
    (if tail
        (first tail)
        :null)))

(defmethod sql-unary+ ((x (eql :null)))
  :null)

(defmethod sql-unary+ (x)
  x)

(defmethod sql-+ ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-+ ((x (eql :null)) (y number))
  :null)

(defmethod sql-+ ((x number) (y (eql :null)))
  :null)

(defmethod sql-+ ((x number) (y number))
  (+ x y))

(defmethod sql-+ (x (y number))
  y)

(defmethod sql-+ ((x number) y)
  x)

(defmethod sql-+ (x y)
  0)

(defmethod sql-unary- ((x (eql :null)))
  :null)

(defmethod sql-unary- (x)
  0)

(defmethod sql-unary- ((x number))
  (- x))

(defmethod sql-- ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-- ((x (eql :null)) (y number))
  :null)

(defmethod sql-- ((x number) (y (eql :null)))
  :null)

(defmethod sql-- ((x number) (y number))
  (- x y))

(defmethod sql-- (x (y number))
  (- y))

(defmethod sql-- ((x number) y)
  x)

(defmethod sql-- (x y)
  0)

(defmethod sql-* ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-* ((x (eql :null)) (y number))
  :null)

(defmethod sql-* ((x number) (y (eql :null)))
  :null)

(defmethod sql-* ((x number) (y number))
  (* x y))

(defmethod sql-* ((x real) (y integer))
  (* x y))

(defmethod sql-* ((x integer) (y real))
  (* x y))

(defmethod sql-* (x (y number))
  (* 0 y))

(defmethod sql-* ((x number) y)
  (* x 0))

(defmethod sql-* (x y)
  0)

(defmethod sql-/ ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-/ ((x (eql :null)) (y number))
  :null)

(defmethod sql-/ ((x number) (y (eql :null)))
  :null)

(defmethod sql-/ ((x integer) (y integer))
  (if (zerop y)
      :null
      (truncate x y)))

(defmethod sql-/ ((x number) (y number))
  (if (zerop y)
      :null
      (/ x y)))

(defmethod sql-/ (x (y number))
  (* 0 y))

(defmethod sql-/ ((x number) y)
  :null)

(defmethod sql-/ (x y)
  :null)

(defmethod sql-% ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-% ((x (eql :null)) (y number))
  :null)

(defmethod sql-% ((x number) (y (eql :null)))
  :null)

(defmethod sql-% ((x number) (y number))
  (if (zerop y)
      :null
      (mod x y)))

(defmethod sql-% (x (y number))
  (* 0 y))

(defmethod sql-% ((x number) y)
  :null)

(defmethod sql-% (x y)
  :null)

(defun sql-in (item xs)
  (block in
    (reduce (lambda (x y)
              (let ((result (sql-= y item)))
                (if (eq t result)
                    (return-from in result)
                    (sql-or x result))))
            xs
            :initial-value nil)))

(defun sql-between (expr lhs rhs)
  (sql-and (sql->= expr lhs) (sql-<= expr rhs)))

(defun sql-exists (rows)
  (not (null rows)))

(defun sql-union (lhs rhs)
  (%sql-distinct (nunion lhs rhs :test 'equal)))

(defun sql-union-all (lhs rhs)
  (nconc lhs rhs))

(defun sql-except (lhs rhs)
  (%sql-distinct (nset-difference lhs rhs :test 'equal)))

(defun sql-intersect (lhs rhs)
  (%sql-distinct (nintersection lhs rhs :test 'equal)))

(defmethod sql-cast ((x (eql :null)) type)
  :null)

(defmethod sql-cast (x (type (eql :varchar)))
  (prin1-to-string x))

(defmethod sql-cast ((x (eql t)) (type (eql :varchar)))
  "1")

(defmethod sql-cast ((x (eql nil)) (type (eql :varchar)))
  "0")

(defmethod sql-cast ((x integer) (type (eql :varchar)))
  (prin1-to-string x))

(defmethod sql-cast ((x string) (type (eql :varchar)))
  x)

(defmethod sql-cast ((x real) (type (eql :varchar)))
  (format nil "~F" x))

(defmethod sql-cast ((x local-time:timestamp) (type (eql :varchar)))
  (if (typep x 'local-time:date)
      (local-time:format-timestring nil x :format local-time:+rfc3339-format/date-only+)
      (local-time:format-timestring nil x)))

(defmethod sql-cast ((x (eql t)) (type (eql :integer)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :integer)))
  0)

(defmethod sql-cast ((x string) (type (eql :integer)))
  (if (ppcre:scan "^-?\\d+$" x)
      (let ((*read-eval* nil))
        (read-from-string x))
      0))

(defmethod sql-cast ((x real) (type (eql :integer)))
  (round x))

(defmethod sql-cast ((x local-time:timestamp) (type (eql :integer)))
  (local-time:timestamp-year x))

(defmethod sql-cast ((x number) (type (eql :signed)))
  (coerce x 'integer))

(defmethod sql-cast ((x (eql t)) (type (eql :signed)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :signed)))
  0)

(defmethod sql-cast ((x string) (type (eql :signed)))
  (multiple-value-bind (token-type value)
      (endb/sql/parser:read-sql-token x)
    (case token-type
      (:- (- (sql-cast (subseq x (1+ (position #\- x))) :signed)))
      ((float integer) value)
      (t 0))))

(defmethod sql-cast ((x number) (type (eql :signed)))
  x)

(defmethod sql-cast ((x (eql t)) (type (eql :decimal)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :decimal)))
  0)

(defmethod sql-cast ((x string) (type (eql :decimal)))
  (multiple-value-bind (token-type value)
      (endb/sql/parser:read-sql-token x)
    (case token-type
      (:- (- (sql-cast (subseq x (1+ (position #\- x))) :decimal)))
      ((float integer) value)
      (t 0))))

(defmethod sql-cast ((x number) (type (eql :decimal)))
  (coerce x 'number))

(defmethod sql-cast ((x (eql t)) (type (eql :real)))
  1.0d0)

(defmethod sql-cast ((x (eql nil)) (type (eql :real)))
  0.0d0)

(defmethod sql-cast ((x string) (type (eql :real)))
  (multiple-value-bind (token-type value)
      (endb/sql/parser:read-sql-token x)
    (case token-type
      (:- (- (sql-cast (subseq x (1+ (position #\- x))) :real)))
      (float value)
      (integer (coerce value 'double-float))
      (t 0.0d0))))

(defmethod sql-cast ((x number) (type (eql :real)))
  (coerce x 'double-float))

(defmethod sql-cast (x (type (eql :date)))
  (sql-date x))

(defun sql-nullif (x y)
  (if (eq t (sql-= x y))
      :null
      x))

(defmethod sql-abs ((x (eql :null)))
  :null)

(defmethod sql-abs ((x number))
  (abs x))

(defmethod sql-date ((x (eql :null)))
  :null)

(defmethod sql-date ((x string))
  (coerce (local-time:parse-timestring x) 'local-time:timestamp))

(defmethod sql-like ((x (eql :null)) (pattern (eql :null)))
  :null)

(defmethod sql-like ((x (eql :null)) y)
  :null)

(defmethod sql-like (x (y (eql :null)))
  :null)

(defmethod sql-like ((x string) (pattern string))
  (let ((regex (concatenate 'string "^" (ppcre:regex-replace-all "%" pattern ".*") "$")))
    (integerp (ppcre:scan regex x))))

(defmethod sql-strftime ((format (eql :null)) (x (eql :null)))
  :null)

(defmethod sql-strftime ((format (eql :null)) x)
  :null)

(defmethod sql-strftime (format (x (eql :null)))
  :null)

(defmethod sql-strftime ((format string) (x local-time:timestamp))
  (local-time:format-timestring nil
                                x
                                :format (if (equal "%Y" format)
                                            '((:year 4))
                                            (error 'sql-runtime-error
                                                   :message (concatenate 'string "Unknown time format: " format)))))

(defmethod sql-substring ((x (eql :null)) (y (eql :null)) &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-substring ((x (eql :null)) y &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-substring (x (y (eql :null)) &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-substring ((x string) (y number) &optional z)
  (if (eq :null z)
      :null
      (let ((y (if (plusp y)
                   (1- y)
                   (+ (length x) y)))
            (z (if z
                   (min (+ y (1- z)) (length x))
                   (length x))))
        (if (and (< y (length x))
                 (<= z (length x)))
            (subseq x y z)
            :null))))

(defun sql-scalar-subquery (rows)
  (when (> 1 (length rows))
    (error 'sql-runtime-error :message "Scalar subquery must return max one row."))
  (if (null rows)
      :null
      (caar rows)))

(defun sql-count-star (xs &key distinct)
  (when distinct
    (error 'sql-runtime-error :message "COUNT(*) does not support DISTINCT."))
  (length xs))

(defun sql-count (xs &key distinct)
  (count-if-not (lambda (x)
                  (eq :null x))
                (if distinct
                    (%sql-distinct xs)
                    xs)))

(defun sql-avg (xs &key distinct)
  (let ((xs-no-nulls (delete :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (sql-/ (reduce #'sql-+ xs-no-nulls)
               (coerce (length xs-no-nulls) 'double-float))
        :null)))

(defun sql-sum (xs &key distinct)
  (let ((xs-no-nulls (delete :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce #'sql-+ xs-no-nulls)
        :null)))

(defun sql-total (xs &key distinct)
  (let ((xs-no-nulls (delete :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce #'sql-+ xs-no-nulls)
        0)))

(defun sql-group_concat (xs &rest args)
  (multiple-value-bind (separator distinct)
      (if (= 2 (length args))
          (destructuring-bind (&key distinct)
              args
            (values "," distinct))
          (destructuring-bind (separator &key distinct)
              args
            (when distinct
              (error 'sql-runtime-error :message "GROUP_CONCAT with argument doesn't support DISTINCT."))
            (values separator distinct)))
    (let ((xs (reverse (delete :null (if distinct
                                         (%sql-distinct xs)
                                         xs)))))
      (apply #'concatenate 'string
             (loop for x in xs
                   for y upto (length xs)
                   collect (if (= y (1- (length xs)))
                               (sql-cast x :varchar)
                               (concatenate 'string (sql-cast x :varchar) separator)))))))

(defun sql-min (xs &key distinct)
  (let ((xs-no-nulls (delete :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce
         (lambda (x y)
           (let ((x (if (numberp x)
                        x
                        0))
                 (y (if (numberp y)
                        y
                        0)))
             (if (sql-< x y)
                 x
                 y)))
         xs-no-nulls)
        :null)))

(defun sql-max (xs &key distinct)
  (let ((xs-no-nulls (delete :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce
         (lambda (x y)
           (let ((x (if (numberp x)
                        x
                        0))
                 (y (if (numberp y)
                        y
                        0)))
             (if (sql-> x y)
                 x
                 y)))
         xs-no-nulls)
        :null)))

(defun %sql-distinct (rows)
  (delete-duplicates rows :test 'equal))

(defun %sql-limit (rows limit offset)
  (subseq rows (or offset 0) (min (length rows)
                                  (if offset
                                      (+ offset limit)
                                      limit))))

(defun %sql-order-by (rows order-by)
  (labels ((asc (x y)
             (cond
               ((eq :null x) t)
               ((eq :null y) nil)
               (t (sql-< x y))))
           (desc (x y)
             (cond
               ((eq :null y) t)
               ((eq :null x) nil)
               (t (sql-> x y)))))
    (sort rows (lambda (x y)
                 (loop for (idx direction) in order-by
                       for cmp = (ecase direction
                                   ((nil :asc) #'asc)
                                   (:desc #'desc))
                       for xv = (nth (1- idx) x)
                       for yv = (nth (1- idx) y)
                       thereis (funcall cmp xv yv)
                       until (funcall cmp yv xv))))))


(defun %sql-group-by (rows group-count group-expr-count)
  (let ((acc (make-hash-table :test 'equal)))
    (if (and (null rows) (zerop group-count))
        (setf (gethash () acc) ())
        (loop for row in rows
              for k = (subseq row 0 group-count)
              do (setf (gethash k acc)
                       (let ((group-acc (or (gethash k acc) (make-list group-expr-count))))
                         (mapcar #'cons (subseq row group-count) group-acc)))))
    acc))

(defstruct base-table
  columns
  rows)

(defun sql-create-table (db table-name columns)
  (unless (gethash table-name db)
    (let ((table (make-base-table :columns columns :rows ())))
      (setf (gethash table-name db) table)
      (values nil t))))

(defun sql-drop-table (db table-name &key if-exists)
  (when (or (remhash table-name db) if-exists)
    (values nil t)))

(defun sql-create-view (db view-name query)
  (unless (gethash view-name db)
    (setf (gethash view-name db) query)
    (values nil t)))

(defun sql-drop-view (db view-name &key if-exists)
  (when (or (remhash view-name db) if-exists)
    (values nil t)))

(defun sql-create-index (db)
  (declare (ignore db))
  (values nil t))

(defun sql-drop-index (db)
  (declare (ignore db)))

(defun sql-insert (db table-name values &key column-names)
  (let ((table (gethash table-name db)))
    (unless (listp table)
      (let* ((rows (base-table-rows table))
             (values (if column-names
                         (let ((column->idx (make-hash-table :test 'equal)))
                           (loop for column in column-names
                                 for idx from 0
                                 do (setf (gethash column column->idx) idx))
                           (mapcar (lambda (row)
                                     (mapcar (lambda (column)
                                               (nth (gethash column column->idx) row))
                                             (base-table-columns table)))
                                   values))
                         values)))
        (setf (base-table-rows table) (append values rows))
        (values nil (length values))))))

(defun sql-delete (db table-name values)
  (let ((table (gethash table-name db)))
    (unless (listp table)
      (let* ((table (gethash table-name db))
             (rows (base-table-rows table)))
        (setf (base-table-rows table) (nset-difference rows values :test 'equal))
        (values nil (length values))))))
