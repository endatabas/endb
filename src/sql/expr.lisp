(defpackage :endb/sql/expr
  (:use :cl)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :endb/sql/parser)
  (:import-from :endb/arrow)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-% #:sql-<<  #:sql->> #:sql-unary+ #:sql-unary-
           #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-union-all #:sql-union #:sql-except #:sql-intersect #:sql-scalar-subquery
           #:sql-cast #:sql-nullif #:sql-abs #:sql-date #:sql-like #:sql-substring #:sql-strftime
           #:make-sql-agg #:sql-agg-accumulate #:sql-agg-finish
           #:sql-create-table #:sql-drop-table #:sql-create-view #:sql-drop-view #:sql-create-index #:sql-drop-index #:sql-insert #:sql-delete
           #:base-table #:base-table-rows #:base-table-deleted-row-ids  #:base-table-columns #:base-table-visible-rows #:base-table-size
           #:non-materialized-view #:non-materialized-view-ast
           #:sql-runtime-error))
(in-package :endb/sql/expr)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (local-time:enable-read-macros))

(define-condition sql-runtime-error (error)
  ((message :initarg :message :reader sql-runtime-error-message))
  (:report (lambda (condition stream)
             (write (sql-runtime-error-message condition) :stream stream))))

(defun %sql-distinct (rows &optional (distinct :distinct))
  (if (eq :distinct distinct)
      (delete-duplicates rows :test 'equal)
      rows))

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

(defmethod sql-cast ((x endb/arrow:arrow-date) (type (eql :varchar)))
  (local-time:format-rfc3339-timestring nil x :omit-time-part t))

(defmethod sql-cast ((x endb/arrow:arrow-time) (type (eql :varchar)))
  (local-time:format-rfc3339-timestring nil x :omit-date-part t))

(defmethod sql-cast ((x local-time:timestamp) (type (eql :varchar)))
  (local-time:format-rfc3339-timestring nil x))

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
  (let ((date (local-time:parse-timestring x)))
    (check-type date local-time:date)
    (make-instance 'endb/arrow:arrow-date :day (local-time:day-of date))))

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

;; Aggregates

(defgeneric make-sql-agg (type &rest args))

(defgeneric sql-agg-accumulate (agg x))
(defgeneric sql-agg-finish (agg))

(defstruct sql-distinct (acc ()) agg)

(defmethod sql-agg-accumulate ((agg sql-distinct) x)
  (with-slots (acc) agg
    (push x acc)
    agg))

(defmethod sql-agg-finish ((agg sql-distinct))
  (with-slots (acc (inner-agg agg)) agg
    (sql-agg-finish (reduce #'sql-agg-accumulate (%sql-distinct acc :distinct)
                            :initial-value inner-agg))))

(defun %make-distinct-sql-agg (agg &optional (distinct :distinct))
  (if (eq :distinct distinct)
      (make-sql-distinct :agg agg)
      agg))

(defstruct sql-sum sum has-value-p)

(defmethod make-sql-agg ((type (eql :sum)) &key distinct)
  (%make-distinct-sql-agg (make-sql-sum) distinct))

(defmethod sql-agg-accumulate ((agg sql-sum) x)
  (with-slots (sum has-value-p) agg
    (if has-value-p
        (setf sum (sql-+ sum x))
        (setf sum x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-sum) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-sum))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        sum
        :null)))

(defstruct (sql-total (:include sql-sum)))

(defmethod make-sql-agg ((type (eql :total)) &key distinct)
  (%make-distinct-sql-agg (make-sql-total) distinct))

(defmethod sql-agg-finish ((agg sql-total))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        sum
        0.0d0)))

(defstruct sql-count (count 0 :type integer))

(defmethod make-sql-agg ((type (eql :count)) &key distinct)
  (%make-distinct-sql-agg (make-sql-count) distinct))

(defmethod sql-agg-accumulate ((agg sql-count) x)
  (incf (sql-count-count agg))
  agg)

(defmethod sql-agg-accumulate ((agg sql-count) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-count))
  (sql-count-count agg))

(defstruct (sql-count-star (:include sql-count)))

(defmethod make-sql-agg ((type (eql :count-star)) &key distinct)
  (when distinct
    (error 'sql-runtime-error :message "COUNT(*) does not support DISTINCT."))
  (make-sql-count-star))

(defmethod sql-agg-accumulate ((agg sql-count-star) x)
  (incf (sql-count-star-count agg))
  agg)

(defstruct sql-avg sum (count 0 :type integer))

(defmethod make-sql-agg ((type (eql :avg)) &key distinct)
  (%make-distinct-sql-agg (make-sql-avg) distinct))

(defmethod sql-agg-accumulate ((agg sql-avg) x)
  (with-slots (sum count) agg
    (setf sum (sql-+ sum x))
    (incf count)
    agg))

(defmethod sql-agg-accumulate ((agg sql-avg) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-avg))
  (with-slots (sum count) agg
    (if (zerop count)
        :null
        (sql-/ sum (coerce count 'double-float)))))

(defstruct sql-min min has-value-p)

(defmethod make-sql-agg ((type (eql :min)) &key distinct)
  (%make-distinct-sql-agg (make-sql-min) distinct))

(defmethod sql-agg-accumulate ((agg sql-min) x)
  (with-slots (min has-value-p) agg
    (if has-value-p
        (setf min (if (sql-< min x)
                      min
                      x))
        (setf min x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-min) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-min))
  (with-slots (min has-value-p) agg
    (if has-value-p
        min
        :null)))

(defstruct sql-max max has-value-p)

(defmethod make-sql-agg ((type (eql :max)) &key distinct)
  (%make-distinct-sql-agg (make-sql-max) distinct))

(defmethod sql-agg-accumulate ((agg sql-max) x)
  (with-slots (max has-value-p) agg
    (if has-value-p
        (setf max (if (sql-> max x)
                      max
                      x))
        (setf max x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-max) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-max))
  (with-slots (max has-value-p) agg
    (if has-value-p
        max
        :null)))

(defstruct sql-group_concat (acc nil :type (or null string)) (separator ",") distinct)

(defmethod make-sql-agg ((type (eql :group_concat)) &rest args)
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
    (%make-distinct-sql-agg (make-sql-group_concat :separator separator :distinct distinct) distinct)))

(defmethod sql-agg-accumulate ((agg sql-group_concat) x)
  (with-slots (acc separator distinct) agg
    (setf acc (cond
                ((and acc (eq :distinct distinct))
                 (concatenate 'string (sql-cast x :varchar) separator acc))
                (acc (concatenate 'string acc separator (sql-cast x :varchar)))
                (t (sql-cast x :varchar))))
    agg))

(defmethod sql-agg-accumulate ((agg sql-group_concat) (x (eql :null)))
  agg)

(defmethod sql-agg-finish ((agg sql-group_concat))
  (or (sql-group_concat-acc agg) :null))

;; Internals

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

;; DML/DDL

(defstruct base-table rows deleted-row-ids)

(defun base-table-columns (base-table)
  (with-slots (rows) base-table
    (mapcar #'car (endb/arrow:arrow-children rows))))

(defun base-table-visible-rows (base-table)
  (with-slots (deleted-row-ids rows) base-table
    (loop for row-id below (endb/arrow:arrow-length rows)
          unless (find row-id deleted-row-ids)
            collect (endb/arrow:arrow-struct-row-get rows row-id))))

(defun base-table-size (base-table)
  (- (endb/arrow:arrow-length (base-table-rows base-table))
     (length (base-table-deleted-row-ids base-table))))

(defun sql-create-table (db table-name columns)
  (unless (gethash table-name db)
    (let ((table (make-base-table :rows (endb/arrow:make-arrow-array-for
                                         (loop for c in columns
                                               collect (cons c :null))))))
      (setf (gethash table-name db) table)
      (values nil t))))

(defun sql-drop-table (db table-name &key if-exists)
  (unless (typep (gethash table-name db) '(or null base-table))
    (error 'sql-runtime-error
           :message (concatenate 'string "Not a table: " table-name)))
  (when (or (remhash table-name db) if-exists)
    (values nil t)))

(defstruct non-materialized-view ast)

(defun sql-create-view (db view-name query)
  (unless (gethash view-name db)
    (setf (gethash view-name db) (make-non-materialized-view :ast query))
    (values nil t)))

(defun sql-drop-view (db view-name &key if-exists)
  (unless (typep (gethash view-name db) '(or null non-materialized-view))
    (error 'sql-runtime-error
           :message (concatenate 'string "Not a view: " view-name)))
  (when (or (remhash view-name db) if-exists)
    (values nil t)))

(defun sql-create-index (db)
  (declare (ignore db))
  (values nil t))

(defun sql-drop-index (db)
  (declare (ignore db)))

(defun sql-insert (db table-name values &key column-names)
  (let ((table (gethash table-name db)))
    (when (typep table 'base-table)
      (with-slots (rows) table
        (let ((values (if column-names
                          (loop with idxs = (loop for column in (base-table-columns table)
                                                  collect (position column column-names :test 'equal))
                                for row in values
                                collect (loop for idx in idxs
                                              collect (nth idx row)))
                          values)))
          (dolist (row values)
            (endb/arrow:arrow-struct-row-push rows row))
          (values nil (length values)))))))

(defun sql-delete (db table-name new-deleted-row-ids)
  (let ((table (gethash table-name db)))
    (when (typep table 'base-table)
      (with-slots (deleted-row-ids rows) table
        (unless deleted-row-ids
          (setf deleted-row-ids (make-instance 'endb/arrow:int64-array)))
        (dolist (row-id new-deleted-row-ids)
          (endb/arrow:arrow-push deleted-row-ids row-id))
        (values nil (length new-deleted-row-ids))))))
