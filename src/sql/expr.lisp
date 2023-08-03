(defpackage :endb/sql/expr
  (:use :cl)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :endb/lib/parser)
  (:import-from :endb/arrow)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :cl-bloom)
  (:import-from :fset)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-% #:sql-<<  #:sql->> #:sql-unary+ #:sql-unary-
           #:sql-access #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-union-all #:sql-union #:sql-except #:sql-intersect #:sql-scalar-subquery
           #:sql-cast #:sql-nullif #:sql-abs #:sql-date #:sql-time #:sql-datetime #:sql-like #:sql-substring #:sql-strftime
           #:make-sql-agg #:sql-agg-accumulate #:sql-agg-finish
           #:sql-create-table #:sql-drop-table #:sql-create-view #:sql-drop-view #:sql-create-index #:sql-drop-index #:sql-insert #:sql-delete
           #:make-db #:copy-db #:db-buffer-pool #:db-wal #:db-object-store #:db-meta-data
           #:base-table #:base-table-rows #:base-table-deleted-row-ids #:table-type #:table-columns
           #:base-table-meta #:base-table-arrow-batches #:base-table-visible-rows #:base-table-size
           #:view-definition #:calculate-stats
           #:sql-runtime-error #:*sqlite-mode*))
(in-package :endb/sql/expr)

(defvar *sqlite-mode* nil)

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

(defmethod sql-= ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (= (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql-= ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (= (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql-= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (= (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod sql-= ((x number) (y number))
  (= x y))

(defmethod sql-= (x y)
  (equal x y))

(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(defmethod sql-is ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (= (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql-is ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (= (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql-is ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (= (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod sql-is ((x number) (y number))
  (= x y))

(defmethod sql-is (x y)
  (equal x y))

(defmethod sql-< (x y)
  :null)

(defmethod sql-< ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-< ((x (eql :null)) y)
  :null)

(defmethod sql-< (x (y (eql :null)))
  :null)

(defmethod sql-< ((x string) (y string))
  (not (null (string< x y))))

(defmethod sql-< ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (< (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql-< ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (< (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql-< ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (< (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod sql-< ((x number) (y number))
  (< x y))

(defmethod sql-< ((x number) (y string))
  t)

(defmethod sql-< ((x string) (y number))
  nil)

(defmethod sql-<= (x y)
  :null)

(defmethod sql-<= ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-<= ((x (eql :null)) y)
  :null)

(defmethod sql-<= (x (y (eql :null)))
  :null)

(defmethod sql-<= ((x string) (y string))
  (not (null (string<= x y))))

(defmethod sql-<= ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (<= (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql-<= ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (<= (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql-<= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (<= (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod sql-<= ((x number) (y number))
  (<= x y))

(defmethod sql-<= ((x number) (y string))
  t)

(defmethod sql-<= ((x string) (y number))
  nil)

(defmethod sql-> (x y)
  :null)

(defmethod sql-> ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql-> ((x (eql :null)) y)
  :null)

(defmethod sql-> (x (y (eql :null)))
  :null)

(defmethod sql-> ((x string) (y string))
  (not (null (string> x y))))

(defmethod sql-> ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (> (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql-> ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (> (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql-> ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (> (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod sql-> ((x number) (y number))
  (> x y))

(defmethod sql-> ((x number) (y string))
  nil)

(defmethod sql-> ((x string) (y number))
  t)

(defmethod sql->= (x y)
  :null)

(defmethod sql->= ((x (eql :null)) (y (eql :null)))
  :null)

(defmethod sql->= ((x (eql :null)) y)
  :null)

(defmethod sql->= (x (y (eql :null)))
  :null)

(defmethod sql->= ((x string) (y string))
  (not (null (string>= x y))))

(defmethod sql->= ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (>= (endb/arrow:arrow-date-days-day x) (endb/arrow:arrow-date-days-day y)))

(defmethod sql->= ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (>= (endb/arrow:arrow-time-micros-us x) (endb/arrow:arrow-time-micros-us y)))

(defmethod sql->= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (>= (endb/arrow:arrow-timestamp-micros-us x) (endb/arrow:arrow-timestamp-micros-us y)))

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

(defmethod sql-concat ((x string) (y string))
  (concatenate 'string x y))

(defmethod sql-concat ((x (eql :null)) y)
  :null)

(defmethod sql-concat (x (y (eql :null)))
  :null)

(defmethod sql-concat (x y)
  (sql-concat (sql-cast x :varchar) (sql-cast y :varchar)))

(defmethod sql-access (x y)
  :null)

(defmethod sql-access ((x vector) (y number))
  (if (and (>= y 0)
           (< y (length x)))
      (aref x y)
      :null))

(defmethod sql-access ((x vector) (y string))
  (loop with acc = (make-array 0 :fill-pointer 0)
        for x across x
        for kv = (when (listp x)
                   (assoc y x :test 'equal))
        when kv
          do (vector-push-extend (cdr kv) acc)
        finally
           (return acc)))

(defmethod sql-access ((x vector) (y (eql :*)))
  (loop with acc = (make-array 0 :fill-pointer 0)
        for x across x
        if (vectorp x)
          do (loop for x across x
                   do (vector-push-extend x acc))
        else
          do (vector-push-extend x acc)
        finally
           (return acc)))

(defmethod sql-access ((x list) (y string))
  (let ((element (assoc y x :test 'equal)))
    (if element
        (cdr element)
        :null)))

(defmethod sql-access ((x list) (y (eql :*)))
  (map 'vector #'cdr x))

(defmethod sql-access ((x (eql :empty-struct)) (y (eql :*)))
  #())

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

(defmethod sql-cast ((x endb/arrow:arrow-date-days) (type (eql :varchar)))
  (format nil "~A" x))

(defmethod sql-cast ((x endb/arrow:arrow-time-micros) (type (eql :varchar)))
  (format nil "~A" x))

(defmethod sql-cast ((x endb/arrow:arrow-timestamp-micros) (type (eql :varchar)))
  (format nil "~A" x))

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

(defmethod sql-cast ((x endb/arrow:arrow-date-days) (type (eql :integer)))
  (local-time:timestamp-year (endb/arrow::%epoch-day-to-timestamp (endb/arrow:arrow-date-days-day x))))

(defmethod sql-cast ((x number) (type (eql :signed)))
  (coerce x 'integer))

(defmethod sql-cast ((x (eql t)) (type (eql :signed)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :signed)))
  0)

(defun %try-parse-number (x)
  (handler-case
      (let ((x (caaadr (endb/lib/parser:parse-sql (concatenate 'string "SELECT " x)))))
        (cond
          ((numberp x) x)
          ((and (listp x)
                (= 2 (length x))
                (eq :- (first x))
                (numberp (second x)))
           (- (second x)))
          (t 0)))
    (endb/lib/parser:sql-parse-error (e)
      (declare (ignore e))
      0)))

(defmethod sql-cast ((x string) (type (eql :signed)))
  (%try-parse-number x))

(defmethod sql-cast ((x number) (type (eql :signed)))
  x)

(defmethod sql-cast ((x (eql t)) (type (eql :decimal)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :decimal)))
  0)

(defmethod sql-cast ((x string) (type (eql :decimal)))
  (%try-parse-number x))

(defmethod sql-cast ((x number) (type (eql :decimal)))
  (coerce x 'number))

(defmethod sql-cast ((x (eql t)) (type (eql :real)))
  1.0d0)

(defmethod sql-cast ((x (eql nil)) (type (eql :real)))
  0.0d0)

(defmethod sql-cast ((x string) (type (eql :real)))
  (coerce (%try-parse-number x) 'double-float))

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
  (endb/arrow:parse-arrow-date-days x))

(defmethod sql-time ((x (eql :null)))
  :null)

(defmethod sql-time ((x string))
  (endb/arrow:parse-arrow-time-micros x))

(defmethod sql-datetime ((x (eql :null)))
  :null)

(defmethod sql-datetime ((x string))
  (endb/arrow:parse-arrow-timestamp-micros x))

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

(defmethod sql-strftime ((format string) (x endb/arrow:arrow-date-days))
  (local-time:format-timestring nil
                                (endb/arrow::%epoch-day-to-timestamp (endb/arrow:arrow-date-days-day x))
                                :format (if (equal "%Y" format)
                                            '((:year 4))
                                            (error 'sql-runtime-error
                                                   :message (format nil "Unknown time format: ~A" format)))
                                :timezone local-time:+utc-zone+))

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
    (error 'sql-runtime-error :message "Scalar subquery must return max one row"))
  (if (null rows)
      :null
      (caar rows)))

;; Aggregates

(defgeneric make-sql-agg (type &rest args))

(defgeneric sql-agg-accumulate (agg x &rest args))
(defgeneric sql-agg-finish (agg))

(defstruct sql-distinct (acc ()) agg)

(defmethod sql-agg-accumulate ((agg sql-distinct) x &rest args)
  (declare (ignore args))
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

(defmethod sql-agg-accumulate ((agg sql-sum) x &rest args)
  (declare (ignore args))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        (setf sum (sql-+ sum x))
        (setf sum x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-sum) (x (eql :null)) &rest args)
  (declare (ignore args))
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

(defmethod sql-agg-accumulate ((agg sql-count) x &rest args)
  (declare (ignore args))
  (incf (sql-count-count agg))
  agg)

(defmethod sql-agg-accumulate ((agg sql-count) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod sql-agg-finish ((agg sql-count))
  (sql-count-count agg))

(defstruct (sql-count-star (:include sql-count)))

(defmethod make-sql-agg ((type (eql :count-star)) &key distinct)
  (when distinct
    (error 'sql-runtime-error :message "COUNT(*) does not support DISTINCT"))
  (make-sql-count-star))

(defmethod sql-agg-accumulate ((agg sql-count-star) x &rest args)
  (declare (ignore args))
  (incf (sql-count-star-count agg))
  agg)

(defstruct sql-avg sum (count 0 :type integer))

(defmethod make-sql-agg ((type (eql :avg)) &key distinct)
  (%make-distinct-sql-agg (make-sql-avg) distinct))

(defmethod sql-agg-accumulate ((agg sql-avg) x &rest args)
  (declare (ignore args))
  (with-slots (sum count) agg
    (setf sum (sql-+ sum x))
    (incf count)
    agg))

(defmethod sql-agg-accumulate ((agg sql-avg) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod sql-agg-finish ((agg sql-avg))
  (with-slots (sum count) agg
    (if (zerop count)
        :null
        (sql-/ sum (coerce count 'double-float)))))

(defstruct sql-min min has-value-p)

(defmethod make-sql-agg ((type (eql :min)) &key distinct)
  (%make-distinct-sql-agg (make-sql-min) distinct))

(defmethod sql-agg-accumulate ((agg sql-min) x &rest args)
  (declare (ignore args))
  (with-slots (min has-value-p) agg
    (if has-value-p
        (setf min (if (sql-< min x)
                      min
                      x))
        (setf min x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-min) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod sql-agg-finish ((agg sql-min))
  (with-slots (min has-value-p) agg
    (if has-value-p
        min
        :null)))

(defstruct sql-max max has-value-p)

(defmethod make-sql-agg ((type (eql :max)) &key distinct)
  (%make-distinct-sql-agg (make-sql-max) distinct))

(defmethod sql-agg-accumulate ((agg sql-max) x &rest args)
  (declare (ignore args))
  (with-slots (max has-value-p) agg
    (if has-value-p
        (setf max (if (sql-> max x)
                      max
                      x))
        (setf max x has-value-p t))
    agg))

(defmethod sql-agg-accumulate ((agg sql-max) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod sql-agg-finish ((agg sql-max))
  (with-slots (max has-value-p) agg
    (if has-value-p
        max
        :null)))

(defstruct sql-group_concat (acc nil :type (or null string)) seen distinct)

(defmethod make-sql-agg ((type (eql :group_concat)) &key distinct)
  (make-sql-group_concat :seen nil :distinct distinct))

(defmethod sql-agg-accumulate ((agg sql-group_concat) x &rest args)
  (with-slots (acc seen distinct) agg
    (when (and (eq :distinct distinct) args)
      (error 'sql-runtime-error :message "GROUP_CONCAT with argument doesn't support DISTINCT"))
    (if (member x seen :test 'equal)
        agg
        (let ((separator (or (first args) ",")))
          (when distinct
            (push x seen))
          (setf acc (if acc
                        (concatenate 'string acc separator (sql-cast x :varchar))
                        (sql-cast x :varchar)))
          agg))))

(defmethod sql-agg-accumulate ((agg sql-group_concat) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod sql-agg-finish ((agg sql-group_concat))
  (or (sql-group_concat-acc agg) :null))

(defstruct sql-array_agg (acc (make-array 0 :fill-pointer 0)) distinct)

(defmethod make-sql-agg ((type (eql :array_agg)) &key distinct)
  (make-sql-array_agg :distinct distinct))

(defmethod sql-agg-accumulate ((agg sql-array_agg) x &rest args)
  (declare (ignore args))
  (with-slots (acc) agg
    (vector-push-extend x acc)
    agg))

(defmethod sql-agg-finish ((agg sql-array_agg))
  (with-slots (acc distinct) agg
    (if (eq :distinct distinct)
        (remove-duplicates acc)
        acc)))

(defstruct sql-object_agg (acc (make-hash-table :test 'equal)))

(defmethod make-sql-agg ((type (eql :object_agg)) &key distinct)
  (declare (ignore distinct))
  (make-sql-object_agg))

(defmethod sql-agg-accumulate ((agg sql-object_agg) (x string) &rest args)
  (unless (eq 1 (length args))
    (error 'sql-runtime-error :message "OBJECT_AGG requires both key and value argument"))
  (with-slots (acc) agg
    (setf (gethash x acc) (first args))
    agg))

(defmethod sql-agg-accumulate ((agg sql-object_agg) x &rest args)
  (declare (ignore args))
  (error 'sql-runtime-error :message (format nil "OBJECT_AGG requires string key argument: ~A" x)))

(defmethod sql-agg-finish ((agg sql-object_agg))
  (with-slots (acc) agg
    (if (zerop (hash-table-count acc))
        :empty-struct
        (alexandria:hash-table-alist acc))))

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

(defvar *default-schema* "main")

(defstruct db wal object-store buffer-pool (meta-data (fset:map ("_last_tx" 0))))

(defun base-table-meta (db table-name)
  (with-slots (meta-data) db
    (or (fset:lookup meta-data table-name) (fset:empty-map))))

(defun base-table-arrow-batches (db table-name arrow-file)
  (with-slots (buffer-pool) db
    (let ((arrow-file-key (format nil "~A/~A" table-name arrow-file)))
      (loop for batch in (endb/storage/buffer-pool:buffer-pool-get buffer-pool arrow-file-key)
            collect (cdar (endb/arrow:arrow-children batch))))))

(defun base-table-visible-rows (db table-name &key arrow-file-idx-row-id-p)
  (let ((table-md (base-table-meta db table-name))
        (acc))
    (fset:do-map (arrow-file arrow-file-md table-md acc)
      (loop with deletes-md = (or (fset:lookup arrow-file-md "deletes") (fset:empty-map))
            for batch in (base-table-arrow-batches db table-name arrow-file)
            for batch-idx from 0
            for batch-deletes = (or (fset:lookup deletes-md (prin1-to-string batch-idx)) (fset:empty-seq))
            do (setf acc (append acc (loop for row-id below (endb/arrow:arrow-length batch)
                                           unless (fset:find row-id batch-deletes)
                                             collect (if arrow-file-idx-row-id-p
                                                         (cons (list arrow-file batch-idx row-id) (mapcar #'cdr (endb/arrow:arrow-get batch row-id)))
                                                         (mapcar #'cdr (endb/arrow:arrow-get batch row-id))))))))))

(defun %information-schema-table-p (table-name)
  (member table-name '("information_schema.columns" "information_schema.tables" "information_schema.views") :test 'equal))

(defun table-type (db table-name)
  (if (%information-schema-table-p table-name)
      "BASE TABLE"
      (let* ((table-row (find-if (lambda (row)
                                   (equal table-name (nth 2 row)))
                                 (base-table-visible-rows db "information_schema.tables"))))
        (nth 3 table-row))))

(defun view-definition (db view-name)
  (let* ((view-row (find-if (lambda (row)
                              (equal view-name (nth 2 row)))
                            (base-table-visible-rows db "information_schema.views")))
         (*read-eval* nil)
         (*read-default-float-format* 'double-float))
    (read-from-string (nth 3 view-row))))

(defun table-columns (db table-name)
  (cond
    ((equal "information_schema.columns" table-name)
     '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position"))
    ((equal "information_schema.tables" table-name)
     '("table_catalog" "table_schema" "table_name" "table_type"))
    ((equal "information_schema.views" table-name)
     '("table_catalog" "table_schema" "table_name" "view_definition"))
    (t (mapcar #'second (%sql-order-by (loop with rows = (base-table-visible-rows db "information_schema.columns")
                                             for (nil nil table c idx) in rows
                                             when (equal table-name table)
                                               collect (list idx c))
                                       (list (list 1 :asc) (list 2 :asc)))))))

(defun base-table-created-p (db table-name)
  (or (%information-schema-table-p table-name)
      (loop with rows = (base-table-visible-rows db "information_schema.columns")
            for (nil nil table nil idx) in rows
            thereis (and (equal table-name table)
                         (plusp idx)))))

(defun base-table-size (db table-name)
  (let ((table-md (base-table-meta db table-name)))
    (fset:reduce (lambda (acc md)
                   (+ acc (- (fset:lookup md "length")
                             (fset:reduce (lambda (acc x)
                                            (+ acc (fset:size x)))
                                          (fset:range (or (fset:lookup md "deletes") (fset:empty-map)))
                                          :initial-value 0))))
                 (fset:range table-md)
                 :initial-value 0)))

(defun %find-arrow-file-idx-row-id (db table-name predicate)
  (loop for (arrow-file-idx-row-id . row) in (base-table-visible-rows db table-name :arrow-file-idx-row-id-p t)
        when (funcall predicate row)
          do (return arrow-file-idx-row-id)))

(defun sql-create-table (db table-name columns)
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "CREATE TABLE not supported"))
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal table-name (nth 2 row))))
    (sql-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
    (sql-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* table-name c idx)))
    (values nil t)))

(defun sql-drop-table (db table-name &key if-exists)
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "DROP TABLE not supported"))
  (with-slots (meta-data) db
    (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                           "information_schema.tables"
                                                           (lambda (row)
                                                             (and (equal table-name (nth 2 row)) (equal "BASE TABLE" (nth 3 row)))))))
      (when batch-file-row-id
        (sql-delete db "information_schema.tables" (list batch-file-row-id))
        (sql-delete db "information_schema.columns" (loop for c in (table-columns db table-name)
                                                          collect (%find-arrow-file-idx-row-id db
                                                                                               "information_schema.columns"
                                                                                               (lambda (row)
                                                                                                 (and (equal table-name (nth 2 row)) (equal c (nth 3 row)))))))

        (setf meta-data (fset:less meta-data table-name)))

      (when (or batch-file-row-id if-exists)
        (values nil t)))))

(defun sql-create-view (db view-name query columns)
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal view-name (nth 2 row))))
    (sql-insert db "information_schema.tables" (list (list :null *default-schema* view-name "VIEW")))
    (sql-insert db "information_schema.views" (list (list :null *default-schema* view-name (prin1-to-string query))))
    (sql-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* view-name c idx)))
    (values nil t)))

(defun sql-drop-view (db view-name &key if-exists)
  (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                         "information_schema.tables"
                                                         (lambda (row)
                                                           (and (equal view-name (nth 2 row)) (equal "VIEW" (nth 3 row)))))))
    (when batch-file-row-id
      (sql-delete db "information_schema.tables" (list batch-file-row-id))
      (sql-delete db "information_schema.columns" (loop for c in (table-columns db view-name)
                                                        collect (%find-arrow-file-idx-row-id db
                                                                                             "information_schema.columns"
                                                                                             (lambda (row)
                                                                                               (and (equal view-name (nth 2 row)) (equal c (nth 3 row)))))))
      (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                             "information_schema.views"
                                                             (lambda (row)
                                                               (equal view-name (nth 2 row))))))
        (sql-delete db "information_schema.views" (list batch-file-row-id))))
    (when (or batch-file-row-id if-exists)
      (values nil t))))

(defun sql-create-index (db)
  (declare (ignore db))
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "CREATE INDEX not supported"))
  (values nil t))

(defun sql-drop-index (db)
  (declare (ignore db))
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "DROP INDEX not supported")))

(defmethod sql-agg-accumulate ((agg cl-bloom::bloom-filter) x &rest args)
  (declare (ignore args))
  (cl-bloom:add agg x)
  agg)

(defmethod sql-agg-finish ((agg cl-bloom::bloom-filter))
  (cffi:with-pointer-to-vector-data (ptr (cl-bloom::filter-array agg))
    (endb/lib/arrow:buffer-to-vector ptr (endb/lib/arrow:vector-byte-size (cl-bloom::filter-array agg)))))

(defun calculate-stats (arrays)
  (let* ((total-length (reduce #'+ (mapcar #'endb/arrow:arrow-length arrays)))
         (bloom-order (* 8 (endb/lib/arrow:vector-byte-size #* (cl-bloom::opt-order total-length)))))
    (labels ((make-col-stats ()
               (fset:map ("count_star" (make-sql-agg :count-star))
                         ("count" (make-sql-agg :count))
                         ("min" (make-sql-agg :min))
                         ("max" (make-sql-agg :max))
                         ("bloom" (make-instance 'cl-bloom::bloom-filter :order bloom-order))))
             (calculate-col-stats (stats kv)
               (destructuring-bind (k . v)
                   kv
                 (let ((col-stats (or (fset:lookup stats k) (make-col-stats))))
                   (fset:with stats k (fset:image
                                       (lambda (agg-k agg-v)
                                         (values agg-k (sql-agg-accumulate agg-v v)))
                                       col-stats))))))
      (let ((stats (reduce
                    (lambda (stats array)
                      (reduce
                       (lambda (stats row)
                         (if (typep row 'endb/arrow:arrow-struct)
                             (reduce #'calculate-col-stats row :initial-value stats)
                             stats))
                       array
                       :initial-value stats))
                    arrays
                    :initial-value (fset:empty-map))))
        (fset:image
         (lambda (k v)
           (values k (fset:image
                      (lambda (k v)
                        (values k (sql-agg-finish v)))
                      v)))
         stats)))))

(defun sql-insert (db table-name values &key column-names)
  (with-slots (buffer-pool meta-data) db
    (let* ((created-p (base-table-created-p db table-name))
           (columns (table-columns db table-name))
           (column-names-set (fset:convert 'fset:set column-names))
           (columns-set (fset:convert 'fset:set columns))
           (new-columns (fset:convert 'list (fset:set-difference column-names-set columns-set)))
           (number-of-columns (length (or column-names columns))))
      (when (and created-p column-names (not (fset:equal? column-names-set columns-set)))
        (error 'sql-runtime-error
               :message (format nil "Cannot insert into table: ~A named columns: ~A doesn't match stored: ~A" table-name column-names columns)))
      (unless (apply #'= number-of-columns (mapcar #'length values))
        (error 'sql-runtime-error
               :message (format nil "Cannot insert into table: ~A without all values containing same number of columns: ~A" table-name number-of-columns)))
      (when new-columns
        (sql-insert db "information_schema.columns" (loop for c in new-columns
                                                          collect (list :null *default-schema* table-name c 0))))
      (if columns
          (let* ((values (if (and created-p column-names)
                             (loop with idxs = (loop for column in columns
                                                     collect (position column column-names :test 'equal))
                                   for row in values
                                   collect (loop for idx in idxs
                                                 collect (nth idx row)))
                             values))
                 (tx-id (1+ (or (fset:lookup meta-data "_last_tx") 0)))
                 (batch-file (format nil "~(~16,'0x~).arrow" tx-id))
                 (batch-key (format nil "~A/~A" table-name batch-file))
                 (table-md (or (fset:lookup meta-data table-name)
                               (fset:empty-map)))
                 (batch-md (fset:lookup table-md batch-file))
                 (batch (if batch-md
                            (car (endb/storage/buffer-pool:buffer-pool-get buffer-pool batch-key))
                            (endb/arrow:make-arrow-array-for (list (cons table-name :null))))))
            (loop for row in values
                  do (endb/arrow:arrow-push batch (list (cons table-name
                                                              (loop for v in row
                                                                    for cn in (if created-p
                                                                                  columns
                                                                                  column-names)
                                                                    collect (cons cn v))))))

            (endb/storage/buffer-pool:buffer-pool-put buffer-pool batch-key (list batch))

            (let* ((inner-batch (cdar (endb/arrow:arrow-children batch)))
                   (batch-md (fset:map-union (or batch-md (fset:empty-map))
                                             (fset:map
                                              ("length" (endb/arrow:arrow-length inner-batch))
                                              ("stats" (calculate-stats (list inner-batch)))))))
              (setf meta-data (fset:with meta-data table-name (fset:with table-md batch-file batch-md))))

            (values nil (length values)))
          (unless (table-type db table-name)
            (sql-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
            (sql-insert db table-name values :column-names column-names))))))

(defun sql-delete (db table-name new-batch-file-idx-deleted-row-ids)
  (with-slots (meta-data) db
    (let* ((table-md (reduce
                      (lambda (acc batch-file-idx-row-id)
                        (destructuring-bind (batch-file batch-idx row-id)
                            batch-file-idx-row-id
                          (let* ((batch-md (fset:lookup acc batch-file))
                                 (deletes-md (or (fset:lookup batch-md "deletes") (fset:empty-map)))
                                 (batch-idx-key (prin1-to-string batch-idx))
                                 (batch-deletes (or (fset:lookup deletes-md batch-idx-key) (fset:empty-seq))))
                            (fset:with acc batch-file (fset:with batch-md "deletes" (fset:with deletes-md batch-idx-key (fset:with-last batch-deletes row-id)))))))
                      new-batch-file-idx-deleted-row-ids
                      :initial-value (fset:lookup meta-data table-name))))
      (setf meta-data (fset:with meta-data table-name table-md))
      (values nil (length new-batch-file-idx-deleted-row-ids)))))
