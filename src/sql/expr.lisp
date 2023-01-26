(defpackage :endb/sql/expr
  (:use :cl)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-%
           #:sql-between #:sql-in #:sql-exists #:sql-coalesce
           #:sql-union-all #:sql-union #:sql-except #:sql-intersect
           #:sql-abs
           #:sql-count-star #:sql-count #:sql-sum #:sql-avg #:sql-min #:sql-max
           #:sql-create-table #:sql-create-index #:sql-insert))
(in-package :endb/sql/expr)

(deftype sql-null ()
  `(eql :null))

(deftype sql-boolean ()
  `(or boolean sql-null))

(deftype sql-number ()
  `(or number sql-null))

(deftype sql-string ()
  `(or string sql-null))

(deftype sql-value ()
  `(or sql-null sql-boolean sql-number sql-string))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-=))
(defun sql-= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (equal x y)))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-<>))
(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(declaim (ftype (function (sql-value sql-value) sql-boolean) sql-is))
(defun sql-is (x y)
  (equal x y))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<))
(defun sql-< (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (< x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql-<=))
(defun sql-<= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (<= x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->))
(defun sql-> (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (> x y)))

(declaim (ftype (function (sql-number sql-number) sql-boolean) sql->=))
(defun sql->= (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (>= x y)))

(declaim (ftype (function (sql-boolean) sql-boolean) sql-not))
(defun sql-not (x)
  (if (eq :null x)
      :null
      (not x)))

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

(declaim (ftype (function (sql-value sql-value &rest sql-value) sql-value) sql-coalesce))
(defun sql-coalesce (x y &rest args)
  (let ((tail (member-if-not (lambda (x)
                               (eq :null x))
                             (cons x (cons y args)))))
    (if tail
        (first tail)
        :null)))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql-+))
(defun sql-+ (x &optional (y 0))
  (if (or (eq :null x) (eq :null y))
      :null
      (+ x y)))

(declaim (ftype (function (sql-number &optional sql-number) sql-number) sql--))
(defun sql-- (x &optional y)
  (if (or (eq :null x) (eq :null y))
      :null
      (if (null y)
          (- x)
          (- x y))))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-*))
(defun sql-* (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (* x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-/))
(defun sql-/ (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (/ x y)))

(declaim (ftype (function (sql-number sql-number) sql-number) sql-%))
(defun sql-% (x y)
  (if (or (eq :null x) (eq :null y))
      :null
      (mod x y)))

(declaim (ftype (function (sql-value sequence) sql-boolean) sql-in))
(defun sql-in (item xs)
  (reduce (lambda (x y)
            (sql-or x (sql-= y item)))
          xs
          :initial-value nil))

(declaim (ftype (function (sql-number sql-number sql-number) sql-boolean) sql-between))
(defun sql-between (expr lhs rhs)
  (sql-and (sql->= expr lhs) (sql-<= expr rhs)))

(declaim (ftype (function (sequence) sql-boolean) sql-exists))
(defun sql-exists (rows)
  (not (null rows)))

(declaim (ftype (function (sequence sequence) sequence) sql-union))
(defun sql-union (lhs rhs)
  (%sql-distinct (union lhs rhs :test 'equal)))

(declaim (ftype (function (sequence sequence) sequence) sql-union-all))
(defun sql-union-all (lhs rhs)
  (append lhs rhs))

(declaim (ftype (function (sequence sequence) sequence) sql-except))
(defun sql-except (lhs rhs)
  (%sql-distinct (set-difference lhs rhs :test 'equal)))

(declaim (ftype (function (sequence sequence) sequence) sql-intersect))
(defun sql-intersect (lhs rhs)
  (%sql-distinct (intersection lhs rhs :test 'equal)))

(declaim (ftype (function (sql-number) sql-number) sql-abs))
(defun sql-abs (x)
  (if (eq :null x)
      :null
      (abs x)))

(declaim (ftype (function (sequence) sql-value) sql-scalar-subquery))
(defun sql-scalar-subquery (rows)
  (assert (<= (length rows) 1))
  (if (null rows)
      :null
      (caar rows)))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-count-star))
(defun sql-count-star (xs &key distinct)
  (declare (ignore distinct))
  (length xs))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-count))
(defun sql-count (xs &key distinct)
  (count-if-not (lambda (x)
                  (eq :null x))
                (if distinct
                    (%sql-distinct xs)
                    xs)))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-avg))
(defun sql-avg (xs &key distinct)
  (let ((xs-no-nulls (remove :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (sql-/ (reduce #'sql-+ xs-no-nulls) (length xs-no-nulls))
        :null)))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-sum))
(defun sql-sum (xs &key distinct)
  (let ((xs-no-nulls (remove :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce #'sql-+ xs-no-nulls)
        :null)))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-min))
(defun sql-min (xs &key distinct)
  (let ((xs-no-nulls (remove :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce
         (lambda (x y)
           (if (sql-< x y)
               x
               y))
         xs-no-nulls)
        :null)))

(declaim (ftype (function (sequence &key (:distinct boolean)) sql-number) sql-max))
(defun sql-max (xs &key distinct)
  (let ((xs-no-nulls (remove :null (if distinct
                                       (%sql-distinct xs)
                                       xs))))
    (if xs-no-nulls
        (reduce
         (lambda (x y)
           (if (sql-> x y)
               x
               y))
         xs-no-nulls)
        :null)))

(declaim (ftype (function (sequence) sequence) %sql-distinct))
(defun %sql-distinct (rows)
  (remove-duplicates rows :test 'equal))

(declaim (ftype (function (sequence cons) sequence) %sql-limit))
(defun %sql-limit (rows limit-offset)
  (destructuring-bind (limit . offset)
      limit-offset
    (subseq rows (or offset 0) (if offset
                                   (+ offset limit)
                                   limit))))

(declaim (ftype (function (sequence list) sequence) %sql-order-by))
(defun %sql-order-by (rows order-by)
  (labels ((asc (x y)
             (cond
               ((eq :null x) t)
               ((eq :null y) nil)
               (t (< x y))))
           (desc (x y)
             (cond
               ((eq :null y) t)
               ((eq :null x) nil)
               (t (> x y)))))
    (sort rows (lambda (x y)
                 (loop for (idx . direction) in order-by
                       for cmp = (ecase direction
                                   (:asc #'asc)
                                   (:desc #'desc))
                       for xv = (nth (1- idx) x)
                       for yv = (nth (1- idx) y)
                       thereis (funcall cmp xv yv)
                       until (funcall cmp yv xv))))))


(declaim (ftype (function (sequence number number) hash-table) %sql-group-by))
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

(defun sql-create-table (db table-name columns)
  (let ((table (make-hash-table)))
    (setf (gethash :columns table) columns)
    (setf (gethash :rows table) ())
    (setf (gethash table-name db) table)
    (values nil t)))

(defun sql-create-index (db)
  (declare (ignore db))
  (values nil t))

(defun sql-insert (db table-name values &key column-names)
  (let* ((table (gethash table-name db))
         (rows (gethash :rows table))
         (values (if column-names
                     (let ((column->idx (make-hash-table :test 'equal)))
                       (loop for column in column-names
                             for idx from 0
                             do (setf (gethash column column->idx) idx))
                       (mapcar (lambda (row)
                                 (mapcar (lambda (column)
                                           (nth (gethash column column->idx) row))
                                         (gethash :columns table)))
                               values))
                     values)))
    (setf (gethash :rows table) (append values rows))
    (values nil (length values))))
