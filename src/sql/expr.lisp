(defpackage :endb/sql/expr
  (:use :cl)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :periods)
  (:import-from :endb/arrow)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :endb/queue)
  (:import-from :fset)
  (:export #:sql-= #:sql-== #:sql-<> #:sql-!= #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-% #:sql-<<  #:sql->> #:sql-~ #:sql-& #:sql-\|
           #:sql-between #:sql-coalesce #:sql-ifnull #:sql-uuid #:sql-uuid_blob #:sql_uuid_str #:sql-base64 #:sql-sha1
           #:sql-object_keys #:sql-object_values #:sql-object_entries #:sql-object_from_entries
           #:sql-\|\| #:sql-concat #:sql-cardinality #:sql-char_length #:sql-character_length #:sql-octet_length #:sql-length
           #:sql-trim #:sql-ltrim #:sql-rtrim #:sql-lower #:sql-upper
           #:sql-replace #:sql-unhex #:sql-hex #:sql-instr #:sql-position
           #:sql-min #:sql-max #:sql-char #:sql-unicode #:sql-random #:sql-glob #:sql-regexp #:sql-randomblob #:sql-zeroblob #:sql-iif
           #:sql-round #:sql-sin #:sql-cos #:sql-tan #:sql-sinh #:sql-cosh #:sql-tanh #:sql-asin #:sqn-acos #:sql-atan #:sql-asinh #:sqn-acosh #:sql-atanh #:sql-atan2
           #:sql-floor #:sql-ceiling #:sql-ceil #:sql-patch #:sql-match
           #:sql-sign #:sql-sqrt #:sql-exp #:sql-power #:sql-pow #:sql-log #:sql-log2 #:sql-log10 #:sql-ln #:sql-degrees #:sql-radians #:sql-pi
           #:sql-nullif #:sql-abs #:sql-date #:sql-time #:sql-datetime #:sql-timestamp #:sql-duration #:sql-like #:sql-substr #:sql-substring #:sql-strftime
           #:sql-typeof #:sql-unixepoch #:sql-julianday #:sql-path_remove #:sql-path_insert #:sql-path_replace #:sql-path_set #:sql-path_extract
           #:sql-contains #:sql-overlaps #:sql-precedes #:sql-succedes #:sql-immediately_precedes #:sql-immediately_succedes
           #:sql-unnest #:sql-generate_series

           #:build-like-regex #:build-glob-regex
           #:syn-access #:syn-access-finish #:syn-interval #:syn-cast #:syn-extract

           #:ra-distinct #:ra-union-all #:ra-union #:ra-except #:ra-intersect #:ra-table-function
           #:ra-scalar-subquery #:ra-in  #:ra-in-query #:ra-in-query-index #:ra-exists #:ra-limit #:ra-order-by #:ra-compute-index-if-absent #:ra-visible-row-p
           #:ra-bloom-hashes #:ra-hash-index #:ra-all-quantified-subquery #:ra-any-quantified-subquery

           #:make-agg #:agg-accumulate #:agg-finish

           #:sql-runtime-error #:*sqlite-mode* #:+unix-epoch-time+ #:+end-of-time+
           #:+hash-table-test+ #:+hash-table-test-no-nulls+ #:equalp-case-sensitive #:equalp-case-sensitive-no-nulls #:equalp-case-sensitive-hash-fn
           #:+impure-functions+ #:+table-functions+))
(in-package :endb/sql/expr)

(defvar *sqlite-mode* nil)

(deftype row-type () '(and vector (not endb/arrow:arrow-binary)))

(defun equalp-case-sensitive (x y)
  (cond
    ((eq :null x) (eq :null y))
    ((typep x 'row-type) (and (typep y 'row-type)
                              (not (loop for x across x
                                         for y across y
                                         thereis (not (equalp-case-sensitive x y))))))
    (t (eq t (sql-= x y)))))

(defun equalp-case-sensitive-no-nulls (x y)
  (if (typep x 'row-type)
      (and (typep y 'row-type)
           (not (loop for x across x
                      for y across y
                      thereis (not (equalp-case-sensitive-no-nulls x y)))))
      (eq t (sql-= x y))))

(defun equalp-case-sensitive-hash-fn (x)
  (#+sbcl sb-int:psxhash #-sbcl sxhash
   (typecase x
     (endb/arrow:arrow-date-millis
      (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x))
     (row-type
      (map 'vector (lambda (x)
                     (if (typep x 'endb/arrow:arrow-date-millis)
                         (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x)
                         x))
           x))
     (t x))))

#+sbcl (sb-impl::define-hash-table-test equalp-case-sensitive equalp-case-sensitive-hash-fn)
(defparameter +hash-table-test+ #+sbcl 'endb/sql/expr:equalp-case-sensitive #-sbcl 'equalp)

#+sbcl (sb-impl::define-hash-table-test equalp-case-sensitive-no-nulls equalp-case-sensitive-hash-fn)
(defparameter +hash-table-test-no-nulls+ #+sbcl 'endb/sql/expr:equalp-case-sensitive-no-nulls #-sbcl 'equalp)

(defparameter +unix-epoch-time+ (endb/arrow:parse-arrow-timestamp-micros "1970-01-01"))
(defparameter +end-of-time+ (endb/arrow:parse-arrow-timestamp-micros "9999-01-01"))

(defparameter +impure-functions+ '(sql-random sql-randomblob sql-uuid))
(defparameter +table-functions+ '(sql-unnest sql-generate_series))

(define-condition sql-runtime-error (error)
  ((message :initarg :message :reader sql-runtime-error-message))
  (:report (lambda (condition stream)
             (write (sql-runtime-error-message condition) :stream stream))))

(defmethod sql-= ((x (eql :null)) y)
  :null)

(defmethod sql-= (x (y (eql :null)))
  :null)

(defmethod sql-= ((x number) (y number))
  (= x y))

(defmethod sql-= ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (equalp (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x) y))

(defmethod sql-= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (equalp x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros y)))

(defmethod sql-= (x y)
  (fset:equal? x y))

(defun sql-== (x y)
  (sql-= x y))

(defun sql-<> (x y)
  (sql-not (sql-= x y)))

(defun sql-!= (x y)
  (sql-<> x y))

(defun sql-is (x y)
  (fset:equal? x y))

(defun %fset-deep-less-than (x y)
  (block compare
    (dotimes (n (min (fset:size x) (fset:size y)) (sql-< (fset:size x) (fset:size y)))
      (let ((x (fset:lookup x n))
            (y (fset:lookup y n)))
        (when (null (sql-is x y))
          (return-from compare (sql-< x y)))))))

(defmethod sql-< (x y)
  (case (fset:compare x y)
    (:less t)
    (:unequal :null)))

(defmethod sql-< ((x vector) (y vector))
  (case (fset:compare-lexicographically x y)
    (:less t)
    (:unequal :null)))

(defmethod sql-< ((x fset:seq) (y fset:seq))
  (%fset-deep-less-than x y))

(defmethod sql-< ((x fset:map) (y fset:map))
  (sql-< (sql-object_entries x) (sql-object_entries y)))

(defmethod sql-< ((x (eql :null)) y)
  :null)

(defmethod sql-< (x (y (eql :null)))
  :null)

(defmethod sql-< ((x number) (y number))
  (< x y))

(defmethod sql-< ((x string) (y string))
  (not (null (string< x y))))

(defmethod sql-< ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (sql-< (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x) y))

(defmethod sql-< ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (sql-< x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros y)))

(defmethod sql-<= (x y)
  (case (fset:compare x y)
    ((:less :equal) t)
    (:unequal :null)))

(defmethod sql-<= ((x vector) (y vector))
  (case (fset:compare-lexicographically x y)
    ((:less :equal) t)
    (:unequal :null)))

(defmethod sql-<= ((x fset:seq) (y fset:seq))
  (sql-not (sql-> x y)))

(defmethod sql-<= ((x fset:map) (y fset:map))
  (sql-<= (sql-object_entries x) (sql-object_entries y)))

(defmethod sql-<= ((x (eql :null)) y)
  :null)

(defmethod sql-<= (x (y (eql :null)))
  :null)

(defmethod sql-<= ((x number) (y number))
  (<= x y))

(defmethod sql-<= ((x string) (y string))
  (not (null (string<= x y))))

(defmethod sql-<= ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (sql-<= (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x) y))

(defmethod sql-<= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (sql-<= x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros y)))

(defmethod sql-> (x y)
  (case (fset:compare x y)
    (:greater t)
    (:unequal :null)))

(defmethod sql-> ((x vector) (y vector))
  (case (fset:compare-lexicographically x y)
    (:greater t)
    (:unequal :null)))

(defmethod sql-> ((x fset:seq) (y fset:seq))
  (%fset-deep-less-than y x))

(defmethod sql-> ((x fset:map) (y fset:map))
  (sql-> (sql-object_entries x) (sql-object_entries y)))

(defmethod sql-> ((x (eql :null)) y)
  :null)

(defmethod sql-> (x (y (eql :null)))
  :null)

(defmethod sql-> ((x number) (y number))
  (> x y))

(defmethod sql-> ((x string) (y string))
  (not (null (string> x y))))

(defmethod sql-> ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (sql-> (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x) y))

(defmethod sql-> ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (sql-> x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros y)))

(defmethod sql->= (x y)
  (case (fset:compare x y)
    ((:greater :equal) t)
    (:unequal :null)))

(defmethod sql->= ((x vector) (y vector))
  (case (fset:compare-lexicographically x y)
    ((:greater :equal) t)
    (:unequal :null)))

(defmethod sql->= ((x fset:seq) (y fset:seq))
  (sql-not (sql-< x y)))

(defmethod sql->= ((x fset:map) (y fset:map))
  (sql->= (sql-object_entries x) (sql-object_entries y)))

(defmethod sql->= ((x (eql :null)) y)
  :null)

(defmethod sql->= (x (y (eql :null)))
  :null)

(defmethod sql->= ((x number) (y number))
  (>= x y))

(defmethod sql->= ((x string) (y string))
  (not (null (string>= x y))))

(defmethod sql->= ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (sql->= (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x) y))

(defmethod sql->= ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (sql->= x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros y)))

(defmethod sql-<< ((x (eql :null)) y)
  :null)

(defmethod sql-<< (x (y (eql :null)))
  :null)

(defmethod sql-<< ((x number) (y number))
  (ash x y))

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
  (alexandria:with-gensyms (x-sym)
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (and ,y :null)
           (and ,x-sym ,y)))))

(defmacro sql-or (x y)
  (alexandria:with-gensyms (x-sym)
    `(let ((,x-sym ,x))
       (if (eq :null ,x-sym)
           (or ,y :null)
           (or ,x-sym ,y)))))

(defmacro sql-iif (x y z)
  `(if (eq t ,x)
       ,y
       ,z))

(defun sql-coalesce (x y &rest args)
  (let ((tail (member-if-not (lambda (x)
                               (eq :null x))
                             (cons x (cons y args)))))
    (if tail
        (first tail)
        :null)))

(defun sql-ifnull (x y)
  (sql-coalesce x y))

(defmethod sql-+ ((x (eql :null)) (y number))
  :null)

(defmethod sql-+ ((x number) (y (eql :null)))
  :null)

(defmethod sql-+ ((x (eql :null)) y)
  :null)

(defmethod sql-+ (x (y (eql :null)))
  :null)

(defmethod sql-+ ((x number) (y number))
  (+ x y))

(defmethod sql-+ (x (y number))
  y)

(defmethod sql-+ ((x number) y)
  x)

(defmethod sql-+ (x y)
  0)

(defmethod sql-+ ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:local-time-to-arrow-timestamp-micros
   (periods:add-time (endb/arrow:arrow-timestamp-micros-to-local-time x)
                     (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))

(defmethod sql-+ ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-timestamp-micros))
  (sql-+ y x))

(defmethod sql-+ ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-interval-month-day-nanos))
  (let ((z (periods:add-time (endb/arrow:arrow-date-millis-to-local-time x)
                             (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))
    (if (zerop (endb/arrow:arrow-interval-month-day-nanos-ns y))
        (endb/arrow:local-time-to-arrow-date-millis z)
        (endb/arrow:local-time-to-arrow-timestamp-micros z))))

(defmethod sql-+ ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-date-millis))
  (sql-+ y x))

(defmethod sql-+ ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:local-time-to-arrow-time-micros
   (periods:add-time (endb/arrow:arrow-time-micros-to-local-time x)
                     (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))

(defmethod sql-+ ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-time-micros))
  (sql-+ y x))

(defmethod sql-+ ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:add-duration (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration x)
                         (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))

(defmethod sql-- ((x (eql :null)) (y number))
  :null)

(defmethod sql-- ((x number) (y (eql :null)))
  :null)

(defmethod sql-- ((x (eql :null)) y)
  :null)

(defmethod sql-- (x (y (eql :null)))
  :null)

(defmethod sql-- ((x number) (y number))
  (- x y))

(defmethod sql-- (x (y number))
  (- y))

(defmethod sql-- ((x number) y)
  x)

(defmethod sql-- (x y)
  0)

(defmethod sql-- ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:local-time-to-arrow-timestamp-micros
   (periods:subtract-time (endb/arrow:arrow-timestamp-micros-to-local-time x)
                          (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))

(defmethod sql-- ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:time-difference (endb/arrow:arrow-timestamp-micros-to-local-time x)
                            (endb/arrow:arrow-timestamp-micros-to-local-time y))))

(defmethod sql-- ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-interval-month-day-nanos))
  (let ((z (periods:subtract-time (endb/arrow:arrow-date-millis-to-local-time x)
                                  (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))
    (if (zerop (endb/arrow:arrow-interval-month-day-nanos-ns y))
        (endb/arrow:local-time-to-arrow-date-millis z)
        (endb/arrow:local-time-to-arrow-timestamp-micros z))))

(defmethod sql-- ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-date-millis))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:time-difference (endb/arrow:arrow-date-millis-to-local-time x)
                            (endb/arrow:arrow-date-millis-to-local-time y))))

(defmethod sql-- ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:time-difference (endb/arrow:arrow-timestamp-micros-to-local-time x)
                            (endb/arrow:arrow-date-millis-to-local-time y))))

(defmethod sql-- ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:time-difference (endb/arrow:arrow-date-millis-to-local-time x)
                            (endb/arrow:arrow-timestamp-micros-to-local-time y))))

(defmethod sql-- ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:local-time-to-arrow-time-micros
   (periods:subtract-time (endb/arrow:arrow-time-micros-to-local-time x)
                          (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))))

(defmethod sql-- ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (periods:time-difference (endb/arrow:arrow-time-micros-to-local-time x)
                            (endb/arrow:arrow-time-micros-to-local-time y))))

(defmethod sql-- ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-interval-month-day-nanos))
  (endb/arrow:periods-duration-to-arrow-interval-month-day-nanos
   (if (> (endb/arrow:arrow-interval-month-day-nanos-uint128 x) (endb/arrow:arrow-interval-month-day-nanos-uint128 y))
       (periods:subtract-duration (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration x)
                                  (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y))
       (periods:subtract-duration (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)
                                  (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration x)))))

(defmethod sql-* ((x (eql :null)) (y number))
  :null)

(defmethod sql-* ((x number) (y (eql :null)))
  :null)

(defmethod sql-* ((x (eql :null)) y)
  :null)

(defmethod sql-* (x (y (eql :null)))
  :null)

(defmethod sql-* ((x number) (y number))
  (* x y))

(defmethod sql-* (x (y number))
  (* 0 y))

(defmethod sql-* ((x number) y)
  (* x 0))

(defmethod sql-* (x y)
  0)

(defmethod sql-/ ((x (eql :null)) (y number))
  :null)

(defmethod sql-/ ((x number) (y (eql :null)))
  :null)

(defmethod sql-/ ((x (eql :null)) y)
  :null)

(defmethod sql-/ (x (y (eql :null)))
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

(defmethod sql-% ((x (eql :null)) (y number))
  :null)

(defmethod sql-% ((x number) (y (eql :null)))
  :null)

(defmethod sql-% ((x (eql :null)) y)
  :null)

(defmethod sql-% (x (y (eql :null)))
  :null)

(defmethod sql-% ((x number) (y number))
  (if (zerop y)
      :null
      (nth-value 1 (truncate x y))))

(defmethod sql-% (x (y number))
  (* 0 y))

(defmethod sql-% ((x number) y)
  :null)

(defmethod sql-% (x y)
  :null)

(defmethod sql-& ((x (eql :null)) (y number))
  :null)

(defmethod sql-& ((x number) (y (eql :null)))
  :null)

(defmethod sql-& ((x (eql :null)) y)
  :null)

(defmethod sql-& (x (y (eql :null)))
  :null)

(defmethod sql-& ((x number) (y number))
  (logand (floor x) (floor y)))

(defmethod sql-& (x (y number))
  (floor y))

(defmethod sql-& ((x number) y)
  (floor x))

(defmethod sql-\| ((x (eql :null)) (y number))
  :null)

(defmethod sql-\| ((x number) (y (eql :null)))
  :null)

(defmethod sql-\| ((x (eql :null)) y)
  :null)

(defmethod sql-\| (x (y (eql :null)))
  :null)

(defmethod sql-\| ((x number) (y number))
  (logior (floor x) (floor y)))

(defmethod sql-\| (x (y number))
  (floor y))

(defmethod sql-\| ((x number) y)
  (floor x))

(defmethod sql-~ (x)
  :null)

(defmethod sql-~ ((x number))
  (lognot (floor x)))

(defun sql-array (&rest xs)
  (fset:convert 'fset:seq xs))

(defmethod sql-\|\| ((x string) (y string))
  (concatenate 'string x y))

(defmethod sql-\|\| ((x vector) (y vector))
  (concatenate 'vector x y))

(defmethod sql-\|\| ((x fset:seq) (y fset:seq))
  (fset:concat x y))

(defmethod sql-\|\| (x (y fset:seq))
  (fset:with-first y x))

(defmethod sql-\|\| ((x fset:seq) y)
  (fset:with-last x y))

(defmethod sql-\|\| ((x (eql :null)) (y fset:seq))
  (fset:with-first y x))

(defmethod sql-\|\| ((x fset:seq) (y (eql :null)))
  (fset:with-last x y))

(defmethod sql-\|\| ((x fset:map) (y fset:map))
  (fset:map-union x y))

(defmethod sql-\|\| ((x (eql :null)) y)
  :null)

(defmethod sql-\|\| (x (y (eql :null)))
  :null)

(defmethod sql-\|\| (x y)
  (sql-\|\| (syn-cast x :varchar) (syn-cast y :varchar)))

(defun sql-concat (x y)
  (sql-\|\| x y))

(defmethod sql-character_length ((x (eql :null)))
  :null)

(defmethod sql-character_length ((x string))
  (length x))

(defun sql-char_length (x)
  (sql-character_length x))

(defmethod sql-octet_length ((x (eql :null)))
  :null)

(defmethod sql-octet_length ((x vector))
  (if (typep x 'endb/arrow:arrow-binary)
      (length x)
      :null))

(defmethod sql-octet_length ((x string))
  (trivial-utf-8:utf-8-byte-length x))

(defmethod sql-length ((x (eql :null)))
  :null)

(defmethod sql-length ((x vector))
  (length x))

(defmethod sql-length ((x fset:collection))
  (fset:size x))

(defun sql-cardinality (x)
  (sql-length x))

(defmethod sql-trim ((x (eql :null)) &optional (y " "))
  (declare (ignore y))
  :null)

(defmethod sql-trim ((x string) &optional (y " "))
  (string-trim y x))

(defmethod sql-ltrim ((x (eql :null)) &optional (y " "))
  (declare (ignore y))
  :null)

(defmethod sql-ltrim ((x string) &optional (y " "))
  (if (eq :null y)
      :null
      (string-left-trim y x)))

(defmethod sql-rtrim ((x (eql :null)) &optional (y " "))
  (declare (ignore y))
  :null)

(defmethod sql-rtrim ((x string) &optional (y " "))
  (if (eq :null y)
      :null
      (string-right-trim y x)))

(defmethod sql-lower ((x (eql :null)))
  :null)

(defmethod sql-lower ((x string))
  (string-downcase x))

(defmethod sql-upper ((x (eql :null)))
  :null)

(defmethod sql-upper ((x string))
  (string-upcase x))

(defmethod sql-replace ((x (eql :null)) y z)
  :null)

(defmethod sql-replace (x (y (eql :null)) z)
  :null)

(defmethod sql-replace (x y (z (eql :null)))
  :null)

(defmethod sql-replace ((x string) (y string) (z string))
  (ppcre:regex-replace-all (ppcre:quote-meta-chars y) x z))

(defmethod sql-unhex ((x (eql :null)) &optional y)
  (declare (ignore y))
  :null)

(defmethod sql-unhex (x &optional (y ""))
  (if (eq :null y)
      :null
      (sql-unhex (syn-cast x :varchar) y)))

(defparameter +hex-scanner+ (ppcre:create-scanner "^(?i:[0-9a-f]{2})+$"))

(defmethod sql-unhex ((x string) &optional (y ""))
  (if (eq :null y)
      :null
      (let ((x (remove-if (lambda (x)
                            (find x y))
                          x)))
        (if (ppcre:scan +hex-scanner+ x)
            (loop with acc = (make-array (/ (length x) 2) :element-type '(unsigned-byte 8))
                  with tmp = (make-string 2)
                  for idx below (length x) by 2
                  for out-idx from 0
                  do (setf (schar tmp 0) (aref x idx))
                     (setf (schar tmp 1) (aref x (1+ idx)))
                     (setf (aref acc out-idx) (parse-integer tmp :radix 16))
                  finally (return acc))
            :null))))

(defmethod sql-hex ((x (eql :null)))
  "")

(defmethod sql-hex (x)
  (sql-hex (syn-cast x :varchar)))

(defmethod sql-hex ((x string))
  (sql-hex (trivial-utf-8:string-to-utf-8-bytes x)))

(defmethod sql-hex ((x vector))
  (format nil "~{~16,2,'0r~}" (coerce x 'list)))

(defmethod sql-patch ((x (eql :null)) y)
  :null)

(defmethod sql-patch (x (y (eql :null)))
  :null)

(defmethod sql-patch ((x fset:map) (y fset:map))
  (if (fset:empty? y)
      x
      (endb/json:json-merge-patch x y)))

(defmethod sql-match (x y)
  (sql-is x y))

(defmethod sql-match ((x fset:seq) (y fset:seq))
  (fset:do-seq (a x :value t)
    (unless (fset:do-seq (b y :value nil)
              (when (if (fset:map? b)
                        (sql-match a b)
                        (sql-is a b))
                (return t)))
      (return nil))))

(defmethod sql-match (x (y fset:seq))
  (numberp (fset:position x y)))

(defmethod sql-match ((x fset:map) (y fset:map))
  (fset:do-map (k v x t)
    (unless (sql-match v (fset:lookup y k))
      (return nil))))

(defun sql-between (expr lhs rhs)
  (sql-and (sql->= expr lhs) (sql-<= expr rhs)))

(defun sql-nullif (x y)
  (if (eq t (sql-= x y))
      :null
      x))

(defun %handle-complex (fn x)
  (if (complexp x)
      (error 'endb/sql/expr:sql-runtime-error
             :message (format nil "Complex number as result: (~f, ~f) to: ~A" (realpart x) (imagpart x) fn))
      x))

(defmethod sql-abs ((x (eql :null)))
  :null)

(defmethod sql-abs ((x number))
  (abs x))

(defmethod sql-round ((x (eql :null)))
  :null)

(defmethod sql-round ((x number))
  (coerce (round x) 'double-float))

(defmethod sql-sin ((x (eql :null)))
  :null)

(defmethod sql-sin ((x number))
  (sin (coerce x 'double-float)))

(defmethod sql-cos ((x (eql :null)))
  :null)

(defmethod sql-cos ((x number))
  (cos (coerce x 'double-float)))

(defmethod sql-tan ((x (eql :null)))
  :null)

(defmethod sql-tan ((x number))
  (tan (coerce x 'double-float)))

(defmethod sql-sinh ((x (eql :null)))
  :null)

(defmethod sql-sinh ((x number))
  (sinh (coerce x 'double-float)))

(defmethod sql-cosh ((x (eql :null)))
  :null)

(defmethod sql-cosh ((x number))
  (cosh (coerce x 'double-float)))

(defmethod sql-tanh ((x (eql :null)))
  :null)

(defmethod sql-tanh ((x number))
  (tanh (coerce x 'double-float)))

(defmethod sql-asin ((x (eql :null)))
  :null)

(defmethod sql-asin ((x number))
  (%handle-complex "ASIN" (asin (coerce x 'double-float))))

(defmethod sql-asinh ((x (eql :null)))
  :null)

(defmethod sql-asinh ((x number))
  (asinh (coerce x 'double-float)))

(defmethod sql-acos ((x (eql :null)))
  :null)

(defmethod sql-acos ((x number))
  (coerce (acos (coerce x 'double-float)) 'double-float))

(defmethod sql-acosh ((x (eql :null)))
  :null)

(defmethod sql-acosh ((x number))
  (%handle-complex "ACOSH" (acosh (coerce x 'double-float))))

(defmethod sql-atan ((x (eql :null)))
  :null)

(defmethod sql-atan ((x number))
  (atan (coerce x 'double-float)))

(defmethod sql-atan2 ((x (eql :null)) y)
  :null)

(defmethod sql-atan2 (x (y (eql :null)))
  :null)

(defmethod sql-atan2 ((x number) (y number))
  (atan (coerce x 'double-float) y))

(defmethod sql-atanh ((x (eql :null)))
  :null)

(defmethod sql-atanh ((x number))
  (%handle-complex "ATANH" (atanh (coerce x 'double-float))))

(defmethod sql-floor ((x (eql :null)))
  :null)

(defmethod sql-floor ((x number))
  (floor x))

(defmethod sql-floor ((x double-float))
  (coerce (floor x) 'double-float))

(defmethod sql-ceiling ((x (eql :null)))
  :null)

(defmethod sql-ceiling ((x number))
  (ceiling x))

(defmethod sql-ceiling ((x double-float))
  (coerce (ceiling x) 'double-float))

(defun sql-ceil (x)
  (sql-ceiling x))

(defmethod sql-sign ((x (eql :null)))
  :null)

(defmethod sql-sign ((x number))
  (round (signum x)))

(defmethod sql-sqrt ((x (eql :null)))
  :null)

(defmethod sql-sqrt ((x number))
  (%handle-complex "SQRT" (sqrt (coerce x 'double-float))))

(defmethod sql-degrees ((x (eql :null)))
  :null)

(defmethod sql-degrees ((x number))
  (* x (/ 180.0d0 pi)))

(defmethod sql-radians ((x (eql :null)))
  :null)

(defmethod sql-radians ((x number))
  (* x (/ pi 180.0d0)))

(defun sql-pi ()
  pi)

(defmethod sql-exp ((x (eql :null)))
  :null)

(defmethod sql-exp ((x number))
  (exp x))

(defmethod sql-power ((x (eql :null)) y)
  :null)

(defmethod sql-power (x (y (eql :null)))
  :null)

(defmethod sql-power ((x number) (y number))
  (expt (coerce x 'double-float) y))

(defun sql-pow (x y)
  (sql-power x y))

(defun sql-mod (x y)
  (sql-% x y))

(defmethod sql-log ((x (eql :null)) &optional y)
  (declare (ignore y))
  :null)

(defmethod sql-log ((x number) &optional (y nil yp))
  (if (zerop x)
      :null
      (%handle-complex "LOG"
                       (cond
                         ((eq :null y) :null)
                         (yp (log y (coerce x 'double-float)))
                         (t (log (coerce x 'double-float) 10))))))

(defmethod sql-log10 ((x (eql :null)))
  :null)

(defmethod sql-log10 ((x number))
  (if (zerop x)
      :null
      (%handle-complex "LOG10" (log (coerce x 'double-float) 10))))

(defmethod sql-log2 ((x (eql :null)))
  :null)

(defmethod sql-log2 ((x number))
  (if (zerop x)
      :null
      (%handle-complex "LOG2" (log (coerce x 'double-float) 2))))

(defmethod sql-ln ((x (eql :null)))
  :null)

(defmethod sql-ln ((x number))
  (if (zerop x)
      :null
      (%handle-complex "LN" (log (coerce x 'double-float)))))

(defmethod sql-date ((x (eql :null)))
  :null)

(defmethod sql-date ((x string))
  (endb/arrow:parse-arrow-date-millis x))

(defmethod sql-date ((x endb/arrow:arrow-date-millis))
  x)

(defmethod sql-time ((x (eql :null)))
  :null)

(defmethod sql-time ((x string))
  (endb/arrow:parse-arrow-time-micros x))

(defmethod sql-time ((x endb/arrow:arrow-time-micros))
  x)

(defmethod sql-datetime ((x (eql :null)))
  :null)

(defmethod sql-datetime ((x string))
  (endb/arrow:parse-arrow-timestamp-micros x))

(defmethod sql-datetime ((x endb/arrow:arrow-timestamp-micros))
  x)

(defun sql-timestamp (x)
  (sql-datetime x))

(defmethod sql-unixepoch ((x (eql :null)))
  :null)

(defmethod sql-unixepoch ((x string))
  (sql-unixepoch (sql-datetime x)))

(defmethod sql-unixepoch ((x endb/arrow:arrow-date-millis))
  (/ (endb/arrow:arrow-date-millis-ms x) 1000.0d0))

(defmethod sql-unixepoch ((x endb/arrow:arrow-timestamp-micros))
  (/ (endb/arrow:arrow-timestamp-micros-us x) 1000000.0d0))

(defmethod sql-julianday ((x (eql :null)))
  :null)

(defmethod sql-julianday ((x string))
  (sql-julianday (sql-datetime x)))

(defmethod sql-julianday ((x endb/arrow:arrow-date-millis))
  (sql-julianday (syn-cast x :timestamp)))

(defmethod sql-julianday ((x endb/arrow:arrow-timestamp-micros))
  (let ((noon-offset 0.5d0)
        (unix-day-offset 11017.0d0))
    (- (/ (endb/arrow:arrow-timestamp-micros-us x)
          1000000.0d0
          local-time:+seconds-per-day+)
       local-time::+astronomical-julian-date-offset+
       noon-offset
       unix-day-offset)))

(defmethod sql-duration ((x (eql :null)))
  :null)

(defmethod sql-duration ((x string))
  (endb/arrow:parse-arrow-interval-month-day-nanos x))

(defmethod sql-like ((x (eql :null)) y &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-like (x (y (eql :null)) &optional z)
  (declare (ignore z))
  :null)

(defparameter +like-default-percent-scanner+ (ppcre:create-scanner "\\\\%"))

(defun build-like-regex (x &optional (z nil zp))
  (when (and zp (or (not (stringp z))
                    (not (= 1 (length z)))))
    (error 'sql-runtime-error :message (format nil "Invalid escape character: ~A" z)))
  (let* ((regex (ppcre:quote-meta-chars x))
         (regex (ppcre:regex-replace-all (if zp
                                             (format nil "(?<![~A])\\\\%" z)
                                             +like-default-percent-scanner+)
                                         regex
                                         ".*"))
         (regex (if zp
                    (ppcre:regex-replace-all (format nil "(?<![~A])_" z) regex ".")
                    (substitute #\. #\_ regex)))
         (regex (if zp
                    (ppcre:regex-replace-all (format nil "~A(_|\\\\%|~A)" z z) regex "\\1")
                    regex)))
    (concatenate 'string "^" regex "$")))

(defmethod sql-like ((x string) (y string) &optional (z nil zp))
  (if (and zp (eq :null z))
      :null
      (integerp (ppcre:scan (if zp
                                (build-like-regex x z)
                                (build-like-regex x))
                            y))))

(defmethod sql-glob ((x (eql :null)) y)
  :null)

(defmethod sql-glob (x (y (eql :null)))
  :null)

(defparameter +glob-star-scanner+ (ppcre:create-scanner "\\\\\\*"))
(defparameter +glob-question-mark-scanner+ (ppcre:create-scanner "\\\\\\?"))

(defun build-glob-regex (x)
  (let* ((regex (ppcre:quote-meta-chars x))
         (regex (ppcre:regex-replace-all +glob-star-scanner+ regex ".*?"))
         (regex (ppcre:regex-replace-all +glob-question-mark-scanner+ regex ".")))
    (concatenate 'string "^" regex "$")))

(defmethod sql-glob ((x string) (y string))
  (integerp (ppcre:scan (build-glob-regex x) y)))

(defmethod sql-regexp ((x (eql :null)) y)
  :null)

(defmethod sql-regexp (x (y (eql :null)))
  :null)

(defmethod sql-regexp ((x string) (y string))
  (integerp (ppcre:scan x y)))

(defmethod sql-strftime ((format (eql :null)) x)
  :null)

(defmethod sql-strftime (format (x (eql :null)))
  :null)

(defmethod sql-strftime ((format string) (x endb/arrow:arrow-date-millis))
  (let ((local-time:*default-timezone* local-time:+utc-zone+))
    (periods:strftime (endb/arrow:arrow-date-millis-to-local-time x) :format format)))

(defmethod sql-strftime ((format string) (x endb/arrow:arrow-time-micros))
  (let ((local-time:*default-timezone* local-time:+utc-zone+))
    (periods:strftime (endb/arrow:arrow-time-micros-to-local-time x) :format format)))

(defmethod sql-strftime ((format string) (x endb/arrow:arrow-timestamp-micros))
  (let ((local-time:*default-timezone* local-time:+utc-zone+))
    (periods:strftime (endb/arrow:arrow-timestamp-micros-to-local-time x) :format format)))

(defun sql-substr (x y &optional (z nil zp))
  (if zp
      (sql-substring x y z)
      (sql-substring x y)))

(defmethod sql-substring ((x (eql :null)) y &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-substring (x (y (eql :null)) &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-substring ((x string) (y number) &optional (z nil zp))
  (cond
    ((eq :null z)
     :null)
    ((and zp (not (integerp z)))
     (error 'endb/sql/expr:sql-runtime-error :message (format nil "Invalid end index: ~A" z)))
    (t (let ((y (max 0 (if (plusp y)
                           (1- y)
                           (+ (length x) y))))
             (z (max 0 (if zp
                           (min (+ y (1- z)) (length x))
                           (length x)))))
         (if (and (< y (length x))
                  (<= z (length x)))
             (subseq x y z)
             "")))))

(defmethod sql-instr ((x (eql :null)) y)
  :null)

(defmethod sql-instr (x (y (eql :null)))
  :null)

(defmethod sql-instr ((x string) (y string))
  (let ((idx (search y x)))
    (if idx
        (1+ idx)
        0)))

(defmethod sql-instr ((x vector) (y vector))
  (let ((idx (search y x)))
    (if idx
        (1+ idx)
        0)))

(defun sql-position (x y)
  (sql-instr y x))

(defun sql-char (&rest args)
  (if (every #'integerp args)
      (coerce (loop for x in args
                    collect (code-char x))
              'string)
      ""))

(defmethod sql-unicode ((x (eql :null)))
  :null)

(defmethod sql-unicode ((x string))
  (if (plusp (length x))
      (char-code (char x 0))
      :null))

(defun sql-random ()
  (let ((x (random (ash 1 64))))
    (- x (ash 1 63))))

(defmethod sql-randomblob ((x (eql :null)))
  :null)

(defmethod sql-randomblob ((x number))
  (make-array x :element-type '(unsigned-byte 8)
                :initial-contents (loop for n below x
                                        collect (random 255))))

(defmethod sql-zeroblob ((x (eql :null)))
  :null)

(defmethod sql-zeroblob ((x number))
  (make-array x :element-type '(unsigned-byte 8)))

(defmethod sql-object_keys ((x (eql :null)))
  :null)

(defmethod sql-object_keys ((x fset:map))
  (fset:convert 'fset:seq (fset:domain x)))

(defmethod sql-object_values ((x (eql :null)))
  :null)

(defmethod sql-object_values ((x fset:map))
  (fset:reduce
   (lambda (acc k v)
     (declare (ignore k))
     (fset:with-last acc v))
   x
   :initial-value (fset:empty-seq)))

(defmethod sql-object_entries ((x (eql :null)))
  :null)

(defmethod sql-object_entries ((x fset:map))
  (fset:reduce
   (lambda (acc k v)
     (fset:with-last acc (fset:seq k v)))
   x
   :initial-value (fset:empty-seq)))

(defmethod sql-object_from_entries ((x (eql :null)))
  :null)

(defmethod sql-object_from_entries ((x fset:seq))
  (fset:convert 'fset:map
                (loop for x in (fset:convert 'list x)
                      if (and (fset:seq? x) (= 2 (fset:size x)))
                        collect (cons (syn-cast (fset:lookup x 0) :varchar)
                                      (fset:lookup x 1))
                      else
                        do (error 'endb/sql/expr:sql-runtime-error :message "Object entries needs to be two element arrays"))))

(defun %path-edit (x paths &key overwritep createp extractp)
  (reduce
   (lambda (outer-acc path-and-arg)
     (block outer
       (destructuring-bind (path &optional (arg nil argp))
           path-and-arg
         (labels ((walk (acc path)
                    (if (fset:collection? acc)
                        (let* ((x (first path))
                               (path (rest path))
                               (lastp (equal "#" x))
                               (z (cond
                                    (lastp
                                     (fset:size acc))
                                    ((and (numberp x) (minusp x))
                                     (+ (fset:size acc) x))
                                    (t x))))
                          (if path
                              (cond
                                ((and (numberp z) (fset:set? acc) (= 0 z) createp (not lastp))
                                 (fset:seq (walk acc path)))
                                ((and (numberp z) (fset:seq? acc) (<= 0 z (1- (fset:size acc))))
                                 (fset:with acc z (walk (fset:lookup acc z) path)))
                                ((and (numberp z) (fset:seq? acc) (= z (fset:size acc)) createp)
                                 (fset:with-last acc (walk (fset:empty-set) path)))
                                ((and (stringp z) (fset:set? acc) createp)
                                 (fset:map (z (walk acc path))))
                                ((and (stringp z) (fset:map? acc))
                                 (multiple-value-bind (child childp)
                                     (fset:lookup acc z)
                                   (cond
                                     (childp (fset:with acc z (walk child path)))
                                     (createp (fset:with acc z (walk (fset:empty-set) path)))
                                     (t acc))))
                                (extractp
                                 (return-from outer :null))
                                ((fset:set? acc)
                                 (return-from outer outer-acc))
                                (t acc))
                              (cond
                                (extractp
                                 (multiple-value-bind (child childp)
                                     (fset:lookup acc z)
                                   (return-from outer (if childp
                                                          child
                                                          :null))))
                                ((and (numberp z) (fset:set? acc) (= 0 z) createp (not lastp))
                                 (fset:seq arg))
                                ((and (numberp z) (fset:seq? acc) (<= 0 z (1- (fset:size acc))))
                                 (cond
                                   ((not argp)  (fset:less acc z))
                                   (overwritep (fset:with acc z arg))
                                   (t acc)))
                                ((and (numberp z) (fset:seq? acc) (= z (fset:size acc)))
                                 (if createp
                                     (fset:with-last acc arg)
                                     acc))
                                ((and (stringp z) (fset:set? acc) createp)
                                 (fset:map (z arg)))
                                ((and (stringp z) (fset:map? acc))
                                 (multiple-value-bind (child childp)
                                     (fset:lookup acc z)
                                   (declare (ignore child))
                                   (cond
                                     ((not argp) (fset:less acc z))
                                     ((and overwritep childp)
                                      (fset:with acc z arg))
                                     ((and createp (not childp))
                                      (fset:with acc z arg))
                                     (t acc))))
                                ((fset:set? acc)
                                 (return-from outer outer-acc))
                                (t acc))))
                        acc)))
           (cond
             ((and (fset:seq? path)
                   (not (fset:empty? path)))
              (walk outer-acc (fset:convert 'list path)))
             (extractp outer-acc)
             (t :null))))))
   paths
   :initial-value x))

(defmethod sql-path_extract ((x (eql :null)) y &rest paths)
  (declare (ignore y paths))
  :null)

(defmethod sql-path_extract (x (y (eql :null)) &rest paths)
  (declare (ignore x paths))
  :null)

(defmethod sql-path_extract (x (y fset:seq) &rest paths)
  (if paths
      (fset:convert 'fset:seq (loop for path in (cons y paths)
                                    collect (%path-edit x (list (list path)) :extractp t)))
      (%path-edit x (list (list y)) :extractp t)))

(defun sql-path_remove (x &rest paths)
  (%path-edit x (loop for path in paths
                      collect (list path))))

(defun %path-pairs (paths)
  (loop for idx below (length paths) by 2
        collect (list (nth idx paths) (nth (1+ idx) paths))))

(defun sql-path_insert (x &rest paths)
  (unless (evenp (length paths))
    (error 'endb/sql/expr:sql-runtime-error :message "Path insert needs even path/argument pairs"))
  (%path-edit x (%path-pairs paths) :createp t))

(defun sql-path_replace (x &rest paths)
  (unless (evenp (length paths))
    (error 'endb/sql/expr:sql-runtime-error :message "Path replace needs even path/argument pairs"))
  (%path-edit x (%path-pairs paths) :overwritep t))

(defun sql-path_set (x &rest paths)
  (unless (evenp (length paths))
    (error 'endb/sql/expr:sql-runtime-error :message "Path set needs even path/argument pairs"))
  (%path-edit x (%path-pairs paths) :createp t :overwritep t))

(defun sql-uuid ()
  (endb/lib:uuid-v4))

(defmethod sql-uuid_blob ((x (eql :null)))
  :null)

(defmethod sql-uuid_blob ((x string))
  (let ((b (sql-unhex x "-{}")))
    (if (vectorp b)
        (sql-uuid_blob b)
        :null)))

(defmethod sql-uuid_blob ((x vector))
  (if (= 16 (length x))
      x
      :null))

(defmethod sql-uuid_str ((x (eql :null)))
  :null)

(defmethod sql-uuid_str ((x string))
  (sql-uuid_str (sql-uuid_blob x)))

(defmethod sql-uuid_str ((x vector))
  (or (endb/lib:uuid-str x) :null))

(defmethod sql-base64 ((x (eql :null)))
  :null)

(defmethod sql-base64 ((x string))
  (or (endb/lib:base64-decode x) :null))

(defmethod sql-base64 ((x vector))
  (endb/lib:base64-encode x))

(defmethod sql-sha1 ((x (eql :null)))
  :null)

(defmethod sql-sha1 ((x vector))
  (endb/lib:sha1 x))

(defmethod sql-sha1 ((x string))
  (endb/lib:sha1 (trivial-utf-8:string-to-utf-8-bytes x)))

(defmethod sql-sha1 (x)
  (sql-sha1 (syn-cast x :varchar)))

(defmethod sql-typeof ((x string))
  "text")

(defmethod sql-typeof ((x integer))
  "integer")

(defmethod sql-typeof ((x double-float))
  "real")

(defmethod sql-typeof ((x vector))
  "blob")

(defmethod sql-typeof ((x endb/arrow:arrow-date-millis))
  "date")

(defmethod sql-typeof ((x endb/arrow:arrow-time-micros))
  "time")

(defmethod sql-typeof ((x endb/arrow:arrow-timestamp-micros))
  "timestamp")

(defmethod sql-typeof ((x endb/arrow:arrow-interval-month-day-nanos))
  "interval")

(defmethod sql-typeof ((x (eql t)))
  "boolean")

(defmethod sql-typeof ((x (eql nil)))
  "boolean")

(defmethod sql-typeof ((x (eql :null)))
  "null")

(defmethod sql-typeof ((x fset:seq))
  "array")

(defmethod sql-typeof ((x fset:map))
  "object")

(defun sql-min (x y &rest args)
  (reduce (lambda (x y)
            (if (or (eq :null x) (eq :null y))
                :null
                (if (sql-< x y)
                    x
                    y)))
          (cons x (cons y args))))

(defun sql-max (x y &rest args)
  (reduce (lambda (x y)
            (if (or (eq :null x) (eq :null y))
                :null
                (if (sql-< x y)
                    y
                    x)))
          (cons x (cons y args))))

(defun sql-unnest (array &rest arrays)
  (let ((arrays (loop for a in (cons array arrays)
                      collect (if (fset:map? a)
                                  (sql-object_entries a)
                                  a))))
    (when (every #'fset:seq? arrays)
      (let ((len (apply #'max (mapcar #'fset:size arrays))))
        (loop for idx below len
              collect (coerce (loop for a in arrays
                                    collect (if (< idx (fset:size a))
                                                (fset:lookup a idx)
                                                :null))
                              'vector))))))

(defun sql-generate_series (start end &optional (step 1))
  (loop for idx from start to end by step
        collect (vector idx)))

;; Period predicates

(defmethod %period-field ((x fset:map) field)
  (syn-access-finish x field nil))

(defmethod %period-field ((x fset:seq) (field (eql "start")))
  (if (= 2 (fset:size x))
      (fset:lookup x 0)
      :null))

(defmethod %period-field ((x fset:seq) (field (eql "end")))
  (if (= 2 (fset:size x))
      (fset:lookup x 1)
      :null))

(defmethod %period-field ((x endb/arrow:arrow-date-millis) field)
  x)

(defmethod %period-field ((x endb/arrow:arrow-timestamp-micros) field)
  x)

(defmethod %period-field (x field)
  :null)

(defun sql-contains (x y)
  (sql-and (sql-<= (%period-field x "start")
                   (%period-field y "start"))
           (sql->= (%period-field x "end")
                   (%period-field y "end"))))

(defun sql-overlaps (x y)
  (sql-and (sql-< (%period-field x "start")
                  (%period-field y "end"))
           (sql-> (%period-field x "end")
                  (%period-field y "start"))))

(defun sql-precedes (x y)
  (sql-<= (%period-field x "end")
          (%period-field y "start")))

(defun sql-succeeds (x y)
  (sql->= (%period-field x "start")
          (%period-field y "end")))

(defun sql-immediately_precedes (x y)
  (sql-= (%period-field x "end")
         (%period-field y "start")))

(defun sql-immediately_succeeds (x y)
  (sql-= (%period-field x "start")
         (%period-field y "end")))

;; Syntax

(defmethod syn-extract (x (y (eql :null)))
  :null)

(defmethod syn-extract ((x (eql :year)) (y endb/arrow:arrow-date-millis))
  (local-time:timestamp-year (endb/arrow:arrow-date-millis-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :month)) (y endb/arrow:arrow-date-millis))
  (local-time:timestamp-month (endb/arrow:arrow-date-millis-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :day)) (y endb/arrow:arrow-date-millis))
  (local-time:timestamp-day (endb/arrow:arrow-date-millis-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :year)) (y endb/arrow:arrow-timestamp-micros))
  (local-time:timestamp-year (endb/arrow:arrow-timestamp-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :month)) (y endb/arrow:arrow-timestamp-micros))
  (local-time:timestamp-month (endb/arrow:arrow-timestamp-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :day)) (y endb/arrow:arrow-timestamp-micros))
  (local-time:timestamp-day (endb/arrow:arrow-timestamp-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :hour)) (y endb/arrow:arrow-timestamp-micros))
  (local-time:timestamp-hour (endb/arrow:arrow-timestamp-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :minute)) (y endb/arrow:arrow-timestamp-micros))
  (local-time:timestamp-minute (endb/arrow:arrow-timestamp-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :second)) (y endb/arrow:arrow-timestamp-micros))
  (let ((z (endb/arrow:arrow-timestamp-micros-to-local-time y)))
    (+ (local-time:timestamp-second z :timezone local-time:+utc-zone+)
       (* 0.000001d0 (local-time:timestamp-microsecond z)))))

(defmethod syn-extract ((x (eql :hour)) (y endb/arrow:arrow-time-micros))
  (local-time:timestamp-hour (endb/arrow:arrow-time-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :minute)) (y endb/arrow:arrow-time-micros))
  (local-time:timestamp-minute (endb/arrow:arrow-time-micros-to-local-time y) :timezone local-time:+utc-zone+))

(defmethod syn-extract ((x (eql :second)) (y endb/arrow:arrow-time-micros))
  (let ((z (endb/arrow:arrow-time-micros-to-local-time y)))
    (+ (local-time:timestamp-second z :timezone local-time:+utc-zone+)
       (* 0.000001d0 (local-time:timestamp-microsecond z)))))

(defmethod syn-extract ((x (eql :year)) (y endb/arrow:arrow-interval-month-day-nanos))
  (periods::duration-years (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)))

(defmethod syn-extract ((x (eql :month)) (y endb/arrow:arrow-interval-month-day-nanos))
  (periods::duration-months (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)))

(defmethod syn-extract ((x (eql :day)) (y endb/arrow:arrow-interval-month-day-nanos))
  (periods::duration-days (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)))

(defmethod syn-extract ((x (eql :hour)) (y endb/arrow:arrow-interval-month-day-nanos))
  (periods::duration-hours (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)))

(defmethod syn-extract ((x (eql :minute)) (y endb/arrow:arrow-interval-month-day-nanos))
  (periods::duration-minutes (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)))

(defmethod syn-extract ((x (eql :second)) (y endb/arrow:arrow-interval-month-day-nanos))
  (coerce (endb/arrow:periods-duration-seconds (endb/arrow:arrow-interval-month-day-nanos-to-periods-duration y)) 'double-float))

(defmethod syn-cast ((x (eql :null)) type)
  :null)

(defmethod syn-cast (x (type list))
  (let ((x (syn-cast x (first type))))
    (if (and (eq :char (first type))
             (numberp (second type)))
        (format nil "~vA" (second type) x)
        x)))

(defmethod syn-cast (x (type (eql :varchar)))
  (princ-to-string x))

(defmethod syn-cast ((x integer) (type (eql :varchar)))
  (prin1-to-string x))

(defmethod syn-cast ((x fset:seq) (type (eql :varchar)))
  (with-output-to-string (out)
    (format out "[")
    (fset:do-seq (v x :index idx)
      (format out (if (stringp v)
                      "\"~A\""
                      "~A")
              (syn-cast v :varchar))
      (when (< idx (1- (fset:size x)))
        (format out ",")))
    (format out "]")))

(defmethod syn-cast ((x fset:map) (type (eql :varchar)))
  (with-output-to-string (out)
    (format out "{")
    (let ((idx 0))
      (fset:do-map (k v x)
        (format out "\"~A\":" k)
        (format out (if (stringp v)
                        "\"~A\""
                        "~A")
                (syn-cast v :varchar))
        (when (< idx (1- (fset:size x)))
          (format out ","))
        (incf idx)))
    (format out "}")))

(defmethod syn-cast ((x (eql t)) (type (eql :varchar)))
  (if *sqlite-mode*
      "1"
      "true"))

(defmethod syn-cast ((x (eql nil)) (type (eql :varchar)))
  (if *sqlite-mode*
      "0"
      "false"))

(defmethod syn-cast ((x string) (type (eql :varchar)))
  x)

(defmethod syn-cast ((x real) (type (eql :varchar)))
  (format nil "~F" x))

(defmethod syn-cast ((x vector) (type (eql :varchar)))
  (trivial-utf-8:utf-8-bytes-to-string x))

(defmethod syn-cast (x (type (eql :char)))
  (syn-cast x :varchar))

(defmethod syn-cast (x (type (eql :text)))
  (syn-cast x :varchar))

(defmethod syn-cast (x (type (eql :blob)))
  (syn-cast (syn-cast x :varchar) :blob))

(defmethod syn-cast ((x string) (type (eql :blob)))
  (trivial-utf-8:string-to-utf-8-bytes x))

(defmethod syn-cast ((x vector) (type (eql :blob)))
  x)

(defmethod syn-cast (x (type (eql :varbinary)))
  (syn-cast x :blob))

(defmethod syn-cast ((x (eql t)) (type (eql :integer)))
  1)

(defmethod syn-cast ((x (eql nil)) (type (eql :integer)))
  0)

(defmethod syn-cast (x (type (eql :boolean)))
  (syn-cast (syn-cast x :varchar) :boolean))

(defmethod syn-cast ((x number) (type (eql :boolean)))
  (not (zerop x)))

(defmethod syn-cast ((x (eql t)) (type (eql :boolean)))
  x)

(defmethod syn-cast ((x (eql nil)) (type (eql :boolean)))
  x)

(defmethod syn-cast ((x string) (type (eql :boolean)))
  (cond
    ((member x '("t" "true") :test 'equalp)
     t)
    ((member x '("f" "false") :test 'equalp)
     nil)
    (t :null)))

(defparameter +integer-scanner+ (ppcre:create-scanner "^\\s*[-+]?\\d+"))

(defmethod syn-cast ((x string) (type (eql :integer)))
  (multiple-value-bind (start end)
      (ppcre:scan +integer-scanner+ x)
    (if start
        (let ((*read-eval* nil))
          (read-from-string (subseq x 0 end)))
        0)))

(defmethod syn-cast ((x real) (type (eql :integer)))
  (round x))

(defmethod syn-cast ((x integer) (type (eql :integer)))
  x)

(defmethod syn-cast (x (type (eql :smallint)))
  (syn-cast x :integer))

(defmethod syn-cast (x (type (eql :bigint)))
  (syn-cast x :integer))

(defmethod syn-cast (x (type (eql :integer)))
  (syn-cast (syn-cast x :varchar) :integer))

(defmethod syn-cast (x (type (eql :decimal)))
  (syn-cast x :real))

(defmethod syn-cast (x (type (eql :numeric)))
  (syn-cast x :real))

(defmethod syn-cast ((x (eql t)) (type (eql :decimal)))
  1)

(defmethod syn-cast ((x (eql nil)) (type (eql :decimal)))
  0)

(defparameter +decimal-scanner+ (ppcre:create-scanner "^\\s*[+-]?(\\d+(\\.\\d*)?([eE][-+]?\\d+)?|\\.\\d+([eE][-+]?\\d+)?)"))

(defmethod syn-cast ((x string) (type (eql :decimal)))
  (multiple-value-bind (start end)
      (ppcre:scan +decimal-scanner+ x)
    (if start
        (let ((*read-eval* nil)
              (*read-default-float-format* 'double-float))
          (read-from-string (subseq x 0 end)))
        0)))

(defmethod syn-cast ((x number) (type (eql :decimal)))
  x)

(defmethod syn-cast (x (type (eql :signed)))
  (syn-cast x :decimal))

(defmethod syn-cast ((x (eql t)) (type (eql :real)))
  1.0d0)

(defmethod syn-cast ((x (eql nil)) (type (eql :real)))
  0.0d0)

(defmethod syn-cast ((x string) (type (eql :real)))
  (coerce (syn-cast x :decimal) 'double-float))

(defmethod syn-cast ((x number) (type (eql :real)))
  (coerce x 'double-float))

(defmethod syn-cast (x (type (eql :real)))
  (syn-cast (syn-cast x :varchar) :real))

(defmethod syn-cast (x (type (eql :double)))
  (syn-cast x :real))

(defmethod syn-cast ((x endb/arrow:arrow-date-millis) (type (eql :timestamp)))
  (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x))

(defmethod syn-cast ((x endb/arrow:arrow-timestamp-micros) (type (eql :date)))
  (endb/arrow:arrow-timestamp-micros-to-arrow-date-millis x))

(defmethod syn-cast ((x endb/arrow:arrow-timestamp-micros) (type (eql :time)))
  (endb/arrow:local-time-to-arrow-time-micros (endb/arrow:arrow-timestamp-micros-to-local-time x)))

(defmethod syn-cast (x (type (eql :timestamp)))
  (sql-timestamp x))

(defmethod syn-cast (x (type (eql :date)))
  (sql-date x))

(defmethod syn-cast (x (type (eql :time)))
  (sql-time x))

(defmethod syn-cast (x (type (eql :interval)))
  (sql-duration x))

(defstruct path-seq acc)

(defun %flatten-path-acc (x)
  (if (typep x 'path-seq)
      (path-seq-acc x)
      (vector x)))

(defun %recursive-path-access (x y)
  (make-path-seq :acc (concatenate 'vector
                                   (%flatten-path-acc (syn-access x y nil))
                                   (path-seq-acc (syn-access (syn-access x :* nil) y t)))))

(defun syn-access-finish (x y recursivep)
  (let ((x (syn-access x y recursivep)))
    (if (typep x 'path-seq)
        (if (path-seq-acc x)
            (fset:convert 'fset:seq (path-seq-acc x))
            :null)
        x)))

(defmethod syn-access (x y recursivep)
  (make-path-seq))

(defmethod syn-access (x y (recursivep (eql t)))
  (make-path-seq :acc #()))

(defmethod syn-access (x (y (eql 0)) recursivep)
  x)

(defmethod syn-access (x (y (eql :*)) recursivep)
  (make-path-seq :acc #()))

(defmethod syn-access ((x vector) (y number) (recursivep (eql nil)))
  (let ((y (if (minusp y)
               (+ (length x) y)
               y)))
    (if (and (>= y 0)
             (< y (length x)))
        (aref x y)
        (make-path-seq))))

(defmethod syn-access ((x vector) (y string) recursivep)
  (syn-access (make-path-seq :acc x) y recursivep))

(defmethod syn-access ((x vector) y (recursivep (eql t)))
  (%recursive-path-access x y))

(defmethod syn-access ((x vector) (y (eql :*)) (recursivep (eql nil)))
  (make-path-seq :acc x))

(defmethod syn-access ((x fset:seq) (y number) (recursivep (eql nil)))
  (let ((y (if (minusp y)
               (+ (fset:size x) y)
               y)))
    (if (and (>= y 0)
             (< y (fset:size x)))
        (fset:lookup x y)
        (make-path-seq))))

(defmethod syn-access ((x fset:seq) (y string) recursivep)
  (syn-access (make-path-seq :acc (fset:convert 'vector x)) y recursivep))

(defmethod syn-access ((x fset:seq) y (recursivep (eql t)))
  (%recursive-path-access x y))

(defmethod syn-access ((x fset:seq) (y (eql :*)) (recursivep (eql nil)))
  (make-path-seq :acc (fset:convert 'vector x)))

(defmethod syn-access ((x fset:map) (y string) (recursivep (eql nil)))
  (multiple-value-bind (v vp)
      (fset:lookup x y)
    (if vp
        v
        (make-path-seq))))

(defun %fset-values (m)
  (fset:reduce (lambda (acc k v)
                 (declare (ignore k))
                 (vector-push-extend v acc)
                 acc)
               m
               :initial-value (make-array 0 :fill-pointer 0)))

(defmethod syn-access ((x fset:map) (y (eql :*)) (recursivep (eql nil)))
  (make-path-seq :acc (%fset-values x)))

(defmethod syn-access ((x fset:map) y (recursivep (eql t)))
  (%recursive-path-access x y))

(defmethod syn-access ((x path-seq) y recursivep)
  (make-path-seq :acc (apply #'concatenate
                             'vector
                             (loop for x across (path-seq-acc x)
                                   for z = (syn-access x y recursivep)
                                   collect (%flatten-path-acc z)))))

(defparameter +interval-time-parts+ '(:hour :minute :second))
(defparameter +interval-parts+ (append '(:year :month :day) +interval-time-parts+))

(defparameter +interval-scanner+ (ppcre:create-scanner "[ :-]"))

(defun syn-interval (x from &optional (to from))
  (let* ((parts (coerce (ppcre:split +interval-scanner+ x) 'list))
         (units (subseq +interval-parts+
                        (position from +interval-parts+)
                        (1+ (position to +interval-parts+)))))
    (unless (= (length parts) (length units))
      (error 'sql-runtime-error
             :message (format nil "Invalid interval: ~A from: ~A to: ~A" x from to)))
    (let ((strs (loop with time-part-seen-p = nil
                      for unit in units
                      for part in parts
                      for s = (format nil "~d~A" part (char-upcase (char (symbol-name unit) 0)))
                      if (and (null time-part-seen-p)
                              (member unit +interval-time-parts+))
                        collect (progn
                                  (setf time-part-seen-p t)
                                  (concatenate 'string "T" s))
                      else
                        collect s)))
      (sql-duration (apply #'concatenate 'string "P" strs)))))

;; RA

(defun ra-distinct (rows &optional (distinct :distinct))
  (if (eq :distinct distinct)
      (let ((seen (make-hash-table :test +hash-table-test+)))
        (loop for row in rows
              unless (gethash row seen)
                collect (progn
                          (setf (gethash row seen) t)
                          row)))
      rows))

(defun ra-in (item xs)
  (block in
    (reduce (lambda (x y)
              (let ((result (sql-= y item)))
                (if (eq t result)
                    (return-from in result)
                    (sql-or x result))))
            xs
            :initial-value nil)))

(defun ra-in-query (index item)
  (or (gethash item index)
      (gethash :null index)
      (when (and (eq :null item)
                 (plusp (hash-table-count index)))
        :null)))

(defun ra-in-query-index (rows)
  (loop with index-table = (make-hash-table :test +hash-table-test+)
        for row in rows
        for in = (aref row 0)
        do (setf (gethash in index-table)
                 (if (eq :null in)
                     :null
                     t))
        finally (return index-table)))

(defun ra-visible-row-p (deleted-rows erased-row-ids row-id)
  (not (or (when deleted-rows
             (fset:find row-id
                        deleted-rows
                        :key (lambda (x)
                               (fset:lookup x "row_id"))))
           (when erased-row-ids
             (fset:find row-id erased-row-ids)))))

(defun ra-exists (rows)
  (not (null rows)))

(defun ra-union (lhs rhs)
  (ra-distinct (ra-union-all lhs rhs)))

(defun ra-union-all (lhs rhs)
  (append lhs rhs))

(defun ra-except (lhs rhs)
  (let ((seen (make-hash-table :test +hash-table-test+)))
    (dolist (row rhs)
      (setf (gethash row seen) t))
    (loop for row in lhs
          unless (gethash row seen)
            collect (progn
                      (setf (gethash row seen) t)
                      row))))

(defun ra-intersect (lhs rhs)
  (let ((seen (make-hash-table :test +hash-table-test+)))
    (dolist (row lhs)
      (setf (gethash row seen) t))
    (loop for row in rhs
          when (gethash row seen)
            collect (progn
                      (remhash row seen)
                      row))))

(defun ra-scalar-subquery (rows)
  (when (> 1 (length rows))
    (error 'sql-runtime-error :message "Scalar subquery must return max one row"))
  (if (null rows)
      :null
      (aref (first rows) 0)))

(defun ra-compute-index-if-absent (index k f)
  (multiple-value-bind (result resultp)
      (gethash k index)
    (if resultp
        result
        (setf (gethash k index)
              (funcall f)))))

(defun ra-table-function (rows &key table-function with-ordinality number-of-columns)
  (when (and rows (not (= number-of-columns (length (first rows)))))
    (format nil "Table function: ~A arity: ~A doesn't match given: ~A" table-function number-of-columns (length (first rows)))
    (error 'sql-runtime-error :message ""))
  (reverse (if (eq :with-ordinality with-ordinality)
               (loop for idx from 0
                     for row in rows
                     collect (concatenate 'vector row (vector idx)))
               rows)))

(defun ra-all-quantified-subquery (op item rows)
  (block all
    (reduce (lambda (x y)
              (sql-and x (funcall op item (aref y 0))))
            rows
            :initial-value t)))

(defun ra-any-quantified-subquery (op item rows)
  (block any
    (reduce (lambda (x y)
              (let ((result (funcall op item (aref y 0))))
                (if (eq t result)
                    (return-from any result)
                    (sql-or x result))))
            rows
            :initial-value nil)))

(defun ra-limit (rows limit offset)
  (subseq rows (or offset 0) (min (length rows)
                                  (if offset
                                      (+ offset limit)
                                      limit))))

(defun ra-order-by (rows order-by)
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
                       for xv = (aref x (1- idx))
                       for yv = (aref y (1- idx))
                       thereis (funcall cmp xv yv)
                       until (funcall cmp yv xv))))))

(defun ra-bloom-hashes (x)
  (cond
    ((numberp x)
     (let* ((float64-array (make-instance 'endb/arrow:float64-array))
            (int64-array (make-instance 'endb/arrow:int64-array))
            (decimal-array (make-instance 'endb/arrow:decimal-array))
            (arrays (etypecase x
                      (double-float
                       (append
                        (list (endb/arrow:arrow-push float64-array x))
                        (when (= x (ceiling x))
                          (list (endb/arrow:arrow-push int64-array (ceiling x))
                                (endb/arrow:arrow-push decimal-array (ceiling x))))))
                      (integer
                       (append
                        (when (= x (coerce x 'double-float))
                          (list (endb/arrow:arrow-push float64-array (coerce x 'double-float))))
                        (list (endb/arrow:arrow-push int64-array x)
                              (endb/arrow:arrow-push decimal-array x)))))))
       (remove-duplicates
        (loop for array in arrays
              collect (endb/lib:xxh64 (endb/arrow:arrow-row-format array 0))))))
    ((typep x 'endb/arrow:arrow-date-millis)
     (let ((timestamp (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros x)))
       (list (endb/lib:xxh64 (endb/arrow:to-arrow-row-format x))
             (endb/lib:xxh64 (endb/arrow:to-arrow-row-format timestamp)))))
    ((typep x 'endb/arrow:arrow-timestamp-micros)
     (let ((date (endb/arrow:arrow-timestamp-micros-to-arrow-date-millis x)))
       (if (equalp x (endb/arrow:arrow-date-millis-to-arrow-timestamp-micros date))
           (list (endb/lib:xxh64 (endb/arrow:to-arrow-row-format date))
                 (endb/lib:xxh64 (endb/arrow:to-arrow-row-format x)))
           (list (endb/lib:xxh64 (endb/arrow:to-arrow-row-format x))))))
    (t (list (endb/lib:xxh64 (endb/arrow:to-arrow-row-format x))))))

(defparameter +hash-index-limit+ 128)
(defparameter +hash-index-min-size+ 32)
(deftype hash-index-type () '(or string (signed-byte 64) boolean (eql :null) endb/arrow:arrow-date-millis))

(defun ra-hash-index (hash-index indexer-queue k batch column-kw x)
  (when (and indexer-queue
             (typep x 'hash-index-type)
             (<= +hash-index-min-size+ (endb/arrow:arrow-length batch)))
    (multiple-value-bind (index foundp)
        (gethash k hash-index)
      (if foundp
          (gethash x index)
          (progn
            (endb/queue:queue-push
             indexer-queue
             (lambda ()
               (when (<= +hash-index-limit+ (hash-table-count hash-index))
                 (clrhash hash-index))
               (ra-compute-index-if-absent
                hash-index
                k
                (lambda ()
                  (let ((index (make-hash-table :test +hash-table-test-no-nulls+)))
                    (dotimes (idx (endb/arrow:arrow-length batch))
                      (let* ((v (endb/arrow:arrow-struct-column-value batch idx column-kw))
                             (idxs (or (gethash v index)
                                       (setf (gethash v index) (make-array 0 :fill-pointer 0 :element-type 'fixnum)))))
                        (vector-push-extend idx idxs)))
                    index)))))
            nil)))))

;; Aggregates

(defgeneric make-agg (type &rest args))

(defgeneric agg-accumulate (agg x &rest args))
(defgeneric agg-finish (agg))

(defstruct agg-distinct (acc ()) agg)

(defmethod agg-accumulate ((agg agg-distinct) x &rest args)
  (declare (ignore args))
  (with-slots (acc) agg
    (push x acc)
    agg))

(defmethod agg-finish ((agg agg-distinct))
  (with-slots (acc (inner-agg agg)) agg
    (agg-finish (reduce #'agg-accumulate (ra-distinct acc :distinct)
                        :initial-value inner-agg))))

(defun %make-distinct-agg (agg &optional (distinct :distinct))
  (if (eq :distinct distinct)
      (make-agg-distinct :agg agg)
      agg))

(defstruct agg-sum sum has-value-p)

(defmethod make-agg ((type (eql :sum)) &key distinct)
  (%make-distinct-agg (make-agg-sum) distinct))

(defmethod agg-accumulate ((agg agg-sum) x &rest args)
  (declare (ignore args))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        (setf sum (sql-+ sum x))
        (setf sum x has-value-p t))
    agg))

(defmethod agg-accumulate ((agg agg-sum) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-sum))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        sum
        :null)))

(defstruct (agg-total (:include agg-sum)))

(defmethod make-agg ((type (eql :total)) &key distinct)
  (%make-distinct-agg (make-agg-total) distinct))

(defmethod agg-finish ((agg agg-total))
  (with-slots (sum has-value-p) agg
    (if has-value-p
        sum
        0.0d0)))

(defstruct agg-count (count 0 :type integer))

(defmethod make-agg ((type (eql :count)) &key distinct)
  (%make-distinct-agg (make-agg-count) distinct))

(defmethod agg-accumulate ((agg agg-count) x &rest args)
  (declare (ignore args))
  (incf (agg-count-count agg))
  agg)

(defmethod agg-accumulate ((agg agg-count) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-count))
  (agg-count-count agg))

(defstruct (agg-count-star (:include agg-count)))

(defmethod make-agg ((type (eql :count-star)) &key distinct)
  (when distinct
    (error 'sql-runtime-error :message "COUNT(*) does not support DISTINCT"))
  (make-agg-count-star))

(defmethod agg-accumulate ((agg agg-count-star) x &rest args)
  (declare (ignore args))
  (incf (agg-count-star-count agg))
  agg)

(defstruct agg-avg sum (count 0 :type integer))

(defmethod make-agg ((type (eql :avg)) &key distinct)
  (%make-distinct-agg (make-agg-avg) distinct))

(defmethod agg-accumulate ((agg agg-avg) x &rest args)
  (declare (ignore args))
  (with-slots (sum count) agg
    (setf sum (sql-+ sum x))
    (incf count)
    agg))

(defmethod agg-accumulate ((agg agg-avg) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-avg))
  (with-slots (sum count) agg
    (if (zerop count)
        :null
        (sql-/ sum (coerce count 'double-float)))))

(defstruct agg-min min has-value-p)

(defmethod make-agg ((type (eql :min)) &key distinct)
  (%make-distinct-agg (make-agg-min) distinct))

(defmethod agg-accumulate ((agg agg-min) x &rest args)
  (declare (ignore args))
  (with-slots (min has-value-p) agg
    (if has-value-p
        (setf min (if (sql-< min x)
                      min
                      x))
        (setf min x has-value-p t))
    agg))

(defmethod agg-accumulate ((agg agg-min) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-min))
  (with-slots (min has-value-p) agg
    (if has-value-p
        min
        :null)))

(defstruct agg-max max has-value-p)

(defmethod make-agg ((type (eql :max)) &key distinct)
  (%make-distinct-agg (make-agg-max) distinct))

(defmethod agg-accumulate ((agg agg-max) x &rest args)
  (declare (ignore args))
  (with-slots (max has-value-p) agg
    (if has-value-p
        (setf max (if (sql-> max x)
                      max
                      x))
        (setf max x has-value-p t))
    agg))

(defmethod agg-accumulate ((agg agg-max) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-max))
  (with-slots (max has-value-p) agg
    (if has-value-p
        max
        :null)))

(defstruct agg-group_concat (acc nil :type (or null string)) seen distinct)

(defmethod make-agg ((type (eql :group_concat)) &key distinct)
  (make-agg-group_concat :seen nil :distinct distinct))

(defmethod agg-accumulate ((agg agg-group_concat) x &rest args)
  (with-slots (acc seen distinct) agg
    (when (and (eq :distinct distinct) args)
      (error 'sql-runtime-error :message "GROUP_CONCAT with argument doesn't support DISTINCT"))
    (if (member x seen :test +hash-table-test+)
        agg
        (let ((separator (syn-cast (or (first args) ",") :varchar)))
          (when distinct
            (push x seen))
          (setf acc (if acc
                        (concatenate 'string acc separator (syn-cast x :varchar))
                        (syn-cast x :varchar)))
          agg))))

(defmethod agg-accumulate ((agg agg-group_concat) (x (eql :null)) &rest args)
  (declare (ignore args))
  agg)

(defmethod agg-finish ((agg agg-group_concat))
  (or (agg-group_concat-acc agg) :null))

(defstruct agg-array_agg acc order-by)

(defmethod make-agg ((type (eql :array_agg)) &key order-by)
  (make-agg-array_agg :order-by order-by))

(defmethod agg-accumulate ((agg agg-array_agg) x &rest args)
  (with-slots (acc order-by) agg
    (push (coerce (cons x args) 'vector) acc)
    agg))

(defmethod agg-finish ((agg agg-array_agg))
  (with-slots (acc order-by) agg
    (fset:convert 'fset:seq (mapcar (lambda (row)
                                      (aref row 0))
                                    (if order-by
                                        (ra-order-by acc order-by)
                                        (reverse acc))))))

(defstruct agg-object_agg (acc (make-hash-table :test +hash-table-test+)))

(defmethod make-agg ((type (eql :object_agg)) &key distinct)
  (declare (ignore distinct))
  (make-agg-object_agg))

(defmethod agg-accumulate ((agg agg-object_agg) x &rest args)
  (unless (eq 1 (length args))
    (error 'sql-runtime-error :message "OBJECT_AGG requires both key and value argument"))
  (with-slots (acc) agg
    (setf (gethash (syn-cast x :varchar) acc) (first args))
    agg))

(defmethod agg-finish ((agg agg-object_agg))
  (with-slots (acc) agg
    (fset:convert 'fset:map acc)))
