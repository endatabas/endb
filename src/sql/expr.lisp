(defpackage :endb/sql/expr
  (:use :cl)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :periods)
  (:import-from :endb/arrow)
  (:import-from :endb/json)
  (:import-from :endb/lib/parser)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :cl-bloom)
  (:import-from :fset)
  (:export #:sql-= #:sql-<> #:sql-is #:sql-not #:sql-and #:sql-or
           #:sql-< #:sql-<= #:sql-> #:sql->=
           #:sql-+ #:sql-- #:sql-* #:sql-/ #:sql-% #:sql-<<  #:sql->> #:sql-~ #:sql-& #:sql-\|
           #:sql-between #:sql-coalesce
           #:sql-object_keys #:sql-object_values #:sql-object_entries #:sql-object_from_entries
           #:sql-\|\| #:sql-cardinality #:sql-char_length #:sql-character_length #:sql-octet_length #:sql-length #:sql-trim #:sql-ltrim #:sql-rtrim #:sql-lower #:sql-upper
           #:sql-replace #:sql-unhex #:sql-hex #:sql-instr #:sql-min #:sql-max #:sql-char #:sql-unicode #:sql-random #:sql-glob #:sql-regexp #:sql-randomblob #:sql-zeroblob #:sql-iif
           #:sql-round #:sql-sin #:sql-cos #:sql-tan #:sql-sinh #:sql-cosh #:sql-tanh #:sql-asin #:sqn-acos #:sql-atan #:sql-asinh #:sqn-acosh #:sql-atanh #:sql-atan2
           #:sql-floor #:sql-ceiling #:sql-ceil #:sql-patch #:sql-match
           #:sql-sign #:sql-sqrt #:sql-exp #:sql-power #:sql-pow #:sql-log #:sql-log2 #:sql-log10 #:sql-ln #:sql-degrees #:sql-radians #:sql-pi
           #:sql-cast #:sql-nullif #:sql-abs #:sql-date #:sql-time #:sql-datetime #:sql-timestamp #:sql-duration #:sql-interval #:sql-like #:sql-substr #:sql-substring #:sql-strftime
           #:sql-typeof #:sql-unixepoch #:sql-julianday #:sql-path_remove #:sql-path_insert #:sql-path_replace #:sql-path_set #:sql-path_extract
           #:sql-contains #:sql-overlaps #:sql-precedes #:sql-succedes #:sql-immediately_precedes #:sql-immediately_succedes

           #:syn-current_date #:syn-current_time #:syn-current_timestamp
           #:syn-access #:syn-access-finish

           #:ra-distinct #:ra-unnest #:ra-union-all #:ra-union #:ra-except #:ra-intersect
           #:ra-scalar-subquery #:ra-in  #:ra-exists #:ra-limit #:ra-order-by

           #:make-agg #:agg-accumulate #:agg-finish
           #:ddl-create-table #:ddl-drop-table #:ddl-create-view #:ddl-drop-view #:ddl-create-index #:ddl-drop-index #:ddl-create-assertion #:ddl-drop-assertion
           #:dml-insert #:dml-insert-objects #:dml-delete

           #:make-db #:copy-db #:db-buffer-pool #:db-wal #:db-object-store #:db-meta-data #:db-current-timestamp
           #:base-table #:base-table-rows #:base-table-deleted-row-ids #:table-type #:table-columns #:constraint-definitions
           #:base-table-meta #:base-table-arrow-batches #:base-table-visible-rows #:base-table-size #:batch-row-system-time-end
           #:view-definition #:calculate-stats
           #:sql-runtime-error #:*sqlite-mode* #:+end-of-time+))
(in-package :endb/sql/expr)

(defvar *sqlite-mode* nil)

(defparameter +end-of-time+ (endb/arrow:parse-arrow-timestamp-micros "9999-01-01"))

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

(defmethod sql-= (x y)
  (fset:equal? x y))

(defun sql-<> (x y)
  (sql-not (sql-= x y)))

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
      (mod x y)))

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
  (sql-\|\| (sql-cast x :varchar) (sql-cast y :varchar)))

(defmethod sql-cardinality ((x vector))
  (length x))

(defmethod sql-cardinality ((x fset:collection))
  (fset:size x))

(defmethod sql-char_length ((x string))
  (length x))

(defmethod sql-character_length ((x string))
  (length x))

(defmethod sql-octet_length ((x vector))
  (if (typep x 'endb/arrow:arrow-binary)
      (length x)
      :null))

(defmethod sql-octet_length ((x string))
  (trivial-utf-8:utf-8-byte-length x))

(defmethod sql-length ((x sequence))
  (length x))

(defmethod sql-length ((x fset:collection))
  (fset:size x))

(defmethod sql-trim ((x string) &optional (y " "))
  (string-trim y x))

(defmethod sql-ltrim ((x string) &optional (y " "))
  (string-left-trim y x))

(defmethod sql-rtrim ((x string) &optional (y " "))
  (string-right-trim y x))

(defmethod sql-lower ((x string))
  (string-downcase x))

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
  (sql-unhex (sql-cast x :varchar) y))

(defparameter +hex-scanner+ (ppcre:create-scanner "^(?i:[0-9a-f]{2})+$"))

(defmethod sql-unhex ((x string) &optional (y ""))
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
        :null)))

(defmethod sql-hex (x)
  (sql-hex (sql-cast x :varchar)))

(defmethod sql-hex ((x string))
  (sql-hex (trivial-utf-8:string-to-utf-8-bytes x)))

(defmethod sql-hex ((x vector))
  (format nil "~{~X~}" (coerce x 'list)))

(defmethod sql-hex ((x (eql :null)))
  "")

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

(defmethod sql-cast ((x (eql :null)) type)
  :null)

(defmethod sql-cast (x (type (eql :varchar)))
  (princ-to-string x))

(defmethod sql-cast ((x integer) (type (eql :varchar)))
  (princ-to-string x))

(defmethod sql-cast ((x fset:seq) (type (eql :varchar)))
  (with-output-to-string (out)
    (format out "[")
    (fset:do-seq (v x :index idx)
      (format out (if (stringp v)
                      "\"~A\""
                      "~A")
              (sql-cast v :varchar))
      (when (< idx (1- (fset:size x)))
        (format out ",")))
    (format out "]")))

(defmethod sql-cast ((x fset:map) (type (eql :varchar)))
  (with-output-to-string (out)
    (format out "{")
    (let ((idx 0))
      (fset:do-map (k v x)
        (format out "\"~A\":" k)
        (format out (if (stringp v)
                        "\"~A\""
                        "~A")
                (sql-cast v :varchar))
        (when (< idx (1- (fset:size x)))
          (format out ","))
        (incf idx)))
    (format out "}")))

(defmethod sql-cast ((x (eql t)) (type (eql :varchar)))
  "1")

(defmethod sql-cast ((x (eql nil)) (type (eql :varchar)))
  "0")

(defmethod sql-cast ((x string) (type (eql :varchar)))
  x)

(defmethod sql-cast ((x real) (type (eql :varchar)))
  (format nil "~F" x))

(defmethod sql-cast ((x vector) (type (eql :varchar)))
  (trivial-utf-8:utf-8-bytes-to-string x))

(defmethod sql-cast (x (type (eql :text)))
  (sql-cast x :varchar))

(defmethod sql-cast (x (type (eql :blob)))
  (sql-cast (sql-cast x :varchar) :blob))

(defmethod sql-cast ((x string) (type (eql :blob)))
  (trivial-utf-8:string-to-utf-8-bytes x))

(defmethod sql-cast ((x vector) (type (eql :blob)))
  x)

(defmethod sql-cast (x (type (eql :varbinary)))
  (sql-cast x :blob))

(defmethod sql-cast ((x (eql t)) (type (eql :integer)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :integer)))
  0)

(defmethod sql-cast ((x string) (type (eql :integer)))
  (multiple-value-bind (start end)
      (ppcre:scan "^-?\\d+" x)
    (if start
        (let ((*read-eval* nil))
          (read-from-string (subseq x 0 end)))
        0)))

(defmethod sql-cast ((x real) (type (eql :integer)))
  (round x))

(defmethod sql-cast ((x integer) (type (eql :integer)))
  x)

(defmethod sql-cast (x (type (eql :bigint)))
  (sql-cast x :integer))

(defmethod sql-cast (x (type (eql :integer)))
  (sql-cast (sql-cast x :varchar) :integer))

(defmethod sql-cast (x (type (eql :decimal)))
  (sql-cast x :real))

(defmethod sql-cast ((x (eql t)) (type (eql :decimal)))
  1)

(defmethod sql-cast ((x (eql nil)) (type (eql :decimal)))
  0)

(defmethod sql-cast ((x string) (type (eql :decimal)))
  (multiple-value-bind (start end)
      (ppcre:scan "^-?\\d+(\\.\\d+)?([eE][-+]?\\d+)?" x)
    (if start
        (let ((*read-eval* nil)
              (*read-default-float-format* 'double-float))
          (read-from-string (subseq x 0 end)))
        0)))

(defmethod sql-cast ((x number) (type (eql :decimal)))
  x)

(defmethod sql-cast (x (type (eql :signed)))
  (sql-cast x :decimal))

(defmethod sql-cast ((x (eql t)) (type (eql :real)))
  1.0d0)

(defmethod sql-cast ((x (eql nil)) (type (eql :real)))
  0.0d0)

(defmethod sql-cast ((x string) (type (eql :real)))
  (coerce (sql-cast x :decimal) 'double-float))

(defmethod sql-cast ((x number) (type (eql :real)))
  (coerce x 'double-float))

(defmethod sql-cast (x (type (eql :real)))
  (sql-cast (sql-cast x :varchar) :real))

(defmethod sql-cast (x (type (eql :double)))
  (sql-cast x :real))

(defmethod sql-cast ((x endb/arrow:arrow-date-millis) (type (eql :timestamp)))
  (endb/arrow:local-time-to-arrow-timestamp-micros (endb/arrow:arrow-date-millis-to-local-time x)))

(defmethod sql-cast ((x endb/arrow:arrow-timestamp-micros) (type (eql :date)))
  (endb/arrow:local-time-to-arrow-date-millis (endb/arrow:arrow-timestamp-micros-to-local-time x)))

(defmethod sql-cast ((x endb/arrow:arrow-timestamp-micros) (type (eql :time)))
  (endb/arrow:local-time-to-arrow-time-micros (endb/arrow:arrow-timestamp-micros-to-local-time x)))

(defmethod sql-cast (x (type (eql :timestamp)))
  (sql-timestamp x))

(defmethod sql-cast (x (type (eql :date)))
  (sql-date x))

(defmethod sql-cast (x (type (eql :time)))
  (sql-time x))

(defmethod sql-cast (x (type (eql :interval)))
  (sql-duration x))

(defun sql-nullif (x y)
  (if (eq t (sql-= x y))
      :null
      x))

(defun %handle-complex (fn x)
  (if (complexp x)
      (error 'endb/sql/expr:sql-runtime-error
             :message (format nil "Complex number as result: ~A to: ~A" x fn))
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
  (%handle-complex "LOG"
                   (cond
                     ((eq :null y) :null)
                     (yp (log y (coerce x 'double-float)))
                     (t (log x 10.0d0)))))

(defmethod sql-log10 ((x (eql :null)))
  :null)

(defmethod sql-log10 ((x number))
  (%handle-complex "LOG10" (log (coerce x 'double-float) 10)))

(defmethod sql-log2 ((x (eql :null)))
  :null)

(defmethod sql-log2 ((x number))
  (%handle-complex "LOG2" (log (coerce x 'double-float) 2)))

(defmethod sql-ln ((x (eql :null)))
  :null)

(defmethod sql-ln ((x number))
  (%handle-complex "LN" (log (coerce x 'double-float))))

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
  (sql-julianday (sql-cast x :timestamp)))

(defmethod sql-julianday ((x endb/arrow:arrow-timestamp-micros))
  (let ((noon-offset 0.5d0)
        (unix-day-offset 11017.0d0))
    (- (/ (endb/arrow:arrow-timestamp-micros-us x)
          1000000.0d0
          local-time:+seconds-per-day+)
       local-time::+astronomical-julian-date-offset+
       noon-offset
       unix-day-offset)))

(defmethod sql-duration ((x string))
  (let ((duration (endb/arrow:parse-arrow-interval-month-day-nanos x)))
    (if duration
        duration
        (error 'sql-runtime-error
               :message (format nil "Invalid duration: ~A" x)))))

(defparameter +interval-time-parts+ '(:hour :minute :second))
(defparameter +interval-parts+ (append '(:year :month :day) +interval-time-parts+))

(defparameter +interval-scanner+ (ppcre:create-scanner "[ :-]"))

(defmethod sql-interval ((x string) from &optional (to from))
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

(defmethod sql-like ((x (eql :null)) y &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-like (x (y (eql :null)) &optional z)
  (declare (ignore z))
  :null)

(defmethod sql-like ((x string) (y string) &optional (z nil zp))
  (when (and zp (not (= 1 (length z))))
    (error 'sql-runtime-error :message (format nil "Invalid escape character: ~A" z)))
  (let* ((regex (ppcre:regex-replace-all (if zp
                                             (format nil "(?<![~A])%" z)
                                             "%")
                                         y
                                         ".*"))
         (regex (ppcre:regex-replace-all (if zp
                                             (format nil "(?<![~A])_" z)
                                             "_")
                                         regex
                                         "."))
         (regex (if zp
                    (ppcre:regex-replace-all (format nil "~A([_%])" z) regex "\\1")
                    regex))
         (regex (concatenate 'string "^" regex "$")))
    (integerp (ppcre:scan regex x))))

(defmethod sql-glob ((x (eql :null)) y)
  :null)

(defmethod sql-glob (x (y (eql :null)))
  :null)

(defmethod sql-glob ((x string) (y string))
  (let ((regex (concatenate 'string "^" (ppcre:regex-replace-all "\\?" (ppcre:regex-replace-all "\\*" x ".*") ".") "$")))
    (integerp (ppcre:scan regex y))))

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
                        collect (cons (sql-cast (fset:lookup x 0) :varchar)
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

(defconstant +random-uuid-part-max+ (ash 1 64))
(defconstant +random-uuid-version+ 4)
(defconstant +random-uuid-variant+ 2)

(defun %random-uuid (&optional (state *random-state*))
  (let ((high (dpb +random-uuid-version+ (byte 4 12) (random +random-uuid-part-max+ state)))
        (low (dpb +random-uuid-variant+ (byte 2 62) (random +random-uuid-part-max+ state))))
    (format nil "~(~4,'0x~4,'0x-~4,'0x-~4,'0x-~4,'0x-~4,'0x~4,'0x~4,'0x~)"
            (ldb (byte 16 48) high)
            (ldb (byte 16 32) high)
            (ldb (byte 16 16) high)
            (ldb (byte 16 0) high)
            (ldb (byte 16 48) low)
            (ldb (byte 16 32) low)
            (ldb (byte 16 16) low)
            (ldb (byte 16 0) low))))

(defparameter +random-uuid-scanner+
  (ppcre:create-scanner "^[\\da-f]{8}-[\\da-f]{4}-4[\\da-f]{3}-[89ab][\\da-f]{3}-[\\da-f]{12}$"))

(defun %random-uuid-p (x)
  (and (stringp x)
       (not (null (ppcre:scan +random-uuid-scanner+ x)))))

(defun sql-uuid ()
  (%random-uuid))

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
  (agg-finish (reduce #'agg-accumulate (cons x (cons y args)) :initial-value (make-agg :min))))

(defun sql-max (x y &rest args)
  (agg-finish (reduce #'agg-accumulate (cons x (cons y args)) :initial-value (make-agg :max))))

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

(defun sql-succedes (x y)
  (sql->= (%period-field x "start")
          (%period-field y "end")))

(defun sql-immediately_precedes (x y)
  (sql-= (%period-field x "end")
         (%period-field y "start")))

(defun sql-immediately_succedes (x y)
  (sql-= (%period-field x "start")
         (%period-field y "end")))

;; Syntax

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

(defun syn-current_date (db)
  (sql-cast (syn-current_timestamp db) :date))

(defun syn-current_time (db)
  (sql-cast (syn-current_timestamp db) :time))

(defun syn-current_timestamp (db)
  (or (db-current-timestamp db)
      (endb/arrow:local-time-to-arrow-timestamp-micros (local-time:now))))

;; RA

(defun ra-distinct (rows &optional (distinct :distinct))
  (if (eq :distinct distinct)
      (delete-duplicates rows :test 'equalp)
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

(defun ra-exists (rows)
  (not (null rows)))

(defun ra-union (lhs rhs)
  (ra-distinct (nunion lhs rhs :test 'equalp)))

(defun ra-union-all (lhs rhs)
  (nconc lhs rhs))

(defun ra-except (lhs rhs)
  (ra-distinct (nset-difference lhs rhs :test 'equalp)))

(defun ra-intersect (lhs rhs)
  (ra-distinct (nintersection lhs rhs :test 'equalp)))

(defun ra-scalar-subquery (rows)
  (when (> 1 (length rows))
    (error 'sql-runtime-error :message "Scalar subquery must return max one row"))
  (if (null rows)
      :null
      (caar rows)))

(defun ra-unnest (arrays &key with-ordinality)
  (let ((arrays (loop for a in arrays
                      collect (if (fset:map? a)
                                  (sql-object_entries a)
                                  a))))
    (when (every #'fset:seq? arrays)
      (let ((len (apply #'max (mapcar #'fset:size arrays))))
        (reverse (if (eq :with-ordinality with-ordinality)
                     (loop for idx below len
                           collect (append (loop for a in arrays
                                                 collect (if (< idx (fset:size a))
                                                             (fset:lookup a idx)
                                                             :null))
                                           (list idx)))
                     (loop for idx below len
                           collect (loop for a in arrays
                                         collect (if (< idx (fset:size a))
                                                     (fset:lookup a idx)
                                                     :null)))))))))

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
                       for xv = (nth (1- idx) x)
                       for yv = (nth (1- idx) y)
                       thereis (funcall cmp xv yv)
                       until (funcall cmp yv xv))))))

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
    (if (member x seen :test 'equalp)
        agg
        (let ((separator (sql-cast (or (first args) ",") :varchar)))
          (when distinct
            (push x seen))
          (setf acc (if acc
                        (concatenate 'string acc separator (sql-cast x :varchar))
                        (sql-cast x :varchar)))
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
    (push (cons x args) acc)
    agg))

(defmethod agg-finish ((agg agg-array_agg))
  (with-slots (acc order-by) agg
    (fset:convert 'fset:seq (mapcar #'car (if order-by
                                              (ra-order-by acc order-by)
                                              (reverse acc))))))

(defstruct agg-object_agg (acc (make-hash-table :test 'equalp)))

(defmethod make-agg ((type (eql :object_agg)) &key distinct)
  (declare (ignore distinct))
  (make-agg-object_agg))

(defmethod agg-accumulate ((agg agg-object_agg) (x string) &rest args)
  (unless (eq 1 (length args))
    (error 'sql-runtime-error :message "OBJECT_AGG requires both key and value argument"))
  (with-slots (acc) agg
    (setf (gethash x acc) (first args))
    agg))

(defmethod agg-accumulate ((agg agg-object_agg) x &rest args)
  (declare (ignore args))
  (error 'sql-runtime-error :message (format nil "OBJECT_AGG requires string key argument: ~A" x)))

(defmethod agg-finish ((agg agg-object_agg))
  (with-slots (acc) agg
    (fset:convert 'fset:map acc)))

;; DML/DDL

(defvar *default-schema* "main")

(defstruct db wal object-store buffer-pool (meta-data (fset:map ("_last_tx" 0))) current-timestamp)

(defun base-table-meta (db table-name)
  (with-slots (meta-data) db
    (or (fset:lookup meta-data table-name) (fset:empty-map))))

(defun base-table-arrow-batches (db table-name arrow-file)
  (with-slots (buffer-pool) db
    (let ((arrow-file-key (format nil "~A/~A" table-name arrow-file)))
      (loop for batch in (endb/storage/buffer-pool:buffer-pool-get buffer-pool arrow-file-key)
            collect (endb/arrow:arrow-children batch)))))

(defun base-table-visible-rows (db table-name &key arrow-file-idx-row-id-p)
  (let ((table-md (base-table-meta db table-name))
        (projection (table-columns db table-name))
        (acc))
    (fset:do-map (arrow-file arrow-file-md table-md acc)
      (loop with deletes-md = (or (fset:lookup arrow-file-md "deletes") (fset:empty-map))
            for batch-row in (base-table-arrow-batches db table-name arrow-file)
            for batch = (cdr (assoc table-name batch-row :test 'equal))
            for batch-idx from 0
            for batch-deletes = (or (fset:lookup deletes-md (prin1-to-string batch-idx)) (fset:empty-seq))
            do (setf acc (append acc (loop for row-id below (endb/arrow:arrow-length batch)
                                           unless (fset:find row-id batch-deletes
                                                             :key (lambda (x)
                                                                    (fset:lookup x "row_id")))
                                             collect (if arrow-file-idx-row-id-p
                                                         (cons (list arrow-file batch-idx row-id)
                                                               (endb/arrow:arrow-struct-projection batch row-id projection))
                                                         (endb/arrow:arrow-struct-projection batch row-id projection)))))))))

(defun batch-row-system-time-end (batch-deletes row-id)
  (fset:lookup (or (fset:find-if (lambda (x)
                                   (= row-id (fset:lookup x "row_id")))
                                 batch-deletes)
                   (fset:map ("system_time_end" +end-of-time+)))
               "system_time_end"))

(defun %information-schema-table-p (table-name)
  (member table-name
          '("information_schema.columns"
            "information_schema.tables"
            "information_schema.views"
            "information_schema.check_constraints")
          :test 'equal))

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
                            (base-table-visible-rows db "information_schema.views"))))
    (endb/lib/parser:parse-sql (nth 3 view-row))))

(defun constraint-definitions (db)
  (let ((*read-eval* nil)
        (*read-default-float-format* 'double-float))
    (fset:convert 'fset:map
                  (loop for constraint-row in (base-table-visible-rows db "information_schema.check_constraints")
                        collect (cons (nth 2 constraint-row)
                                      (endb/lib/parser:parse-sql (format nil "SELECT ~A" (nth 3 constraint-row))))))))

(defun table-columns (db table-name)
  (cond
    ((equal "information_schema.columns" table-name)
     '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position"))
    ((equal "information_schema.tables" table-name)
     '("table_catalog" "table_schema" "table_name" "table_type"))
    ((equal "information_schema.views" table-name)
     '("table_catalog" "table_schema" "table_name" "view_definition"))
    ((equal "information_schema.check_constraints" table-name)
     '("constraint_catalog" "constraint_schema" "constraint_name" "check_clause"))
    (t (mapcar #'second (ra-order-by (loop with rows = (base-table-visible-rows db "information_schema.columns")
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

(defun %fset-values (m)
  (fset:reduce (lambda (acc k v)
                 (declare (ignore k))
                 (vector-push-extend v acc)
                 acc)
               m
               :initial-value (make-array 0 :fill-pointer 0)))

(defun base-table-size (db table-name)
  (let ((table-md (base-table-meta db table-name)))
    (reduce (lambda (acc md)
              (+ acc (- (fset:lookup md "length")
                        (reduce (lambda (acc x)
                                  (+ acc (fset:size x)))
                                (%fset-values (or (fset:lookup md "deletes") (fset:empty-map)))
                                :initial-value 0))))
            (%fset-values table-md)
            :initial-value 0)))

(defun %find-arrow-file-idx-row-id (db table-name predicate)
  (loop for (arrow-file-idx-row-id . row) in (base-table-visible-rows db table-name :arrow-file-idx-row-id-p t)
        when (funcall predicate row)
          do (return arrow-file-idx-row-id)))

(defun ddl-create-table (db table-name columns)
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "CREATE TABLE not supported"))
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal table-name (nth 2 row))))
    (dml-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
    (dml-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* table-name c idx)))
    (values nil t)))

(defun ddl-drop-table (db table-name &key if-exists)
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "DROP TABLE not supported"))
  (with-slots (meta-data) db
    (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                           "information_schema.tables"
                                                           (lambda (row)
                                                             (and (equal table-name (nth 2 row)) (equal "BASE TABLE" (nth 3 row)))))))
      (when batch-file-row-id
        (dml-delete db "information_schema.tables" (list batch-file-row-id))
        (dml-delete db "information_schema.columns" (loop for c in (table-columns db table-name)
                                                          collect (%find-arrow-file-idx-row-id db
                                                                                               "information_schema.columns"
                                                                                               (lambda (row)
                                                                                                 (and (equal table-name (nth 2 row)) (equal c (nth 3 row)))))))

        (setf meta-data (fset:less meta-data table-name)))

      (when (or batch-file-row-id if-exists)
        (values nil t)))))

(defun ddl-create-view (db view-name query columns)
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal view-name (nth 2 row))))
    (dml-insert db "information_schema.tables" (list (list :null *default-schema* view-name "VIEW")))
    (dml-insert db "information_schema.views" (list (list :null *default-schema* view-name query)))
    (dml-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* view-name c idx)))
    (values nil t)))

(defun ddl-drop-view (db view-name &key if-exists)
  (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                         "information_schema.tables"
                                                         (lambda (row)
                                                           (and (equal view-name (nth 2 row)) (equal "VIEW" (nth 3 row)))))))
    (when batch-file-row-id
      (dml-delete db "information_schema.tables" (list batch-file-row-id))
      (dml-delete db "information_schema.columns" (loop for c in (table-columns db view-name)
                                                        collect (%find-arrow-file-idx-row-id db
                                                                                             "information_schema.columns"
                                                                                             (lambda (row)
                                                                                               (and (equal view-name (nth 2 row)) (equal c (nth 3 row)))))))
      (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                             "information_schema.views"
                                                             (lambda (row)
                                                               (equal view-name (nth 2 row))))))
        (dml-delete db "information_schema.views" (list batch-file-row-id))))
    (when (or batch-file-row-id if-exists)
      (values nil t))))

(defun ddl-create-assertion (db constraint-name check-clause)
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.check_constraints"
                                       (lambda (row)
                                         (equal constraint-name (nth 2 row))))
    (dml-insert db "information_schema.check_constraints"
                (list (list :null *default-schema* constraint-name check-clause)))
    (values nil t)))

(defun ddl-drop-assertion (db constraint-name &key if-exists)
  (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                         "information_schema.check_constraints"
                                                         (lambda (row)
                                                           (equal constraint-name (nth 2 row))))))
    (when batch-file-row-id
      (dml-delete db "information_schema.check_constraints" (list batch-file-row-id)))
    (when (or batch-file-row-id if-exists)
      (values nil t))))

(defun ddl-create-index (db)
  (declare (ignore db))
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "CREATE INDEX not supported"))
  (values nil t))

(defun ddl-drop-index (db)
  (declare (ignore db))
  (unless *sqlite-mode*
    (error 'sql-runtime-error :message "DROP INDEX not supported")))

(defmethod agg-accumulate ((agg cl-bloom::bloom-filter) x &rest args)
  (declare (ignore args))
  (cl-bloom:add agg x)
  agg)

(defmethod agg-finish ((agg cl-bloom::bloom-filter))
  (cffi:with-pointer-to-vector-data (ptr (cl-bloom::filter-array agg))
    (endb/lib/arrow:buffer-to-vector ptr (endb/lib/arrow:vector-byte-size (cl-bloom::filter-array agg)))))

(defun calculate-stats (arrays)
  (let* ((total-length (reduce #'+ (mapcar #'endb/arrow:arrow-length arrays)))
         (bloom-order (* 8 (endb/lib/arrow:vector-byte-size #* (cl-bloom::opt-order total-length)))))
    (labels ((make-col-stats ()
               (fset:map ("count_star" (make-agg :count-star))
                         ("count" (make-agg :count))
                         ("min" (make-agg :min))
                         ("max" (make-agg :max))
                         ("bloom" (make-instance 'cl-bloom::bloom-filter :order bloom-order))))
             (calculate-col-stats (stats k v)
               (let ((col-stats (or (fset:lookup stats k) (make-col-stats))))
                 (fset:with stats k (fset:image
                                     (lambda (agg-k agg-v)
                                       (values agg-k (agg-accumulate agg-v v)))
                                     col-stats)))))
      (let ((stats (reduce
                    (lambda (stats array)
                      (reduce
                       (lambda (stats row)
                         (if (typep row 'endb/arrow:arrow-struct)
                             (fset:reduce #'calculate-col-stats row :initial-value stats)
                             stats))
                       array
                       :initial-value stats))
                    arrays
                    :initial-value (fset:empty-map))))
        (fset:image
         (lambda (k v)
           (values k (fset:image
                      (lambda (k v)
                        (values k (agg-finish v)))
                      v)))
         stats)))))

(defparameter +ident-scanner+ (ppcre:create-scanner "^[a-zA-Z_][a-zA-Z0-9_]*$"))

(defun dml-insert (db table-name values &key column-names)
  (with-slots (buffer-pool meta-data current-timestamp) db
    (let* ((created-p (base-table-created-p db table-name))
           (columns (table-columns db table-name))
           (column-names-set (fset:convert 'fset:set column-names))
           (columns-set (fset:convert 'fset:set columns))
           (new-columns (fset:convert 'list (fset:set-difference column-names-set columns-set)))
           (number-of-columns (length (or column-names columns))))
      (when (member "system_time" column-names :test 'equal)
        (error 'sql-runtime-error :message "Cannot insert value into SYSTEM_TIME column"))
      (loop for c in column-names
            do (unless (ppcre:scan +ident-scanner+ c)
                 (error 'sql-runtime-error
                        :message (format nil "Cannot insert into table: ~A invalid column name: ~A" table-name c))))
      (when (and created-p column-names (not (fset:equal? column-names-set columns-set)))
        (error 'sql-runtime-error
               :message (format nil "Cannot insert into table: ~A named columns: ~A doesn't match stored: ~A" table-name column-names columns)))
      (unless (apply #'= number-of-columns (mapcar #'length values))
        (error 'sql-runtime-error
               :message (format nil "Cannot insert into table: ~A without all values containing same number of columns: ~A" table-name number-of-columns)))
      (when new-columns
        (dml-insert db "information_schema.columns" (loop for c in new-columns
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
                            (endb/arrow:make-arrow-array-for (fset:map (table-name :null)
                                                                       ("system_time_start" current-timestamp))))))
            (loop for row in values
                  do (endb/arrow:arrow-push batch (fset:map (table-name
                                                             (fset:convert 'fset:map
                                                                           (loop for v in row
                                                                                 for cn in (if created-p
                                                                                               columns
                                                                                               column-names)
                                                                                 collect (cons cn v))))
                                                            ("system_time_start" current-timestamp))))

            (endb/storage/buffer-pool:buffer-pool-put buffer-pool batch-key (list batch))

            (let* ((inner-batch (cdr (assoc table-name (endb/arrow:arrow-children batch) :test 'equal)))
                   (batch-md (fset:map-union (or batch-md (fset:empty-map))
                                             (fset:map
                                              ("length" (endb/arrow:arrow-length inner-batch))
                                              ("stats" (calculate-stats (list inner-batch)))))))
              (setf meta-data (fset:with meta-data table-name (fset:with table-md batch-file batch-md))))

            (values nil (length values)))
          (unless (table-type db table-name)
            (dml-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
            (dml-insert db table-name values :column-names column-names))))))

(defun dml-insert-objects (db table-name objects)
  (loop for object in objects
        if (fset:empty? object)
          do (error 'sql-runtime-error :message "Cannot insert empty object")
        else
          do (dml-insert db table-name
                         (list (coerce (%fset-values object) 'list))
                         :column-names (fset:convert 'list (fset:domain object))))
  (values nil (length objects)))

(defun dml-delete (db table-name new-batch-file-idx-deleted-row-ids)
  (with-slots (meta-data current-timestamp) db
    (let* ((table-md (reduce
                      (lambda (acc batch-file-idx-row-id)
                        (destructuring-bind (batch-file batch-idx row-id)
                            batch-file-idx-row-id
                          (let* ((batch-md (fset:lookup acc batch-file))
                                 (deletes-md (or (fset:lookup batch-md "deletes") (fset:empty-map)))
                                 (batch-idx-key (prin1-to-string batch-idx))
                                 (batch-deletes (or (fset:lookup deletes-md batch-idx-key) (fset:empty-seq)))
                                 (delete-entry (fset:map ("row_id" row-id) ("system_time_end" current-timestamp))))
                            (fset:with acc batch-file (fset:with batch-md "deletes" (fset:with deletes-md batch-idx-key (fset:with-last batch-deletes delete-entry)))))))
                      new-batch-file-idx-deleted-row-ids
                      :initial-value (fset:lookup meta-data table-name))))
      (setf meta-data (fset:with meta-data table-name table-md))
      (values nil (length new-batch-file-idx-deleted-row-ids)))))
