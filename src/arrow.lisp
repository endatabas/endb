(defpackage :endb/arrow
  (:use :cl)
  (:export #:arrow-date-millis #:arrow-time-micros #:arrow-timestamp-micros #:arrow-interval-month-day-nanos #:arrow-binary #:arrow-struct
           #:parse-arrow-date-millis #:parse-arrow-timestamp-micros #:parse-arrow-time-micros #:parse-arrow-interval-month-day-nanos
           #:arrow-date-millis-ms #:arrow-time-micros-us #:arrow-timestamp-micros-us #:arrow-interval-month-day-nanos-ns #:arrow-interval-month-day-nanos-uint128
           #:arrow-timestamp-micros-to-local-time #:arrow-time-micros-to-local-time #:arrow-date-millis-to-local-time #:arrow-interval-month-day-nanos-to-periods-duration
           #:local-time-to-arrow-timestamp-micros #:local-time-to-arrow-date-millis #:local-time-to-arrow-time-micros #:periods-duration-to-arrow-interval-month-day-nanos
           #:to-arrow #:make-arrow-array-for #:arrow-class-for-format
           #:arrow-push #:arrow-valid-p #:arrow-get #:arrow-value
           #:arrow-length #:arrow-null-count #:arrow-data-type #:arrow-lisp-type
           #:arrow-children #:arrow-buffers
           #:arrow-struct-projection #:arrow-struct-children #:arrow-struct-row-get #:arrow-struct-row-push
           #:arrow-array #:validity-array #:null-array #:int32-array #:int64-array #:float64-array
           #:date-millis-array #:timestamp-micros-array #:time-micros-array #:binary-array #:utf8-array #:list-array #:struct-array #:dense-union-array)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :cl-murmurhash)
  (:import-from :fset)
  (:import-from :local-time)
  (:import-from :periods)
  (:import-from :trivial-utf-8))
(in-package :endb/arrow)

(deftype uint8 () '(unsigned-byte 8))
(deftype uint128 () '(unsigned-byte 128))
(deftype int8 () '(signed-byte 8))
(deftype int32 () '(signed-byte 32))
(deftype int64 () '(signed-byte 64))
(deftype int128 () '(signed-byte 128))
(deftype float64 () 'double-float)

(defstruct arrow-timestamp-micros (us 0 :type int64))

(defun %timestamp-to-micros (x)
  (let* ((sec (local-time:timestamp-to-unix x))
         (nsec (local-time:nsec-of x)))
    (+ (* 1000000 sec) (/ nsec 1000))))

(defun parse-arrow-timestamp-micros (s)
  (let ((s (if (and (> (length s) 10)
                    (eq #\Space (char s 10)))
               (let ((s (copy-seq s)))
                 (setf (char s 10) #\T)
                 s)
               s)))
    (local-time-to-arrow-timestamp-micros (local-time:parse-timestring s))))

(defun %micros-to-timestamp (us)
  (let* ((ns (* 1000 us)))
    (multiple-value-bind (sec ns)
        (floor ns 1000000000)
      (local-time:unix-to-timestamp sec :nsec ns))))

(defun arrow-timestamp-micros-to-local-time (x)
  (%micros-to-timestamp (arrow-timestamp-micros-us x)))

(defun local-time-to-arrow-timestamp-micros (x)
  (make-arrow-timestamp-micros :us (%timestamp-to-micros x)))

(defmethod print-object ((object arrow-timestamp-micros) stream)
  (cond
    (local-time::*debug-timestamp*
     (call-next-method))
    (t
     (when *print-escape*
       (write-char #\@ stream))
     (let ((timestamp (arrow-timestamp-micros-to-local-time object)))
       (local-time:format-rfc3339-timestring stream timestamp :timezone local-time:+utc-zone+)))))

(defstruct arrow-date-millis (ms 0 :type int64))

(defun arrow-date-millis-to-local-time (x)
  (%micros-to-timestamp (* (arrow-date-millis-ms x) 1000)))

(defparameter +millis-per-day+ (* local-time:+seconds-per-day+ 1000))

(defun local-time-to-arrow-date-millis (x)
  (let* ((ms (truncate (%timestamp-to-micros x) 1000))
         (ms (* +millis-per-day+ (truncate ms +millis-per-day+))))
    (make-arrow-date-millis :ms ms)))

(defmethod print-object ((object arrow-date-millis) stream)
  (cond
    (local-time::*debug-timestamp*
     (call-next-method))
    (t
     (when *print-escape*
       (write-char #\@ stream))
     (let ((date (arrow-date-millis-to-local-time object)))
       (local-time:format-rfc3339-timestring stream date :omit-time-part t :omit-timezone-part t :timezone local-time:+utc-zone+)))))

(defun parse-arrow-date-millis (s)
  (local-time-to-arrow-date-millis (local-time:parse-timestring s :allow-missing-time-part t)))

(defstruct arrow-time-micros (us 0 :type int64))

(defun %time-to-micros (x)
  (let* ((sec (local-time:sec-of x))
         (nsec (local-time:nsec-of x)))
    (+ (* 1000000 sec) (/ nsec 1000))))

(defun parse-arrow-time-micros (s)
  (local-time-to-arrow-time-micros (local-time:parse-timestring s :allow-missing-date-part t)))

(defun %micros-to-time (us)
  (let* ((ns (* 1000 us)))
    (multiple-value-bind (sec ns)
        (floor ns 1000000000)
      (local-time:make-timestamp :sec sec :nsec ns))))

(defun arrow-time-micros-to-local-time (x)
  (%micros-to-time (arrow-time-micros-us x)))

(defun local-time-to-arrow-time-micros (x)
  (make-arrow-time-micros :us (%time-to-micros x)))

(defmethod print-object ((object arrow-time-micros) stream)
  (cond
    (local-time::*debug-timestamp*
     (call-next-method))
    (t
     (when *print-escape*
       (write-char #\@ stream))
     (let ((time (arrow-time-micros-to-local-time object)))
       (local-time:format-rfc3339-timestring stream time :omit-date-part t :omit-timezone-part t :timezone local-time:+utc-zone+)))))

(defstruct arrow-interval-month-day-nanos (day 0 :type int32) (month 0 :type int32) (ns 0 :type int64))

(defun arrow-interval-month-day-nanos-uint128 (x)
  (dpb (arrow-interval-month-day-nanos-ns x) (byte 64 0)
       (dpb (arrow-interval-month-day-nanos-day x) (byte 32 64)
            (dpb (arrow-interval-month-day-nanos-month x) (byte 32 96) 0))))

(defun periods-duration-to-arrow-interval-month-day-nanos (duration)
  (let ((month (+ (* (periods::duration-years duration) 12)
                  (periods::duration-months duration)))
        (day (periods::duration-days duration))
        (ns (+ (* (periods::duration-hours duration) 60 60 1000000000)
               (* (periods::duration-minutes duration) 60 1000000000)
               (* (periods:duration-seconds duration) 1000000000)
	       (* (periods::duration-milliseconds duration) 1000000)
	       (* (periods::duration-microseconds duration) 1000)
	       (periods::duration-nanoseconds duration))))
    (make-arrow-interval-month-day-nanos :month month :day day :ns ns)))

(defparameter +iso-duration-scanner+ (ppcre:create-scanner "^P(\\d+Y)?(\\d+M)?(\\d+D)?(?:T(\\d+H)?(\\d+M)?(\\d+(?:\\.\\d+)?S)?)?$"))

(defun parse-arrow-interval-month-day-nanos (s)
  (let ((*read-eval* nil)
        (*read-default-float-format* 'double-float))
    (labels ((parse-number (x)
               (if x
                   (read-from-string (subseq x 0 (1- (length x))))
                   0)))
      (multiple-value-bind (matchp parts) (ppcre:scan-to-strings +iso-duration-scanner+ s)
        (when matchp
          (destructuring-bind (years months days hours minutes seconds)
              (coerce parts 'list)
            (let ((years (parse-number years))
                  (months (parse-number months))
                  (days (parse-number days))
                  (hours (parse-number hours))
                  (minutes (parse-number minutes))
                  (seconds (parse-number seconds)))
              (make-arrow-interval-month-day-nanos :month (+ (* years 12) months)
                                                   :day days
                                                   :ns (round (+ (* hours 60 60 1000000000)
                                                                 (* minutes 60 1000000000)
                                                                 (* seconds 1000000000)))))))))))

(defun arrow-interval-month-day-nanos-to-periods-duration (x)
  (with-slots (month day ns) x
    (multiple-value-bind (years months)
        (floor month 12)
      (multiple-value-bind (microseconds nanoseconds)
          (floor ns 1000)
        (multiple-value-bind (milliseconds microseconds)
            (floor microseconds 1000)
          (multiple-value-bind (seconds milliseconds)
              (floor milliseconds 1000)
            (multiple-value-bind (minutes seconds)
                (floor seconds 60)
              (multiple-value-bind (hours minutes)
                  (floor minutes 60)
                (periods:duration :years years
                                  :months months
                                  :days day
                                  :hours hours
                                  :minutes minutes
                                  :seconds seconds
                                  :milliseconds milliseconds
                                  :microseconds microseconds
                                  :nanoseconds nanoseconds)))))))))

(defmethod print-object ((object arrow-interval-month-day-nanos) stream)
  (let* ((duration (arrow-interval-month-day-nanos-to-periods-duration object))
         (ns (+ (* (periods:duration-seconds duration) 1000000000)
	        (* (periods::duration-milliseconds duration) 1000000)
	        (* (periods::duration-microseconds duration) 1000)
	        (periods::duration-nanoseconds duration)))
         (time-part (concatenate 'string
                                 (unless (zerop (periods::duration-hours duration))
                                   (format nil "~AH" (periods::duration-hours duration)))
                                 (unless (zerop (periods::duration-minutes duration))
                                   (format nil "~AM" (periods::duration-minutes duration)))
                                 (unless (zerop ns)
                                   (let ((seconds (/ ns 1000000000)))
                                     (format nil (if (integerp seconds)
                                                     "~AS"
                                                     "~fS")
                                             seconds))))))
    (when *print-escape*
      (write-char #\@ stream))
    (format stream (concatenate 'string
                                "P"
                                (unless (zerop (periods::duration-years duration))
                                  (format nil "~AY" (periods::duration-years duration)))
                                (unless (zerop (periods::duration-months duration))
                                  (format nil "~AM" (periods::duration-months duration)))
                                (unless (zerop (periods::duration-days duration))
                                  (format nil "~AD" (periods::duration-days duration)))
                                (unless (equal "" time-part)
                                  (format nil "T~A" time-part))))))

(defmethod murmurhash:murmurhash ((x endb/arrow:arrow-timestamp-micros) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (endb/arrow:arrow-timestamp-micros-us x) :seed seed :mix-only mix-only))

(defmethod murmurhash:murmurhash ((x endb/arrow:arrow-date-millis) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (endb/arrow:arrow-date-millis-ms x) :seed seed :mix-only mix-only))

(defmethod murmurhash:murmurhash ((x endb/arrow:arrow-time-micros) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (endb/arrow:arrow-time-micros-us x) :seed seed :mix-only mix-only))

(defmethod murmurhash:murmurhash ((x endb/arrow:arrow-interval-month-day-nanos) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (arrow-interval-month-day-nanos-uint128 x) :seed seed :mix-only mix-only))

(defmethod murmurhash:murmurhash ((x fset:seq) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (fset:convert 'vector x) :seed seed :mix-only mix-only))

(defmethod murmurhash:murmurhash ((x fset:map) &key (seed murmurhash:*default-seed*) mix-only)
  (murmurhash:murmurhash (fset:convert 'hash-table x) :seed seed :mix-only mix-only))

(fset:define-cross-type-compare-methods endb/arrow:arrow-date-millis)
(fset:define-cross-type-compare-methods endb/arrow:arrow-timestamp-micros)
(fset:define-cross-type-compare-methods endb/arrow:arrow-time-micros)
(fset:define-cross-type-compare-methods endb/arrow:arrow-interval-month-day-nanos)

(defmethod fset:compare ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-date-millis))
  (fset:compare (endb/arrow:arrow-date-millis-ms x)
                (endb/arrow:arrow-date-millis-ms y)))

(defmethod fset:compare ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (fset:compare (endb/arrow:arrow-timestamp-micros-us x)
                (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod fset:compare ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (fset:compare (endb/arrow:arrow-time-micros-us x)
                (endb/arrow:arrow-time-micros-us y)))

(defmethod fset:compare ((x endb/arrow:arrow-interval-month-day-nanos) (y endb/arrow:arrow-interval-month-day-nanos))
  (fset:compare (endb/arrow:arrow-interval-month-day-nanos-uint128 x)
                (endb/arrow:arrow-interval-month-day-nanos-uint128 y)))

(defmethod fset:compare ((x endb/arrow:arrow-date-millis) (y endb/arrow:arrow-timestamp-micros))
  (fset:compare (endb/arrow:local-time-to-arrow-timestamp-micros (endb/arrow:arrow-date-millis-to-local-time x)) y))


(defmethod fset:compare ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-date-millis))
  (fset:compare x (endb/arrow:local-time-to-arrow-timestamp-micros (endb/arrow:arrow-date-millis-to-local-time y))))

(deftype arrow-binary ()
  '(vector uint8))

(deftype arrow-list ()
  'fset:seq)

(deftype arrow-struct ()
  'fset:map)

(defgeneric arrow-push (array x))
(defgeneric arrow-valid-p (array n))
(defgeneric arrow-get (array n))
(defgeneric arrow-value (array n))
(defgeneric arrow-length (array))
(defgeneric arrow-null-count (array))
(defgeneric arrow-data-type (array))
(defgeneric arrow-lisp-type (array))
(defgeneric arrow-children (array))
(defgeneric arrow-buffers (array))

(defclass arrow-array (sequence standard-object) ())

(defmethod print-object ((obj arrow-array) stream)
  (print-unreadable-object (obj stream :type t)
    (format stream "#(")
    (dotimes (n (arrow-length obj))
      (format stream "~s" (arrow-get obj n))
      (when (< n (1- (arrow-length obj)))
        (format stream ", ")))
    (format stream ")")))

(defmethod sequence:elt ((array arrow-array) (n fixnum))
  (arrow-get array n))

(defmethod sequence:length ((array arrow-array))
  (arrow-length array))

(defmethod (setf sequence:elt) (x (array arrow-array) (n fixnum))
  (if (= n (arrow-length array))
      (progn
        (arrow-push array x)
        x)
      (call-next-method)))

(defmethod sequence:make-sequence-like ((array arrow-array) length &key (initial-element nil initial-element-p) (initial-contents nil initial-contents-p))
  (let ((new-array (make-instance (type-of array))))
    (cond
      (initial-element-p
       (dotimes (n length)
         (arrow-push new-array initial-element)))
      (initial-contents-p
       (dolist (x (subseq initial-contents 0 (min length (length initial-contents))))
         (arrow-push new-array x)))
      (t
       (dotimes (n (min length (arrow-length array)))
         (arrow-push new-array (arrow-get array n)))))
    (loop while (< (arrow-length new-array) length)
          do (arrow-push new-array :null))
    new-array))

(defmethod sequence:adjust-sequence ((array arrow-array) length &key (initial-element nil initial-element-p) (initial-contents nil initial-contents-p))
  (apply #'sequence:make-sequence-like array length
         (append (when initial-element-p
                   (list :initial-element initial-element))
                 (when initial-contents-p
                   (list :initial-contents initial-contents)))))

(defmethod sequence:subseq ((array arrow-array) start &optional end)
  (loop with new-array = (make-instance (type-of array))
        for n from start below (or end (arrow-length array))
        do (arrow-push new-array (arrow-get array n))
        finally (return new-array)))

(defun %array-class-for (x)
  (etypecase x
    ((eql :null) 'null-array)
    (boolean 'boolean-array)
    (int64 'int64-array)
    (int128 'decimal-array)
    ((and number (not int128)) 'float64-array)
    (arrow-date-millis 'date-millis-array)
    (arrow-time-micros 'time-micros-array)
    (arrow-timestamp-micros 'timestamp-micros-array)
    (arrow-interval-month-day-nanos 'interval-month-day-nanos-array)
    (arrow-struct 'struct-array)
    (arrow-binary 'binary-array)
    (string 'utf8-array)
    (arrow-list 'list-array)))

(defun make-arrow-array-for (x)
  (let ((c (%array-class-for x)))
    (if (eq 'struct-array c)
        (make-instance c :children (fset:reduce
                                    (lambda (acc k v)
                                      (append acc (list (cons k (make-arrow-array-for v)))))
                                    x
                                    :initial-value ()))
        (make-instance c))))

(defun arrow-class-for-format (format)
  (cond
    ((equal "n" format) 'null-array)
    ((equal "b" format) 'boolean-array)
    ((equal "i" format) 'int32-array)
    ((equal "l" format) 'int64-array)
    ((equal "g" format) 'float64-array)
    ((equal "tsu:" format) 'timestamp-micros-array)
    ((equal "tdm" format) 'date-millis-array)
    ((equal "ttu" format) 'time-micros-array)
    ((equal "tin" format) 'interval-month-day-nanos-array)
    ((equal "d:38,0" format) 'decimal-array)
    ((equal "z" format) 'binary-array)
    ((equal "u" format) 'utf8-array)
    ((equal "+s" format) 'struct-array)
    ((equal "+l" format) 'list-array)
    (t (if (alexandria:starts-with-subseq "+ud:" format)
           'dense-union-array
           (error "unknown format: ~s" format)))))

(defun to-arrow (xs)
  (reduce
   (lambda (acc x)
     (arrow-push acc x))
   xs :initial-value (make-instance 'null-array)))

(defmethod arrow-push ((array arrow-array) x)
  (let ((len (arrow-length array))
        (new-array (arrow-push (make-arrow-array-for x) x)))
    (if (zerop len)
        new-array
        (let* ((type-ids (make-array len :element-type 'int8
                                         :fill-pointer len
                                         :initial-element 0))
               (offsets (make-array len :element-type 'int32
                                        :fill-pointer len))
               (children (list (cons nil array) (cons nil new-array))))
          (vector-push-extend 1 type-ids)
          (dotimes (n len)
            (setf (aref offsets n) n))
          (vector-push-extend 0 offsets)
          (make-instance 'dense-union-array
                         :children children
                         :offsets offsets
                         :type-ids type-ids)))))

(defmethod arrow-children ((array arrow-array)))

(defclass null-array (arrow-array)
  ((null-count :type integer)))

(defmethod initialize-instance :after ((array null-array) &key (length 0) (null-count 0))
  (assert (= length null-count))
  (setf (slot-value array 'null-count) null-count))

(defmethod arrow-push ((array null-array) x)
  (let ((new-array (make-arrow-array-for x)))
    (dotimes (n (arrow-length array))
      (arrow-push new-array :null))
    (arrow-push new-array x)))

(defmethod arrow-push ((array null-array) (x (eql :null)))
  (with-slots (null-count) array
    (incf null-count)
    array))

(defmethod arrow-valid-p ((array null-array) (n fixnum))
  nil)

(defmethod arrow-get ((array null-array) (n fixnum))
  :null)

(defmethod arrow-value ((array null-array) (n fixnum))
  :null)

(defmethod arrow-length ((array null-array))
  (with-slots (null-count) array
    null-count))

(defmethod arrow-null-count ((array null-array))
  (with-slots (null-count) array
    null-count))

(defmethod arrow-data-type ((array null-array))
  "n")

(defmethod arrow-lisp-type ((array null-array))
  '(eql :null))

(defmethod arrow-buffers ((array null-array)))

(defclass validity-array (arrow-array)
  ((validity :initform nil :type (or null bit-vector))))

(defmethod initialize-instance :after ((array validity-array) &key length null-count)
  (when (and length null-count)
    (assert (<= null-count length))
    (when (plusp null-count)
      (with-slots (validity) array
        (setf validity (make-array length :element-type 'bit))))))

(defmethod arrow-valid-p ((array validity-array) (n fixnum))
  (with-slots (validity) array
    (or (null validity)
        (= 1 (aref validity n)))))

(defmethod arrow-get ((array validity-array) (n fixnum))
  (with-slots (validity) array
    (if (or (null validity)
            (= 1 (aref validity n)))
        (arrow-value array n)
        :null)))

(defmethod arrow-null-count ((array validity-array))
  (with-slots (validity) array
    (count-if #'zerop validity)))

(defun %push-valid (array)
  (with-slots (validity) array
    (when validity
      (vector-push-extend 1 validity))))

(defun %ensure-validity-buffer (array)
  (with-slots (validity) array
    (let ((len (arrow-length array)))
      (unless validity
        (setf validity (make-array len :element-type 'bit :fill-pointer len :initial-element 1))))))

(defun %push-invalid (array)
  (with-slots (validity) array
    (%ensure-validity-buffer array)
    (vector-push-extend 0 validity)))

(defmethod arrow-buffers ((array validity-array))
  (with-slots (validity) array
    (list validity)))

(defclass primitive-array (validity-array)
  ((values :type vector)
   (element-type)))

(defmethod initialize-instance :after ((array primitive-array) &key (length 0 lengthp))
  (with-slots (values element-type) array
    (setf values (make-array length :element-type element-type
                                    :fill-pointer (unless lengthp
                                                    length)))))

(defmethod (setf sequence:elt) (x (array primitive-array) (n fixnum))
  (with-slots (values validity) array
    (if (eq :null x)
        (let ((len (arrow-length array)))
          (unless validity
            (setf validity (make-array len :element-type 'bit :fill-pointer len :initial-element 1)))
          (setf (aref validity n) 0))
        (progn
          (setf (aref values n) x)
          (when validity
            (setf (aref validity n) 1))))
    x))

(defmethod arrow-push ((array primitive-array) (x (eql :null)))
  (with-slots (values) array
    (%push-invalid array)
    (adjust-array values (1+ (length values)) :fill-pointer (1+ (fill-pointer values)))
    array))

(defmethod arrow-length ((array primitive-array))
  (with-slots (values) array
    (length values)))

(defmethod arrow-buffers ((array primitive-array))
  (with-slots (values) array
    (append (call-next-method) (list values))))

(defmethod arrow-lisp-type ((array primitive-array))
  (with-slots (element-type) array
    element-type))

(defclass int32-array (primitive-array)
  ((values :type (vector int32))
   (element-type :initform 'int32)))

(defmethod arrow-push ((array int32-array) (x integer))
  (with-slots (values) array
    (%push-valid array)
    (vector-push-extend x values)
    array))

(defmethod arrow-value ((array int32-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array int32-array))
  "i")

(defclass int64-array (primitive-array)
  ((values :type (vector int64))
   (element-type :initform 'int64)))

(defmethod arrow-push ((array int64-array) (x integer))
  (if (typep x 'int64)
      (with-slots (values) array
        (%push-valid array)
        (vector-push-extend x values)
        array)
      (call-next-method)))

(defmethod arrow-value ((array int64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array int64-array))
  "l")

(defclass timestamp-micros-array (int64-array) ())

(defmethod arrow-push ((array timestamp-micros-array) (x arrow-timestamp-micros))
  (arrow-push array (arrow-timestamp-micros-us x)))

(defmethod arrow-value ((array timestamp-micros-array) (n fixnum))
  (make-arrow-timestamp-micros :us (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array timestamp-micros-array))
  "tsu:")

(defmethod arrow-lisp-type ((array timestamp-micros-array))
  'arrow-timestamp-micros)

(defclass date-millis-array (int64-array) ())

(defmethod arrow-push ((array date-millis-array) (x arrow-date-millis))
  (arrow-push array (arrow-date-millis-ms x)))

(defmethod arrow-value ((array date-millis-array) (n fixnum))
  (make-arrow-date-millis :ms (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array date-millis-array))
  "tdm")

(defmethod arrow-lisp-type ((array date-millis-array))
  'arrow-date-millis)

(defclass time-micros-array (int64-array) ())

(defmethod arrow-push ((array time-micros-array) (x arrow-time-micros))
  (arrow-push array (arrow-time-micros-us x)))

(defmethod arrow-value ((array time-micros-array) (n fixnum))
  (make-arrow-time-micros :us  (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array time-micros-array))
  "ttu")

(defmethod arrow-lisp-type ((array time-micros-array))
  'arrow-time-micros)

(defclass float64-array (primitive-array)
  ((values :type (vector float64))
   (element-type :initform 'float64)))

(defmethod arrow-push ((array float64-array) (x number))
  (if (typep x 'int64)
      (call-next-method)
      (with-slots (values) array
        (%push-valid array)
        (vector-push-extend (coerce x 'float64) values)
        array)))

(defmethod arrow-value ((array float64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array float64-array))
  "g")

(defmethod arrow-lisp-type ((array float64-array))
  '(and number (not int64)))

(defclass boolean-array (primitive-array)
  ((values :type (vector bit))
   (element-type :initform 'bit)))

(defmethod (setf sequence:elt) (x (array boolean-array) (n fixnum))
  (call-next-method (if x 1 0) array n)
  x)

(defmethod arrow-push ((array boolean-array) x)
  (with-slots (values) array
    (%push-valid array)
    (vector-push-extend (if x 1 0) values)
    array))

(defmethod arrow-push ((array boolean-array) (x (eql :null)))
  (with-slots (values) array
    (%push-invalid array)
    (vector-push-extend 0 values)
    array))

(defmethod arrow-value ((array boolean-array) (n fixnum))
  (= 1 (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array boolean-array))
  "b")

(defmethod arrow-lisp-type ((array boolean-array))
  'boolean)

(defclass fixed-width-binary-array (validity-array)
  ((values :type (vector uint8))
   (element-size :type int8)))

(defmethod initialize-instance :after ((array fixed-width-binary-array) &key (element-size nil element-size-p) (length 0 lengthp))
  (when element-size-p
    (setf (slot-value array 'element-size) element-size))
  (with-slots (values element-size) array
    (let ((length (* element-size length)))
      (setf values (make-array length :element-type 'uint8
                                      :fill-pointer (unless lengthp
                                                      length))))))

(defmethod arrow-push ((array fixed-width-binary-array) (x (eql :null)))
  (with-slots (values element-size) array
    (%push-invalid array)
    (adjust-array values (+ element-size (length values)) :fill-pointer (+ element-size (fill-pointer values)))
    array))

(defmethod arrow-push ((array fixed-width-binary-array) (x vector))
  (with-slots (values element-size) array
    (if (= element-size (length x))
        (progn
          (%push-valid array)
          (loop for y across x
                do (vector-push-extend y values))
          array)
        (call-next-method))))

(defmethod arrow-value ((array fixed-width-binary-array) (n fixnum))
  (with-slots (values element-size) array
    (let* ((start (* element-size n) )
           (storage #+sbcl (sb-ext:array-storage-vector values)
                    #-sbcl vlaues))
      (make-array element-size :element-type 'uint8 :displaced-to storage :displaced-index-offset start))))

(defmethod arrow-length ((array fixed-width-binary-array))
  (with-slots (values element-size) array
    (nth-value 0 (truncate (length values) element-size))))

(defmethod arrow-buffers ((array fixed-width-binary-array))
  (with-slots (values) array
    (append (call-next-method) (list values))))

(defmethod arrow-data-type ((array fixed-width-binary-array))
  (format nil "w:~d" (slot-value array 'element-size)))

(defmethod arrow-lisp-type ((array fixed-width-binary-array))
  '(vector uint8))

(defclass decimal-array (fixed-width-binary-array)
  ((element-size :initform 16)))

(defmethod arrow-push ((array decimal-array) (x integer))
  (if (typep x 'int128)
      (with-slots (values element-size) array
        (%push-valid array)
        (loop for idx from (1- element-size) downto 0
              do (vector-push-extend (ldb (byte 8 (* 8 idx)) x) values))
        array)
      (call-next-method)))

(defmethod arrow-value ((array decimal-array) (n fixnum))
  (with-slots (values element-size) array
    (loop with v = 0
          for x across (call-next-method)
          do (setf v (logior (ash v 8) x))
          finally (return (if (and (= (integer-length v) 128)
                                   (plusp v))
                              (- v (ash 1 128))
                              v)))))

(defmethod arrow-lisp-type ((array decimal-array))
  'integer)

(defmethod arrow-data-type ((array decimal-array))
  "d:38,0")

(defclass interval-month-day-nanos-array (fixed-width-binary-array)
  ((element-size :initform 16)))

(defmethod arrow-push ((array interval-month-day-nanos-array) (x arrow-interval-month-day-nanos))
  (with-slots (values element-size) array
    (%push-valid array)
    (loop with x = (arrow-interval-month-day-nanos-uint128 x)
          for idx from (1- element-size) downto 0
          do (vector-push-extend (ldb (byte 8 (* 8 idx)) x) values))
    array))

(defmethod arrow-value ((array interval-month-day-nanos-array) (n fixnum))
  (with-slots (values element-size) array
    (loop with v = 0
          for x across (call-next-method)
          do (setf v (logior (ash v 8) x))
          finally (return (make-arrow-interval-month-day-nanos :month (ldb (byte 32 96) v)
                                                               :day (ldb (byte 32 64) v)
                                                               :ns (ldb (byte 64 0) v))))))

(defmethod arrow-data-type ((array interval-month-day-nanos-array))
  "tin")

(defmethod arrow-lisp-type ((array interval-month-day-nanos-array))
  'arrow-interval-month-day-nanos)

(defclass binary-array (validity-array)
  ((offsets :type (vector int32))
   (data :type (or null (vector uint8)))))

(defmethod initialize-instance :after ((array binary-array) &key (length 0 lengthp))
  (with-slots (offsets data) array
    (setf offsets (make-array (1+ length) :element-type 'int32
                                          :fill-pointer (unless lengthp
                                                          (1+ length))))
    (setf data (unless lengthp
                 (make-array 0 :element-type 'uint8 :fill-pointer 0)))))

(defmethod arrow-push ((array binary-array) (x vector))
  (with-slots (offsets data) array
    (%push-valid array)
    (loop for y across x
          do (vector-push-extend y data))
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-push ((array binary-array) (x (eql :null)))
  (with-slots (offsets data) array
    (%push-invalid array)
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-value ((array binary-array) (n fixnum))
  (with-slots (offsets data) array
    (let* ((start (aref offsets n) )
           (end (aref offsets (1+ n)))
           (len (- end start))
           (storage #+sbcl (sb-ext:array-storage-vector data)
                    #-sbcl data))
      (make-array len :element-type 'uint8 :displaced-to storage :displaced-index-offset start))))

(defmethod arrow-length ((array binary-array))
  (with-slots (offsets) array
    (1- (length offsets))))

(defmethod arrow-data-type ((array binary-array))
  "z")

(defmethod arrow-lisp-type ((array binary-array))
  '(vector uint8))

(defmethod arrow-buffers ((array binary-array))
  (with-slots (offsets data) array
    (append (call-next-method) (list offsets data))))

(defclass utf8-array (binary-array) ())

(defmethod arrow-push ((array utf8-array) (x string))
  (with-slots (offsets data) array
    (%push-valid array)
    (macrolet ((byte-out (byte)
                 `(vector-push-extend ,byte data)))
      (loop for y across x
            do (trivial-utf-8::as-utf-8-bytes y byte-out)))
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-value ((array utf8-array) (n fixnum))
  (with-slots (offsets data) array
    (let* ((start (aref offsets n) )
           (end (aref offsets (1+ n)))
           (storage #+sbcl (sb-ext:array-storage-vector data)
                    #-sbcl data))
      (trivial-utf-8:utf-8-bytes-to-string storage :start start :end end))))

(defmethod arrow-data-type ((array utf8-array))
  "u")

(defmethod arrow-lisp-type ((array utf8-array))
  'string)

(defclass list-array (validity-array)
  ((offsets :type (vector int32))
   (values :type arrow-array)))

(defmethod initialize-instance :after ((array list-array) &key (length 0 lengthp) children)
  (with-slots (offsets values) array
    (setf offsets (make-array (1+ length) :element-type 'int32
                                          :fill-pointer (unless lengthp
                                                          (1+ length))))
    (if children
        (progn
          (assert (= 1 (length children)))
          (setf values (cdr (first children))))
        (setf values (make-instance 'null-array)))))

(defmethod arrow-push ((array list-array) (x fset:seq))
  (with-slots (offsets values) array
    (%push-valid array)
    (fset:do-seq (y x)
      (setf values (arrow-push values y)))
    (vector-push-extend (arrow-length values) offsets)
    array))

(defmethod arrow-push ((array list-array) (x (eql :null)))
  (with-slots (offsets values) array
    (%push-invalid array)
    (vector-push-extend (length values) offsets)
    array))

(defmethod arrow-value ((array list-array) (n fixnum))
  (with-slots (offsets values) array
    (let* ((start (aref offsets n))
           (end (aref offsets (1+ n))))
      (fset:convert 'fset:seq
                    (loop for src-idx from start below end
                          for dst-idx from 0
                          collect (arrow-get values src-idx))))))

(defmethod arrow-length ((array list-array))
  (with-slots (offsets) array
    (1- (length offsets))))

(defmethod arrow-data-type ((array list-array))
  "+l")

(defmethod arrow-lisp-type ((array list-array))
  'arrow-list)

(defmethod arrow-buffers ((array list-array))
  (with-slots (offsets) array
    (append (call-next-method) (list offsets))))

(defmethod arrow-children ((array list-array))
  (with-slots (values) array
    (list (cons "item" values))))

(defun %same-struct-fields-p (array x)
  (with-slots (children) array
    (equal (fset:convert 'list (fset:domain x))
           (mapcar #'car children))))

(defclass struct-array (validity-array)
  ((children :initarg :children :initform () :type list)))

(defmethod arrow-push ((array struct-array) (x fset:map))
  (if (%same-struct-fields-p array x)
      (with-slots (children) array
        (when (fset:empty? x)
          (%ensure-validity-buffer array))
        (%push-valid array)
        (fset:do-map (k v x)
          (let ((kv-pair (assoc k children :test 'equal)))
            (setf (cdr kv-pair) (arrow-push (cdr kv-pair) v))))
        array)
      (call-next-method)))

(defmethod arrow-push ((array struct-array) (x (eql :null)))
  (with-slots (children) array
    (%push-invalid array)
    (loop for kv-pair in children
          do (setf (cdr kv-pair) (arrow-push (cdr kv-pair) :null)))
    array))

(defmethod arrow-value ((array struct-array) (n fixnum))
  (with-slots (children) array
    (reduce
     (lambda (acc kv)
       (fset:with acc (car kv) (arrow-get (cdr kv) n)))
     children
     :initial-value (fset:empty-map))))

(defmethod arrow-length ((array struct-array))
  (with-slots (children validity) array
    (let ((first-array (cdr (first children))))
      (if first-array
          (arrow-length first-array)
          (length validity)))))

(defmethod arrow-data-type ((array struct-array))
  "+s")

(defmethod arrow-lisp-type ((array struct-array))
  'arrow-struct)

(defmethod arrow-children ((array struct-array))
  (with-slots (children) array
    children))

(defun arrow-struct-children (array projection)
  (with-slots (children) array
    (loop with len = (arrow-length array)
          with missing-column = (make-instance 'null-array :null-count len :length len)
          with children = children
          for c in projection
          collect (or (cdr (assoc c children :test 'equal)) missing-column))))

(defun arrow-struct-projection (array n projection)
  (loop with row = (arrow-get array n)
        for c in projection
        collect (multiple-value-bind (v vp)
                    (fset:lookup row c)
                  (if vp
                      v
                      :null))))

(defun arrow-struct-row-get (array n)
  (with-slots (children) array
    (loop for (nil . v) in children
          collect (arrow-get v n))))

(defun arrow-struct-row-push (array row)
  (with-slots (children) array
    (%push-valid array)
    (loop for kv-pair in children
          for v in row
          do (setf (cdr kv-pair) (arrow-push (cdr kv-pair) v)))
    array))

(defclass dense-union-array (arrow-array)
  ((type-ids :initarg :type-ids :initform nil :type (or null (vector int8)))
   (offsets :initarg :offsets :initform nil :type (or null (vector int32)))
   (children :type vector)))

(defmethod initialize-instance :after ((array dense-union-array) &key (length 0 lengthp) (null-count 0) children)
  (assert (zerop null-count))
  (with-slots (type-ids offsets) array
    (unless type-ids
      (setf type-ids (make-array length :element-type 'int8
                                        :fill-pointer (unless lengthp
                                                        length))))
    (unless offsets
      (setf offsets (make-array length :element-type 'int32
                                       :fill-pointer (unless lengthp
                                                       length))))
    (let ((children (mapcar #'cdr children)))
      (setf (slot-value array 'children)
            (make-array (length children) :fill-pointer (unless lengthp
                                                          (length children))
                                          :initial-contents children)))))

(defmethod arrow-valid-p ((array dense-union-array) (n fixnum))
  (with-slots (type-ids offsets children) array
    (arrow-valid-p (nth (aref type-ids n) children) (aref offsets n))))

(defmethod arrow-push ((array dense-union-array) x)
  (with-slots (type-ids offsets children) array
    (loop for id below (length children)
          for c = (aref children id)
          for c-type = (arrow-lisp-type c)
          when (and (typep x c-type)
                    (or (not (eql 'arrow-struct c-type))
                        (%same-struct-fields-p c x)))
            do (progn
                 (vector-push-extend id type-ids)
                 (vector-push-extend (arrow-length c) offsets)
                 (setf (aref children id) (arrow-push c x))
                 (return array))
          finally
             (let ((new-array (arrow-push (make-arrow-array-for x) x))
                   (new-type-id (length children)))
               (vector-push-extend new-type-id type-ids)
               (vector-push-extend 0 offsets)
               (vector-push-extend new-array children)
               (return array)))))

(defmethod arrow-push ((array dense-union-array) (x (eql :null)))
  (with-slots (type-ids offsets children) array
    (let* ((id 0)
           (c (aref children id)))
      (vector-push-extend (1- (arrow-length (arrow-push c :null))) offsets)
      (vector-push-extend id type-ids)
      array)))

(defmethod arrow-get ((array dense-union-array) (n fixnum))
  (with-slots (type-ids offsets children) array
    (arrow-get (aref children (aref type-ids n)) (aref offsets n))))

(defmethod arrow-value ((array dense-union-array) (n fixnum))
  (with-slots (type-ids offsets children) array
    (arrow-value (aref children (aref type-ids n)) (aref offsets n))))

(defmethod arrow-length ((array dense-union-array))
  (with-slots (type-ids) array
    (length type-ids)))

(defmethod arrow-null-count ((array dense-union-array))
  (with-slots (children) array
    (loop for c across children
          sum (arrow-null-count c))))

(defmethod arrow-data-type ((array dense-union-array))
  (with-slots (children) array
    (format nil "+ud:~{~a~^,~}" (loop for id below (length children)
                                      collect id))))

(defmethod arrow-lisp-type ((array dense-union-array))
  't)

(defmethod arrow-buffers ((array dense-union-array))
  (with-slots (type-ids offsets) array
    (list type-ids offsets)))

(defmethod arrow-children ((array dense-union-array))
  (with-slots (children) array
    (loop for c across children
          collect (cons (arrow-data-type c) c))))
