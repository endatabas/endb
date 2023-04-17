(defpackage :endb/arrow
  (:use :cl)
  (:import-from :local-time)
  (:import-from :trivial-utf-8))
(in-package :endb/arrow)

(deftype uint8 () '(unsigned-byte 8))
(deftype int32 () '(signed-byte 32))
(deftype int64 () '(signed-byte 64))
(deftype float64 () 'double-float)

(defgeneric arrow-push (array x))
(defgeneric arrow-valid-p (array n))
(defgeneric arrow-get (array n))
(defgeneric arrow-value (array n))
(defgeneric arrow-length (array))
(defgeneric arrow-null-count (array))
(defgeneric arrow-data-type (array))

(defclass arrow-array (sequence standard-object) ())

(defmethod print-object ((obj arrow-array) stream)
  (print-unreadable-object (obj stream :type t)
    (format stream "#(")
    (dotimes (n (arrow-length obj))
      (format stream "~a" (arrow-get obj n))
      (when (< n (1- (arrow-length obj)))
        (format stream ", ")))
    (format stream ")")))

(defmethod sequence:elt ((array arrow-array) (n fixnum))
  (arrow-get array n))

(defmethod sequence:length ((array arrow-array))
  (arrow-length array))

(defmethod (setf sequence:elt) (x (array arrow-array) (n fixnum))
  (if (= n (arrow-length array))
      (arrow-push array x)
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
       (dotimes (n (min length (length array)))
         (if (arrow-valid-p array n)
             (arrow-push new-array (arrow-value array n))
             (arrow-push new-array :null)))))
    (loop
      (if (< (arrow-length new-array) length)
          (arrow-push new-array :null)
          (return new-array)))))

(defclass primitive-array (arrow-array)
  ((validity :initarg :validity :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector)
   (values :initarg :values :type vector)))

(defmethod arrow-valid-p ((array primitive-array) (n fixnum))
  (with-slots (validity) array
    (= 1 (aref validity n))))

(defmethod arrow-push ((array primitive-array) (x (eql :null)))
  (with-slots (values validity) array
    (vector-push-extend 0 validity)
    (adjust-array values (1+ (length values)) :fill-pointer (1+ (fill-pointer values)))
    array))

(defmethod arrow-get ((array primitive-array) (n fixnum))
  (with-slots (validity) array
    (if (= 1 (aref validity n))
      (arrow-value array n)
      :null)))

(defmethod arrow-length ((array primitive-array))
  (with-slots (values) array
    (length values)))

(defmethod arrow-null-count ((array primitive-array))
  (with-slots (validity) array
    (count-if #'zerop validity)))

(defclass int64-array (primitive-array)
  ((values :initform (make-array 0 :element-type 'int64 :fill-pointer 0) :type (vector int64))))

(defmethod arrow-push ((array int64-array) (x integer))
  (with-slots (validity values) array
    (vector-push-extend x values)
    (vector-push-extend 1 validity)
    array))

(defmethod arrow-value ((array int64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array int64-array))
  "l")

(defclass timestamp-micros-array (int64-array) ())

(defun %timestamp-to-micros (x)
  (let* ((sec (local-time:timestamp-to-unix x))
         (nsec (local-time:nsec-of x)))
    (+ (* 1000000 sec) (/ nsec 1000))))

(defmethod arrow-push ((array timestamp-micros-array) (x local-time:timestamp))
  (arrow-push array (%timestamp-to-micros x)))

(defun %micros-to-timestamp (us)
  (let* ((ns (* 1000 us)))
    (multiple-value-bind (sec ns)
        (floor ns 1000000000)
      (local-time:unix-to-timestamp sec :nsec ns))))

(defmethod arrow-value ((array timestamp-micros-array) (n fixnum))
  (%micros-to-timestamp (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array timestamp-micros-array))
  "tsu:")

(defclass float64-array (primitive-array)
  ((values :initform (make-array 0 :element-type 'float64 :fill-pointer 0) :type (vector float64))))

(defmethod arrow-push ((array float64-array) (x float))
  (with-slots (validity values) array
    (vector-push-extend (coerce x 'float64) values)
    (vector-push-extend 1 validity)
    array))

(defmethod arrow-value ((array float64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array float64-array))
  "g")

(defclass boolean-array (primitive-array)
  ((values :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type (vector bit))))

(defmethod arrow-push ((array boolean-array) x)
  (with-slots (validity values) array
    (vector-push-extend (if x 1 0) values)
    (vector-push-extend 1 validity)
    array))

(defmethod arrow-value ((array boolean-array) (n fixnum))
  (= 1 (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array boolean-array))
  "b")

(defclass binary-array (arrow-array)
  ((validity :initarg :validity :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector)
   (offsets :initarg :offsets :initform (make-array 1 :element-type 'int32 :fill-pointer 1 :initial-element 0) :type (vector int32))
   (data :initarg :data :initform (make-array 0 :element-type 'uint8 :fill-pointer 0) :type (vector uint8))))

(defmethod arrow-valid-p ((array binary-array) (n fixnum))
  (with-slots (validity) array
    (= 1 (aref validity n))))

(defmethod arrow-push ((array binary-array) (x vector))
  (with-slots (validity offsets data) array
    (vector-push-extend 1 validity)
    (loop for y across x
          do (vector-push-extend y data (length x)))
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-push ((array binary-array) (x (eql :null)))
  (with-slots (validity offsets data) array
    (vector-push-extend 0 validity)
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-value ((array binary-array) (n fixnum))
  (with-slots (offsets data) array
    (let* ((start (aref offsets n) )
           (end (aref offsets (1+ n)))
           (len (- end start)))
      (make-array len :element-type 'uint8 :displaced-to data :displaced-index-offset start))))

(defmethod arrow-get ((array binary-array) (n fixnum))
  (with-slots (validity) array
    (if (= 1 (aref validity n))
      (arrow-value array n)
      :null)))

(defmethod arrow-length ((array binary-array))
  (with-slots (offsets) array
    (1- (length offsets))))

(defmethod arrow-null-count ((array binary-array))
  (with-slots (validity) array
    (count-if #'zerop validity)))

(defmethod arrow-data-type ((array binary-array))
  "z")

(defclass utf8-array (binary-array) ())

(defmethod arrow-push ((array utf8-array) (x string))
  (with-slots (validity offsets data) array
    (vector-push-extend 1 validity)
    (macrolet ((byte-out (byte)
                 `(vector-push-extend ,byte data)))
      (loop for y across x
            do (trivial-utf-8::as-utf-8-bytes y byte-out)))
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-value ((array utf8-array) (n fixnum))
  (with-slots (offsets data) array
    (let* ((start (aref offsets n) )
           (end (aref offsets (1+ n))))
      (trivial-utf-8:utf-8-bytes-to-string data :start start :end end))))

(defmethod arrow-data-type ((array utf8-array))
  "u")

;; (loop for x being the element in (arrow-push (make-instance 'timestamp-micros-array) (local-time:now))
;;       collect x)

;; (pprint (arrow-push (arrow-push (make-instance 'int64-array) :null) 1))

;; (loop for x being the element in (arrow-push (arrow-push (make-instance 'int64-array) :null) 1)
;;       collect x)

;; (setf (elt (arrow-push (arrow-push (make-instance 'int64-array) :null) 1) 2) 2)

;; (arrow-get (arrow-push (arrow-push (make-instance 'int64-array) :null) 1) 0)

;; (arrow-get (arrow-push (arrow-push (make-instance 'float64-array) :null) 1.0d0) 1)

;; (arrow-get (arrow-push (arrow-push (make-instance 'boolean-array) :null) t) 1)

;; (arrow-get (arrow-push (arrow-push (arrow-push (make-instance 'binary-array) :null) #(0 1 2))  #(3 4)) 2)

;; (arrow-get (arrow-push (arrow-push (arrow-push (make-instance 'utf8-array) :null) "hello") "world") 2)

;; (arrow-null-count (arrow-push (arrow-push (arrow-push (make-instance 'utf8-array) :null) "hello") "world"))

;; (arrow-length (arrow-push (arrow-push (arrow-push (make-instance 'utf8-array) :null) "hello") "world"))

;; (arrow-data-type (make-instance 'utf8-array))
