(defpackage :endb/arrow
  (:use :cl)
  (:import-from :trivial-utf-8))
(in-package :endb/arrow)

(defgeneric arrow-push (array x))
(defgeneric arrow-push-null (array))
(defgeneric arrow-valid-p (array n))
(defgeneric arrow-get (array n))
(defgeneric arrow-value (array n))
(defgeneric arrow-length (array))
(defgeneric arrow-null-count (array))
(defgeneric arrow-data-type (array))

(defclass arrow-array (sequence standard-object) ())

(defmethod print-object ((obj arrow-array) stream)
  (print-unreadable-object (obj stream :type t)
    (format stream "~a" (coerce obj 'vector))))

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
             (arrow-push-null new-array)))))
    (loop
      (if (< (arrow-length new-array) length)
          (arrow-push-null new-array)
          (return new-array)))))

(defclass primitive-array (arrow-array)
  ((validity :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector)
   (values :type vector)))

(defmethod arrow-valid-p ((array primitive-array) (n fixnum))
  (with-slots (validity) array
    (= 1 (aref validity n))))

(defmethod arrow-push ((array primitive-array) x)
  (with-slots (validity) array
    (vector-push-extend 1 validity)
    array))

(defmethod arrow-push-null ((array primitive-array))
  (with-slots (values validity) array
    (vector-push-extend 0 validity)
    (adjust-array values (1+ (length values)) :fill-pointer (1+ (fill-pointer values)))
    array))

(defmethod arrow-get ((array primitive-array) (n fixnum))
  (when (arrow-valid-p array n)
    (arrow-value array n)))

(defmethod arrow-length ((array primitive-array))
  (with-slots (validity) array
    (length validity)))

(defmethod arrow-null-count ((array primitive-array))
  (with-slots (validity) array
    (count-if #'zerop validity)))

(defclass int64-array (primitive-array)
  ((values :initform (make-array 0 :element-type '(signed-byte 64) :fill-pointer 0) :type (vector (signed-byte 64)))))

(defmethod arrow-push ((array int64-array) (x integer))
  (with-slots (values) array
    (vector-push-extend x values)
    (call-next-method)
    array))

(defmethod arrow-value ((array int64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array int64-array))
  "l")

(defclass float64-array (primitive-array)
  ((values :initform (make-array 0 :element-type 'double-float :fill-pointer 0) :type (vector double-float))))

(defmethod arrow-push ((array float64-array) (x float))
  (with-slots (values) array
    (vector-push-extend (coerce x 'double-float) values)
    (call-next-method)
    array))

(defmethod arrow-value ((array float64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defmethod arrow-data-type ((array float64-array))
  "g")

(defclass boolean-array (primitive-array)
  ((values :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type (vector bit))))

(defmethod arrow-push ((array boolean-array) x)
  (with-slots (values) array
    (vector-push-extend (if x 1 0) values)
    (call-next-method)
    array))

(defmethod arrow-value ((array boolean-array) (n fixnum))
  (= 1 (aref (slot-value array 'values) n)))

(defmethod arrow-data-type ((array boolean-array))
  "b")

(defclass binary-array (arrow-array)
  ((validity :initform (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector)
   (offsets :initform (make-array 1 :element-type '(signed-byte 32) :fill-pointer 1) :type (vector (signed-byte 32)))
   (data :initform (make-array 0 :element-type '(unsigned-byte 8) :fill-pointer 0) :type (vector (unsigned-byte 8)))))

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

(defmethod arrow-push-null ((array binary-array))
  (with-slots (validity offsets data) array
    (vector-push-extend 0 validity)
    (vector-push-extend (length data) offsets)
    array))

(defmethod arrow-value ((array binary-array) (n fixnum))
  (with-slots (offsets data) array
    (let* ((start (aref offsets n) )
           (end (aref offsets (1+ n)))
           (len (- end start)))
      (make-array len :element-type '(unsigned-byte 8) :displaced-to data :displaced-index-offset start))))

(defmethod arrow-get ((array binary-array) (n fixnum))
  (with-slots (validity) array
    (when (= 1 (aref validity n))
      (arrow-value array n))))

(defmethod arrow-length ((array binary-array))
  (with-slots (validity) array
    (length validity)))

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

;; (pprint (arrow-push (arrow-push-null (make-instance 'int64-array)) 1))

;; (loop for x being the element in (arrow-push (arrow-push-null (make-instance 'int64-array)) 1)
;;       collect x)

;; (setf (elt (arrow-push (arrow-push-null (make-instance 'int64-array)) 1) 2) 2)

;; (arrow-get (arrow-push (arrow-push-null (make-instance 'int64-array)) 1) 0)

;; (arrow-get (arrow-push (arrow-push-null (make-instance 'float64-array)) 1.0d0) 1)

;; (arrow-get (arrow-push (arrow-push-null (make-instance 'boolean-array)) t) 1)

;; (arrow-get (arrow-push (arrow-push (arrow-push-null (make-instance 'binary-array)) #(0 1 2))  #(3 4)) 2)

;; (arrow-get (arrow-push (arrow-push (arrow-push-null (make-instance 'utf8-array)) "hello") "world") 2)

;; (arrow-null-count (arrow-push (arrow-push (arrow-push-null (make-instance 'utf8-array)) "hello") "world"))

;; (arrow-length (arrow-push (arrow-push (arrow-push-null (make-instance 'utf8-array)) "hello") "world"))

;; (arrow-data-type (make-instance 'utf8-array))
