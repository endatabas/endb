(defpackage :endb/arrow
  (:use :cl)
  (:import-from :trivial-utf-8))
(in-package :endb/arrow)

(defstruct arrow-array
  (data-type nil :type string :read-only t))

(defgeneric arrow-push (array x))
(defgeneric arrow-push-null (array))
(defgeneric arrow-valid-p (array n))
(defgeneric arrow-get (array n))
(defgeneric arrow-value (array n))
(defgeneric arrow-length (array))
(defgeneric arrow-null-count (array))

(defstruct (primitive-array (:include arrow-array))
  (validity (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector :read-only t)
  (values nil :type vector :read-only t))

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

(defstruct (int64-array (:include primitive-array
                         (data-type "l")
                         (values (make-array 0 :element-type '(signed-byte 64) :fill-pointer 0) :type (vector (signed-byte 64))))))

(defmethod arrow-push ((array int64-array) (x integer))
  (with-slots (values) array
    (vector-push-extend x values)
    (call-next-method)
    array))

(defmethod arrow-value ((array int64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defstruct (float64-array (:include primitive-array
                           (data-type "g")
                           (values (make-array 0 :element-type 'double-float :fill-pointer 0) :type (vector double-float)))))

(defmethod arrow-push ((array float64-array) (x float))
  (with-slots (values) array
    (vector-push-extend (coerce x 'double-float) values)
    (call-next-method)
    array))

(defmethod arrow-value ((array float64-array) (n fixnum))
  (aref (slot-value array 'values) n))

(defstruct (boolean-array (:include primitive-array
                           (data-type "b")
                           (values (make-array 0 :element-type 'bit :fill-pointer 0) :type (vector bit)))))

(defmethod arrow-push ((array boolean-array) x)
  (with-slots (values) array
    (vector-push-extend (if x 1 0) values)
    (call-next-method)
    array))

(defmethod arrow-value ((array boolean-array) (n fixnum))
  (= 1 (aref (slot-value array 'values) n)))

(defstruct (binary-array (:include arrow-array
                          (data-type "z" :type string :read-only t)))
  (validity (make-array 0 :element-type 'bit :fill-pointer 0) :type bit-vector :read-only t)
  (offsets (make-array 1 :element-type '(signed-byte 32) :fill-pointer 1) :type (vector (signed-byte 32)) :read-only t)
  (data (make-array 0 :element-type '(unsigned-byte 8) :fill-pointer 0) :type (vector (unsigned-byte 8)) :read-only t))

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

(defstruct (utf8-array (:include binary-array
                        (data-type "u"))))

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

;; (arrow-get (arrow-push (arrow-push-null (make-int64-array)) 1) 0)

;; (arrow-get (arrow-push (arrow-push-null (make-float64-array)) 1.0d0) 1)

;; (arrow-get (arrow-push (arrow-push-null (make-boolean-array)) t) 1)

;; (arrow-get (arrow-push (arrow-push (arrow-push-null (make-binary-array)) #(0 1 2))  #(3 4)) 2)

;; (arrow-get (arrow-push (arrow-push (arrow-push-null (make-utf8-array)) "hello") "world") 2)

;; (arrow-null-count (arrow-push (arrow-push (arrow-push-null (make-utf8-array)) "hello") "world"))

;; (arrow-length (arrow-push (arrow-push (arrow-push-null (make-utf8-array)) "hello") "world"))
