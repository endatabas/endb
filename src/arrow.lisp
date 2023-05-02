(defpackage :endb/arrow
  (:use :cl)
  (:export #:to-arrow #:make-arrow-array-for #:arrow-class-for-format
           #:arrow-push #:arrow-valid-p #:arrow-get #:arrow-value
           #:arrow-length #:arrow-null-count #:arrow-data-type #:arrow-lisp-type
           #:arrow-children #:arrow-buffers
           #:arrow-struct-children #:arrow-struct-row-get #:arrow-struct-row-push
           #:arrow-array #:validity-array #:null-array #:int32-array #:int64-array #:float64-array
           #:date-days-array #:timestamp-micros-array #:binary-array #:utf8-array #:list-array #:struct-array #:dense-union-array)
  (:import-from :cl-ppcre)
  (:import-from :local-time)
  (:import-from :trivial-utf-8))
(in-package :endb/arrow)

(deftype uint8 () '(unsigned-byte 8))
(deftype int8 () '(signed-byte 8))
(deftype int32 () '(signed-byte 32))
(deftype int64 () '(signed-byte 64))
(deftype float64 () 'double-float)

(deftype arrow-binary ()
  '(vector uint8))

(deftype arrow-list ()
  '(and vector (not (or string arrow-binary))))

(defun %alistp (x)
  (and (listp x)
       (every #'consp x)))

(deftype alist () '(satisfies %alistp))

(defun %arrow-struct-p (x)
  (or (and (%alistp x)
           (plusp (length x)))
      (eq :empty-struct x)))

(deftype arrow-struct () '(satisfies %arrow-struct-p))

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
    ((and number (not int64)) 'float64-array)
    (local-time:date 'date-days-array)
    (local-time:timestamp 'timestamp-micros-array)
    (arrow-struct 'struct-array)
    (arrow-binary 'binary-array)
    (string 'utf8-array)
    (arrow-list 'list-array)))

(defun make-arrow-array-for (x)
  (let ((c (%array-class-for x)))
    (if (eq 'struct-array c)
        (make-instance c :children (unless (equal :empty-struct x)
                                     (loop for (k . v) in x
                                           collect (cons k (make-arrow-array-for v)))))
        (make-instance c))))

(defun arrow-class-for-format (format)
  (cond
    ((equal "n" format) 'null-array)
    ((equal "b" format) 'boolean-array)
    ((equal "i" format) 'int32-array)
    ((equal "l" format) 'int64-array)
    ((equal "g" format) 'float64-array)
    ((equal "tsu:" format) 'timestamp-micros-array)
    ((equal "tdD" format) 'date-days-array)
    ((equal "z" format) 'binary-array)
    ((equal "u" format) 'utf8-array)
    ((equal "+s" format) 'struct-array)
    ((equal "+l" format) 'list-array)
    (t (if (cl-ppcre:scan "^\\+ud:" format)
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
  (with-slots (values) array
    (%push-valid array)
    (vector-push-extend x values)
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

(defmethod arrow-lisp-type ((array timestamp-micros-array))
  'local-time:timestamp)

(defclass date-days-array (int32-array) ())

(defconstant +offset-from-epoch-day+ 11017)

(defmethod arrow-push ((array date-days-array) (x local-time:timestamp))
  (check-type x local-time:date)
  (arrow-push array (+ (local-time:day-of x) +offset-from-epoch-day+)))

(defmethod arrow-value ((array date-days-array) (n fixnum))
  (local-time:make-timestamp :day (- (aref (slot-value array 'values) n)  +offset-from-epoch-day+)))

(defmethod arrow-data-type ((array date-days-array))
  "tdD")

(defmethod arrow-lisp-type ((array date-days-array))
  'local-time:date)

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

(defmethod arrow-push ((array list-array) (x vector))
  (with-slots (offsets values) array
    (%push-valid array)
    (loop for y across x
          do (setf values (arrow-push values y)))
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
           (end (aref offsets (1+ n)))
           (len (- end start)))
      (if (and (typep values 'primitive-array)
               (zerop (arrow-null-count values)))
          (make-array len :element-type (slot-value values 'element-type)
                          :displaced-to (slot-value values 'values)
                          :displaced-index-offset start)
          (loop with acc = (make-array len)
                for src-idx from start below end
                for dst-idx from 0
                do (setf (aref acc dst-idx) (arrow-get values src-idx))
                finally (return acc))))))

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
    (if (eq :empty-struct x)
        (null children)
        (equal (mapcar #'car x)
               (mapcar #'car children)))))

(defclass struct-array (validity-array)
  ((children :initarg :children :initform () :type alist)))

(defmethod arrow-push ((array struct-array) (x list))
  (if (%same-struct-fields-p array x)
      (with-slots (children) array
        (%push-valid array)
        (loop for kv-pair in children
              for (k . v) in x
              do (setf (cdr kv-pair) (arrow-push (cdr kv-pair) v)))
        array)
      (call-next-method)))

(defmethod arrow-push ((array struct-array) (x (eql :null)))
  (with-slots (children) array
    (%push-invalid array)
    (loop for kv-pair in children
          do (setf (cdr kv-pair) (arrow-push (cdr kv-pair) :null)))
    array))

(defmethod arrow-push ((array struct-array) (x (eql :empty-struct)))
  (with-slots (children) array
    (if (null children)
        (progn
          (%ensure-validity-buffer array)
          (%push-valid array)
          array)
        (call-next-method))))

(defmethod arrow-value ((array struct-array) (n fixnum))
  (with-slots (children) array
    (or (loop for (k . v) in children
              collect (cons k (arrow-get v n)))
        :empty-struct)))

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
          with children = (reverse children)
          for c in projection
          collect (or (cdr (assoc c children :test 'equal)) missing-column))))

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
