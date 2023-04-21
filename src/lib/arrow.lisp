(defpackage :endb/lib/arrow
  (:use :cl)
  (:export #:write-arrow-arrays-to-ipc-buffer
           #:read-arrow-arrays-from-ipc-pointer #:read-arrow-arrays-from-ipc-buffer
           #:buffer-to-vector)
  (:import-from :endb/arrow)
  (:import-from :endb/lib)
  (:import-from :cffi))
(in-package :endb/lib/arrow)

(cffi:defbitfield arrow-flags
  (:dictionary-encoded 1)
  (:nullable 2)
  (:map-keys-sorted 4))

(cffi:defcstruct ArrowSchema
  (format :pointer)
  (name :pointer)
  (metadata :pointer)
  (flags arrow-flags)
  (n_children :int64)
  (children (:pointer (:pointer (:struct ArrowSchema))))
  (dictionary (:pointer (:struct ArrowSchema)))
  (release :pointer)
  (private_data :pointer))

(defvar *arrow-schema-release*
  (lambda (c-schema)
    (cffi:with-foreign-slots ((format name n_children children release) c-schema (:struct ArrowSchema))
      (unless (cffi:null-pointer-p release)
        (cffi:foreign-free format)
        (cffi:foreign-free name)
        (unless (cffi:null-pointer-p children)
          (dotimes (n n_children)
            (let ((child-ptr (cffi:mem-aref children :pointer n)))
              (funcall *arrow-schema-release* child-ptr)
              (cffi:foreign-free child-ptr)))
          (cffi:foreign-free children))
        (setf release (cffi:null-pointer))))))

(cffi:defcallback arrow-schema-release :void
    ((schema (:pointer (:struct ArrowSchema))))
  (funcall *arrow-schema-release* schema))

(cffi:defcstruct ArrowArray
  (length :int64)
  (null_count :int64)
  (offset :int64)
  (n_buffers :int64)
  (n_children :int64)
  (buffers (:pointer (:pointer :void)))
  (children (:pointer (:pointer (:struct ArrowArray))))
  (dictionary (:pointer (:struct ArrowArray)))
  (release :pointer)
  (private_data :pointer))

(defvar *arrow-array-release*
  (lambda (c-array)
    (cffi:with-foreign-slots ((buffers n_children children release) c-array (:struct ArrowArray))
      (unless (cffi:null-pointer-p release)
        (unless (cffi:null-pointer-p buffers)
          (cffi:foreign-free buffers))
        (unless (cffi:null-pointer-p children)
          (dotimes (n n_children)
            (let ((child-ptr (cffi:mem-aref children :pointer n)))
              (funcall *arrow-array-release* child-ptr)
              (cffi:foreign-free child-ptr)))
          (cffi:foreign-free children))
        (setf release (cffi:null-pointer))))))

(cffi:defcallback arrow-array-release :void
    ((array (:pointer (:struct ArrowArray))))
  (funcall *arrow-array-release* array))

(cffi:defcstruct ArrowArrayStream
  (get_schema :pointer)
  (get_next :pointer)
  (get_last_error :pointer)
  (release :pointer)
  (private_data :pointer))

(defvar *arrow-array-stream-get-schema*)

(cffi:defcallback arrow-array-stream-get-schema :int
    ((stream (:pointer (:struct ArrowArrayStream)))
     (schema (:pointer (:struct ArrowSchema))))
  (funcall *arrow-array-stream-get-schema* stream schema))

(defvar *arrow-array-stream-get-next*)

(cffi:defcallback arrow-array-stream-get-next :int
    ((stream (:pointer (:struct ArrowArrayStream)))
     (array (:pointer (:struct ArrowArray))))
  (funcall *arrow-array-stream-get-next* stream array))

(defvar *arrow-array-stream-get-last-error*)

(cffi:defcallback arrow-array-stream-get-last-error :string
    ((stream (:pointer (:struct ArrowArrayStream))))
  (funcall *arrow-array-stream-get-last-error* stream))

(defvar *arrow-array-stream-release*
  (lambda (c-stream)
    (cffi:with-foreign-slots ((release) c-stream (:struct ArrowArrayStream))
      (unless (cffi:null-pointer-p release)
        (setf release (cffi:null-pointer))))))

(cffi:defcallback arrow-array-stream-release :void
    ((stream (:pointer (:struct ArrowArrayStream))))
  (funcall *arrow-array-stream-release* stream))

(cffi:defcallback arrow-array-stream-producer-on-error :void
    ((err :string))
  (error err))

(cffi:defcfun "endb_arrow_array_stream_producer" :void
  (stream (:pointer (:struct ArrowArrayStream)))
  (buffer-ptr :pointer)
  (buffer-size :size)
  (on-error :pointer))

(defvar *arrow-array-stream-consumer-init-stream*
  (lambda (c-stream)
    (cffi:with-foreign-slots ((get_schema get_next get_last_error release) c-stream (:struct ArrowArrayStream))
      (setf get_schema (cffi:callback arrow-array-stream-get-schema))
      (setf get_next (cffi:callback arrow-array-stream-get-next))
      (setf get_last_error (cffi:callback arrow-array-stream-get-last-error))
      (setf release (cffi:callback arrow-array-stream-release)))))

(cffi:defcallback arrow-array-stream-consumer-init-stream :void
    ((stream (:pointer (:struct ArrowArrayStream))))
  (funcall *arrow-array-stream-consumer-init-stream* stream))

(defvar *arrow-array-stream-consumer-on-success*)

(cffi:defcallback arrow-array-stream-consumer-on-success :void
    ((buffer-ptr :pointer)
     (buffer-size :size))
  (funcall *arrow-array-stream-consumer-on-success* buffer-ptr buffer-size))

(cffi:defcallback arrow-array-stream-consumer-on-error :void
    ((err :string))
  (error err))

(cffi:defcfun "endb_arrow_array_stream_consumer" :void
  (init-stream :pointer)
  (on-success :pointer)
  (on-error :pointer))

(defstruct arrow-schema format name children)

(defun arrow-array-to-schema (field-name array)
  (make-arrow-schema :format (endb/arrow:arrow-data-type array)
                     :name field-name
                     :children (loop for (k . v) in (endb/arrow:arrow-children array)
                                     collect (arrow-array-to-schema k v))))

(defun import-arrow-schema (c-schema)
  (cffi:with-foreign-slots ((format name flags n_children children) c-schema (:struct ArrowSchema))
    (make-arrow-schema :format (cffi:foreign-string-to-lisp format)
                       :name (cffi:foreign-string-to-lisp name)
                       :children (loop for n below n_children
                                       collect (import-arrow-schema (cffi:mem-aref children :pointer n))))))

(defun export-arrow-schema (schema c-schema)
  (let ((ptrs))
    (labels ((track-alloc (ptr)
               (push ptr ptrs)
               ptr))
      (handler-case
          (cffi:with-foreign-slots ((format name metadata flags n_children children dictionary release) c-schema (:struct ArrowSchema))
            (setf format (track-alloc (cffi:foreign-string-alloc (arrow-schema-format schema))))
            (setf name (track-alloc (cffi:foreign-string-alloc (arrow-schema-name schema))))
            (setf metadata (cffi:null-pointer))
            (setf flags '(:nullable))

            (let ((schemas (arrow-schema-children schema)))
              (setf n_children (length schemas))
              (if schemas
                  (let ((children-ptr (track-alloc (cffi:foreign-alloc :pointer :count (length schemas)))))
                    (loop for s in schemas
                          for n from 0
                          for schema-ptr = (track-alloc (cffi:foreign-alloc '(:struct ArrowSchema)))
                          do (export-arrow-schema s schema-ptr)
                             (setf (cffi:mem-aref children-ptr :pointer n) schema-ptr))
                    (setf children children-ptr))
                  (setf children (cffi:null-pointer))))

            (setf dictionary (cffi:null-pointer))
            (setf release (cffi:callback arrow-schema-release)))
        (error (e)
          (dolist (ptr ptrs)
            (cffi:foreign-free ptr))
          (error e))))))

(defun export-arrow-array (array c-array)
  (let ((ptrs))
    (labels ((track-alloc (ptr)
               (push ptr ptrs)
               ptr))
      (handler-case
          (cffi:with-foreign-slots ((length null_count offset n_buffers n_children buffers children dictionary release) c-array (:struct ArrowArray))
            (setf length (endb/arrow:arrow-length array))
            (setf null_count (endb/arrow:arrow-null-count array))
            (setf offset 0)

            (let ((buffers-list (endb/arrow:arrow-buffers array)))
              (setf n_buffers (length buffers-list))
              (if buffers-list
                  (let ((buffers-ptr (track-alloc (cffi:foreign-alloc :pointer :count (length buffers-list)))))
                    (loop for b in buffers-list
                          for n from 0
                          do (if b
                                 (cffi:with-pointer-to-vector-data (ptr #+sbcl (sb-ext:array-storage-vector b)
                                                                        #-sbcl b)
                                   (setf (cffi:mem-aref buffers-ptr :pointer n) ptr))
                                 (setf (cffi:mem-aref buffers-ptr :pointer n) (cffi:null-pointer))))
                    (setf buffers buffers-ptr))
                  (setf buffers (cffi:null-pointer))))

            (let ((children-alist (endb/arrow:arrow-children array)))
              (setf n_children (length children-alist))
              (if children-alist
                  (let ((children-ptr (track-alloc (cffi:foreign-alloc :pointer :count (length children-alist)))))
                    (loop for (nil . c) in children-alist
                          for n from 0
                          for array-ptr = (track-alloc (cffi:foreign-alloc '(:struct ArrowArray)))
                          do (export-arrow-array c array-ptr)
                             (setf (cffi:mem-aref children-ptr :pointer n) array-ptr)
                             (setf children children-ptr)))
                  (setf children (cffi:null-pointer))))

            (setf dictionary (cffi:null-pointer))
            (setf release (cffi:callback arrow-array-release)))
        (error (e)
          (dolist (ptr ptrs)
            (cffi:foreign-free ptr))
          (error e))))))

(cffi:defcfun "memcpy" :pointer
  (dest :pointer)
  (src :pointer)
  (n :size))

(defun buffer-to-vector (buffer-ptr buffer-size &optional out)
  (let ((out (or out (make-array buffer-size :element-type '(unsigned-byte 8)))))
    (cffi:with-pointer-to-vector-data (out-ptr out)
      (memcpy out-ptr buffer-ptr buffer-size))
    out))

(defun write-arrow-arrays-to-ipc-buffer (arrays on-success)
  (endb/lib:init-lib)
  (let* ((last-error (cffi:null-pointer))
         (schemas (remove-duplicates (loop for a in arrays
                                           collect (arrow-array-to-schema "" a))
                                     :test 'equalp))
         (schema (first schemas))
         (result))
    (assert (= 1 (length schemas)))
    (unwind-protect
         (#+sbcl sb-sys:with-pinned-objects
          #+sbcl (arrays)
          #-sbcl progn
          (let* ((*arrow-array-stream-get-schema* (lambda (c-stream c-schema)
                                                    (declare (ignore c-stream))
                                                    (handler-case
                                                        (progn
                                                          (export-arrow-schema schema c-schema)
                                                          0)
                                                      (error (e)
                                                        (unless (cffi:null-pointer-p last-error)
                                                          (cffi:foreign-free last-error))
                                                        (setf last-error (cffi:foreign-string-alloc (princ-to-string e)))
                                                        1))
                                                    0))
                 (*arrow-array-stream-get-next* (lambda (c-stream c-array)
                                                  (declare (ignore c-stream))
                                                  (if arrays
                                                      (handler-case
                                                          (progn
                                                            (export-arrow-array (pop arrays) c-array)
                                                            0)
                                                        (error (e)
                                                          (unless (cffi:null-pointer-p last-error)
                                                            (cffi:foreign-free last-error))
                                                          (setf last-error (cffi:foreign-string-alloc (princ-to-string e)))
                                                          1))
                                                      (cffi:with-foreign-slots ((release) c-array (:struct ArrowArray))
                                                        (setf release (cffi:null-pointer))
                                                        0))))
                 (*arrow-array-stream-get-last-error* (lambda (c-stream)
                                                        (declare (ignore c-stream))
                                                        last-error))
                 (*arrow-array-stream-consumer-on-success* (lambda (buffer-ptr buffer-size)
                                                             (setf result (funcall on-success buffer-ptr buffer-size)))))
            (endb-arrow-array-stream-consumer (cffi:callback arrow-array-stream-consumer-init-stream)
                                              (cffi:callback arrow-array-stream-consumer-on-success)
                                              (cffi:callback arrow-array-stream-consumer-on-error))
            result))
      (unless (cffi:null-pointer-p last-error)
        (cffi:foreign-free last-error)))))

(defun vector-byte-size (b length)
  (etypecase b
    ((vector bit) (truncate (+ 7 length) 8))
    ((vector (unsigned-byte 8)) length)
    ((vector (signed-byte 8)) length)
    ((vector (signed-byte 32)) (* 4 length))
    ((vector (signed-byte 64)) (* 8 length))
    ((vector double-float) (* 8 length))))

(defun import-arrow-array (schema c-array)
  (cffi:with-foreign-slots ((length null_count n_buffers buffers n_children children) c-array (:struct ArrowArray))
    (let* ((format (arrow-schema-format schema))
           (array-class (endb/arrow:arrow-class-for-format format))
           (array-children (loop for n below n_children
                                 for schema in (arrow-schema-children schema)
                                 collect (cons (arrow-schema-name schema)
                                               (import-arrow-array schema (cffi:mem-aref children :pointer n)))))
           (array (apply #'make-instance array-class
                         (append (list :length length
                                       :null-count null_count)
                                 (when array-children
                                   (list :children array-children))))))
      (unless (cffi:null-pointer-p buffers)
        (loop with buffers-list = (endb/arrow:arrow-buffers array)
              for n below n_buffers
              for b in buffers-list
              for src-ptr = (cffi:mem-aref buffers :pointer n)
              when (not (cffi:null-pointer-p src-ptr))
                do (let ((b (if (and (null b)
                                     (typep array 'endb/arrow::binary-array)
                                     (= 2 n))
                                (let* ((offsets (nth 1 buffers-list))
                                       (data (make-array (aref offsets length) :element-type '(unsigned-byte 8))))
                                  (setf (slot-value array 'endb/arrow::data) data))
                                b)))
                     (buffer-to-vector src-ptr (vector-byte-size b (length b)) b))))
      array)))

(defun %call-release (release c-obj)
  (unless (cffi:null-pointer-p release)
    (cffi:foreign-funcall-pointer release () :pointer c-obj :void)))

(defun read-arrow-arrays-from-ipc-pointer (buffer-ptr buffer-size)
  (endb/lib:init-lib)
  (cffi:with-foreign-objects ((c-stream '(:struct ArrowArrayStream))
                              (c-schema '(:struct ArrowSchema))
                              (c-array '(:struct ArrowArray)))
    (endb-arrow-array-stream-producer
     c-stream
     buffer-ptr
     buffer-size
     (cffi:callback arrow-array-stream-producer-on-error))
    (cffi:with-foreign-slots ((get_schema get_next get_last_error release) c-stream (:struct ArrowArrayStream))
      (unwind-protect
           (let ((result (cffi:foreign-funcall-pointer get_schema () :pointer c-stream :pointer c-schema :int)))
             (if (zerop result)
                 (cffi:with-foreign-slots ((release) c-schema (:struct ArrowSchema))
                   (unwind-protect
                        (loop with acc = ()
                              with schema = (import-arrow-schema c-schema)
                              for result = (cffi:foreign-funcall-pointer get_next () :pointer c-stream :pointer c-array :int)
                              do (cffi:with-foreign-slots ((release) c-array (:struct ArrowArray))
                                   (unwind-protect
                                        (if (zerop result)
                                            (if (cffi:null-pointer-p release)
                                                (return acc)
                                                (setf acc (append acc (list (import-arrow-array schema c-array)))))
                                            (error (cffi:foreign-funcall-pointer get_last_error () :pointer c-stream :string)))
                                     (%call-release release c-array))))
                     (%call-release release c-schema)))
                 (error (cffi:foreign-funcall-pointer get_last_error () :pointer c-stream :string))))
        (%call-release release c-stream)))))

(defun read-arrow-arrays-from-ipc-buffer (buffer)
  (check-type buffer (vector (unsigned-byte 8)))
  (cffi:with-pointer-to-vector-data (buffer-ptr buffer)
    (read-arrow-arrays-from-ipc-pointer buffer-ptr (length buffer))))
