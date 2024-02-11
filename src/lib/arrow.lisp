(defpackage :endb/lib/arrow
  (:use :cl)
  (:export #:write-arrow-arrays-to-ipc-buffer #:write-arrow-arrays-to-ipc-file #:make-arrow-schema
           #:read-arrow-arrays-from-ipc-pointer #:read-arrow-arrays-from-ipc-buffer #:read-arrow-arrays-from-ipc-file)
  (:import-from :alexandria)
  (:import-from :endb/arrow)
  (:import-from :endb/lib)
  (:import-from :cffi))
(in-package :endb/lib/arrow)

(cffi:defbitfield (arrow-flags :int64)
  (:dictionary-encoded 1)
  (:nullable 2)
  (:map-keys-sorted 4))

(cffi:defcstruct ArrowSchema
  (format :pointer)
  (name :pointer)
  (metadata :pointer)
  (flags arrow-flags #+wasm32 :offset #+wasm32 16)
  (n_children :int64)
  (children (:pointer (:pointer (:struct ArrowSchema))))
  (dictionary (:pointer (:struct ArrowSchema)))
  (release :pointer)
  (private_data :pointer))

(cffi:defcallback arrow-schema-release :void
    ((schema (:pointer (:struct ArrowSchema))))
  (cffi:with-foreign-slots ((format name n_children children release) schema (:struct ArrowSchema))
    (unless (cffi:null-pointer-p release)
      (cffi:foreign-free format)
      (cffi:foreign-free name)
      (unless (cffi:null-pointer-p children)
        (dotimes (n n_children)
          (let ((child-ptr (cffi:mem-aref children :pointer n)))
            (cffi:foreign-funcall-pointer release () :pointer child-ptr :void)
            (cffi:foreign-free child-ptr)))
        (cffi:foreign-free children))
      (setf release (cffi:null-pointer)))))

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

(cffi:defcallback arrow-array-release :void
    ((array (:pointer (:struct ArrowArray))))
  (cffi:with-foreign-slots ((buffers n_children children release) array (:struct ArrowArray))
    (unless (cffi:null-pointer-p release)
      (unless (cffi:null-pointer-p buffers)
        (cffi:foreign-free buffers))
      (unless (cffi:null-pointer-p children)
        (dotimes (n n_children)
          (let ((child-ptr (cffi:mem-aref children :pointer n)))
            (cffi:foreign-funcall-pointer release () :pointer child-ptr :void)
            (cffi:foreign-free child-ptr)))
        (cffi:foreign-free children))
      (setf release (cffi:null-pointer)))))

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

(cffi:defcallback arrow-array-stream-release :void
    ((stream (:pointer (:struct ArrowArrayStream))))
  (cffi:with-foreign-slots ((release) stream (:struct ArrowArrayStream))
    (unless (cffi:null-pointer-p release)
      (setf release (cffi:null-pointer)))))

(defvar *arrow-array-stream-producer-on-error*)

(cffi:defcallback arrow-array-stream-producer-on-error :void
    ((err :string))
  (funcall *arrow-array-stream-producer-on-error* err))

(cffi:defcfun "endb_arrow_array_stream_producer" :void
  (stream (:pointer (:struct ArrowArrayStream)))
  (buffer-ptr :pointer)
  (buffer-size #-wasm32 :size #+wasm32 :uint32)
  (on-error :pointer))

(defvar *arrow-array-stream-consumer-on-init-stream*)

(cffi:defcallback arrow-array-stream-consumer-on-init-stream :void
    ((stream (:pointer (:struct ArrowArrayStream))))
  (cffi:with-foreign-slots ((get_schema get_next get_last_error release) stream (:struct ArrowArrayStream))
    (setf get_schema (cffi:callback arrow-array-stream-get-schema))
    (setf get_next (cffi:callback arrow-array-stream-get-next))
    (setf get_last_error (cffi:callback arrow-array-stream-get-last-error))
    (setf release (cffi:callback arrow-array-stream-release))))

(defvar *arrow-array-stream-consumer-on-success*)

(cffi:defcallback arrow-array-stream-consumer-on-success :void
    ((buffer-ptr :pointer)
     (buffer-size #-wasm32 :size #+wasm32 :uint32))
  (funcall *arrow-array-stream-consumer-on-success* buffer-ptr buffer-size))

(defvar *arrow-array-stream-consumer-on-error*)

(cffi:defcallback arrow-array-stream-consumer-on-error :void
    ((err :string))
  (funcall *arrow-array-stream-consumer-on-error* err))

(cffi:defcfun "endb_arrow_array_stream_consumer" :void
  (on-init-stream :pointer)
  (on-success :pointer)
  (on-error :pointer)
  (ipc-stream :char))

(defstruct arrow-schema format name children)

(defun arrow-array-to-schema (field-name array &optional projection)
  (make-arrow-schema :format (endb/arrow:arrow-data-type array)
                     :name field-name
                     :children (if projection
                                   (loop for k in projection
                                         for (nil . v) in (endb/arrow:arrow-children array)
                                         collect (arrow-array-to-schema k v))
                                   (loop for (k . v) in (endb/arrow:arrow-children array)
                                         collect (arrow-array-to-schema k v)))))

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

(defun write-arrow-arrays-to-ipc-buffer (arrays &key (on-success #'endb/lib:buffer-to-vector) ipc-stream-p projection)
  (endb/lib:init-lib)
  (let* ((last-error (cffi:null-pointer))
         (schemas (remove-duplicates (loop for a in arrays
                                           collect (arrow-array-to-schema "" a projection))
                                     :test 'equalp))
         (schema (first schemas))
         (result)
         (err))
    (assert (= 1 (length schemas)))
    (unwind-protect
         (#+sbcl sb-sys:with-pinned-objects
          #+sbcl ((mapcan #'endb/arrow:arrow-all-buffers arrays))
          #-sbcl progn
          #+ecl (ffi:c-inline () () :void "GC_disable()" :one-liner t)
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
                                                        1))))
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
                                                             (setf result (funcall on-success buffer-ptr buffer-size))))
                 (*arrow-array-stream-consumer-on-error* (lambda (e)
                                                           (setf err e))))
            (endb-arrow-array-stream-consumer (cffi:callback arrow-array-stream-consumer-on-init-stream)
                                              (cffi:callback arrow-array-stream-consumer-on-success)
                                              (cffi:callback arrow-array-stream-consumer-on-error)
                                              (if ipc-stream-p
                                                  1
                                                  0))
            (when err
              (error err))
            result))
      (unless (cffi:null-pointer-p last-error)
        (cffi:foreign-free last-error))
      #+ecl (ffi:c-inline () () :void "GC_enable()" :one-liner t))))

(defun write-arrow-arrays-to-ipc-file (file arrays &key ipc-stream-p)
  (alexandria:write-byte-vector-into-file
   (write-arrow-arrays-to-ipc-buffer arrays :ipc-stream-p ipc-stream-p) file :if-exists :supersede :if-does-not-exist :create)
  file)

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
                     (endb/lib:buffer-to-vector src-ptr (endb/lib:vector-byte-size b) b))))
      array)))

(defun %call-release (release c-obj)
  (unless (cffi:null-pointer-p release)
    (cffi:foreign-funcall-pointer release () :pointer c-obj :void)))

(defun read-arrow-arrays-from-ipc-pointer (buffer-ptr buffer-size)
  (endb/lib:init-lib)
  (cffi:with-foreign-objects ((c-stream '(:struct ArrowArrayStream))
                              (c-schema '(:struct ArrowSchema))
                              (c-array '(:struct ArrowArray)))
    (let* ((err)
           (*arrow-array-stream-producer-on-error* (lambda (e)
                                                     (setf err e))))
      (endb-arrow-array-stream-producer
       c-stream
       buffer-ptr
       buffer-size
       (cffi:callback arrow-array-stream-producer-on-error))
      (when err
        (error err)))
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
                                                (return (values acc schema))
                                                (setf acc (append acc (list (import-arrow-array schema c-array)))))
                                            (error (cffi:foreign-funcall-pointer get_last_error () :pointer c-stream :string)))
                                     (%call-release release c-array))))
                     (%call-release release c-schema)))
                 (error (cffi:foreign-funcall-pointer get_last_error () :pointer c-stream :string))))
        (%call-release release c-stream)))))

(defun read-arrow-arrays-from-ipc-buffer (buffer)
  (check-type buffer (vector (unsigned-byte 8)))
  (cffi:with-pointer-to-vector-data (buffer-ptr #+sbcl (sb-ext:array-storage-vector buffer)
                                                #-sbcl buffer)
    (read-arrow-arrays-from-ipc-pointer buffer-ptr (length buffer))))

(defun read-arrow-arrays-from-ipc-file (file)
  (read-arrow-arrays-from-ipc-buffer (alexandria:read-file-into-byte-vector file)))
