(defpackage :endb/storage/object-store
  (:use :cl)
  (:export #:object-store-get #:object-store-put #:object-store-delete #:object-store-list #:object-store-close
           #:open-tar-object-store #:extract-tar-into-object-store
           #:make-directory-object-store #:make-memory-object-store)
  (:import-from :alexandria)
  (:import-from :archive)
  (:import-from :endb/lib)
  (:import-from :flexi-streams)
  (:import-from :uiop)
  (:import-from :bordeaux-threads))
(in-package :endb/storage/object-store)

(defgeneric object-store-get (os path))
(defgeneric object-store-put (os path buffer-or-stream))
(defgeneric object-store-delete (os path))
(defgeneric object-store-list (os &key prefix start-after))
(defgeneric object-store-close (os))

(defvar *tar-object-store-lock* (bt:make-lock))

(defun open-tar-object-store (&key (stream (flex:make-in-memory-input-stream (make-array 0 :element-type '(unsigned-byte 8)))))
  (let ((archive (archive:open-archive 'archive:tar-archive stream)))
    (when (typep stream 'flex:vector-stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

(defun extract-tar-into-object-store (archive target-os &key skip-if)
  (let* ((stream (archive::archive-stream archive))
         (pos (file-position stream)))
    (file-position stream 0)
    (unwind-protect
         (loop for entry = (%wal-read-entry-safe archive)
               while entry
               if (and skip-if (funcall skip-if (archive:name entry)))
                 do (archive:discard-entry archive entry)
               else
                 do (endb/storage/object-store:object-store-put
                     target-os
                     (archive:name entry)
                     (%extract-entry archive entry))
               finally (return target-os))
      (file-position stream pos))))

(defun %extract-entry (archive entry)
  (flex:with-output-to-sequence (out)
    (archive::transfer-entry-data-to-stream archive entry out)))

(defun %wal-read-entry-safe (archive)
  (when (listen (archive::archive-stream archive))
    (archive:read-entry-from-archive archive)))

(defmethod object-store-get ((archive archive:tar-archive) path)
  (bt:with-lock-held (*tar-object-store-lock*)
    (let* ((stream (archive::archive-stream archive))
           (pos (file-position stream)))
      (file-position stream 0)
      (unwind-protect
           (loop for entry = (%wal-read-entry-safe archive)
                 while entry
                 if (equal path (archive:name entry))
                   do (return (%extract-entry archive entry))
                 else
                   do (archive:discard-entry archive entry))
        (file-position stream pos)))))

(defmethod object-store-put ((archive archive:tar-archive) path buffer))

(defmethod object-store-delete ((archive archive:tar-archive) path))

(defun %object-store-list-filter (files prefix start-after)
  (loop for f in (sort files #'string<)
        when (and (string> f start-after) (alexandria:starts-with-subseq prefix f))
          collect f))

(defmethod object-store-list ((archive archive:tar-archive) &key (prefix "") (start-after ""))
  (bt:with-lock-held (*tar-object-store-lock*)
    (let* ((stream (archive::archive-stream archive))
           (pos (file-position stream)))
      (file-position stream 0)
      (unwind-protect
           (%object-store-list-filter
            (loop for entry = (%wal-read-entry-safe archive)
                  while entry
                  do (archive:discard-entry archive entry)
                  collect (archive:name entry))
            prefix
            start-after)
        (file-position stream pos)))))

(defmethod object-store-close ((archive archive:tar-archive))
  (archive:close-archive archive))

(defstruct directory-object-store path)

(defmethod object-store-get ((os directory-object-store) path)
  (endb/lib:with-trace-kvs-span "object_store_get" (fset:map ("path" path))
   (let ((path (merge-pathnames path (uiop:ensure-directory-pathname (directory-object-store-path os)))))
     (when (probe-file path)
       (let ((buffer (alexandria:read-file-into-byte-vector path)))
         (endb/lib:metric-monotonic-counter "object_store_bytes_read" (length buffer))
         buffer)))))

(defmethod object-store-put ((os directory-object-store) path (buffer vector))
  (flex:with-input-from-sequence (in buffer)
    (object-store-put os path in)))

(defmethod object-store-put ((os directory-object-store) path (in stream))
  (endb/lib:with-trace-kvs-span "object_store_put" (fset:map ("path" path))
   (let* ((os-path (uiop:ensure-directory-pathname (directory-object-store-path os)))
          (write-path (merge-pathnames (endb/lib:uuid-v4) os-path)))
     (ensure-directories-exist (merge-pathnames path os-path))
     (let ((bytes-written (with-open-file (out write-path :direction :output :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create)
                            (alexandria:copy-stream in out :element-type '(unsigned-byte 8)))))
       (rename-file write-path (uiop:enough-pathname path (truename os-path)))
       (endb/lib:metric-monotonic-counter "object_store_bytes_written" bytes-written)))))

(defmethod object-store-delete ((os directory-object-store) path)
  (endb/lib:with-trace-kvs-span "object_store_delete" (fset:map ("path" path))
   (let ((path (merge-pathnames path (uiop:ensure-directory-pathname (directory-object-store-path os)))))
     (uiop:delete-file-if-exists path))))

(defmethod object-store-list ((os directory-object-store) &key (prefix "") (start-after ""))
  (endb/lib:with-trace-kvs-span "object_store_list" (fset:map ("prefix" prefix))
   (let* ((path (uiop:ensure-directory-pathname (directory-object-store-path os))))
     (when (uiop:directory-exists-p path)
       (%object-store-list-filter (loop for p in (directory (merge-pathnames "**/*.*" path))
                                        unless (uiop:directory-pathname-p p)
                                          collect (namestring (uiop:enough-pathname p (truename path))))
                                  prefix start-after)))))

(defmethod object-store-close ((os directory-object-store)))

(defun make-memory-object-store ()
  (make-hash-table :synchronized t :test 'equal))

(defmethod object-store-get ((os hash-table) path)
  (gethash path os))

(defmethod object-store-put ((os hash-table) path (buffer vector))
  (setf (gethash path os) buffer))

(defmethod object-store-put ((os hash-table) path (in stream))
  (object-store-put os path (alexandria:read-stream-content-into-byte-vector in)))

(defmethod object-store-delete ((os hash-table) path)
  (remhash path os))

(defmethod object-store-list  ((os hash-table) &key (prefix "") (start-after ""))
  (#+sbcl sb-ext:with-locked-hash-table
   #+sbcl (os)
   #-sbcl progn
    (%object-store-list-filter (alexandria:hash-table-keys os) prefix start-after)))

(defmethod object-store-close ((os hash-table))
  (clrhash os))
