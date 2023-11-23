(defpackage :endb/storage/object-store
  (:use :cl)
  (:export #:object-store-get #:object-store-put #:object-store-list #:object-store-close
           #:open-tar-object-store #:make-directory-object-store #:make-layered-object-store #:make-memory-object-store)
  (:import-from :alexandria)
  (:import-from :archive)
  (:import-from :flexi-streams)
  (:import-from :uiop)
  (:import-from :bordeaux-threads))
(in-package :endb/storage/object-store)

(defgeneric object-store-get (os path))
(defgeneric object-store-put (os path buffer))
(defgeneric object-store-list (os &key prefix start-after))
(defgeneric object-store-close (os))

(defvar *tar-object-store-lock* (bt:make-lock))

(defun open-tar-object-store (&key (stream (flex:make-in-memory-input-stream (make-array 0 :element-type '(unsigned-byte 8)))))
  (let ((archive (archive:open-archive 'archive:tar-archive stream)))
    (when (typep stream 'flex:vector-stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

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

(defun %object-store-list-filter (files prefix start-after)
  (let ((prefix-scanner (ppcre:create-scanner (format nil "^~A" prefix))))
    (loop for f in (sort files #'string<)
          when (and (string> f start-after) (ppcre:scan prefix-scanner f))
            collect f)))

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
  (let ((path (merge-pathnames path (uiop:ensure-directory-pathname (directory-object-store-path os)))))
    (when (probe-file path)
      (alexandria:read-file-into-byte-vector path))))

(defmethod object-store-put ((os directory-object-store) path buffer)
  (let ((path (merge-pathnames path (uiop:ensure-directory-pathname (directory-object-store-path os)))))
    (ensure-directories-exist path)
    (alexandria:write-byte-vector-into-file buffer path)))

(defmethod object-store-list ((os directory-object-store) &key (prefix "") (start-after ""))
  (let* ((path (uiop:ensure-directory-pathname (directory-object-store-path os))))
    (%object-store-list-filter (loop for p in (directory (merge-pathnames "**/*.*" path))
                                     unless (uiop:directory-pathname-p p)
                                       collect (namestring (uiop:enough-pathname p path)))
                               prefix start-after)))

(defmethod object-store-close ((os directory-object-store)))

(defstruct layered-object-store overlay-object-store underlying-object-store)

(defmethod object-store-get ((os layered-object-store) path)
  (or (object-store-get (layered-object-store-overlay-object-store os) path)
      (object-store-get (layered-object-store-underlying-object-store os) path)))

(defmethod object-store-put ((os layered-object-store) path buffer)
  (object-store-put (layered-object-store-underlying-object-store os) path buffer))

(defmethod object-store-list ((os layered-object-store) &key (prefix "") (start-after ""))
  (let ((files (remove-duplicates
                (append (object-store-list (layered-object-store-overlay-object-store os) :prefix prefix :start-after start-after)
                        (object-store-list (layered-object-store-underlying-object-store os) :prefix prefix :start-after start-after))
                :test 'equal)))
    (sort files #'string<)))

(defmethod object-store-close ((os layered-object-store))
  (object-store-close (layered-object-store-overlay-object-store os))
  (object-store-close (layered-object-store-underlying-object-store os)))

(defun make-memory-object-store ()
  (make-hash-table :synchronized t :test 'equal))

(defmethod object-store-get ((os hash-table) path)
  (gethash path os))

(defmethod object-store-put ((os hash-table) path buffer)
  (setf (gethash path os) buffer))

(defmethod object-store-list  ((os hash-table) &key (prefix "") (start-after ""))
  (%object-store-list-filter (alexandria:hash-table-keys os) prefix start-after))

(defmethod object-store-close ((os hash-table))
  (clrhash os))
