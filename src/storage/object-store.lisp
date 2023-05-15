(defpackage :endb/storage/object-store
  (:use :cl)
  (:export #:open-tar-object-store #:object-store-get #:object-store-put #:object-store-list #:object-store-close #:make-directory-object-store)
  (:import-from :alexandria)
  (:import-from :archive)
  (:import-from :fast-io)
  (:import-from :uiop))
(in-package :endb/storage/object-store)

(defgeneric object-store-get (os path))
(defgeneric object-store-put (os path buffer))
(defgeneric object-store-list (os &key prefix start-after))
(defgeneric object-store-close (os))

(defun open-tar-object-store (&key (stream (make-instance 'fast-io:fast-input-stream)))
  (let ((archive (archive:open-archive 'archive:tar-archive stream)))
    (when (typep stream 'fast-io:fast-input-stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

(defun %extract-entry (archive entry)
  (let* ((out (make-instance 'fast-io:fast-output-stream :buffer-size (archive::size entry))))
    (archive::transfer-entry-data-to-stream archive entry out)
    (fast-io:finish-output-stream out)))

(defmethod object-store-get ((archive archive:tar-archive) path)
  (let* ((stream (archive::archive-stream archive))
         (pos (file-position stream)))
    (unwind-protect
         (handler-case
             (loop for entry = (archive:read-entry-from-archive archive)
                   while entry
                   if (equal path (archive:name entry))
                     do (return (%extract-entry archive entry))
                   else
                     do (archive:discard-entry archive entry))
           (error (e)
             (unless (zerop (file-position stream))
               (error e))))
      (file-position stream pos))))

(defun %object-store-list-filter (files prefix start-after)
  (let ((prefix-scanner (ppcre:create-scanner (format nil "^~A" prefix))))
    (loop for f in (sort files #'string<)
          when (and (string> f start-after) (ppcre:scan prefix-scanner f))
            collect f)))

(defmethod object-store-list ((archive archive:tar-archive) &key (prefix "") (start-after ""))
  (let* ((stream (archive::archive-stream archive))
         (pos (file-position stream)))
    (unwind-protect
         (handler-case
             (%object-store-list-filter
              (loop for entry = (archive:read-entry-from-archive archive)
                    while entry
                    do (archive:discard-entry archive entry)
                    collect (archive:name entry))
              prefix
              start-after)
           (error (e)
             (unless (zerop (file-position stream))
               (error e))))
      (file-position stream pos))))

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
