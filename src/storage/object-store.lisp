(defpackage :endb/storage/object-store
  (:use :cl)
  (:export #:open-tar-object-store #:object-store-get #:object-store-put #:object-store-close)
  (:import-from :archive)
  (:import-from :fast-io)
  (:import-from :trivial-gray-streams))
(in-package :endb/storage/object-store)

(defgeneric object-store-get (os path))
(defgeneric object-store-put (os path buffer))
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
         (loop for entry = (archive:read-entry-from-archive archive)
               while entry
               if (equal path (archive:name entry))
                 do (return (%extract-entry archive entry))
               else
                 do (archive:discard-entry archive entry))
      (setf (trivial-gray-streams:stream-file-position stream) pos))))

(defmethod object-store-close ((archive archive:tar-archive))
  (archive:close-archive archive))
