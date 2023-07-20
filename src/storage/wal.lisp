(defpackage :endb/storage/wal
  (:use :cl)
  (:export #:open-tar-wal #:tar-wal-position-stream-at-end #:wal-append-entry #:wal-read-next-entry #:wal-find-entry #:wal-fsync #:wal-close #:make-memory-wal)
  (:import-from :archive)
  (:import-from :fast-io)
  (:import-from :local-time))
(in-package :endb/storage/wal)

(defgeneric wal-append-entry (wal path buffer))
(defgeneric wal-read-next-entry (wal &key skip-if))
(defgeneric wal-fsync (wal))
(defgeneric wal-close (wal))

(defun open-tar-wal (&key (stream (make-instance 'fast-io:fast-output-stream)) (direction :output))
  (let ((archive (archive:open-archive 'archive:tar-archive stream :direction direction)))
    (when (input-stream-p stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

(defun tar-wal-position-stream-at-end (stream)
  (when (plusp (file-length stream))
    (file-position stream 0)
    (loop with archive = (archive:open-archive 'archive:tar-archive stream)
          for pos = (file-position stream)
          for entry = (archive:read-entry-from-archive archive)
          when entry
            do (archive:discard-entry archive entry)
          while entry
          finally (file-position stream pos))))

(defconstant +wal-file-mode+ #o100664)

(defmethod wal-append-entry ((archive archive:tar-archive) path buffer)
  (let* ((entry (make-instance 'archive::tar-entry
                               :pathname (pathname path)
                               :mode +wal-file-mode+
                               :typeflag archive::+tar-regular-file+
                               :size (length buffer)
                               :mtime (local-time:timestamp-to-unix (local-time:now))))
         (stream (make-instance 'fast-io:fast-input-stream :vector buffer)))
    (archive:write-entry-to-archive archive entry :stream stream)))

(defun %extract-entry (archive entry)
  (let* ((out (make-instance 'fast-io:fast-output-stream :buffer-size (archive::size entry))))
    (archive::transfer-entry-data-to-stream archive entry out)
    (fast-io:finish-output-stream out)))

(defmethod wal-read-next-entry ((archive archive:tar-archive) &key skip-if)
  (let* ((entry (archive:read-entry-from-archive archive))
         (stream (archive::archive-stream archive)))
    (values (when entry
              (if (and skip-if (funcall skip-if (archive:name entry)))
                  (archive:discard-entry archive entry)
                  (%extract-entry archive entry)))
            (when entry
              (archive:name entry))
            (file-position stream))))

(defmethod wal-fsync ((archive archive:tar-archive))
  (finish-output (archive::archive-stream archive)))

(defmethod wal-close ((archive archive:tar-archive))
  (archive:finalize-archive archive)
  (archive:close-archive archive))

(defstruct memory-wal (wal (make-array 0 :fill-pointer 0)) (pos 0))

(defmethod wal-append-entry ((wal memory-wal) path buffer)
  (vector-push-extend (cons path buffer) (slot-value wal 'wal)))

(defmethod wal-read-next-entry ((wal memory-wal) &key skip-if)
  (unless (= (length (slot-value wal 'wal)) (slot-value wal 'pos))
    (let ((entry (aref (slot-value wal 'wal) (slot-value wal 'pos))))
      (incf (slot-value wal 'pos))
      (values (when entry
                (unless (and skip-if (funcall skip-if (car entry)))
                  (cdr entry)))
              (car entry)))))

(defmethod wal-fsync ((wal memory-wal)))

(defmethod wal-close ((wal memory-wal)))
