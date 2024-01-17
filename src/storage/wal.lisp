(defpackage :endb/storage/wal
  (:use :cl)
  (:export #:open-tar-wal #:tar-wal-position-stream-at-end #:wal-append-entry #:wal-read-next-entry #:wal-find-entry #:wal-fsync #:wal-size #:wal-close)
  (:import-from :archive)
  (:import-from :flexi-streams)
  (:import-from :local-time))
(in-package :endb/storage/wal)

(defgeneric wal-append-entry (wal path buffer &key mtime))
(defgeneric wal-read-next-entry (wal &key skip-if))
(defgeneric wal-fsync (wal))
(defgeneric wal-size (wal))
(defgeneric wal-close (wal))

(defun open-tar-wal (&key (stream (flex:make-in-memory-output-stream)) (direction :output))
  (let ((archive (archive:open-archive 'archive:tar-archive stream :direction direction)))
    (when (typep stream 'flex:vector-stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

(defun %stream-length (stream)
  (etypecase stream
    (flex:in-memory-output-stream (flex:output-stream-sequence-length stream))
    (flex::vector-input-stream (flex::vector-stream-end stream))
    (t (file-length stream))))

(defun %corrupt-archive-error-p (e)
  (equal "Corrupt archive" (format nil "~A" e)))

(defun tar-wal-position-stream-at-end (stream &key allow-corrupt-p)
  (when (listen stream)
    (file-position stream 0)
    (loop with archive = (archive:open-archive 'archive:tar-archive stream)
          for pos = (file-position stream)
          for entry = (handler-case
                          (archive:read-entry-from-archive archive)
                        (error (e)
                          (unless (and (%corrupt-archive-error-p e) allow-corrupt-p)
                            (error e))
                          (return pos)))
          when entry
            do (archive:discard-entry archive entry)
          while entry
          finally (file-position stream pos))))

(defconstant +wal-file-mode+ #o100664)

(defmethod wal-append-entry ((archive archive:tar-archive) path buffer &key mtime)
  (endb/lib:with-trace-kvs-span "wal_append_entry" (fset:map ("path" path))
    (let* ((entry (make-instance 'archive::tar-entry
                                 :pathname (pathname path)
                                 :mode +wal-file-mode+
                                 :typeflag archive::+tar-regular-file+
                                 :size (length buffer)
                                 :mtime (local-time:timestamp-to-unix (or mtime (local-time:now))))))
      (flex:with-input-from-sequence (in buffer)
        (archive:write-entry-to-archive archive entry :stream in)
        (endb/lib:metric-monotonic-counter "wal_bytes_written" (length buffer))))))

(defun %extract-entry (archive entry)
  (flex:with-output-to-sequence (out)
    (archive::transfer-entry-data-to-stream archive entry out)))

(defun %wal-read-entry-safe (archive)
  (when (listen (archive::archive-stream archive))
    (archive:read-entry-from-archive archive)))

(defmethod wal-read-next-entry ((archive archive:tar-archive) &key skip-if)
  (endb/lib:with-trace-span "wal_read_next_entry"
   (let* ((entry (%wal-read-entry-safe archive))
          (stream (archive::archive-stream archive)))
     (values (when entry
               (if (and skip-if (funcall skip-if (archive:name entry)))
                   (archive:discard-entry archive entry)
                   (let ((buffer (%extract-entry archive entry)))
                     (endb/lib:metric-monotonic-counter "wal_bytes_read" (length buffer))
                     buffer)))
             (when entry
               (archive:name entry))
             (file-position stream)))))

(defmethod wal-fsync ((archive archive:tar-archive))
  (endb/lib:with-trace-span "wal_fsync"
   (finish-output (archive::archive-stream archive))))

(defmethod wal-size ((archive archive:tar-archive))
  (%stream-length (archive::archive-stream archive)))

(defmethod wal-close ((archive archive:tar-archive))
  (when (output-stream-p (archive::archive-stream archive))
    (archive:finalize-archive archive)
    (wal-fsync archive))
  (archive:close-archive archive))
