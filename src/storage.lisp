(defpackage :endb/storage
  (:use :cl)
  (:export #:*tx-log-version* store-replay store-write-tx store-get-object store-put-object store-close disk-store in-memory-store)
  (:import-from :alexandria)
  (:import-from :bordeaux-threads)
  (:import-from :flexi-streams)
  (:import-from :fset)
  (:import-from :endb/json)
  (:import-from :endb/lib)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/storage/wal)
  (:import-from :endb/queue)
  (:import-from :trivial-utf-8)
  (:import-from :uiop))
(in-package :endb/storage)

(defvar *tx-log-version* 3)
(defvar *wal-target-size* (* 8 1024 1024))
(defvar *wals-per-snapshot* 8)

(defvar *wal-directory* "wal")
(defvar *log-directory* "_log")

(defvar *object-store-directory* "object_store")
(defvar *snapshot-directory* "_snapshot")
(defvar *wal-archive-directory* "_wal")

(defun %log-entry-filename (tx-id)
  (format nil "~A/~(~16,'0x~).json" *log-directory* tx-id))

(defun %snapshot-filename (tx-id)
  (format nil "~A/~(~16,'0x~).json" *snapshot-directory* tx-id))

(defun %latest-snapshot-filename ()
  (format nil "~A/latest_snapshot.json" *snapshot-directory*))

(defun %wal-filename (tx-id)
  (format nil "~A/~(~16,'0x~).tar" *wal-directory* tx-id))

(defun %wal-archive-filename (tx-id)
  (format nil "~A/~(~16,'0x~).tar" *wal-archive-directory* tx-id))

(defgeneric store-replay (store))
(defgeneric store-write-tx (store tx-id md md-diff arrow-buffers-map &key fsyncp mtime))
(defgeneric store-get-object (store path))
(defgeneric store-put-object (store path buffer))
(defgeneric store-close (store))

(defclass disk-store ()
  ((directory :initarg :directory)
   wal
   mem-table-object-store
   backing-object-store
   (pending-wals :initform 0)
   (snapshot-queue :initform (endb/queue:make-queue))
   snapshot-thread))

(defun %wal-files (directory)
  (let ((wal-directory-path (merge-pathnames *wal-directory* (uiop:ensure-directory-pathname directory))))
    (sort (loop for p in (directory (merge-pathnames "*/*.tar" wal-directory-path))
                unless (uiop:directory-pathname-p p)
                  collect (namestring p))
          #'string<)))

(defun %latest-wal-file (directory)
  (let ((wal-files (%wal-files directory)))
    (or (car (last wal-files))
        (merge-pathnames (%wal-filename 1) (uiop:ensure-directory-pathname directory)))))

(defmethod initialize-instance :after ((store disk-store) &key &allow-other-keys)
  (with-slots (directory wal mem-table-object-store backing-object-store snapshot-queue snapshot-thread) store
    (let* ((active-wal-file (%latest-wal-file directory))
           (object-store-path (merge-pathnames *object-store-directory* (uiop:ensure-directory-pathname directory))))
      (ensure-directories-exist active-wal-file)

      (when (uiop:file-exists-p active-wal-file)
        (multiple-value-bind (corrupt-pos)
            (with-open-file (wal-in active-wal-file :direction :input
                                                    :element-type '(unsigned-byte 8))
              (endb/storage/wal:tar-wal-position-stream-at-end wal-in :allow-corrupt-p t))
          (when corrupt-pos
            (endb/lib:log-warn "truncating damaged ~A" (uiop:enough-pathname active-wal-file (truename directory)))
            (alexandria:copy-file active-wal-file (format nil "~A.damaged.bak" active-wal-file))
            (with-open-file (out active-wal-file :direction :output :element-type '(unsigned-byte 8) :if-exists :append)
              (endb/storage/wal:wal-close (endb/storage/wal:open-tar-wal :stream out))))))

      (let ((write-io (open active-wal-file :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create)))
        (setf wal (endb/storage/wal:open-tar-wal :stream write-io)
              mem-table-object-store (endb/storage/object-store:make-memory-object-store)
              backing-object-store (endb/storage/object-store:make-directory-object-store :path object-store-path)
              snapshot-thread (bt:make-thread
                               (lambda ()
                                 (loop for job = (endb/queue:queue-pop snapshot-queue)
                                       if (null job)
                                         do (return-from nil)
                                       else
                                         do (funcall job)))
                               :name "endb snapshot thread"))
        (endb/storage/wal:tar-wal-position-stream-at-end write-io)))))

(defun %validate-tx-log-version (tx-md)
  (let* ((tx-tx-log-version (fset:lookup tx-md "_tx_log_version")))
    (assert (equal *tx-log-version* tx-tx-log-version)
            nil
            (format nil "Transaction log version mismatch: ~A does not match stored: ~A" *tx-log-version* tx-tx-log-version))))

(defun %replay-wal (wal-in md)
  (if (plusp (file-length wal-in))
      (loop with read-wal = (endb/storage/wal:open-tar-wal :stream wal-in :direction :input)
            for (buffer . name) = (multiple-value-bind (buffer name)
                                      (endb/storage/wal:wal-read-next-entry
                                       read-wal
                                       :skip-if (lambda (x)
                                                  (not (alexandria:starts-with-subseq *log-directory* x))))
                                    (cons buffer name))
            when buffer
              do (let ((tx-md (endb/json:json-parse buffer)))
                   (%validate-tx-log-version tx-md)
                   (setf md (endb/json:json-merge-patch md tx-md)))
            while name
            finally (return md))
      md))

(defun %extract-tar-into-object-store (stream os)
  (let ((wal-os (endb/storage/object-store:open-tar-object-store :stream stream)))
    (unwind-protect
         (endb/storage/object-store:extract-tar-into-object-store
          wal-os
          os
          :skip-if (lambda (name)
                     (alexandria:starts-with-subseq "_" name)))
      (endb/storage/wal:wal-close wal-os))))

(defun %read-latest-snapshot (os)
  (let ((latest-snapshot-json-bytes (endb/storage/object-store:object-store-get os (%latest-snapshot-filename))))
    (when latest-snapshot-json-bytes
      (endb/json:json-parse latest-snapshot-json-bytes))))

(defun %build-snapshot (store wal-files md tx-id)
  (with-slots (directory backing-object-store) store
    (let ((wal-files (loop for wal-file in wal-files
                           when (uiop:file-exists-p wal-file)
                             collect wal-file)))
      (loop for wal-file in wal-files
            do (with-open-file (wal-in wal-file :element-type '(unsigned-byte 8) :if-does-not-exist :create)
                 (%extract-tar-into-object-store wal-in backing-object-store))
               (endb/lib:log-info "unpacked ~A" (uiop:enough-pathname wal-file (truename directory))))

      (let* ((previous-latest-snapshot (%read-latest-snapshot backing-object-store))
             (md-bytes (trivial-utf-8:string-to-utf-8-bytes (endb/json:json-stringify md)))
             (latest-snapshot-json-bytes (trivial-utf-8:string-to-utf-8-bytes
                                          (endb/json:json-stringify
                                           (fset:map ("path" (%snapshot-filename tx-id))
                                                     ("tx_id" tx-id)
                                                     ("sha1" (string-downcase (endb/lib:sha1 md-bytes))))))))
        (endb/storage/object-store:object-store-put backing-object-store (%snapshot-filename tx-id) md-bytes)
        (endb/storage/object-store:object-store-put backing-object-store (%latest-snapshot-filename) latest-snapshot-json-bytes)
        (endb/lib:log-info "stored ~A" (%snapshot-filename tx-id))
        (when previous-latest-snapshot
          (let ((previous-snapshot-path (fset:lookup previous-latest-snapshot "path")))
            (endb/storage/object-store:object-store-delete backing-object-store previous-snapshot-path)
            (endb/lib:log-info "deleted ~A" previous-snapshot-path)))

        (dolist (wal-file wal-files)
          (uiop:delete-file-if-exists wal-file)
          (endb/lib:log-info "deleted ~A" (uiop:enough-pathname wal-file (truename directory))))))))

(defmethod store-replay ((store disk-store))
  (with-slots (directory mem-table-object-store backing-object-store pending-wals) store
    (let* ((latest-snapshot (%read-latest-snapshot backing-object-store))
           (snapshot-md (if latest-snapshot
                            (let ((snapshot-path (fset:lookup latest-snapshot "path")))
                              (endb/lib:log-info "fetching snapshot ~A" snapshot-path)
                              (let* ((snapshot-md-bytes (endb/storage/object-store:object-store-get backing-object-store snapshot-path))
                                     (snapshot-md (endb/json:json-parse snapshot-md-bytes)))
                                (endb/lib:log-info "using snapshot ~A" snapshot-path)
                                (when (fset:lookup latest-snapshot "sha1")
                                  (let ((snapshot-sha1 (string-downcase (endb/lib:sha1 snapshot-md-bytes))))
                                    (assert (equal (fset:lookup latest-snapshot "sha1") snapshot-sha1)
                                            nil
                                            (format nil "Snapshot SHA1 mismatch: ~A does not match stored: ~A" (fset:lookup latest-snapshot "sha1") snapshot-sha1))))
                                (%validate-tx-log-version snapshot-md)
                                snapshot-md))
                            (fset:empty-map)))
           (archived-wal-files (when latest-snapshot
                                 (endb/storage/object-store:object-store-list backing-object-store
                                                                              :start-after (%wal-archive-filename (fset:lookup latest-snapshot "tx_id"))
                                                                              :prefix *wal-archive-directory*)))
           (wal-directory (uiop:ensure-directory-pathname (merge-pathnames *wal-directory* (uiop:ensure-directory-pathname directory)))))
      (loop for remote-wal-file in archived-wal-files
            when (alexandria:ends-with-subseq ".tar" remote-wal-file)
              do (let ((buffer (endb/storage/object-store:object-store-get backing-object-store remote-wal-file))
                       (wal-file (merge-pathnames (file-namestring remote-wal-file) wal-directory)))
                   (alexandria:write-byte-vector-into-file buffer wal-file :if-exists :overwrite :if-does-not-exist :create)
                   (endb/lib:log-info "fetched ~A" (uiop:enough-pathname wal-file (truename directory)))))

      (let* ((wal-files-to-apply (remove-if
                                  (lambda (wal-file)
                                    (and (not (fset:empty? snapshot-md))
                                         (string< (file-namestring wal-file)
                                                  (file-namestring (%wal-filename (fset:lookup latest-snapshot "tx_id"))))))
                                  (%wal-files directory)))
             (md (reduce
                  (lambda (md wal-file)
                    (endb/lib:log-info "applying ~A" (uiop:enough-pathname wal-file (truename directory)))
                    (with-open-file (wal-in wal-file :element-type '(unsigned-byte 8))
                      (%replay-wal wal-in md)))
                  wal-files-to-apply
                  :initial-value snapshot-md)))

        (setf pending-wals (max 0 (1- (length wal-files-to-apply))))

        (loop for wal-file in wal-files-to-apply
              when (uiop:file-exists-p wal-file)
                do (with-open-file (wal-in wal-file :element-type '(unsigned-byte 8))
                     (%extract-tar-into-object-store wal-in mem-table-object-store)))

        (endb/lib:log-info "active wal is ~A" (uiop:enough-pathname (%latest-wal-file directory) (truename directory)))
        md))))

(defun %rotate-wal (store tx-id md)
  (with-slots (directory wal mem-table-object-store backing-object-store pending-wals snapshot-queue) store
    (endb/storage/wal:wal-close wal)
    (let ((latest-wal-file (%latest-wal-file directory)))
      (endb/lib:log-info "rotating ~A" (uiop:enough-pathname latest-wal-file (truename directory)))
      (with-open-file (wal-in latest-wal-file :element-type '(unsigned-byte 8))
        (endb/storage/object-store:object-store-put backing-object-store
                                                    (merge-pathnames (file-namestring latest-wal-file)
                                                                     (uiop:ensure-directory-pathname *wal-archive-directory*))
                                                    wal-in))
      (incf pending-wals)
      (endb/lib:log-info "archived ~A" (uiop:enough-pathname latest-wal-file (truename directory)))

      (when (>= pending-wals *wals-per-snapshot*)
        (setf pending-wals 0)
        (let ((wal-files-to-unpack (%wal-files directory))
              (objects-to-delete (endb/storage/object-store:object-store-list mem-table-object-store)))
          (endb/queue:queue-push
           snapshot-queue
           (lambda ()
             (%build-snapshot store wal-files-to-unpack md tx-id)
             (dolist (path objects-to-delete)
               (endb/storage/object-store:object-store-delete mem-table-object-store path)))))))

    (let* ((active-wal-file (merge-pathnames (%wal-filename (1+ tx-id)) (truename directory)))
           (write-io (open active-wal-file :direction :output :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create))
           (active-wal (endb/storage/wal:open-tar-wal :stream write-io)))
      (endb/lib:log-info "active wal is ~A" (uiop:enough-pathname active-wal-file (truename directory)))
      (setf wal active-wal))))

(defmethod store-write-tx ((store disk-store) tx-id md md-diff arrow-buffers-map &key (fsyncp t) mtime)
  (with-slots (directory wal mem-table-object-store) store
    (let ((md-diff-bytes (trivial-utf-8:string-to-utf-8-bytes (endb/json:json-stringify md-diff))))
      (maphash
       (lambda (k buffer)
         (endb/storage/wal:wal-append-entry wal k buffer :mtime mtime)
         (endb/storage/object-store:object-store-put mem-table-object-store k buffer))
       arrow-buffers-map)
      (endb/storage/wal:wal-append-entry wal (%log-entry-filename tx-id) md-diff-bytes :mtime mtime)
      (cond
        ((<= *wal-target-size* (endb/storage/wal:wal-size wal))
         (%rotate-wal store tx-id md))
        (fsyncp
         (endb/storage/wal:wal-fsync wal))))))

(defmethod store-get-object ((store disk-store) path)
  (with-slots (mem-table-object-store backing-object-store) store
    (or (endb/storage/object-store:object-store-get mem-table-object-store path)
        (endb/storage/object-store:object-store-get backing-object-store path))))

(defmethod store-put-object ((store disk-store) path buffer)
  (with-slots (backing-object-store) store
    (endb/storage/object-store:object-store-put backing-object-store path buffer)))

(defmethod store-close ((store disk-store))
  (with-slots (wal mem-table-object-store backing-object-store snapshot-queue snapshot-thread) store
    (endb/queue:queue-close snapshot-queue)
    (bt:join-thread snapshot-thread)
    (endb/storage/wal:wal-close wal)
    (endb/storage/object-store:object-store-close mem-table-object-store)
    (endb/storage/object-store:object-store-close backing-object-store)))

(defclass in-memory-store ()
  ((object-store :initform (endb/storage/object-store:make-memory-object-store))))

(defmethod store-replay ((store in-memory-store))
  (fset:empty-map))

(defmethod store-write-tx ((store in-memory-store) tx-id md md-diff arrow-buffers-map &key fsyncp mtime)
  (declare (ignore fsyncp mtime))
  (with-slots (object-store) store
    (maphash
     (lambda (k buffer)
       (endb/storage/object-store:object-store-put object-store k buffer))
     arrow-buffers-map)))

(defmethod store-get-object ((store in-memory-store) path)
  (endb/storage/object-store:object-store-get (slot-value store 'object-store) path))

(defmethod store-put-object ((store in-memory-store) path buffer)
  (endb/storage/object-store:object-store-put (slot-value store 'object-store) path buffer))

(defmethod store-close ((store in-memory-store))
  (endb/storage/object-store:object-store-close (slot-value store 'object-store)))
