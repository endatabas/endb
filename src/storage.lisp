(defpackage :endb/storage
  (:use :cl)
  (:export #:*tx-log-version* store-replay store-write-tx store-get-object store-close disk-store in-memory-store)
  (:import-from :fset)
  (:import-from :endb/json)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/storage/wal)
  (:import-from :trivial-utf-8)
  (:import-from :uiop))
(in-package :endb/storage)

(defvar *tx-log-version* 2)
(defvar *wal-target-size* (* 4 1024 1024))
(defvar *wals-per-snapshot* 2)

(defvar *wal-directory* "wal")
(defvar *object-store-directory* "object_store")

(defvar *log-directory* "_log")
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

(defun %validate-tx-log-version (tx-md)
  (let* ((tx-tx-log-version (fset:lookup tx-md "_tx_log_version")))
    (assert (equal *tx-log-version* tx-tx-log-version)
            nil
            (format nil "Transaction log version mismatch: ~A does not match stored: ~A" *tx-log-version* tx-tx-log-version))))

(defun %replay-wal (wal-in md)
  (if (plusp (file-length wal-in))
      (loop with read-wal = (endb/storage/wal:open-tar-wal :stream wal-in :direction :input)
            for (buffer . name) = (multiple-value-bind (buffer name)
                                      (endb/storage/wal:wal-read-next-entry read-wal :skip-if (lambda (x)
                                                                                                (not (alexandria:starts-with-subseq "_log/" x))))
                                    (cons buffer name))
            when buffer
              do (let ((tx-md (endb/json:json-parse buffer)))
                   (%validate-tx-log-version tx-md)
                   (setf md (endb/json:json-merge-patch md tx-md)))
            while name
            finally (return md))
      md))

(defgeneric store-replay (store))
(defgeneric store-write-tx (store tx-id md md-diff arrow-buffers-map &key fsyncp))
(defgeneric store-get-object (store path))
(defgeneric store-close (os))

(defclass disk-store () ((directory :initarg :directory) wal mem-table-object-store backing-object-store (pending-wals :initform 0)))

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
  (with-slots (directory wal mem-table-object-store backing-object-store) store
    (let* ((active-wal-file (%latest-wal-file directory))
           (object-store-path (merge-pathnames *object-store-directory* (uiop:ensure-directory-pathname directory))))
      (ensure-directories-exist active-wal-file)
      (let ((write-io (open active-wal-file :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create)))
        (setf wal (endb/storage/wal:open-tar-wal :stream write-io)
              mem-table-object-store (endb/storage/object-store:make-memory-object-store)
              backing-object-store (endb/storage/object-store:make-directory-object-store :path object-store-path))
        (endb/storage/wal:tar-wal-position-stream-at-end write-io)))))

(defun %extract-tar-into-object-store (stream os)
  (let ((wal-os (endb/storage/object-store:open-tar-object-store :stream stream)))
    (unwind-protect
         (endb/storage/object-store:extract-tar-into-object-store
          wal-os
          os
          :skip-if (lambda (name)
                     (alexandria:starts-with-subseq *log-directory* name)))
      (endb/storage/wal:wal-close wal-os))))

(defmethod store-replay ((store disk-store))
  (with-slots (directory mem-table-object-store backing-object-store pending-wals) store
    (let* ((latest-snapshot-json-bytes (endb/storage/object-store:object-store-get backing-object-store (%latest-snapshot-filename)))
           (latest-snapshot (when latest-snapshot-json-bytes
                              (endb/json:json-parse latest-snapshot-json-bytes)))
           (snapshot-md (if latest-snapshot
                            (let* ((snapshot-path (fset:lookup latest-snapshot "path"))
                                   (snapshot-md-bytes (endb/storage/object-store:object-store-get backing-object-store snapshot-path))
                                   (snapshot-sha1 (string-downcase (sha1:sha1-hex snapshot-md-bytes)))
                                   (snapshot-md (endb/json:json-parse snapshot-md-bytes)))
                              (endb/lib:log-info "using snapshot ~A" snapshot-path)
                              (assert (equal (fset:lookup latest-snapshot "sha1")  snapshot-sha1)
                                      nil
                                      (format nil "Snapshot SHA1 mismatch: ~A does not match stored: ~A" (fset:lookup latest-snapshot "sha1") snapshot-sha1))
                              (%validate-tx-log-version snapshot-md)
                              snapshot-md)
                            (fset:empty-map)))
           (archived-wal-files (when latest-snapshot
                                 (endb/storage/object-store:object-store-list backing-object-store
                                                                              :start-after (%wal-archive-filename (fset:lookup latest-snapshot "tx_id"))
                                                                              :prefix *wal-archive-directory*))))
      (dolist (remote-wal-file archived-wal-files)
        (alexandria:write-byte-vector-into-file (endb/storage/object-store:object-store-get backing-object-store remote-wal-file)
                                                (merge-pathnames (file-namestring remote-wal-file)
                                                                 (merge-pathnames *wal-directory* (uiop:ensure-directory-pathname directory)))
                                                :if-exists :overwrite
                                                :if-does-not-exist :create))
      (let* ((wal-files-to-apply (remove-if
                                  (lambda (wal-file)
                                    (and (not (fset:empty? snapshot-md))
                                         (string< (file-namestring wal-file)
                                                  (file-namestring (%wal-filename (fset:lookup latest-snapshot "tx_id"))))))
                                  (%wal-files directory)))
             (md (reduce
                  (lambda (md wal-file)
                    (endb/lib:log-info "applying ~A" (uiop:enough-pathname wal-file (truename directory)))
                    (with-open-file (wal-in wal-file :direction :io
                                                     :element-type '(unsigned-byte 8)
                                                     :if-exists :overwrite
                                                     :if-does-not-exist :create)
                      (%replay-wal wal-in md)))
                  wal-files-to-apply
                  :initial-value snapshot-md)))

        (setf pending-wals (length wal-files-to-apply))

        (loop for wal-file in wal-files-to-apply
              when (uiop:file-exists-p wal-file)
                do (with-open-file (wal-in wal-file :element-type '(unsigned-byte 8) :if-does-not-exist :create)
                     (%extract-tar-into-object-store wal-in mem-table-object-store)))

        (endb/lib:log-info "active wal is ~A" (uiop:enough-pathname (%latest-wal-file directory) (truename directory)))
        md))))

(defun %rotate-wal (store tx-id md)
  (with-slots (directory wal mem-table-object-store backing-object-store pending-wals) store
    (endb/storage/wal:wal-close wal)
    (let ((latest-wal-file (%latest-wal-file directory)))
      (endb/lib:log-info "rotating ~A" (uiop:enough-pathname latest-wal-file (truename directory)))
      (let* ((latest-wal-bytes (alexandria:read-file-into-byte-vector latest-wal-file)))

        (endb/storage/object-store:object-store-put backing-object-store
                                                    (merge-pathnames (file-namestring latest-wal-file)
                                                                     (uiop:ensure-directory-pathname *wal-archive-directory*))
                                                    latest-wal-bytes)
        (incf pending-wals)
        (endb/lib:log-info "archived ~A" (uiop:enough-pathname latest-wal-file (truename directory)))

        (%extract-tar-into-object-store (flex:make-in-memory-input-stream latest-wal-bytes)
                                        backing-object-store)
        (endb/lib:log-info "unpacked ~A" (uiop:enough-pathname latest-wal-file (truename directory)))

        (when (= pending-wals *wals-per-snapshot*)
          (let* ((md-bytes (trivial-utf-8:string-to-utf-8-bytes (endb/json:json-stringify md)))
                 (md-sha1 (string-downcase (sha1:sha1-hex md-bytes)))
                 (latest-snapshot-json-bytes (trivial-utf-8:string-to-utf-8-bytes
                                              (endb/json:json-stringify (fset:map ("path" (%snapshot-filename tx-id))
                                                                                  ("tx_id" tx-id)
                                                                                  ("sha1" md-sha1))))))
            (endb/storage/object-store:object-store-put backing-object-store (%snapshot-filename tx-id) md-bytes)
            (endb/storage/object-store:object-store-put backing-object-store (%latest-snapshot-filename) latest-snapshot-json-bytes)
            (endb/lib:log-info "stored ~A" (%snapshot-filename tx-id))
            (dolist (wal-file (%wal-files directory))
              (delete-file wal-file)
              (endb/lib:log-info "deleted ~A" (uiop:enough-pathname wal-file (truename directory))))
            (setf pending-wals 0)))))

    (let* ((active-wal-file (merge-pathnames (%wal-filename (1+ tx-id)) (uiop:ensure-directory-pathname directory)))
           (write-io (open active-wal-file :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create))
           (active-wal (endb/storage/wal:open-tar-wal :stream write-io)))
      (endb/lib:log-info "active wal is ~A" (uiop:enough-pathname active-wal-file (truename directory)))
      (setf wal active-wal
            mem-table-object-store (endb/storage/object-store:make-memory-object-store)))))

(defmethod store-write-tx ((store disk-store) tx-id md md-diff arrow-buffers-map &key (fsyncp t))
  (with-slots (directory wal mem-table-object-store) store
    (let ((md-diff-bytes (trivial-utf-8:string-to-utf-8-bytes (endb/json:json-stringify md-diff))))
      (maphash
       (lambda (k buffer)
         (endb/storage/wal:wal-append-entry wal k buffer)
         (endb/storage/object-store:object-store-put mem-table-object-store k buffer))
       arrow-buffers-map)
      (endb/storage/wal:wal-append-entry wal (%log-entry-filename tx-id) md-diff-bytes)
      (cond
        ((<= *wal-target-size* (endb/storage/wal:wal-size wal))
         (%rotate-wal store tx-id md))
        (fsyncp
         (endb/storage/wal:wal-fsync wal))))))

(defmethod store-get-object ((store disk-store) path)
  (with-slots (mem-table-object-store backing-object-store) store
    (or (endb/storage/object-store:object-store-get mem-table-object-store path)
        (endb/storage/object-store:object-store-get backing-object-store path))))

(defmethod store-close ((store disk-store))
  (with-slots (wal mem-table-object-store backing-object-store) store
    (endb/storage/wal:wal-close wal)
    (endb/storage/object-store:object-store-close mem-table-object-store)
    (endb/storage/object-store:object-store-close backing-object-store)))

(defclass in-memory-store ()
  ((object-store :initform (endb/storage/object-store:make-memory-object-store))))

(defmethod store-replay ((store in-memory-store))
  (fset:empty-map))

(defmethod store-write-tx ((store in-memory-store) tx-id md md-diff arrow-buffers-map &key fsyncp)
  (declare (ignore fsyncp))
  (with-slots (object-store) store
    (maphash
     (lambda (k buffer)
       (endb/storage/object-store:object-store-put object-store k buffer))
     arrow-buffers-map)))

(defmethod store-get-object ((store in-memory-store) path)
  (endb/storage/object-store:object-store-get (slot-value store 'object-store) path))

(defmethod store-close ((store in-memory-store))
  (endb/storage/object-store:object-store-close (slot-value store 'object-store)))

;; TODO:

;; * wal log rotation and md snapshot.
;;   * the <tx_id> in wal and snapshot file names is the first tx id, not the last one actually stored inside the file.
;;   * wals are around 64Mb-256Mb in size, snapshots are an optimisation and aren't necessary to take for every wal uploaded.
;;   * start a new local active wal, this happens within the current tx.
;;   * store/upload the previous active wal into the object store as backup as _wal/<tx_id>_wal.tar - this is the remote commit point and happens within th current tx.
;;   * (async) the above step could also be done async to avoid blocking, at the cost of more complex startup.
;;   * (async) write the md for the start tx id matching the remote commit point as _snapshot/<tx_id>_snapshot.json into object store.
;;   * (async) update _snapshot/_latest_snapshot.json in object store to point to the above.
;;   * (async) unpack/upload all Arrow files from the wal, potentially compacting them (see below).
;;   * (async) update _wal/_latest_unpacked_wal.json with the name of the latest unpacked wal.

;; * init state at startup.
;;   * check _wal/_latest_unpacked_wal.json in the object store for wals that needs to be downloaded and used as object store overlays.
;;   * read _latest_snapshot.json in object store and resolve and download the <tx_id>_snapshot.json it points to.
;;   * list to find any archived wals _wal/<tx_id>_wal.tar greater or equal to the earliest of these, and download them.
;;   * also re-download locally present wals, to avoid any node issues.
;;   * local wals less than equal to the minimum of the latest unpacked wal or latest snapshot wal are deleted locally.
;;   * wals later than the snapshot (already downloaded as per above) are replayed on top of the snapshot, together with any later potential local-only wals.
;;   * if a remote wal or a local-only but not latest is damaged or missing, the node cannot start.
;;   * local-only but not latest wals are also uploaded and have potential snapshots taken sync during replay (if the remote commit isn't sync, see above).
;;   * if the latest local-only wal is damaged, create a local backup of it and then truncate it, keeping and replaying the healthy part.
;;   * provide a flag that requires manual intervention to truncate the local-only wal.
;;   * node ready.

;; * basic compaction of Arrow files.
;;   * (async) a tx is needed to update the md to point to the new files and remove the old ones.
;;   * (async) this is a special tx that can would need to consolidate later deletes and erasures into the compacted file's meta data.
;;   * (async) superseded files could be deleted from the object store after the above commit, may need gc list in md, or simply stop a node trying to access old missing files.
;;   * it is possible to compact sync within the current wal during the original remote commit, but this would block the tx processing.
