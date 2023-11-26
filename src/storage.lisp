(defpackage :endb/storage
  (:use :cl)
  (:export #:*tx-log-version* store-replay store-write-tx store-get-object-store store-close disk-store in-memory-store)
  (:import-from :fset)
  (:import-from :endb/json)
  (:import-from :endb/lib/arrow)
  (:import-from :endb/storage/object-store)
  (:import-from :endb/storage/wal)
  (:import-from :trivial-utf-8)
  (:import-from :uiop))
(in-package :endb/storage)

(defvar *tx-log-version* 1)

(defun %log-filename (tx-id)
  (format nil "_log/~(~16,'0x~).json" tx-id))

(defun %replay-wal (wal-in md)
  (if (plusp (file-length wal-in))
      (loop with read-wal = (endb/storage/wal:open-tar-wal :stream wal-in :direction :input)
            for (buffer . name) = (multiple-value-bind (buffer name)
                                      (endb/storage/wal:wal-read-next-entry read-wal :skip-if (lambda (x)
                                                                                                (not (alexandria:starts-with-subseq "_log/" x))))
                                    (cons buffer name))
            when buffer
              do (let* ((tx-md (endb/json:json-parse buffer))
                        (tx-tx-log-version (fset:lookup tx-md "_tx_log_version")))
                   (assert (equal *tx-log-version* tx-tx-log-version)
                           nil
                           (format nil "Transaction log version mismatch: ~A does not match stored: ~A" *tx-log-version* tx-tx-log-version))
                   (setf md (endb/json:json-merge-patch md tx-md)))
            while name
            finally (return md))
      md))

(defun %write-arrow-buffers (arrow-arrays-map write-buffer-fn)
  (loop for k being the hash-key
          using (hash-value v)
            of arrow-arrays-map
        for buffer = (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer v)
        do (funcall write-buffer-fn k buffer)))

(defgeneric store-replay (store))
(defgeneric store-write-tx (store tx-id md-diff arrow-arrays-map &key fsyncp))
(defgeneric store-get-object-store (store))
(defgeneric store-close (os))

(defclass disk-store () ((directory :initarg :directory) wal object-store))

(defmethod initialize-instance :after ((store disk-store) &key wal-only-p)
  (with-slots (directory) store
    (let ((wal-file (merge-pathnames "wal.log" (uiop:ensure-directory-pathname directory)))
          (object-store-path (merge-pathnames "object_store" (uiop:ensure-directory-pathname directory))))
      (ensure-directories-exist wal-file)
      (let* ((wal-os (endb/storage/object-store:open-tar-object-store :stream (open wal-file :element-type '(unsigned-byte 8) :if-does-not-exist :create)))
             (os (if wal-only-p
                     wal-os
                     (endb/storage/object-store:make-layered-object-store
                      :overlay-object-store (endb/storage/object-store:make-layered-object-store
                                             :overlay-object-store (endb/storage/object-store:extract-tar-object-store
                                                                    wal-os
                                                                    (endb/storage/object-store:make-memory-object-store))
                                             :underlying-object-store wal-os)
                      :underlying-object-store (endb/storage/object-store:make-directory-object-store :path object-store-path))))
             (write-io (open wal-file :direction :io :element-type '(unsigned-byte 8) :if-exists :overwrite :if-does-not-exist :create))
             (wal (endb/storage/wal:open-tar-wal :stream write-io)))
        (endb/storage/wal:tar-wal-position-stream-at-end write-io)
        (setf (slot-value store 'wal) wal
              (slot-value store 'object-store) os)))))

(defmethod store-replay ((store disk-store))
  (with-slots (directory) store
    (let ((wal-file (merge-pathnames "wal.log" (uiop:ensure-directory-pathname directory))))
      (with-open-file (wal-in wal-file :direction :io
                                       :element-type '(unsigned-byte 8)
                                       :if-exists :overwrite
                                       :if-does-not-exist :create)
        (%replay-wal wal-in (fset:empty-map))))))

(defmethod store-write-tx ((store disk-store) tx-id md-diff arrow-arrays-map &key (fsyncp t))
  (with-slots (wal) store
    (let ((md-diff-bytes (trivial-utf-8:string-to-utf-8-bytes (endb/json:json-stringify md-diff))))
      (%write-arrow-buffers arrow-arrays-map (lambda (k buffer)
                                               (endb/storage/wal:wal-append-entry wal k buffer)))
      (endb/storage/wal:wal-append-entry wal (%log-filename tx-id) md-diff-bytes)
      (when fsyncp
        (endb/storage/wal:wal-fsync wal)))))

(defmethod store-get-object-store ((store disk-store))
  (slot-value store 'object-store))

(defmethod store-close ((store disk-store))
  (endb/storage/wal:wal-close (slot-value store 'wal))
  (endb/storage/object-store:object-store-close (slot-value store 'object-store)))

(defclass in-memory-store ()
  ((object-store :initform (endb/storage/object-store:make-memory-object-store))))

(defmethod store-replay ((store in-memory-store))
  (fset:empty-map))

(defmethod store-write-tx ((store in-memory-store) tx-id md-diff arrow-arrays-map &key fsyncp)
  (declare (ignore fsyncp))
  (with-slots (object-store) store
    (%write-arrow-buffers arrow-arrays-map (lambda (k buffer)
                                             (endb/storage/object-store:object-store-put object-store k buffer)))))

(defmethod store-get-object-store ((store in-memory-store))
  (slot-value store 'object-store))

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
