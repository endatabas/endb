(defpackage :endb/wal
  (:use :cl)
  (:export #:make-wal #:wal-append-entry #:wal-read-next-entry #:wal-fsync #:wal-close)
  (:import-from :archive)
  (:import-from :fast-io)
  (:import-from :local-time))
(in-package :endb/wal)

(defgeneric wal-append-entry (wal path buffer))
(defgeneric wal-read-next-entry (wal))
(defgeneric wal-fsync (wal))
(defgeneric wal-close (wal))

(defun make-wal (&key (stream (make-instance 'fast-io:fast-output-stream)) (direction :output))
  (archive:open-archive 'archive:tar-archive stream :direction direction))

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

(defmethod wal-read-next-entry ((archive archive:tar-archive))
  (let ((entry (archive:read-entry-from-archive archive)))
    (when entry
      (let* ((out (make-instance 'fast-io:fast-output-stream)))
        (archive::transfer-entry-data-to-stream archive entry out)
        (values (fast-io:finish-output-stream out) (archive:name entry))))))

(defmethod wal-fsync ((archive archive:tar-archive))
  (finish-output (archive::archive-stream archive)))

(defmethod wal-close ((archive archive:tar-archive))
  (archive:finalize-archive archive)
  (archive:close-archive archive))

;; https://github.com/delta-io/delta/blob/master/PROTOCOL.md

;; {"meta_data":{"table":"foo","columns":["value"],"system_time":1674064791593}}
;; {"add":{"table":"foo","path":"foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow","system_time":1674064791593,"data_change":true,"dv_path":"foo/deletion_vector_44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow","stats":{"num_records":100,"min_values":{"value":4},"max_values":{"value":1967},"null_count":{"value":0}},"base_row_id":0}}
;; {"remove":{"table":"foo","path":"foo/a9bb3ce8-afba-47ec-8451-13edcd855b15.arrow","system_time":1674064797399,"data_change":true,"dv_path":"foo/deletion_vector_44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"}}

;; (to-arrow
;;  '((("meta_data" .
;;      (("table" . "foo")
;;       ("columns" . #("value"))
;;       ("system_time" . @2023-05-08T08:56:43.280788Z))))
;;    (("add" .
;;      (("table" . "foo")
;;       ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;       ("system_time" . @2023-05-08T08:56:43.280788Z)
;;       ("data_change" . t)
;;       ("dv_path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")
;;       ("stats" . (("num_records" . 100)
;;                   ("min_values" . (("value" . 4)))
;;                   ("max_values" . (("value" . 1967)))
;;                   ("null_count" . (("value" . 0)))))
;;       ("base_row_id" . 0))))
;;    (("remove" .
;;      (("table" . "foo")
;;       ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;       ("system_time" . @2023-05-08T08:56:43.280788Z)
;;       ("data_change" . t)
;;       ("dv_path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"))))))
