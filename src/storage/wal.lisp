(defpackage :endb/storage/wal
  (:use :cl)
  (:export #:open-tar-wal #:wal-append-entry #:wal-read-next-entry #:wal-find-entry #:wal-fsync #:wal-close #:random-uuid)
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
    (when (typep stream 'fast-io:fast-input-stream)
      (setf (slot-value archive 'archive::skippable-p) t))
    archive))

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

(defconstant +random-uuid-part-max+ (ash 1 64))
(defconstant +random-uuid-version+ 4)
(defconstant +random-uuid-variant+ 2)

(defun random-uuid (&optional (state *random-state*))
  (let ((high (dpb +random-uuid-version+ (byte 4 12) (random +random-uuid-part-max+ state)))
        (low (dpb +random-uuid-variant+ (byte 2 62) (random +random-uuid-part-max+ state))))
    (format nil "~(~4,'0x~)~(~4,'0x~)-~(~4,'0x~)-~(~4,'0x~)-~(~4,'0x~)-~(~4,'0x~)~(~4,'0x~)~(~4,'0x~)"
            (ldb (byte 16 48) high)
            (ldb (byte 16 32) high)
            (ldb (byte 16 16) high)
            (ldb (byte 16 0) high)
            (ldb (byte 16 48) low)
            (ldb (byte 16 32) low)
            (ldb (byte 16 16) low)
            (ldb (byte 16 0) low))))

;; https://github.com/delta-io/delta/blob/master/PROTOCOL.md

;; {"meta_data":{"table":"foo","columns":["value"],"system_time":1674064791593}}
;; {"add":{"table":"foo","path":"foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow","system_time":1674064791593,"data_change":true,"dv_path":"foo/deletion_vector_44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow","stats":{"num_records":100,"min_values":{"value":4},"max_values":{"value":1967},"null_count":{"value":0}},"base_row_id":0}}
;; {"remove":{"table":"foo","path":"foo/a9bb3ce8-afba-47ec-8451-13edcd855b15.arrow","system_time":1674064797399,"data_change":true,"dv_path":"foo/deletion_vector_44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"}}

;; (defstruct action table system-time)

;; (defstruct (meta-data-action (:include action)) columns)

;; (defstruct (add-action (:include action)) path (data-change t) dv-path stats (base-row-id 0))

;; (defstruct (remove-action (:include action)) path (data-change t) dv-path)

;; (defstruct column-stats num-records min-values max-values null-count)

;; (make-meta-data-action :table "foo"
;;                        :system-time @2023-05-08T08:56:43.280788Z
;;                        :columns #("value"))

;; '(:action :meta-data
;;   :table "foo"
;;   :system-time @2023-05-08T08:56:43.280788Z
;;   :columns #("value"))

;; (make-add-action :table "foo"
;;                  :system-time @2023-05-08T08:56:43.280788Z
;;                  :path "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow"
;;                  :dv-path "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"
;;                  :stats (make-column-stats :num-records 100
;;                                            :min-values '(:value 4)
;;                                            :max-values '(:value 1967)
;;                                            :null-count '(:value 0)))

;; '(:action :add
;;   :table "foo"
;;   :system-time @2023-05-08T08:56:43.280788Z
;;   :path "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow"
;;   :dv-path "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"
;;   :stats (:num-records 100
;;           :min-values (:value 4)
;;           :max-values (:value 1967)
;;           :null-count (:value 0)))

;; (make-remove-action :table "foo"
;;                     :system-time @2023-05-08T08:56:43.280788Z
;;                     :path "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow"
;;                     :dv-path "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")

;; '(:action :remove
;;   :table "foo"
;;   :system-time @2023-05-08T08:56:43.280788Z
;;   :path "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow"
;;   :dv-path "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")

;; (defmethod clop::serialize ((table clop:table) (style (eql :plist)))
;;   (loop with children = (clop:children table)
;;         for key being the hash-keys of children
;;         append (list (alexandria:make-keyword (string-upcase key)) (clop::serialize (gethash key children) style))))

;; (defmethod clop::serialize ((table clop:inline-table) (style (eql :plist)))
;;   (loop with children = (clop:children table)
;;         for key being the hash-keys of children
;;         append (list (alexandria:make-keyword (string-upcase key)) (clop::serialize (gethash key children) style))))

;; (defmethod clop::serialize ((table clop:table-array) (style (eql :plist)))
;;   (map 'vector (lambda (it) (clop::serialize it style))
;;        (clop::children table)))

;; (defmethod clop::serialize ((thing list) (style (eql :plist)))
;;   (if (listp (cdr thing))
;;       (map 'vector (lambda (it) (clop::serialize it style)) thing)
;;       thing))

;; (defmethod clop::serialize ((table clop:table-array) (style (eql :alist)))
;;   (map 'vector (lambda (it) (clop::serialize it style))
;;        (clop::children table)))

;; (defmethod clop::serialize ((thing list) (style (eql :alist)))
;;   (if (listp (cdr thing))
;;       (map 'vector (lambda (it) (clop::serialize it style)) thing)
;;       thing))

;; ;; (endb/arrow:to-arrow (rest (first (clop:parse (alexandria:read-file-into-string "target/log.toml")))))

;; (alexandria:write-byte-vector-into-file
;;  (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
;;   (list (endb/arrow:to-arrow (rest (first (clop:parse (alexandria:read-file-into-string "target/log.toml")))))))
;;  (pathname "target/log.arrow")
;;  :if-exists :overwrite)

;; (alexandria:write-byte-vector-into-file
;;  (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer
;;   (list
;;    (endb/arrow:to-arrow
;;     '((("meta_data" .
;;         (("table" . "foo")
;;          ("columns" . #("value"))
;;          ("system_time" . @2023-05-08T08:56:43.280788Z))))
;;       (("add" .
;;         (("table" . "foo")
;;          ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;          ("system_time" . @2023-05-08T08:56:43.280788Z)
;;          ("data_change" . t)
;;          ("dv" . (("path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")
;;                   ("cardinality" . 10)))
;;          ("stats" . (("num_records" . 100)
;;                      ("min_values" . (("value" . 4)))
;;                      ("max_values" . (("value" . 1967)))
;;                      ("null_count" . (("value" . 0)))))
;;          ("base_row_id" . 0))))
;;       (("remove" .
;;         (("table" . "foo")
;;          ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;          ("system_time" . @2023-05-08T08:56:43.280788Z)
;;          ("data_change" . t)
;;          ("dv" . (("path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")
;;                   ("cardinality" . 10))))))))))
;;  (pathname "target/log2.arrow")
;;  :if-exists :overwrite)


;; (("actions"
;;   (("op" . "meta_data") ("table" . "foo") ("columns" "value")
;;    ("system_time" . @2023-05-09T09:01:06.384631+02:00))
;;   (("op" . "add") ("table" . "foo")
;;    ("system_time" . @2023-05-09T09:01:06.384631+02:00) ("date_change" . t)
;;    ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;    ("dv_path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow")
;;    ("stats" ("num_records" . 0) ("min_values" ("value" . 4))
;;     ("max_values" ("value" . 1967)))
;;    ("stat" ("null_count" ("value" . 0))) ("base_row_id" . 0))
;;   (("op" . "remove") ("table" . "foo")
;;    ("system_time" . @2023-05-09T09:01:06.384631+02:00) ("date_change" . t)
;;    ("path" . "foo/f5c18e7b-d1bf-4ba5-85dd-e63ddc5931bf.arrow")
;;    ("dv_path" . "foo/_dv/44ccbf3f-b223-4581-9cd8-a7e569120ada.arrow"))))
