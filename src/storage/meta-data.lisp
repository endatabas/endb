(defpackage :endb/storage/meta-data
  (:use :cl)
  (:export #:json->fset #:fset->json #:fset-merge-patch #:random-uuid #:random-uuid-p)
  (:import-from :com.inuoe.jzon)
  (:import-from :fset))
(in-package :endb/storage/meta-data)

(defun %jzon->fset (x)
  (cond
    ((hash-table-p x)
     (loop with acc = (fset:empty-map)
           for k being the hash-key
             using (hash-value v)
               of x
           do (setf acc (fset:with acc k (%jzon->fset v)))
           finally (return acc)))
    ((and (vectorp x) (not (stringp x)))
     (fset:convert 'fset:seq (map 'vector #'%jzon->fset x)))
    (t x)))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value fset:map))
  (com.inuoe.jzon:with-object writer
    (fset:reduce
     (lambda (w k v)
       (com.inuoe.jzon:write-key w k)
       (com.inuoe.jzon:write-value w v)
       w)
     value
     :initial-value writer)))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value fset:seq))
  (com.inuoe.jzon:with-array writer
    (fset:reduce
     (lambda (w v)
       (com.inuoe.jzon:write-value w v)
       w)
     value
     :initial-value writer)))

(defun json->fset (in)
  (%jzon->fset (com.inuoe.jzon:parse in)))

(defun fset->json (x &key stream pretty)
  (com.inuoe.jzon:stringify x :stream stream :pretty pretty))

;; https://datatracker.ietf.org/doc/html/rfc7386

(defun fset-merge-patch (target patch)
  (if (fset:map? patch)
      (fset:reduce
       (lambda (target k v)
         (if (eq 'null v)
             (fset:less target k)
             (let ((target-v (fset:lookup target k)))
               (fset:with target k (fset-merge-patch target-v v)))))
       patch
       :initial-value (if (fset:map? target)
                          target
                          (fset:empty-map)))
      patch))

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

(defparameter +random-uuid-scanner+
  (ppcre:create-scanner "^[\\da-f]{8}-[\\da-f]{4}-4[\\da-f]{3}-[89ab][\\da-f]{3}-[\\da-f]{12}$"))

(defun random-uuid-p (x)
  (and (stringp x)
       (not (null (ppcre:scan +random-uuid-scanner+ x)))))
