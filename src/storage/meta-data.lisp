(defpackage :endb/storage/meta-data
  (:use :cl)
  (:export #:json->meta-data #:meta-data->json #:meta-data-merge-patch #:meta-data-diff #:random-uuid #:random-uuid-p)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :endb/arrow)
  (:import-from :fset)
  (:import-from :qbase64))
(in-package :endb/storage/meta-data)

;; https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
;; https://www.w3.org/TR/xmlschema11-2/

(defun %jzon->meta-data (x)
  (cond
    ((hash-table-p x)
     (if (and (= 2 (hash-table-count x))
              (subsetp '("@value" "@type")
                       (alexandria:hash-table-keys x)
                       :test 'equal))
         (let ((k (gethash "@type" x))
               (v (gethash "@value" x)))
           (cond
             ((equal "xsd:date" k)
              (endb/arrow:parse-arrow-date-days v))
             ((equal "xsd:dateTime" k)
              (endb/arrow:parse-arrow-timestamp-micros v))
             ((equal "xsd:time" k)
              (endb/arrow:parse-arrow-time-micros v))
             ((equal "xsd:base64Binary" k)
              (qbase64:decode-string v))))
         (loop with acc = (fset:empty-map)
               for k being the hash-key
                 using (hash-value v)
                   of x
               do (setf acc (fset:with acc k (%jzon->meta-data v)))
               finally (return acc))))
    ((and (vectorp x) (not (stringp x)))
     (fset:convert 'fset:seq (map 'vector #'%jzon->meta-data x)))
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

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-timestamp-micros))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "@value")
    (com.inuoe.jzon:write-value writer (format nil "~A" value))
    (com.inuoe.jzon:write-key writer "@type")
    (com.inuoe.jzon:write-value writer "xsd:dateTime")))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-date-days))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "@value")
    (com.inuoe.jzon:write-value writer (format nil "~A" value))
    (com.inuoe.jzon:write-key writer "@type")
    (com.inuoe.jzon:write-value writer "xsd:date")))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-time-micros))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "@value")
    (com.inuoe.jzon:write-value writer (format nil "~A" value))
    (com.inuoe.jzon:write-key writer "@type")
    (com.inuoe.jzon:write-value writer "xsd:time")))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value vector))
  (check-type value endb/arrow:arrow-binary)
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "@value")
    (com.inuoe.jzon:write-value writer (qbase64:encode-bytes value))
    (com.inuoe.jzon:write-key writer "@type")
    (com.inuoe.jzon:write-value writer "xsd:base64Binary")))

(defun json->meta-data (in)
  (%jzon->meta-data (com.inuoe.jzon:parse in)))

(defun meta-data->json (x &key stream pretty)
  (com.inuoe.jzon:stringify x :stream stream :pretty pretty))

(defmethod fset:compare ((x endb/arrow:arrow-date-days) (y endb/arrow:arrow-date-days))
  (fset:compare (endb/arrow:arrow-date-days-day x)
                (endb/arrow:arrow-date-days-day y)))

(defmethod fset:compare ((x endb/arrow:arrow-timestamp-micros) (y endb/arrow:arrow-timestamp-micros))
  (fset:compare (endb/arrow:arrow-timestamp-micros-us x)
                (endb/arrow:arrow-timestamp-micros-us y)))

(defmethod fset:compare ((x endb/arrow:arrow-time-micros) (y endb/arrow:arrow-time-micros))
  (fset:compare (endb/arrow:arrow-time-micros-us x)
                (endb/arrow:arrow-time-micros-us y)))

(fset:define-cross-type-compare-methods endb/arrow:arrow-date-days)
(fset:define-cross-type-compare-methods endb/arrow:arrow-timestamp-micros)
(fset:define-cross-type-compare-methods endb/arrow:arrow-time-micros)

;; https://datatracker.ietf.org/doc/html/rfc7386

(defun meta-data-merge-patch (target patch)
  (if (fset:map? patch)
      (fset:reduce
       (lambda (target k v)
         (if (eq 'null v)
             (fset:less target k)
             (let ((target-v (fset:lookup target k)))
               (fset:with target k (meta-data-merge-patch target-v v)))))
       patch
       :initial-value (if (fset:map? target)
                          target
                          (fset:empty-map)))
      patch))

(defun meta-data-diff (version-a version-b)
  (if (and (fset:map? version-a)
           (fset:map? version-b))
      (fset:reduce
       (lambda (diff k b-v)
         (let ((a-v (fset:lookup version-a k)))
           (if (fset:equal? a-v b-v)
               diff
               (fset:with diff k (meta-data-diff a-v b-v)))))
       version-b
       :initial-value
       (fset:reduce
        (lambda (diff k)
          (fset:with diff k 'null))
        (fset:set-difference (fset:domain version-a)
                             (fset:domain version-b))
        :initial-value (fset:empty-map)))
      version-b))

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
