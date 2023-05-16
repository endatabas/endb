(defpackage :endb/storage/meta-data
  (:use :cl)
  (:export #:json->meta-data #:meta-data->json #:meta-data-merge-patch #:random-uuid #:random-uuid-p)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :com.inuoe.jzon)
  (:import-from :endb/arrow)
  (:import-from :endb/sql/parser)
  (:import-from :fset)
  (:import-from :local-time))
(in-package :endb/storage/meta-data)

;; https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
;; https://www.w3.org/TR/xmlschema11-2/

(defun %jzon->meta-data (x)
  (cond
    ((hash-table-p x)
     (if (and (= 1 (hash-table-count x))
              (member (first (alexandria:hash-table-keys x))
                      '("xsd:date" "xsd:dateTime" "xsd:time" "xsd:hexBinary")
                      :test 'equal))
         (destructuring-bind (k v)
             (alexandria:hash-table-plist x)
           (cond
             ((equal "xsd:date" k)
              (let ((date (local-time:parse-timestring v :allow-missing-time-part t)))
                (make-instance 'endb/arrow:arrow-date :day (local-time:day-of date))))
             ((equal "xsd:dateTime" k)
              (local-time:parse-timestring v))
             ((equal "xsd:time" k)
              (let ((time (local-time:parse-timestring v :allow-missing-date-part t)))
                (make-instance 'endb/arrow:arrow-time :sec (local-time:sec-of time) :nsec (local-time:nsec-of time))))
             ((equal "xsd:hexBinary" k)
              (endb/sql/parser::hex-to-binary v))))
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

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value local-time:timestamp))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "xsd:dateTime")
    (com.inuoe.jzon:write-value writer (local-time:format-rfc3339-timestring nil value :timezone local-time:+utc-zone+))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-date))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "xsd:date")
    (com.inuoe.jzon:write-value writer (local-time:format-rfc3339-timestring nil value :omit-time-part t :omit-timezone-part t :timezone local-time:+utc-zone+))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-time))
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "xsd:time")
    (com.inuoe.jzon:write-value writer (local-time:format-rfc3339-timestring nil value :omit-date-part t :omit-timezone-part t :timezone local-time:+utc-zone+))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value vector))
  (check-type value endb/arrow:arrow-binary)
  (com.inuoe.jzon:with-object writer
    (com.inuoe.jzon:write-key writer "xsd:hexBinary")
    (com.inuoe.jzon:write-value writer (format nil "~{~02,'0X~}" (coerce value 'list)))))

(defun json->meta-data (in)
  (%jzon->meta-data (com.inuoe.jzon:parse in)))

(defun meta-data->json (x &key stream pretty)
  (com.inuoe.jzon:stringify x :stream stream :pretty pretty))

(defmethod fset:compare ((a local-time:timestamp) (b local-time:timestamp))
  (cond ((local-time:timestamp< a b) ':less)
        ((local-time:timestamp> a b) ':greater)
        (t ':equal)))

(fset:define-cross-type-compare-methods local-time:timestamp)

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
