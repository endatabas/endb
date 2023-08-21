(defpackage :endb/json
  (:use :cl)
  (:export #:json-parse #:*json-ld-scalars* #:json-stringify #:json-merge-patch #:json-diff
           #:resolve-json-ld-xsd-scalars
           #:binary-to-bloom #:binary-bloom-member-p #:calculate-stats
           #:random-uuid #:random-uuid-p)
  (:import-from :alexandria)
  (:import-from :cl-ppcre)
  (:import-from :cffi)
  (:import-from :com.inuoe.jzon)
  (:import-from :endb/arrow)
  (:import-from :endb/lib/arrow)
  (:import-from :fset)
  (:import-from :qbase64)
  (:import-from :cl-bloom))
(in-package :endb/json)

;; https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
;; https://www.w3.org/TR/xmlschema11-2/

(defun %json-to-fset (x)
  (cond
    ((hash-table-p x)
     (loop with acc = (fset:empty-map)
           for k being the hash-key
             using (hash-value v)
               of x
           do (setf acc (fset:with acc k (%json-to-fset v)))
           finally (return acc)))
    ((and (vectorp x) (not (stringp x)))
     (fset:convert 'fset:seq (map 'vector #'%json-to-fset x)))
    (t x)))

(defun resolve-json-ld-xsd-scalars (x)
  (cond
    ((fset:map? x)
     (if (and (= 2 (fset:size x))
              (equalp (fset:set "@value" "@type")
                      (fset:domain x)))
         (let ((k (fset:lookup x "@type"))
               (v (fset:lookup x "@value")))
           (cond
             ((or (equal "xsd:date" k)
                  (equal "http://www.w3.org/2001/XMLSchema#date" k))
              (endb/arrow:parse-arrow-date-millis v))
             ((or (equal "xsd:dateTime" k)
                  (equal "http://www.w3.org/2001/XMLSchema#dateTime" k))
              (endb/arrow:parse-arrow-timestamp-micros v))
             ((or (equal "xsd:time" k)
                  (equal "http://www.w3.org/2001/XMLSchema#time" k))
              (endb/arrow:parse-arrow-time-micros v))
             ((or (equal "xsd:duration" k)
                  (equal "http://www.w3.org/2001/XMLSchema#duration" k))
              (endb/arrow:parse-arrow-interval-month-day-nanos v))
             ((or (equal "xsd:base64Binary" k)
                  (equal "http://www.w3.org/2001/XMLSchema#base64Binary" k))
              (qbase64:decode-string v))
             ((or (equal "xsd:integer" k)
                  (equal "http://www.w3.org/2001/XMLSchema#integer" k))
              (let ((x (parse-integer v)))
                (if (> (integer-length x) 127)
                    (coerce x 'double-float)
                    x)))
             (t x)))
         (fset:reduce (lambda (acc k v)
                        (fset:with acc k (resolve-json-ld-xsd-scalars v)))
                      x
                      :initial-value (fset:empty-map))))
    ((fset:seq? x)
     (fset:reduce (lambda (acc x)
                    (fset:with-last acc (resolve-json-ld-xsd-scalars x)))
                  x
                  :initial-value (fset:empty-seq)))
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

(defvar *json-ld-scalars* t)

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-timestamp-micros))
  (if *json-ld-scalars*
      (com.inuoe.jzon:with-object writer
        (com.inuoe.jzon:write-key writer "@value")
        (com.inuoe.jzon:write-value writer (format nil "~A" value))
        (com.inuoe.jzon:write-key writer "@type")
        (com.inuoe.jzon:write-value writer "xsd:dateTime"))
      (com.inuoe.jzon:write-value writer (format nil "~A" value))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-date-millis))
  (if *json-ld-scalars*
      (com.inuoe.jzon:with-object writer
        (com.inuoe.jzon:write-key writer "@value")
        (com.inuoe.jzon:write-value writer (format nil "~A" value))
        (com.inuoe.jzon:write-key writer "@type")
        (com.inuoe.jzon:write-value writer "xsd:date"))
      (com.inuoe.jzon:write-value writer (format nil "~A" value))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-time-micros))
  (if *json-ld-scalars*
      (com.inuoe.jzon:with-object writer
        (com.inuoe.jzon:write-key writer "@value")
        (com.inuoe.jzon:write-value writer (format nil "~A" value))
        (com.inuoe.jzon:write-key writer "@type")
        (com.inuoe.jzon:write-value writer "xsd:time"))
      (com.inuoe.jzon:write-value writer (format nil "~A" value))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value endb/arrow:arrow-interval-month-day-nanos))
  (if *json-ld-scalars*
      (com.inuoe.jzon:with-object writer
        (com.inuoe.jzon:write-key writer "@value")
        (com.inuoe.jzon:write-value writer (format nil "~A" value))
        (com.inuoe.jzon:write-key writer "@type")
        (com.inuoe.jzon:write-value writer "xsd:duration"))
      (com.inuoe.jzon:write-value writer (format nil "~A" value))))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value vector))
  (if (typep value 'endb/arrow:arrow-binary)
      (if *json-ld-scalars*
          (com.inuoe.jzon:with-object writer
            (com.inuoe.jzon:write-key writer "@value")
            (com.inuoe.jzon:write-value writer (qbase64:encode-bytes value))
            (com.inuoe.jzon:write-key writer "@type")
            (com.inuoe.jzon:write-value writer "xsd:base64Binary"))
          (com.inuoe.jzon:write-value writer (qbase64:encode-bytes value)))
      (call-next-method)))

(defparameter +max-safe-integer+ (1- (ash 1 53)))
(defparameter +min-safe-integer+ (- +max-safe-integer+))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value integer))
  (if (or (> value +max-safe-integer+)
          (< value +min-safe-integer+))
      (if *json-ld-scalars*
          (com.inuoe.jzon:with-object writer
            (com.inuoe.jzon:write-key writer "@value")
            (com.inuoe.jzon:write-value writer (format nil "~A" value))
            (com.inuoe.jzon:write-key writer "@type")
            (com.inuoe.jzon:write-value writer "xsd:integer"))
          (com.inuoe.jzon:write-value writer (format nil "~A" value)))
      (com.inuoe.jzon::%write-json-atom writer value)))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value (eql :null)))
  (com.inuoe.jzon:write-value writer 'null))

(defun json-parse (in)
  (resolve-json-ld-xsd-scalars (%json-to-fset (com.inuoe.jzon:parse in))))

(defun json-stringify (x &key stream pretty)
  (com.inuoe.jzon:stringify x :stream stream :pretty pretty))

;; https://datatracker.ietf.org/doc/html/rfc7386

(defun json-merge-patch (target patch)
  (if (fset:map? patch)
      (fset:reduce
       (lambda (target k v)
         (if (eq 'null v)
             (fset:less target k)
             (let ((target-v (fset:lookup target k)))
               (fset:with target k (json-merge-patch target-v v)))))
       patch
       :initial-value (if (fset:map? target)
                          target
                          (fset:empty-map)))
      patch))

(defun json-diff (version-a version-b)
  (if (and (fset:map? version-a)
           (fset:map? version-b))
      (fset:reduce
       (lambda (diff k b-v)
         (let ((a-v (fset:lookup version-a k)))
           (if (fset:equal? a-v b-v)
               diff
               (fset:with diff k (json-diff a-v b-v)))))
       version-b
       :initial-value
       (fset:reduce
        (lambda (diff k)
          (fset:with diff k 'null))
        (fset:set-difference (fset:domain version-a)
                             (fset:domain version-b))
        :initial-value (fset:empty-map)))
      version-b))

(defun binary-to-bloom (binary)
  (let ((bloom (make-instance 'cl-bloom::bloom-filter :order (* (length binary) 8))))
    (cffi:with-pointer-to-vector-data (ptr binary)
      (endb/lib/arrow:buffer-to-vector ptr (length binary) (cl-bloom::filter-array bloom)))
    bloom))

(defun binary-bloom-member-p (binary element)
  (let* ((order (* 8 (length binary)))
         (hash1 (murmurhash:murmurhash element :seed murmurhash:*default-seed*))
         (hash2 (murmurhash:murmurhash element :seed hash1)))
    (loop for i to (1- (cl-bloom::opt-degree))
          for index = (cl-bloom::fake-hash hash1 hash2 i order)
          always (multiple-value-bind (byte-index bit-index)
                     (truncate index 8)
                   (logbitp bit-index (aref binary byte-index))))))
