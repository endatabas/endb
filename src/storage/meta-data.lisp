(defpackage :endb/storage/meta-data
  (:use :cl)
  (:export #:json->hamt #:hamt->json #:hamt-deep-equal #:hamt-merge-patch)
  (:import-from :cl-hamt)
  (:import-from :com.inuoe.jzon))
(in-package :endb/storage/meta-data)

(defun %jzon->hamt (x)
  (cond
    ((hash-table-p x)
     (loop with acc = (hamt:empty-dict)
           for k being the hash-key
             using (hash-value v)
               of x
           do (setf acc (hamt:dict-insert acc k (%jzon->hamt v)))
           finally (return acc)))
    ((and (vectorp x) (not (stringp x)))
     (map 'vector #'%jzon->hamt x))
    (t x)))

(defmethod com.inuoe.jzon:write-value ((writer com.inuoe.jzon:writer) (value hamt:hash-dict))
  (com.inuoe.jzon:with-object writer
    (hamt:dict-reduce
     (lambda (w k v)
       (com.inuoe.jzon:write-key w k)
       (com.inuoe.jzon:write-value w v)
       w)
     value
     writer)))

(defun json->hamt (in)
  (%jzon->hamt (com.inuoe.jzon:parse in)))

(defun hamt->json (x)
  (com.inuoe.jzon:stringify x))

(defun hamt-deep-equal (x y)
  (equal (hamt->json x) (hamt->json y)))

;; https://datatracker.ietf.org/doc/html/rfc7386

(defun hamt-merge-patch (target patch)
  (if (typep patch 'hamt:hash-dict)
      (hamt:dict-reduce
       (lambda (target k v)
         (if (eq 'null v)
             (hamt:dict-remove target k)
             (let ((target-v (hamt:dict-lookup target k)))
               (hamt:dict-insert target k (hamt-merge-patch target-v v)))))
       patch
       (if (typep target 'hamt:hash-dict)
           target
           (hamt:empty-dict)))
      patch))
