(defpackage :endb/bloom
  (:use :cl)
  (:export #:make-sbbf #:sbbf-insert #:sbbf-check-p))
(in-package :endb/bloom)

;; Split Block Bloom Filter:
;; https://github.com/apache/parquet-format/blob/master/BloomFilter.md

(defparameter +sbbf-block-size+ 256)
(defparameter +sbbf-block-shift+ 8)
(defparameter +sbbf-word-size+ 32)
(defparameter +sbbf-word-shift+ 5)

(defparameter +sbbf-salt+ (make-array 8
                                      :initial-contents (list #x47b6137b #x44974d91 #x8824ad5b
                                                              #xa2b7289d #x705495c7 #x2df1424b
                                                              #x9efc4947 #x5c6bfb31)
                                      :element-type '(unsigned-byte 32)))

(defun %sbbf-bit-size (expected-size fill-ratio)
  (* +sbbf-block-size+ (ceiling (/ expected-size fill-ratio) +sbbf-block-size+)))

(defun make-sbbf (expected-size &key (fill-ratio 1/10))
  (make-array (ash (%sbbf-bit-size expected-size fill-ratio) -3) :element-type '(unsigned-byte 8)))

(defun %sbbf-block-element-idx (block-bit-idx i x)
  (+ block-bit-idx
     (ash i +sbbf-word-shift+)
     (ash (ldb (byte 32 0) (* x (aref +sbbf-salt+ i))) -27)))

(defun %sbbf-block-insert (filter block-bit-idx x)
  (dotimes (i +sbbf-block-shift+)
    (let* ((element-idx (%sbbf-block-element-idx block-bit-idx i x))
           (byte-idx (ash element-idx -3))
           (bit-idx (ldb (byte 3 0) element-idx)))
      (setf (aref filter byte-idx)
            (logior (aref filter byte-idx) (ash 1 bit-idx))))))

(defun %sbbf-block-check-p (filter block-bit-idx x)
  (dotimes (i +sbbf-block-shift+)
    (let* ((element-idx (%sbbf-block-element-idx block-bit-idx i x))
           (byte-idx (ash element-idx -3))
           (bit-idx (ldb (byte 3 0) element-idx)))
      (unless (logbitp bit-idx (aref filter byte-idx))
        (return-from %sbbf-block-check-p nil))))
  t)

(defun %sbbf-number-of-blocks (filter)
  (ash (length filter) (- +sbbf-block-shift+)))

(defun %sbbf-block-idx (filter x)
  (ldb (byte 32 32) (* (ldb (byte 32 32) x) (%sbbf-number-of-blocks filter))))

(defun sbbf-insert (filter x)
  (let* ((block-bit-idx (ash (%sbbf-block-idx filter x) +sbbf-block-shift+)))
    (%sbbf-block-insert filter block-bit-idx (ldb (byte 32 0) x))))

(defun sbbf-check-p (filter x)
  (when filter
    (let* ((block-bit-idx (ash (%sbbf-block-idx filter x) +sbbf-block-shift+)))
      (%sbbf-block-check-p filter block-bit-idx (ldb (byte 32 0) x)))))
