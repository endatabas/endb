(defpackage :endb-test/lib/arrow
  (:use :cl :fiveam :endb/lib/arrow)
  (:import-from :endb/arrow)
  (:import-from :asdf))
(in-package :endb-test/lib/arrow)

(in-suite* :all-tests)

(test arrow-ffi
  (let ((expected (coerce #(65 82 82 79 87 49 0 0 255 255 255 255 120 0 0 0 4 0 0 0 242 255 255 255 20 0
                            0 0 4 0 1 0 0 0 10 0 11 0 8 0 10 0 4 0 248 255 255 255 12 0 0 0 8 0 8 0 0 0 4
                            0 1 0 0 0 4 0 0 0 236 255 255 255 56 0 0 0 32 0 0 0 24 0 0 0 1 2 0 0 16 0 18
                            0 4 0 16 0 17 0 8 0 0 0 12 0 0 0 0 0 244 255 255 255 64 0 0 0 1 0 0 0 8 0 9 0
                            4 0 8 0 1 0 0 0 97 0 0 0 255 255 255 255 136 0 0 0 4 0 0 0 236 255 255 255 64
                            0 0 0 0 0 0 0 20 0 0 0 4 0 3 0 12 0 19 0 16 0 18 0 12 0 4 0 230 255 255 255 1
                            0 0 0 0 0 0 0 64 0 0 0 20 0 0 0 0 0 0 0 0 0 10 0 20 0 4 0 12 0 16 0 2 0 0 0 0
                            0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 8 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0
                            1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
                            0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
                            0 0 0 0 0 0 255 255 255 255 0 0 0 0 8 0 0 0 0 0 0 0 236 255 255 255 64 0 0 0
                            56 0 0 0 20 0 0 0 4 0 0 0 12 0 18 0 16 0 4 0 8 0 12 0 1 0 0 0 136 0 0 0 0 0 0
                            0 144 0 0 0 0 0 0 0 64 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 248 255 255 255 12 0 0 0
                            8 0 8 0 0 0 4 0 1 0 0 0 4 0 0 0 236 255 255 255 56 0 0 0 32 0 0 0 24 0 0 0 1
                            2 0 0 16 0 18 0 4 0 16 0 17 0 8 0 0 0 12 0 0 0 0 0 244 255 255 255 64 0 0 0 1
                            0 0 0 8 0 9 0 4 0 8 0 1 0 0 0 97 0 166 0 0 0 65 82 82 79 87 49)
                          '(vector (unsigned-byte 8))))
        (array '((("a" . 1)))))

    (is (equalp expected (write-arrow-arrays-to-ipc-buffer
                          (list (endb/arrow:to-arrow array)))))
    (is (equal (list array)
               (loop for x in (read-arrow-arrays-from-ipc-buffer expected)
                     collect (coerce x 'list))))

    (dolist (array '((1 :null 2 4 8)
                     (1 2 3 4 8)
                     (#(12 -7 25) :null #(0 -127 127 50) #())
                     (#(#(1 2) #(3 4)) #(#(5 6 7) :null #(8)) #(#(9 10)))
                     (1.2d0 :null 3.4d0 5)
                     ;; wrapped in a list as a top-level row cannot be null.
                     (#((("name" . "joe") ("id" . 1))
                        (("name" . :null) ("id" . 2))
                        :null
                        (("name" . "mark") ("id" . 4))))))
      (is (equalp (list array)
                  (loop for x in (read-arrow-arrays-from-ipc-buffer
                                  (write-arrow-arrays-to-ipc-buffer
                                   (list (endb/arrow:to-arrow array))))
                        collect (mapcar #'cdar (coerce x 'list))))))

    (let ((arrays '((1 :null 2 4 8)
                    (1 2 3 4 8))))
      (is (equalp arrays
                  (loop for x in (read-arrow-arrays-from-ipc-buffer
                                  (write-arrow-arrays-to-ipc-buffer
                                   (mapcar #'endb/arrow:to-arrow arrays)))
                        collect (mapcar #'cdar (coerce x 'list))))))))

(test arrow-ffi-ipc-files
  (let* ((target-dir (asdf:system-relative-pathname :endb-test "target/"))
         (test-file (merge-pathnames "example.arrow" target-dir))
         (arrays '(((("a" . 1)))
                   ((("a" . 2))))))
    (ensure-directories-exist target-dir)
    (unwind-protect
         (progn
           (write-arrow-arrays-to-ipc-file test-file (mapcar #'endb/arrow:to-arrow arrays))
           (is (equal arrays (loop for x in (read-arrow-arrays-from-ipc-file test-file)
                                   collect (coerce x 'list)))))
      (when (probe-file test-file)
        (delete-file test-file)))))