(defpackage :endb/sql/db
  (:use :cl)
  (:import-from :bordeaux-threads)
  (:import-from :cl-bloom)
  (:import-from :cl-ppcre)
  (:import-from :endb/arrow)
  (:import-from :endb/sql/expr)
  (:import-from :endb/lib/parser)
  (:import-from :endb/storage/buffer-pool)
  (:import-from :local-time)
  (:import-from :fset)
  (:export #:ddl-create-table #:ddl-drop-table #:ddl-create-view #:ddl-drop-view #:ddl-create-index #:ddl-drop-index #:ddl-create-assertion #:ddl-drop-assertion
           #:dml-insert #:dml-insert-objects #:dml-delete #:dml-erase

           #:syn-current_date #:syn-current_time #:syn-current_timestamp

           #:make-db #:copy-db #:db-buffer-pool #:db-store #:db-meta-data #:db-current-timestamp #:db-write-lock
           #:base-table #:base-table-rows #:base-table-deleted-row-ids #:table-type #:table-columns #:constraint-definitions
           #:base-table-meta #:base-table-arrow-batches #:base-table-visible-rows #:base-table-size #:batch-row-system-time-end
           #:view-definition #:calculate-stats #:arrow-files-compacted-filename #:find-files-to-compact #:merge-files #:compact-files))
(in-package :endb/sql/db)

;; DML/DDL

(defvar *default-schema* "main")

(defstruct db
  store
  buffer-pool
  (meta-data (fset:map ("_last_tx" 0)))
  current-timestamp
  (information-schema-cache (make-hash-table :weakness :key :test 'eq))
  (write-lock (bt:make-lock)))

(defun syn-current_date (db)
  (endb/sql/expr:syn-cast (syn-current_timestamp db) :date))

(defun syn-current_time (db)
  (endb/sql/expr:syn-cast (syn-current_timestamp db) :time))

(defun syn-current_timestamp (db)
  (or (db-current-timestamp db)
      (endb/arrow:local-time-to-arrow-timestamp-micros (local-time:now))))

(defun base-table-meta (db table-name)
  (with-slots (meta-data) db
    (or (fset:lookup meta-data table-name) (fset:empty-map))))

(defun %batch-key (table-name batch-file)
  (format nil "~A/~A" table-name batch-file))

(defun base-table-arrow-batches (db table-name arrow-file &key sha1)
  (with-slots (buffer-pool) db
    (endb/storage/buffer-pool:buffer-pool-get buffer-pool (%batch-key table-name arrow-file) :sha1 sha1)))

(defun base-table-visible-rows (db table-name &key arrow-file-idx-row-id-p)
  (with-slots (information-schema-cache) db
    (let ((table-md (base-table-meta db table-name))
          (cachep (and (%information-schema-table-p table-name)
                       (not arrow-file-idx-row-id-p))))
      (or (when cachep
            (gethash table-md information-schema-cache))
          (let ((kw-projection (loop for c in (table-columns db table-name)
                                     collect (intern c :keyword)))
                (acc))
            (fset:do-map (arrow-file arrow-file-md table-md)
              (loop with deleted-md = (or (fset:lookup arrow-file-md "deleted") (fset:empty-map))
                    with erased-md = (or (fset:lookup arrow-file-md "erased") (fset:empty-map))
                    for batch-row in (base-table-arrow-batches db table-name arrow-file :sha1 (fset:lookup arrow-file-md "sha1"))
                    for batch = (endb/arrow:arrow-struct-column-array batch-row (intern table-name :keyword))
                    for batch-idx from 0
                    for batch-idx-string = (prin1-to-string batch-idx)
                    for batch-deleted = (or (fset:lookup deleted-md batch-idx-string) (fset:empty-seq))
                    for batch-erased = (or (fset:lookup erased-md batch-idx-string) (fset:empty-seq))
                    do (setf acc (append acc (loop for row-id below (endb/arrow:arrow-length batch)
                                                   unless (or (fset:find row-id batch-deleted
                                                                         :key (lambda (x)
                                                                                (fset:lookup x "row_id")))
                                                              (fset:find row-id batch-erased))
                                                     collect (if arrow-file-idx-row-id-p
                                                                 (cons (list arrow-file batch-idx row-id)
                                                                       (endb/arrow:arrow-struct-projection batch row-id kw-projection))
                                                                 (endb/arrow:arrow-struct-projection batch row-id kw-projection)))))))
            (when cachep
              (setf (gethash table-md information-schema-cache) acc))
            acc)))))

(defun batch-row-system-time-end (batch-deleted row-id)
  (fset:lookup (or (fset:find-if (lambda (x)
                                   (= row-id (fset:lookup x "row_id")))
                                 batch-deleted)
                   (fset:map ("system_time_end" endb/sql/expr:+end-of-time+)))
               "system_time_end"))

(defun %information-schema-table-p (table-name)
  (member table-name
          '("information_schema.columns"
            "information_schema.tables"
            "information_schema.views"
            "information_schema.check_constraints")
          :test 'equal))

(defun table-type (db table-name)
  (if (%information-schema-table-p table-name)
      "BASE TABLE"
      (let* ((table-row (find-if (lambda (row)
                                   (equal table-name (nth 2 row)))
                                 (base-table-visible-rows db "information_schema.tables"))))
        (nth 3 table-row))))

(defun view-definition (db view-name)
  (let* ((view-row (find-if (lambda (row)
                              (equal view-name (nth 2 row)))
                            (base-table-visible-rows db "information_schema.views"))))
    (endb/lib/parser:parse-sql (nth 3 view-row))))

(defun constraint-definitions (db)
  (if endb/sql/expr:*sqlite-mode*
      (fset:empty-map)
      (fset:convert 'fset:map
                    (loop for constraint-row in (base-table-visible-rows db "information_schema.check_constraints")
                          collect (cons (nth 2 constraint-row)
                                        (endb/lib/parser:parse-sql (format nil "SELECT ~A" (nth 3 constraint-row))))))))

(defun table-columns (db table-name)
  (cond
    ((equal "information_schema.columns" table-name)
     '("table_catalog" "table_schema" "table_name" "column_name" "ordinal_position"))
    ((equal "information_schema.tables" table-name)
     '("table_catalog" "table_schema" "table_name" "table_type"))
    ((equal "information_schema.views" table-name)
     '("table_catalog" "table_schema" "table_name" "view_definition"))
    ((equal "information_schema.check_constraints" table-name)
     '("constraint_catalog" "constraint_schema" "constraint_name" "check_clause"))
    (t (mapcar #'second (endb/sql/expr:ra-order-by (loop with rows = (base-table-visible-rows db "information_schema.columns")
                                                         for (nil nil table c idx) in rows
                                                         when (equal table-name table)
                                                           collect (list idx c))
                                                   (list (list 1 :asc) (list 2 :asc)))))))

(defun base-table-created-p (db table-name)
  (or (%information-schema-table-p table-name)
      (loop with rows = (base-table-visible-rows db "information_schema.columns")
            for (nil nil table nil idx) in rows
            thereis (and (equal table-name table)
                         (plusp idx)))))

(defun %fset-values (m)
  (fset:reduce (lambda (acc k v)
                 (declare (ignore k))
                 (vector-push-extend v acc)
                 acc)
               m
               :initial-value (make-array 0 :fill-pointer 0)))

(defun base-table-size (db table-name)
  (let ((table-md (base-table-meta db table-name)))
    (reduce (lambda (acc md)
              (+ acc (- (fset:lookup md "length")
                        (reduce (lambda (acc x)
                                  (+ acc (fset:size x)))
                                (%fset-values (or (fset:lookup md "deleted") (fset:empty-map)))
                                :initial-value 0))))
            (%fset-values table-md)
            :initial-value 0)))

(defun %find-arrow-file-idx-row-id (db table-name predicate)
  (loop for (arrow-file-idx-row-id . row) in (base-table-visible-rows db table-name :arrow-file-idx-row-id-p t)
        when (funcall predicate row)
          do (return arrow-file-idx-row-id)))

(defun ddl-create-table (db table-name columns)
  (unless endb/sql/expr:*sqlite-mode*
    (error 'endb/sql/expr:sql-runtime-error :message "CREATE TABLE not supported"))
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal table-name (nth 2 row))))
    (dml-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
    (dml-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* table-name c idx)))
    (values nil t)))

(defun ddl-drop-table (db table-name &key if-exists)
  (unless endb/sql/expr:*sqlite-mode*
    (error 'endb/sql/expr:sql-runtime-error :message "DROP TABLE not supported"))
  (with-slots (meta-data) db
    (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                           "information_schema.tables"
                                                           (lambda (row)
                                                             (and (equal table-name (nth 2 row)) (equal "BASE TABLE" (nth 3 row)))))))
      (when batch-file-row-id
        (dml-delete db "information_schema.tables" (list batch-file-row-id))
        (dml-delete db "information_schema.columns" (loop for c in (table-columns db table-name)
                                                          collect (%find-arrow-file-idx-row-id db
                                                                                               "information_schema.columns"
                                                                                               (lambda (row)
                                                                                                 (and (equal table-name (nth 2 row)) (equal c (nth 3 row)))))))

        (setf meta-data (fset:less meta-data table-name)))

      (when (or batch-file-row-id if-exists)
        (values nil t)))))

(defun ddl-create-view (db view-name query columns)
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.tables"
                                       (lambda (row)
                                         (equal view-name (nth 2 row))))
    (dml-insert db "information_schema.tables" (list (list :null *default-schema* view-name "VIEW")))
    (dml-insert db "information_schema.views" (list (list :null *default-schema* view-name query)))
    (dml-insert db "information_schema.columns" (loop for c in columns
                                                      for idx from 1
                                                      collect  (list :null *default-schema* view-name c idx)))
    (values nil t)))

(defun ddl-drop-view (db view-name &key if-exists)
  (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                         "information_schema.tables"
                                                         (lambda (row)
                                                           (and (equal view-name (nth 2 row)) (equal "VIEW" (nth 3 row)))))))
    (when batch-file-row-id
      (dml-delete db "information_schema.tables" (list batch-file-row-id))
      (dml-delete db "information_schema.columns" (loop for c in (table-columns db view-name)
                                                        collect (%find-arrow-file-idx-row-id db
                                                                                             "information_schema.columns"
                                                                                             (lambda (row)
                                                                                               (and (equal view-name (nth 2 row)) (equal c (nth 3 row)))))))
      (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                             "information_schema.views"
                                                             (lambda (row)
                                                               (equal view-name (nth 2 row))))))
        (dml-delete db "information_schema.views" (list batch-file-row-id))))
    (when (or batch-file-row-id if-exists)
      (values nil t))))

(defun ddl-create-assertion (db constraint-name check-clause)
  (unless (%find-arrow-file-idx-row-id db
                                       "information_schema.check_constraints"
                                       (lambda (row)
                                         (equal constraint-name (nth 2 row))))
    (dml-insert db "information_schema.check_constraints"
                (list (list :null *default-schema* constraint-name check-clause)))
    (values nil t)))

(defun ddl-drop-assertion (db constraint-name &key if-exists)
  (let* ((batch-file-row-id (%find-arrow-file-idx-row-id db
                                                         "information_schema.check_constraints"
                                                         (lambda (row)
                                                           (equal constraint-name (nth 2 row))))))
    (when batch-file-row-id
      (dml-delete db "information_schema.check_constraints" (list batch-file-row-id)))
    (when (or batch-file-row-id if-exists)
      (values nil t))))

(defun ddl-create-index (db)
  (declare (ignore db))
  (unless endb/sql/expr:*sqlite-mode*
    (error 'endb/sql/expr:sql-runtime-error :message "CREATE INDEX not supported"))
  (values nil t))

(defun ddl-drop-index (db)
  (declare (ignore db))
  (unless endb/sql/expr:*sqlite-mode*
    (error 'endb/sql/expr:sql-runtime-error :message "DROP INDEX not supported")))

(defmethod endb/sql/expr:agg-accumulate ((agg cl-bloom::bloom-filter) x &rest args)
  (declare (ignore args))
  (cl-bloom:add agg x)
  agg)

(defmethod endb/sql/expr:agg-finish ((agg cl-bloom::bloom-filter))
  (cffi:with-pointer-to-vector-data (ptr (cl-bloom::filter-array agg))
    (endb/lib/arrow:buffer-to-vector ptr (endb/lib/arrow:vector-byte-size (cl-bloom::filter-array agg)))))

(defstruct col-stats count_star count min max bloom)

(defmethod endb/sql/expr:agg-accumulate ((agg col-stats) x &rest args)
  (declare (ignore args))
  (with-slots (count_star count min max bloom) agg
    (endb/sql/expr:agg-accumulate count_star x)
    (endb/sql/expr:agg-accumulate count x)
    (endb/sql/expr:agg-accumulate min x)
    (endb/sql/expr:agg-accumulate max x)
    (endb/sql/expr:agg-accumulate bloom x))
  agg)

(defmethod endb/sql/expr:agg-finish ((agg col-stats))
  (with-slots (count_star count min max bloom) agg
    (fset:map ("count_star" (endb/sql/expr:agg-finish count_star))
              ("count" (endb/sql/expr:agg-finish count))
              ("min" (endb/sql/expr:agg-finish min))
              ("max" (endb/sql/expr:agg-finish max))
              ("bloom" (endb/sql/expr:agg-finish bloom)))))

(defun calculate-stats (arrays)
  (let* ((total-length (reduce #'+ (mapcar #'endb/arrow:arrow-length arrays)))
         (bloom-order (* 8 (endb/lib/arrow:vector-byte-size #* (cl-bloom::opt-order total-length))))
         (stats (make-hash-table :test 'equal))
         (acc (fset:empty-map)))
    (labels ((get-col-stats (k)
               (or (gethash k stats)
                   (setf (gethash k stats)
                         (make-col-stats
                          :count_star (endb/sql/expr:make-agg :count-star)
                          :count (endb/sql/expr:make-agg :count)
                          :min (endb/sql/expr:make-agg :min)
                          :max (endb/sql/expr:make-agg :max)
                          :bloom (make-instance 'cl-bloom::bloom-filter :order bloom-order))))))
      (dolist (array arrays)
        (if (typep array 'endb/arrow:dense-union-array)
            (loop for idx below (endb/arrow:arrow-length array)
                  for row = (endb/arrow:arrow-get array idx)
                  when (typep row 'endb/arrow:arrow-struct)
                    do (fset:do-map (k v row)
                         (endb/sql/expr:agg-accumulate (get-col-stats k) v)))
            (maphash
             (lambda (k v)
               (loop with col-stats = (get-col-stats (symbol-name k))
                     for idx below (endb/arrow:arrow-length v)
                     when (endb/arrow:arrow-valid-p array idx)
                       do (endb/sql/expr:agg-accumulate col-stats (endb/arrow:arrow-get v idx))))
             (endb/arrow:arrow-struct-children array))))
      (maphash
       (lambda (k v)
         (setf acc (fset:with acc k (endb/sql/expr:agg-finish v))))
       stats)
      acc)))

(defun find-files-to-compact (db table-name target-size)
  (let ((table-md (base-table-meta db table-name))
        (size 0)
        (acc))
    (sort (or (fset:do-map (arrow-file arrow-file-md table-md)
                (unless (> (+ size (fset:lookup arrow-file-md "byte_size")) target-size)
                  (push arrow-file acc)
                  (incf size (fset:lookup arrow-file-md "byte_size")))
                (when (>= size target-size)
                  (return-from nil acc)))
              acc)
          #'string<)))

(defun arrow-files-compacted-filename (arrow-files)
  (format nil "~A_~A.arrow"
          (pathname-name (first arrow-files))
          (pathname-name (car (last arrow-files)))))

(defun merge-files (db table-name arrow-files)
  (let* ((batch (endb/arrow:to-arrow
                 (loop with table-md = (base-table-meta db table-name)
                       for arrow-file in arrow-files
                       for arrow-file-md = (fset:lookup table-md arrow-file)
                       append (loop with erased-md = (or (fset:lookup arrow-file-md "erased") (fset:empty-map))
                                    for batch-row in (base-table-arrow-batches db table-name arrow-file :sha1 (fset:lookup arrow-file-md "sha1"))
                                    for batch-idx from 0
                                    for batch-idx-string = (prin1-to-string batch-idx)
                                    for batch-erased = (or (fset:lookup erased-md batch-idx-string) (fset:empty-seq))
                                    append (loop for row-id below (endb/arrow:arrow-length batch-row)
                                                 if (fset:find row-id batch-erased)
                                                   collect :null
                                                 else
                                                   collect (endb/arrow:arrow-get batch-row row-id))))))
         (table-array (endb/arrow:arrow-struct-column-array batch (intern table-name :keyword)))
         (buffer (endb/lib/arrow:write-arrow-arrays-to-ipc-buffer batch)))
    (list buffer
          (fset:map ("length" (endb/arrow:arrow-length table-array))
                    ("stats" (calculate-stats (list table-array)))
                    ("byte_size" (length buffer))
                    ("derived_from" (fset:convert 'fset:seq arrow-files))))))

(defun compact-files (db table-name batch-md)
  (with-slots (meta-data) db
    (let* ((arrow-files (fset:lookup batch-md "derived_from"))
           (table-md (base-table-meta db table-name))
           (batch-file (arrow-files-compacted-filename arrow-files))
           (merged-batch-idx 0)
           (merged-batch-idx-string (prin1-to-string merged-batch-idx))
           (deleted-md (fset:convert
                        'fset:seq
                        (let ((row-id 0))
                          (loop for arrow-file in arrow-files
                                for arrow-file-md = (fset:lookup table-md arrow-file)
                                append (loop with deleted-md = (or (fset:lookup arrow-file-md "deleted") (fset:empty-map))
                                             for batch-row in (base-table-arrow-batches db table-name arrow-file)
                                             for batch-idx from 0
                                             for batch-idx-string = (prin1-to-string batch-idx)
                                             for batch-deleted = (or (fset:lookup deleted-md batch-idx-string) (fset:empty-seq))
                                             append (let ((batch-deleted
                                                            (fset:image
                                                             (lambda (delete-entry)
                                                               (fset:with delete-entry "row_id" (+ row-id (fset:lookup delete-entry "row_id"))))
                                                             batch-deleted)))
                                                      (incf row-id (endb/arrow:arrow-length batch-row))
                                                      (fset:convert 'list batch-deleted)))))))
           (erased-md (fset:convert
                       'fset:seq
                       (let ((row-id 0))
                         (loop for arrow-file in arrow-files
                               for arrow-file-md = (fset:lookup table-md arrow-file)
                               append (loop with deleted-md = (or (fset:lookup arrow-file-md "erased") (fset:empty-map))
                                            for batch-row in (base-table-arrow-batches db table-name arrow-file)
                                            for batch-idx from 0
                                            for batch-idx-string = (prin1-to-string batch-idx)
                                            for batch-erased = (or (fset:lookup deleted-md batch-idx-string) (fset:empty-seq))
                                            append (let ((batch-erased
                                                           (fset:image
                                                            (lambda (erased-row-id)
                                                              (+ row-id erased-row-id))
                                                            batch-erased)))
                                                     (incf row-id (endb/arrow:arrow-length batch-row))
                                                     (fset:convert 'list batch-erased)))))))
           (batch-md (if (fset:empty? deleted-md)
                         (fset:with batch-md "deleted" (fset:map (merged-batch-idx-string deleted-md)))
                         batch-md))
           (batch-md (if (fset:empty? erased-md)
                         (fset:with batch-md "erased" (fset:map (merged-batch-idx-string erased-md)))
                         batch-md))
           (new-table-md (fset:reduce #'fset:less
                                      arrow-files
                                      :initial-value (fset:with table-md batch-file batch-md))))
      ;; (setf meta-data (fset:with meta-data table-name new-table-md))
      new-table-md)))

(defparameter +ident-scanner+ (ppcre:create-scanner "^[a-zA-Z_][a-zA-Z0-9_]*$"))

(defun dml-insert (db table-name values &key column-names)
  (with-slots (buffer-pool meta-data current-timestamp) db
    (let* ((created-p (base-table-created-p db table-name))
           (columns (table-columns db table-name))
           (column-names-set (fset:convert 'fset:set column-names))
           (columns-set (fset:convert 'fset:set columns))
           (new-columns (fset:convert 'list (fset:set-difference column-names-set columns-set)))
           (number-of-columns (length (or column-names columns))))
      (when (member "system_time" column-names :test 'equal)
        (error 'endb/sql/expr:sql-runtime-error :message "Cannot insert value into SYSTEM_TIME column"))
      (loop for c in column-names
            do (unless (ppcre:scan +ident-scanner+ c)
                 (error 'endb/sql/expr:sql-runtime-error
                        :message (format nil "Cannot insert into table: ~A invalid column name: ~A" table-name c))))
      (when (and created-p column-names (not (fset:equal? column-names-set columns-set)))
        (error 'endb/sql/expr:sql-runtime-error
               :message (format nil "Cannot insert into table: ~A named columns: ~A doesn't match stored: ~A" table-name column-names columns)))
      (unless (apply #'= number-of-columns (mapcar #'length values))
        (error 'endb/sql/expr:sql-runtime-error
               :message (format nil "Cannot insert into table: ~A without all values containing same number of columns: ~A" table-name number-of-columns)))
      (when new-columns
        (dml-insert db "information_schema.columns" (loop for c in new-columns
                                                          collect (list :null *default-schema* table-name c 0))))

      (if columns
          (let* ((permutation (if (and created-p column-names)
                                  (loop for column in columns
                                        collect (position column column-names :test 'equal))
                                  (loop for idx below number-of-columns
                                        collect idx)))
                 (tx-id (1+ (or (fset:lookup meta-data "_last_tx") 0)))
                 (batch-file (format nil "~(~16,'0x~).arrow" tx-id))
                 (batch-key (%batch-key table-name batch-file))
                 (table-md (or (fset:lookup meta-data table-name)
                               (fset:empty-map)))
                 (batch-md (fset:lookup table-md batch-file))
                 (batch (if batch-md
                            (car (endb/storage/buffer-pool:buffer-pool-get buffer-pool batch-key))
                            (endb/arrow:make-arrow-array-for (fset:map (table-name :null)
                                                                       ("system_time_start" current-timestamp)))))
                 (batch-children (endb/arrow:arrow-struct-children batch))
                 (kw-columns (loop for cn in (if created-p
                                                 columns
                                                 column-names)
                                   collect (intern cn :keyword)))
                 (kw-table-name (intern table-name :keyword))
                 (system-time-start-array (gethash :|system_time_start| batch-children))
                 (table-array (gethash kw-table-name batch-children)))
            (labels ((row-to-map (row)
                       (loop with acc = (fset:empty-map)
                             for idx in permutation
                             for cn in kw-columns
                             do (setf acc (fset:with acc cn (nth idx row)))
                             finally (return acc))))

              (dotimes (n (length values))
                (endb/arrow:arrow-push system-time-start-array current-timestamp))

              (let ((row (first values)))
                (setf table-array (endb/arrow:arrow-push table-array (row-to-map row))))

              (if (typep table-array 'endb/arrow:dense-union-array)
                  (dolist (row (rest values))
                    (setf table-array (endb/arrow:arrow-push table-array (row-to-map row))))

                  (loop with children = (endb/arrow:arrow-struct-children table-array)
                        with values = (rest values)
                        for idx in permutation
                        for cn in kw-columns
                        for a = (gethash cn children)
                        do (dolist (row values)
                             (setf a (endb/arrow:arrow-push a (nth idx row))))
                        finally (setf (gethash cn children) a))))

            (setf (gethash kw-table-name batch-children) table-array)
            (endb/storage/buffer-pool:buffer-pool-put buffer-pool batch-key (list batch))

            (let ((batch-md (fset:map-union (or batch-md (fset:empty-map))
                                            (fset:map
                                             ("length" (endb/arrow:arrow-length table-array))
                                             ("stats" (calculate-stats (list table-array)))))))
              (setf meta-data (fset:with meta-data table-name (fset:with table-md batch-file batch-md))))

            (values nil (length values)))
          (unless (table-type db table-name)
            (dml-insert db "information_schema.tables" (list (list :null *default-schema* table-name "BASE TABLE")))
            (dml-insert db table-name values :column-names column-names))))))

(defun dml-insert-objects (db table-name objects)
  (loop for object in objects
        if (fset:empty? object)
          do (error 'endb/sql/expr:sql-runtime-error :message "Cannot insert empty object")
        else
          do (dml-insert db table-name
                         (list (coerce (%fset-values object) 'list))
                         :column-names (fset:convert 'list (fset:domain object))))
  (values nil (length objects)))

(defun dml-delete (db table-name new-batch-file-idx-deleted-row-ids)
  (with-slots (meta-data current-timestamp) db
    (let* ((table-md (reduce
                      (lambda (acc batch-file-idx-row-id)
                        (destructuring-bind (batch-file batch-idx row-id)
                            batch-file-idx-row-id
                          (let* ((batch-md (fset:lookup acc batch-file))
                                 (deleted-md (or (fset:lookup batch-md "deleted") (fset:empty-map)))
                                 (batch-idx-key (prin1-to-string batch-idx))
                                 (batch-deleted (or (fset:lookup deleted-md batch-idx-key) (fset:empty-seq)))
                                 (delete-entry (fset:map ("row_id" row-id) ("system_time_end" current-timestamp))))
                            (fset:with acc batch-file (fset:with batch-md "deleted" (fset:with deleted-md batch-idx-key (fset:with-last batch-deleted delete-entry)))))))
                      new-batch-file-idx-deleted-row-ids
                      :initial-value (fset:lookup meta-data table-name))))
      (setf meta-data (fset:with meta-data table-name table-md))
      (values nil (length new-batch-file-idx-deleted-row-ids)))))

(defun dml-erase (db table-name new-batch-file-idx-erased-row-ids)
  (with-slots (meta-data) db
    (let* ((table-md (reduce
                      (lambda (acc batch-file-idx-row-id)
                        (destructuring-bind (batch-file batch-idx row-id)
                            batch-file-idx-row-id
                          (let* ((batch-md (fset:lookup acc batch-file))
                                 (erased-md (or (fset:lookup batch-md "erased") (fset:empty-map)))
                                 (batch-idx-key (prin1-to-string batch-idx))
                                 (batch-erased (or (fset:lookup erased-md batch-idx-key) (fset:empty-seq))))
                            (fset:with acc batch-file (fset:with batch-md "erased" (fset:with erased-md batch-idx-key (fset:with-last batch-erased row-id)))))))
                      new-batch-file-idx-erased-row-ids
                      :initial-value (fset:lookup meta-data table-name))))
      (setf meta-data (fset:with meta-data table-name table-md))
      (values nil (length new-batch-file-idx-erased-row-ids)))))
