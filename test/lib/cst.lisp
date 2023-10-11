(defpackage :endb-test/lib/cst
  (:use :cl :fiveam :endb/lib/cst)
  (:import-from :endb/json)
  (:import-from :fset)
  (:import-from :alexandria)
  (:import-from :trivial-utf-8))
(in-package :endb-test/lib/cst)

(in-suite* :lib)

(test parse-cst
      (is (equal '(:|sql_stmt_list|
                    (:|sql_stmt|
                      (:|select_stmt|
                        (:|select_core| ("SELECT" 0 6)
                          (:|result_expr_list|
                            (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))))))))
                 (parse-sql-cst "SELECT 1"))))

(defvar report-json
  "{\"kind\":\"Error\",\"msg\":\"parse error: unexpected SEL\",\"note\":\"/ sql_stmt_list / sql_stmt / select_stmt / with_clause\",\"location\":[\"/sql\",0],\"labels\":[{\"span\":[\"/sql\",{\"start\":0,\"end\":3}],\"msg\":\"expected WITH\",\"color\":\"Red\",\"order\":0,\"priority\":0},{\"span\":[\"/sql\",{\"start\":0,\"end\":0}],\"msg\":\"while parsing with_clause\",\"color\":\"Blue\",\"order\":1,\"priority\":0}],\"source\":\"SEL\"}")

(test json-error-report
  (let ((report (fset:map ("kind" "Error")
                          ("msg" "parse error: unexpected SEL")
                          ("note" "/ sql_stmt_list / sql_stmt / select_stmt / with_clause")
                          ("location" (fset:seq "/sql" 0))
                          ("source" "SEL")
                          ("labels" (fset:seq (fset:map ("span" (fset:seq "/sql" (fset:map ("start" 0) ("end" 3))))
                                                        ("msg" "expected WITH")
                                                        ("color" "Red")
                                                        ("order" 0)
                                                        ("priority" 0))
                                              (fset:map ("span" (fset:seq "/sql" (fset:map ("start" 0) ("end" 0))))
                                                        ("msg" "while parsing with_clause")
                                                        ("color" "Blue")
                                                        ("order" 1)
                                                        ("priority" 0)))))))
    (is (equalp report (endb/json:json-parse report-json)))
    (is (equal
         (render-error-report report)
         (render-error-report
          (endb/json:json-parse report-json))))
    (is (alexandria:starts-with-subseq "Error: parse error: unexpected SEL"
                                       (render-error-report report)))))
