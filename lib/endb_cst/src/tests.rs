use insta::assert_snapshot;

fn parse(sql: &str) -> String {
    let mut state = crate::ParseState {
        track_errors: true,
        ..crate::ParseState::default()
    };
    match crate::sql::sql_stmt_list(sql, 0, &mut state) {
        Ok(_) => crate::events_to_sexp(&sql, &state.events).unwrap(),
        Err(_) => {
            crate::parse_errors_to_string("/sql", sql, &crate::events_to_errors(&state.errors))
                .unwrap()
        }
    }
}

#[test]
fn identifier_expr() {
    assert_snapshot!(
        parse("SELECT foo"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("foo" 7 10)))))))))"###);

    assert_snapshot!(
        parse("SELECT x.y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|table_name| ("x" 7 8)) ("." 8 9) (:|column_name| ("y" 9 10)))))))))"###);
}

#[test]
fn parameter_expr() {
    assert_snapshot!(
        parse("SELECT ?"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|bind_parameter| ("?" 7 8))))))))"###);
    assert_snapshot!(
        parse("SELECT :name"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|bind_parameter| (":name" 7 12))))))))"###);
}

#[test]
fn comments() {
    assert_snapshot!(
        parse("SELECT 1 -- a comment"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))))))))"###);
    assert_snapshot!(
        parse("-- a comment\nSELECT 1"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 13 19) (:|result_column| (:|expr| (:|numeric_literal| ("1" 20 21))))))))"###);
    assert_snapshot!(
        parse("SELECT 1 + -- a comment\n 2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)) ("+" 9 10) (:|numeric_literal| ("2" 25 26))))))))"###);
}

#[test]
fn number_expr() {
    assert_snapshot!(
        parse("SELECT 2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("2" 7 8))))))))"###);
    assert_snapshot!(
        parse("SELECT 0xcaFE"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("0xcaFE" 7 13))))))))"###);

    assert_snapshot!(
        parse("SELECT 2.2e2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("2.2e2" 7 12))))))))"###);
    assert_snapshot!(
        parse("SELECT 2e-2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("2e-2" 7 11))))))))"###);
    assert_snapshot!(
        parse("SELECT 9223372036854775808"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("9223372036854775808" 7 26))))))))"###);
    assert_snapshot!(
        parse("SELECT 170141183460469231731687303715884105727, -170141183460469231731687303715884105727"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("170141183460469231731687303715884105727" 7 46)))) ("," 46 47) (:|result_column| (:|expr| ("-" 48 49) (:|numeric_literal| ("170141183460469231731687303715884105727" 49 88))))))))"###);
    assert_snapshot!(
        parse("SELECT 170141183460469231731687303715884105728, -170141183460469231731687303715884105728"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("170141183460469231731687303715884105728" 7 46)))) ("," 46 47) (:|result_column| (:|expr| ("-" 48 49) (:|numeric_literal| ("170141183460469231731687303715884105728" 49 88))))))))"###)
}

#[test]
fn operator_expr() {
    assert_snapshot!(
        parse("SELECT 2 < x"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("2" 7 8)) ("<" 9 10) (:|column_reference| (:|column_name| ("x" 11 12)))))))))"###);
    assert_snapshot!(
        parse("SELECT 3 > 2.1"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("3" 7 8)) (">" 9 10) (:|numeric_literal| ("2.1" 11 14))))))))"###);
    assert_snapshot!(
        parse("SELECT x>=y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) (">=" 8 10) (:|column_reference| (:|column_name| ("y" 10 11)))))))))"###);
    assert_snapshot!(
        parse("SELECT x<>y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("<>" 8 10) (:|column_reference| (:|column_name| ("y" 10 11)))))))))"###);
    assert_snapshot!(
        parse("SELECT x!=y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("!=" 8 10) (:|column_reference| (:|column_name| ("y" 10 11)))))))))"###);
    assert_snapshot!(
        parse("SELECT x == y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("==" 9 11) (:|column_reference| (:|column_name| ("y" 12 13)))))))))"###);
    assert_snapshot!(
        parse("SELECT x AND y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("AND" 9 12) (:|column_reference| (:|column_name| ("y" 13 14)))))))))"###);
    assert_snapshot!(
        parse("SELECT x and y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("and" 9 12) (:|column_reference| (:|column_name| ("y" 13 14)))))))))"###);
    assert_snapshot!(
        parse("SELECT x IS NOT y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("IS" 9 11) ("NOT" 12 15) (:|column_reference| (:|column_name| ("y" 16 17)))))))))"###);
    assert_snapshot!(
        parse("SELECT x LIKE '%y' ESCAPE 'foo'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("LIKE" 9 13) (:|string_literal| ("'%y'" 14 18)) ("ESCAPE" 19 25) (:|string_literal| ("'foo'" 26 31))))))))"###);

    assert_snapshot!(
        parse("SELECT x REGEXP '.*y'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("REGEXP" 9 15) (:|string_literal| ("'.*y'" 16 21))))))))"###);

    assert_snapshot!(
        parse("SELECT x IS y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("IS" 9 11) (:|column_reference| (:|column_name| ("y" 12 13)))))))))"###);
    assert_snapshot!(
        parse("SELECT x BETWEEN y AND 2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("BETWEEN" 9 16) (:|column_reference| (:|column_name| ("y" 17 18))) ("AND" 19 22) (:|numeric_literal| ("2" 23 24))))))))"###);
    assert_snapshot!(
        parse("SELECT x NOT BETWEEN y AND 2"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("NOT" 9 12) ("BETWEEN" 13 20) (:|column_reference| (:|column_name| ("y" 21 22))) ("AND" 23 26) (:|numeric_literal| ("2" 27 28))))))))"###);
    assert_snapshot!(
        parse("SELECT x NOT NULL"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("NOT" 9 12) ("NULL" 13 17)))))))"###);

    assert_snapshot!(
        parse("SELECT x || 'foo'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("||" 9 11) (:|string_literal| ("'foo'" 12 17))))))))"###);

    assert_snapshot!(
        parse("SELECT x IMMEDIATELY PRECEDES y"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("x" 7 8))) ("IMMEDIATELY" 9 20) ("PRECEDES" 21 29) (:|column_reference| (:|column_name| ("y" 30 31)))))))))"###);
}

#[test]
fn case_expr() {
    assert_snapshot!(
        parse("SELECT CASE WHEN 2 THEN 1 END"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|case_expr| ("CASE" 7 11) (:|case_when_then_expr| ("WHEN" 12 16) (:|expr| (:|numeric_literal| ("2" 17 18))) ("THEN" 19 23) (:|expr| (:|numeric_literal| ("1" 24 25)))) ("END" 26 29))))))))"###);
    assert_snapshot!(
        parse("SELECT CASE 3 WHEN 2 THEN 1 END"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|case_expr| ("CASE" 7 11) (:|expr| (:|numeric_literal| ("3" 12 13))) (:|case_when_then_expr| ("WHEN" 14 18) (:|expr| (:|numeric_literal| ("2" 19 20))) ("THEN" 21 25) (:|expr| (:|numeric_literal| ("1" 26 27)))) ("END" 28 31))))))))"###);
    assert_snapshot!(
        parse("SELECT CASE WHEN 2 THEN 1 ELSE 0 END"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|case_expr| ("CASE" 7 11) (:|case_when_then_expr| ("WHEN" 12 16) (:|expr| (:|numeric_literal| ("2" 17 18))) ("THEN" 19 23) (:|expr| (:|numeric_literal| ("1" 24 25)))) ("ELSE" 26 30) (:|expr| (:|numeric_literal| ("0" 31 32))) ("END" 33 36))))))))"###);
}

#[test]
fn string_expr() {
    assert_snapshot!(
        parse("SELECT 'foo'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("'foo'" 7 12))))))))"###);
    assert_snapshot!(
        parse("SELECT 'fo''o'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("'fo''o'" 7 14))))))))"###);

    assert_snapshot!(
        parse("SELECT \"foo\""),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("\"foo\"" 7 12))))))))"###);
    assert_snapshot!(
        parse("SELECT \"josé\""),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("\"josé\"" 7 14))))))))"###);
    assert_snapshot!(
        parse("SELECT \"f\\n\\uABCDoo\""),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("\"f\n\uABCDoo\"" 7 20))))))))"###);
    assert_snapshot!(
        parse("SELECT \"f\\\"oo\""),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|string_literal| ("\"f\\"oo\"" 7 14))))))))"###);
}

#[test]
fn binary_expr() {
    assert_snapshot!(
        parse("SELECT x'01'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|blob_literal| ("x'01'" 7 12))))))))"###);
    assert_snapshot!(
        parse("SELECT X'AF01'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|blob_literal| ("X'AF01'" 7 14))))))))"###);
    assert_snapshot!(
        parse("SELECT x'AF01'"),
        @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|blob_literal| ("x'AF01'" 7 14))))))))"###);
}

#[test]
fn function_expr() {
    assert_snapshot!(parse("SELECT foo(2, y)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("foo" 7 10)) ("(" 10 11) (:|expr| (:|numeric_literal| ("2" 11 12))) ("," 12 13) (:|expr| (:|column_reference| (:|column_name| ("y" 14 15)))) (")" 15 16))))))))"###);
    assert_snapshot!(parse("SELECT count(y)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("count" 7 12)) ("(" 12 13) (:|expr| (:|column_reference| (:|column_name| ("y" 13 14)))) (")" 14 15))))))))"###);
    assert_snapshot!(parse("SELECT TOTAL(y)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("TOTAL" 7 12)) ("(" 12 13) (:|expr| (:|column_reference| (:|column_name| ("y" 13 14)))) (")" 14 15))))))))"###);
    assert_snapshot!(parse("SELECT count(*)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("count" 7 12)) ("(" 12 13) ("*" 13 14) (")" 14 15))))))))"###);

    assert_snapshot!(parse("SELECT ARRAY_AGG(x ORDER BY y DESC)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("ARRAY_AGG" 7 16)) ("(" 16 17) (:|expr| (:|column_reference| (:|column_name| ("x" 17 18)))) (:|order_by_clause| ("ORDER" 19 24) ("BY" 25 27) (:|ordering_term| (:|expr| (:|column_reference| (:|column_name| ("y" 28 29)))) ("DESC" 30 34))) (")" 34 35))))))))"###);

    assert_snapshot!(parse("SELECT count(*) FILTER (WHERE x > 2)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("count" 7 12)) ("(" 12 13) ("*" 13 14) (")" 14 15) ("FILTER" 16 22) ("(" 23 24) ("WHERE" 24 29) (:|expr| (:|column_reference| (:|column_name| ("x" 30 31))) (">" 32 33) (:|numeric_literal| ("2" 34 35))) (")" 35 36))))))))"###);

    assert_snapshot!(parse("SELECT CAST ( - 69 AS INTEGER )"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|cast_expr| ("CAST" 7 11) ("(" 12 13) (:|expr| ("-" 14 15) (:|numeric_literal| ("69" 16 18))) ("AS" 19 21) (:|type_name| ("INTEGER" 22 29)) (")" 30 31))))))))"###);

    assert_snapshot!(parse("SELECT EXTRACT ( DAY FROM x )"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|extract_expr| ("EXTRACT" 7 14) ("(" 15 16) (:|datetime_field| ("DAY" 17 20)) ("FROM" 21 25) (:|expr| (:|column_reference| (:|column_name| ("x" 26 27)))) (")" 28 29))))))))"###)
}

#[test]
fn simple_select() {
    assert_snapshot!(parse("SELECT 123"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("123" 7 10))))))))"###);
    assert_snapshot!(parse("SELECT *"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8)))))))"###);

    assert_snapshot!(parse("SELECT x.* FROM x"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|qualified_asterisk| (:|table_name| ("x" 7 8)) ("." 8 9) ("*" 9 10))) (:|from_clause| ("FROM" 11 15) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 16 17)))))))))"###);
}

#[test]
fn select_as() {
    assert_snapshot!(parse("SELECT 1 AS x, 2 y FROM z, w AS foo, (SELECT bar) AS baz WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))) ("AS" 9 11) (:|column_alias| ("x" 12 13))) ("," 13 14) (:|result_column| (:|expr| (:|numeric_literal| ("2" 15 16))) (:|column_alias| ("y" 17 18))) (:|from_clause| ("FROM" 19 23) (:|join_clause| (:|table_or_subquery| (:|table_name| ("z" 24 25))) (:|join_operator| ("," 25 26)) (:|table_or_subquery| (:|table_name| ("w" 27 28)) ("AS" 29 31) (:|table_alias| ("foo" 32 35))) (:|join_constraint|) (:|join_operator| ("," 35 36)) (:|table_or_subquery| ("(" 37 38) (:|select_stmt| (:|select_core| ("SELECT" 38 44) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("bar" 45 48))))))) (")" 48 49) ("AS" 50 52) (:|table_alias| ("baz" 53 56))) (:|join_constraint|))) (:|where_clause| ("WHERE" 57 62) (:|expr| ("FALSE" 63 68)))))))"###);
    assert_snapshot!(parse("SELECT 1 AS from"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))) ("AS" 9 11) (:|column_alias| ("from" 12 16)))))))"###);
}

#[test]
fn select() {
    assert_snapshot!(parse("SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("a" 7 8))))) ("," 8 9) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("b" 10 11))))) ("," 11 12) (:|result_column| (:|expr| (:|numeric_literal| ("123" 13 16)))) ("," 16 17) (:|result_column| (:|expr| (:|function_call_expr| (:|function_name| ("myfunc" 18 24)) ("(" 24 25) (:|expr| (:|column_reference| (:|column_name| ("b" 25 26)))) (")" 26 27)))) (:|from_clause| ("FROM" 28 32) (:|join_clause| (:|table_or_subquery| (:|table_name| ("table_1" 33 40))))) (:|where_clause| ("WHERE" 41 46) (:|expr| (:|column_reference| (:|column_name| ("a" 47 48))) (">" 49 50) (:|column_reference| (:|column_name| ("b" 51 52))) ("AND" 53 56) (:|column_reference| (:|column_name| ("b" 57 58))) ("<" 59 60) (:|numeric_literal| ("100" 61 64))))) (:|order_by_clause| ("ORDER" 65 70) ("BY" 71 73) (:|ordering_term| (:|expr| (:|column_reference| (:|column_name| ("a" 74 75)))) ("DESC" 76 80)) ("," 80 81) (:|ordering_term| (:|expr| (:|column_reference| (:|column_name| ("b" 82 83)))))))))"###);
    assert_snapshot!(parse("SELECT 1 FROM x ORDER BY x.a + 1"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15)))))) (:|order_by_clause| ("ORDER" 16 21) ("BY" 22 24) (:|ordering_term| (:|expr| (:|column_reference| (:|table_name| ("x" 25 26)) ("." 26 27) (:|column_name| ("a" 27 28))) ("+" 29 30) (:|numeric_literal| ("1" 31 32))))))))"###);

    assert_snapshot!(parse("SELECT 1 FROM x CROSS JOIN y"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15))) (:|join_operator| ("CROSS" 16 21) ("JOIN" 22 26)) (:|table_or_subquery| (:|table_name| ("y" 27 28))) (:|join_constraint|)))))))"###);
    assert_snapshot!(parse("SELECT 1 FROM (x CROSS JOIN y)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| ("(" 14 15) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 15 16))) (:|join_operator| ("CROSS" 17 22) ("JOIN" 23 27)) (:|table_or_subquery| (:|table_name| ("y" 28 29))) (:|join_constraint|)) (")" 29 30))))))))"###);
    assert_snapshot!(parse("SELECT 1 INTERSECT SELECT 2 UNION SELECT 3"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))))) (:|compound_operator| ("INTERSECT" 9 18)) (:|select_core| ("SELECT" 19 25) (:|result_column| (:|expr| (:|numeric_literal| ("2" 26 27))))) (:|compound_operator| ("UNION" 28 33)) (:|select_core| ("SELECT" 34 40) (:|result_column| (:|expr| (:|numeric_literal| ("3" 41 42))))))))"###);
    assert_snapshot!(parse("VALUES (1, 2), (3, 4)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("VALUES" 0 6) ("(" 7 8) (:|expr| (:|numeric_literal| ("1" 8 9))) ("," 9 10) (:|expr| (:|numeric_literal| ("2" 11 12))) (")" 12 13) ("," 13 14) ("(" 15 16) (:|expr| (:|numeric_literal| ("3" 16 17))) ("," 17 18) (:|expr| (:|numeric_literal| ("4" 19 20))) (")" 20 21)))))"###);
    assert_snapshot!(parse("OBJECTS {a: 2}, {a: 4}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("OBJECTS" 0 7) (:|object_expr| ("{" 8 9) (:|object_key_value_pair| ("a" 9 10) (":" 10 11) (:|expr| (:|numeric_literal| ("2" 12 13)))) ("}" 13 14)) ("," 14 15) (:|object_expr| ("{" 16 17) (:|object_key_value_pair| ("a" 17 18) (":" 18 19) (:|expr| (:|numeric_literal| ("4" 20 21)))) ("}" 21 22))))))"###);

    assert_snapshot!(parse("SELECT * FROM (VALUES (1, 2), (3, 4)) AS foo(a, b)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| ("(" 14 15) (:|select_stmt| (:|select_core| ("VALUES" 15 21) ("(" 22 23) (:|expr| (:|numeric_literal| ("1" 23 24))) ("," 24 25) (:|expr| (:|numeric_literal| ("2" 26 27))) (")" 27 28) ("," 28 29) ("(" 30 31) (:|expr| (:|numeric_literal| ("3" 31 32))) ("," 32 33) (:|expr| (:|numeric_literal| ("4" 34 35))) (")" 35 36))) (")" 36 37) ("AS" 38 40) (:|table_alias| ("foo" 41 44) ("(" 44 45) ("a" 45 46) ("," 46 47) ("b" 48 49) (")" 49 50)))))))))"###);
}

#[test]
fn group_by_having() {
    assert_snapshot!(parse("SELECT 1 FROM x GROUP BY y HAVING TRUE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15))))) (:|group_by_clause| ("GROUP" 16 21) ("BY" 22 24) (:|expr| (:|column_reference| (:|column_name| ("y" 25 26))))) (:|having_clause| ("HAVING" 27 33) (:|expr| ("TRUE" 34 38)))))))"###);
}

#[test]
fn select_distinct() {
    assert_snapshot!(parse("SELECT DISTINCT 1 FROM x"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) ("DISTINCT" 7 15) (:|result_column| (:|expr| (:|numeric_literal| ("1" 16 17)))) (:|from_clause| ("FROM" 18 22) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 23 24)))))))))"###);
}

#[test]
fn select_limit_offset() {
    assert_snapshot!(parse("SELECT 1 FROM x LIMIT 1 OFFSET 2"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15)))))) (:|limit_offset_clause| ("LIMIT" 16 21) (:|expr| (:|numeric_literal| ("1" 22 23))) ("OFFSET" 24 30) (:|expr| (:|numeric_literal| ("2" 31 32)))))))"###);
    assert_snapshot!(parse("SELECT 1 FROM x LIMIT 1, 2"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15)))))) (:|limit_offset_clause| ("LIMIT" 16 21) (:|expr| (:|numeric_literal| ("1" 22 23))) ("," 23 24) (:|expr| (:|numeric_literal| ("2" 25 26)))))))"###);
    assert_snapshot!(parse("SELECT 1 FROM x LIMIT 1"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8)))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("x" 14 15)))))) (:|limit_offset_clause| ("LIMIT" 16 21) (:|expr| (:|numeric_literal| ("1" 22 23)))))))"###);
}

#[test]
fn dml() {
    assert_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|insert_stmt| ("INSERT" 0 6) ("INTO" 7 11) (:|table_name| ("foo" 12 15)) ("(" 16 17) (:|column_name| ("x" 17 18)) (")" 18 19) (:|select_stmt| (:|select_core| ("VALUES" 20 26) ("(" 27 28) (:|expr| (:|numeric_literal| ("1" 28 29))) (")" 29 30) ("," 30 31) ("(" 32 33) (:|expr| (:|numeric_literal| ("2" 33 34))) (")" 34 35))))))"###);

    assert_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2) ON CONFLICT (x) DO NOTHING"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|insert_stmt| ("INSERT" 0 6) ("INTO" 7 11) (:|table_name| ("foo" 12 15)) ("(" 16 17) (:|column_name| ("x" 17 18)) (")" 18 19) (:|select_stmt| (:|select_core| ("VALUES" 20 26) ("(" 27 28) (:|expr| (:|numeric_literal| ("1" 28 29))) (")" 29 30) ("," 30 31) ("(" 32 33) (:|expr| (:|numeric_literal| ("2" 33 34))) (")" 34 35))) ("ON" 36 38) ("CONFLICT" 39 47) ("(" 48 49) (:|column_name| ("x" 49 50)) (")" 50 51) ("DO" 52 54) ("NOTHING" 55 62))))"###);
    assert_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2) ON CONFLICT (x) DO UPDATE SET x = 2"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|insert_stmt| ("INSERT" 0 6) ("INTO" 7 11) (:|table_name| ("foo" 12 15)) ("(" 16 17) (:|column_name| ("x" 17 18)) (")" 18 19) (:|select_stmt| (:|select_core| ("VALUES" 20 26) ("(" 27 28) (:|expr| (:|numeric_literal| ("1" 28 29))) (")" 29 30) ("," 30 31) ("(" 32 33) (:|expr| (:|numeric_literal| ("2" 33 34))) (")" 34 35))) ("ON" 36 38) ("CONFLICT" 39 47) ("(" 48 49) (:|column_name| ("x" 49 50)) (")" 50 51) ("DO" 52 54) ("UPDATE" 55 61) (:|update_body| ("SET" 62 65) (:|column_name| ("x" 66 67)) ("=" 68 69) (:|expr| (:|numeric_literal| ("2" 70 71)))))))"###);

    assert_snapshot!(parse("ERASE FROM foo WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|erase_stmt| ("ERASE" 0 5) ("FROM" 6 10) (:|table_name| ("foo" 11 14)) ("WHERE" 15 20) (:|expr| ("FALSE" 21 26)))))"###);

    assert_snapshot!(parse("DELETE FROM foo WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|delete_stmt| ("DELETE" 0 6) ("FROM" 7 11) (:|table_name| ("foo" 12 15)) ("WHERE" 16 21) (:|expr| ("FALSE" 22 27)))))"###);
    assert_snapshot!(parse("DELETE FROM foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|delete_stmt| ("DELETE" 0 6) ("FROM" 7 11) (:|table_name| ("foo" 12 15)))))"###);
    assert_snapshot!(parse("UPDATE foo SET x = 1, y = 2 WHERE NULL"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|update_stmt| ("UPDATE" 0 6) (:|table_name| ("foo" 7 10)) (:|update_body| ("SET" 11 14) (:|column_name| ("x" 15 16)) ("=" 17 18) (:|expr| (:|numeric_literal| ("1" 19 20))) ("," 20 21) (:|column_name| ("y" 22 23)) ("=" 24 25) (:|expr| (:|numeric_literal| ("2" 26 27))) ("WHERE" 28 33) (:|expr| ("NULL" 34 38))))))"###);

    assert_snapshot!(parse("UPDATE foo UNSET z, w WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|update_stmt| ("UPDATE" 0 6) (:|table_name| ("foo" 7 10)) (:|update_body| ("UNSET" 11 16) (:|column_name| ("z" 17 18)) ("," 18 19) (:|column_name| ("w" 20 21)) ("WHERE" 22 27) (:|expr| ("FALSE" 28 33))))))"###);
    assert_snapshot!(parse("UPDATE foo SET x = 1, y = 2 REMOVE z, w WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|update_stmt| ("UPDATE" 0 6) (:|table_name| ("foo" 7 10)) (:|update_body| ("SET" 11 14) (:|column_name| ("x" 15 16)) ("=" 17 18) (:|expr| (:|numeric_literal| ("1" 19 20))) ("," 20 21) (:|column_name| ("y" 22 23)) ("=" 24 25) (:|expr| (:|numeric_literal| ("2" 26 27))) ("REMOVE" 28 34) (:|column_name| ("z" 35 36)) ("," 36 37) (:|column_name| ("w" 38 39)) ("WHERE" 40 45) (:|expr| ("FALSE" 46 51))))))"###);
    assert_snapshot!(parse("UPDATE foo PATCH { z: 2 } WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|update_stmt| ("UPDATE" 0 6) (:|table_name| ("foo" 7 10)) (:|update_body| ("PATCH" 11 16) (:|object_expr| ("{" 17 18) (:|object_key_value_pair| ("z" 19 20) (":" 20 21) (:|expr| (:|numeric_literal| ("2" 22 23)))) ("}" 24 25)) ("WHERE" 26 31) (:|expr| ("FALSE" 32 37))))))"###);
    assert_snapshot!(parse("UPDATE foo SET $.a[0] = 1 WHERE FALSE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|update_stmt| ("UPDATE" 0 6) (:|table_name| ("foo" 7 10)) (:|update_body| ("SET" 11 14) (:|path_expr| ("$" 15 16) ("." 16 17) ("a" 17 18) ("[" 18 19) (:|expr| (:|numeric_literal| ("0" 19 20))) ("]" 20 21)) ("=" 22 23) (:|expr| (:|numeric_literal| ("1" 24 25))) ("WHERE" 26 31) (:|expr| ("FALSE" 32 37))))))"###);
}

#[test]
fn ddl() {
    assert_snapshot!(parse("CREATE UNIQUE INDEX foo ON t1(a1,b1)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|create_index_stmt| ("CREATE" 0 6) ("UNIQUE" 7 13) ("INDEX" 14 19) ("foo" 20 23) ("ON" 24 26) (:|table_name| ("t1" 27 29)) ("(" 29 30) (:|ordering_term| (:|expr| (:|column_reference| (:|column_name| ("a1" 30 32))))) ("," 32 33) (:|ordering_term| (:|expr| (:|column_reference| (:|column_name| ("b1" 33 35))))) (")" 35 36))))"###);
    assert_snapshot!(parse("DROP INDEX foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|ddl_drop_stmt| ("DROP" 0 4) ("INDEX" 5 10) ("foo" 11 14))))"###);
    assert_snapshot!(parse("CREATE TEMP VIEW foo AS SELECT 1"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|create_view_stmt| ("CREATE" 0 6) ("TEMP" 7 11) ("VIEW" 12 16) ("foo" 17 20) ("AS" 21 23) (:|select_stmt| (:|select_core| ("SELECT" 24 30) (:|result_column| (:|expr| (:|numeric_literal| ("1" 31 32)))))))))"###);
    assert_snapshot!(parse("CREATE TEMP VIEW foo(bar) AS SELECT 1"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|create_view_stmt| ("CREATE" 0 6) ("TEMP" 7 11) ("VIEW" 12 16) ("foo" 17 20) ("(" 20 21) (:|column_name| ("bar" 21 24)) (")" 24 25) ("AS" 26 28) (:|select_stmt| (:|select_core| ("SELECT" 29 35) (:|result_column| (:|expr| (:|numeric_literal| ("1" 36 37)))))))))"###);
    assert_snapshot!(parse("DROP VIEW IF EXISTS foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|ddl_drop_stmt| ("DROP" 0 4) ("VIEW" 5 9) ("IF" 10 12) ("EXISTS" 13 19) ("foo" 20 23))))"###);
    assert_snapshot!(parse("CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INTEGER, x1 VARCHAR(40), FOREIGN KEY (y1) REFERENCES t2(z1), PRIMARY KEY(a1, b2))"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|create_table_stmt| ("CREATE" 0 6) ("TABLE" 7 12) (:|table_name| ("t1" 13 15)) ("(" 15 16) (:|column_definition| (:|column_name| ("a1" 16 18)) (:|type_name| ("INTEGER" 19 26)) ("PRIMARY" 27 34) ("KEY" 35 38)) ("," 38 39) (:|column_definition| (:|column_name| ("b1" 40 42)) (:|type_name| ("INTEGER" 43 50))) ("," 50 51) (:|column_definition| (:|column_name| ("x1" 52 54)) (:|type_name| ("VARCHAR" 55 62)) ("(" 62 63) (:|numeric_literal| ("40" 63 65)) (")" 65 66)) ("," 66 67) (:|column_definition| ("FOREIGN" 68 75) ("KEY" 76 79) ("(" 80 81) (:|column_name| ("y1" 81 83)) (")" 83 84) ("REFERENCES" 85 95) (:|table_name| ("t2" 96 98)) ("(" 98 99) (:|column_name| ("z1" 99 101)) (")" 101 102)) ("," 102 103) (:|column_definition| ("PRIMARY" 104 111) ("KEY" 112 115) ("(" 115 116) (:|column_name| ("a1" 116 118)) ("," 118 119) (:|column_name| ("b2" 120 122)) (")" 122 123)) (")" 123 124))))"###);
    assert_snapshot!(parse("DROP TABLE foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|ddl_drop_stmt| ("DROP" 0 4) ("TABLE" 5 10) ("foo" 11 14))))"###);
    assert_snapshot!(parse("CREATE ASSERTION foo CHECK (1 == 1)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|create_assertion_stmt| ("CREATE" 0 6) ("ASSERTION" 7 16) ("foo" 17 20) ("CHECK" 21 26) ("(" 27 28) (:|expr| (:|numeric_literal| ("1" 28 29)) ("==" 30 32) (:|numeric_literal| ("1" 33 34))) (")" 34 35))))"###);
    assert_snapshot!(parse("DROP ASSERTION foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|ddl_drop_stmt| ("DROP" 0 4) ("ASSERTION" 5 14) ("foo" 15 18))))"###);
}

#[test]
fn multiple() {
    assert_snapshot!(parse("SELECT 1; SELECT 1;"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))))))) (";" 8 9) (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 10 16) (:|result_column| (:|expr| (:|numeric_literal| ("1" 17 18))))))) (";" 18 19))"###);
    assert_snapshot!(parse("SELECT 1; SELECT 1"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|numeric_literal| ("1" 7 8))))))) (";" 8 9) (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 10 16) (:|result_column| (:|expr| (:|numeric_literal| ("1" 17 18))))))))"###);
}

#[test]
fn with() {
    assert_snapshot!(parse("WITH foo(a) AS (SELECT 1) SELECT * FROM foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|with_clause| ("WITH" 0 4) (:|common_table_expression| (:|table_name| ("foo" 5 8)) ("(" 8 9) (:|column_name| ("a" 9 10)) (")" 10 11) ("AS" 12 14) ("(" 15 16) (:|select_stmt| (:|select_core| ("SELECT" 16 22) (:|result_column| (:|expr| (:|numeric_literal| ("1" 23 24)))))) (")" 24 25))) (:|select_core| ("SELECT" 26 32) (:|result_column| (:|asterisk| ("*" 33 34))) (:|from_clause| ("FROM" 35 39) (:|join_clause| (:|table_or_subquery| (:|table_name| ("foo" 40 43)))))))))"###);
    assert_snapshot!(parse("WITH foo AS (SELECT 1), bar(a, b) AS (SELECT 1, 2) SELECT * FROM foo, bar"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|with_clause| ("WITH" 0 4) (:|common_table_expression| (:|table_name| ("foo" 5 8)) ("AS" 9 11) ("(" 12 13) (:|select_stmt| (:|select_core| ("SELECT" 13 19) (:|result_column| (:|expr| (:|numeric_literal| ("1" 20 21)))))) (")" 21 22)) ("," 22 23) (:|common_table_expression| (:|table_name| ("bar" 24 27)) ("(" 27 28) (:|column_name| ("a" 28 29)) ("," 29 30) (:|column_name| ("b" 31 32)) (")" 32 33) ("AS" 34 36) ("(" 37 38) (:|select_stmt| (:|select_core| ("SELECT" 38 44) (:|result_column| (:|expr| (:|numeric_literal| ("1" 45 46)))) ("," 46 47) (:|result_column| (:|expr| (:|numeric_literal| ("2" 48 49)))))) (")" 49 50))) (:|select_core| ("SELECT" 51 57) (:|result_column| (:|asterisk| ("*" 58 59))) (:|from_clause| ("FROM" 60 64) (:|join_clause| (:|table_or_subquery| (:|table_name| ("foo" 65 68))) (:|join_operator| ("," 68 69)) (:|table_or_subquery| (:|table_name| ("bar" 70 73))) (:|join_constraint|)))))))"###);
    assert_snapshot!(parse("WITH RECURSIVE foo(a) AS (SELECT 1) SELECT * FROM foo"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|with_clause| ("WITH" 0 4) ("RECURSIVE" 5 14) (:|common_table_expression| (:|table_name| ("foo" 15 18)) ("(" 18 19) (:|column_name| ("a" 19 20)) (")" 20 21) ("AS" 22 24) ("(" 25 26) (:|select_stmt| (:|select_core| ("SELECT" 26 32) (:|result_column| (:|expr| (:|numeric_literal| ("1" 33 34)))))) (")" 34 35))) (:|select_core| ("SELECT" 36 42) (:|result_column| (:|asterisk| ("*" 43 44))) (:|from_clause| ("FROM" 45 49) (:|join_clause| (:|table_or_subquery| (:|table_name| ("foo" 50 53)))))))))"###);
}

#[test]
fn information_schema() {
    assert_snapshot!(parse("SELECT * FROM information_schema.tables"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("information_schema" 14 32) ("." 32 33) ("tables" 33 39)))))))))"###);

    assert_snapshot!(parse("SELECT * FROM information_schema.columns AS c"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("information_schema" 14 32) ("." 32 33) ("columns" 33 40)) ("AS" 41 43) (:|table_alias| ("c" 44 45)))))))))"###);
}

#[test]
fn system_time() {
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF ?"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("AS" 34 36) ("OF" 37 39) (:|bind_parameter| ("?" 40 41)))))))))"###);

    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME ALL"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("ALL" 34 37))))))))"###);

    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF TIMESTAMP '2011-01-02 00:00:00'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("AS" 34 36) ("OF" 37 39) (:|timestamp_literal| ("TIMESTAMP" 40 49) ("'2011-01-02 00:00:00'" 50 71)))))))))"###);
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF 2011-01-02 AS e"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("AS" 34 36) ("OF" 37 39) (:|iso_date_literal| ("2011-01-02" 40 50)) ("AS" 51 53) (:|table_alias| ("e" 54 55)))))))))"###);
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME FROM TIMESTAMP '2011-01-02 00:00:00' TO TIMESTAMP '2011-12-31 00:00:00'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("FROM" 34 38) (:|timestamp_literal| ("TIMESTAMP" 39 48) ("'2011-01-02 00:00:00'" 49 70)) ("TO" 71 73) (:|timestamp_literal| ("TIMESTAMP" 74 83) ("'2011-12-31 00:00:00'" 84 105)))))))))"###);
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME FROM 2011-01-02T00:00:00 TO 2011-12-31T00:00:00"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("FROM" 34 38) (:|iso_timestamp_literal| ("2011-01-02T00:00:00" 39 58)) ("TO" 59 61) (:|iso_timestamp_literal| ("2011-12-31T00:00:00" 62 81)))))))))"###);
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME BETWEEN TIMESTAMP '2011-01-02 00:00:00' AND TIMESTAMP '2011-12-31 00:00:00'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("BETWEEN" 34 41) (:|timestamp_literal| ("TIMESTAMP" 42 51) ("'2011-01-02 00:00:00'" 52 73)) ("AND" 74 77) (:|timestamp_literal| ("TIMESTAMP" 78 87) ("'2011-12-31 00:00:00'" 88 109)))))))))"###);
    assert_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME BETWEEN 2011-01-02T00:00:00 AND 2011-12-31T00:00:00"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("Emp" 14 17)) ("FOR" 18 21) ("SYSTEM_TIME" 22 33) ("BETWEEN" 34 41) (:|iso_timestamp_literal| ("2011-01-02T00:00:00" 42 61)) ("AND" 62 65) (:|iso_timestamp_literal| ("2011-12-31T00:00:00" 66 85)))))))))"###);
}

#[test]
fn temporal_scalars() {
    assert_snapshot!(parse("SELECT 2001-01-01"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_date_literal| ("2001-01-01" 7 17))))))))"###);
    assert_snapshot!(parse("SELECT DATE '2001-01-01'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|date_literal| ("DATE" 7 11) ("'2001-01-01'" 12 24))))))))"###);
    assert_snapshot!(parse("SELECT DATE \"2001-01-01\""), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|date_literal| ("DATE" 7 11) ("\"2001-01-01\"" 12 24))))))))"###);

    assert_snapshot!(parse("SELECT 12:01:20"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_time_literal| ("12:01:20" 7 15))))))))"###);
    assert_snapshot!(parse("SELECT TIME '12:01:20'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|time_literal| ("TIME" 7 11) ("'12:01:20'" 12 22))))))))"###);

    assert_snapshot!(parse("SELECT 2023-05-16T14:43:39.970062Z"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_timestamp_literal| ("2023-05-16T14:43:39.970062Z" 7 34))))))))"###);
    assert_snapshot!(parse("SELECT TIMESTAMP '2023-05-16 14:43:39'"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|timestamp_literal| ("TIMESTAMP" 7 16) ("'2023-05-16 14:43:39'" 17 38))))))))"###);
    assert_snapshot!(parse("SELECT P3Y6M4DT12H30M5S"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_duration_literal| ("P3Y6M4DT12H30M5S" 7 23))))))))"###);
    assert_snapshot!(parse("SELECT P3Y6M"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_duration_literal| ("P3Y6M" 7 12))))))))"###);
    assert_snapshot!(parse("SELECT PT12H30M5S"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|iso_duration_literal| ("PT12H30M5S" 7 17))))))))"###);

    assert_snapshot!(parse("SELECT CURRENT_DATE"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| ("CURRENT_DATE" 7 19)))))))"###);

    assert_snapshot!(parse("SELECT INTERVAL '1' HOUR"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|interval_literal| ("INTERVAL" 7 15) ("'1'" 16 19) (:|datetime_field| ("HOUR" 20 24)))))))))"###);
    assert_snapshot!(parse("SELECT INTERVAL '01:40' MINUTE TO SECOND"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|interval_literal| ("INTERVAL" 7 15) ("'01:40'" 16 23) (:|datetime_field| ("MINUTE" 24 30)) ("TO" 31 33) (:|datetime_field| ("SECOND" 34 40)))))))))"###);
    assert_snapshot!(parse("SELECT INTERVAL '2-3' YEAR TO MONTH"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|interval_literal| ("INTERVAL" 7 15) ("'2-3'" 16 21) (:|datetime_field| ("YEAR" 22 26)) ("TO" 27 29) (:|datetime_field| ("MONTH" 30 35)))))))))"###);
}

#[test]
fn semi_structured() {
    assert_snapshot!(parse("SELECT []"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("[" 7 8) ("]" 8 9))))))))"###);
    assert_snapshot!(parse("SELECT ARRAY []"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("ARRAY" 7 12) ("[" 13 14) ("]" 14 15))))))))"###);
    assert_snapshot!(parse("SELECT [1, 2]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("[" 7 8) (:|expr| (:|numeric_literal| ("1" 8 9))) ("," 9 10) (:|expr| (:|numeric_literal| ("2" 11 12))) ("]" 12 13))))))))"###);

    assert_snapshot!(parse("SELECT ARRAY [1, 2,]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("ARRAY" 7 12) ("[" 13 14) (:|expr| (:|numeric_literal| ("1" 14 15))) ("," 15 16) (:|expr| (:|numeric_literal| ("2" 17 18))) ("," 18 19) ("]" 19 20))))))))"###);

    assert_snapshot!(parse("SELECT ARRAY (SELECT 1)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("ARRAY" 7 12) (:|subquery| ("(" 13 14) (:|select_stmt| (:|select_core| ("SELECT" 14 20) (:|result_column| (:|expr| (:|numeric_literal| ("1" 21 22)))))) (")" 22 23)))))))))"###);

    assert_snapshot!(parse("SELECT {}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) ("}" 8 9))))))))"###);
    assert_snapshot!(parse("SELECT OBJECT()"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("OBJECT" 7 13) ("(" 13 14) (")" 14 15))))))))"###);
    assert_snapshot!(parse("SELECT {foo: 2, bar: 'baz'}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) (:|object_key_value_pair| ("foo" 8 11) (":" 11 12) (:|expr| (:|numeric_literal| ("2" 13 14)))) ("," 14 15) (:|object_key_value_pair| ("bar" 16 19) (":" 19 20) (:|expr| (:|string_literal| ("'baz'" 21 26)))) ("}" 26 27))))))))"###);
    assert_snapshot!(parse("SELECT {\"foo\": 2, 'bar': 'baz',}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) (:|object_key_value_pair| (:|string_literal| ("\"foo\"" 8 13)) (":" 13 14) (:|expr| (:|numeric_literal| ("2" 15 16)))) ("," 16 17) (:|object_key_value_pair| (:|string_literal| ("'bar'" 18 23)) (":" 23 24) (:|expr| (:|string_literal| ("'baz'" 25 30)))) ("," 30 31) ("}" 31 32))))))))"###);
    assert_snapshot!(parse("SELECT {foo = 2, bar = 'baz'}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) (:|object_key_value_pair| ("foo" 8 11) ("=" 12 13) (:|expr| (:|numeric_literal| ("2" 14 15)))) ("," 15 16) (:|object_key_value_pair| ("bar" 17 20) ("=" 21 22) (:|expr| (:|string_literal| ("'baz'" 23 28)))) ("}" 28 29))))))))"###);
    assert_snapshot!(parse("SELECT OBJECT(foo: 2, bar: 'baz')"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("OBJECT" 7 13) ("(" 13 14) (:|object_key_value_pair| ("foo" 14 17) (":" 17 18) (:|expr| (:|numeric_literal| ("2" 19 20)))) ("," 20 21) (:|object_key_value_pair| ("bar" 22 25) (":" 25 26) (:|expr| (:|string_literal| ("'baz'" 27 32)))) (")" 32 33))))))))"###);

    assert_snapshot!(parse("SELECT {address: {street: 'Street', number: 42}, friends: [1, 2]}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) (:|object_key_value_pair| ("address" 8 15) (":" 15 16) (:|expr| (:|object_expr| ("{" 17 18) (:|object_key_value_pair| ("street" 18 24) (":" 24 25) (:|expr| (:|string_literal| ("'Street'" 26 34)))) ("," 34 35) (:|object_key_value_pair| ("number" 36 42) (":" 42 43) (:|expr| (:|numeric_literal| ("42" 44 46)))) ("}" 46 47)))) ("," 47 48) (:|object_key_value_pair| ("friends" 49 56) (":" 56 57) (:|expr| (:|array_expr| ("[" 58 59) (:|expr| (:|numeric_literal| ("1" 59 60))) ("," 60 61) (:|expr| (:|numeric_literal| ("2" 62 63))) ("]" 63 64)))) ("}" 64 65))))))))"###);

    assert_snapshot!(parse("SELECT { :baz, foo, ...bar, ['baz' || 'boz']: 42, fob.* }"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) (:|object_key_value_pair| (:|bind_parameter| (":baz" 9 13))) ("," 13 14) (:|object_key_value_pair| (:|column_reference| (:|column_name| ("foo" 15 18)))) ("," 18 19) (:|object_key_value_pair| ("..." 20 23) (:|expr| (:|column_reference| (:|column_name| ("bar" 23 26))))) ("," 26 27) (:|object_key_value_pair| ("[" 28 29) (:|expr| (:|string_literal| ("'baz'" 29 34)) ("||" 35 37) (:|string_literal| ("'boz'" 38 43))) ("]" 43 44) (":" 44 45) (:|expr| (:|numeric_literal| ("42" 46 48)))) ("," 48 49) (:|object_key_value_pair| (:|table_name| ("fob" 50 53)) ("." 53 54) ("*" 54 55)) ("}" 56 57))))))))"###);
    assert_snapshot!(parse("SELECT [ 1, 2, ... bar, 4 ]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("[" 7 8) (:|expr| (:|numeric_literal| ("1" 9 10))) ("," 10 11) (:|expr| (:|numeric_literal| ("2" 12 13))) ("," 13 14) ("..." 15 18) (:|expr| (:|column_reference| (:|column_name| ("bar" 19 22)))) ("," 22 23) (:|expr| (:|numeric_literal| ("4" 24 25))) ("]" 26 27))))))))"###);

    assert_snapshot!(parse("INSERT INTO users {foo: 2, bar: 'baz'}, {foo: 3}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|insert_stmt| ("INSERT" 0 6) ("INTO" 7 11) (:|table_name| ("users" 12 17)) (:|object_expr| ("{" 18 19) (:|object_key_value_pair| ("foo" 19 22) (":" 22 23) (:|expr| (:|numeric_literal| ("2" 24 25)))) ("," 25 26) (:|object_key_value_pair| ("bar" 27 30) (":" 30 31) (:|expr| (:|string_literal| ("'baz'" 32 37)))) ("}" 37 38)) ("," 38 39) (:|object_expr| ("{" 40 41) (:|object_key_value_pair| ("foo" 41 44) (":" 44 45) (:|expr| (:|numeric_literal| ("3" 46 47)))) ("}" 47 48)))))"###);

    assert_snapshot!(parse("INSERT INTO users OBJECTS {foo: 3}"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|insert_stmt| ("INSERT" 0 6) ("INTO" 7 11) (:|table_name| ("users" 12 17)) (:|select_stmt| (:|select_core| ("OBJECTS" 18 25) (:|object_expr| ("{" 26 27) (:|object_key_value_pair| ("foo" 27 30) (":" 30 31) (:|expr| (:|numeric_literal| ("3" 32 33)))) ("}" 33 34)))))))"###);
}

#[test]
fn nested_access() {
    assert_snapshot!(parse("SELECT [][1]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|array_expr| ("[" 7 8) ("]" 8 9)) ("[" 9 10) (:|expr| (:|numeric_literal| ("1" 10 11))) ("]" 11 12)))))))"###);
    assert_snapshot!(parse("SELECT {}.bar"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) ("}" 8 9)) ("." 9 10) ("bar" 10 13)))))))"###);

    assert_snapshot!(parse("SELECT {}.bar[x].baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) ("}" 8 9)) ("." 9 10) ("bar" 10 13) ("[" 13 14) (:|expr| (:|column_reference| (:|column_name| ("x" 14 15)))) ("]" 15 16) ("." 16 17) ("baz" 17 20)))))))"###);

    assert_snapshot!(parse("SELECT foo.bar.baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|table_name| ("foo" 7 10)) ("." 10 11) (:|column_name| ("bar" 11 14))) ("." 14 15) ("baz" 15 18)))))))"###);

    assert_snapshot!(parse("SELECT foo['bar'].baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|column_reference| (:|column_name| ("foo" 7 10))) ("[" 10 11) (:|expr| (:|string_literal| ("'bar'" 11 16))) ("]" 16 17) ("." 17 18) ("baz" 18 21)))))))"###);

    assert_snapshot!(parse("SELECT {}.bar[*].baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) ("}" 8 9)) ("." 9 10) ("bar" 10 13) ("[" 13 14) ("*" 14 15) ("]" 15 16) ("." 16 17) ("baz" 17 20)))))))"###);

    assert_snapshot!(parse("SELECT {}.bar..baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|object_expr| ("{" 7 8) ("}" 8 9)) ("." 9 10) ("bar" 10 13) (".." 13 15) ("baz" 15 18)))))))"###);

    assert_snapshot!(parse("SELECT $.bar.baz"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|path_expr| ("$" 7 8) ("." 8 9) ("bar" 9 12) ("." 12 13) ("baz" 13 16))))))))"###);

    assert_snapshot!(parse("SELECT $[#]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|path_expr| ("$" 7 8) ("[" 8 9) ("#" 9 10) ("]" 10 11))))))))"###);
    assert_snapshot!(parse("SELECT $[# - 1]"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|expr| (:|path_expr| ("$" 7 8) ("[" 8 9) ("#" 9 10) ("-" 11 12) (:|expr| (:|numeric_literal| ("1" 13 14))) ("]" 14 15))))))))"###);

    assert_snapshot!(parse("SELECT * FROM foo, UNNEST(foo.bar) AS bar"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("foo" 14 17))) (:|join_operator| ("," 17 18)) (:|table_or_subquery| ("UNNEST" 19 25) ("(" 25 26) (:|expr| (:|column_reference| (:|table_name| ("foo" 26 29)) ("." 29 30) (:|column_name| ("bar" 30 33)))) (")" 33 34) ("AS" 35 37) (:|table_alias| ("bar" 38 41))) (:|join_constraint|)))))))"###);

    assert_snapshot!(parse("SELECT * FROM foo, UNNEST(foo.bar, foo.baz) WITH ORDINALITY AS bar(x, y)"), @r###"(:|sql_stmt_list| (:|sql_stmt| (:|select_stmt| (:|select_core| ("SELECT" 0 6) (:|result_column| (:|asterisk| ("*" 7 8))) (:|from_clause| ("FROM" 9 13) (:|join_clause| (:|table_or_subquery| (:|table_name| ("foo" 14 17))) (:|join_operator| ("," 17 18)) (:|table_or_subquery| ("UNNEST" 19 25) ("(" 25 26) (:|expr| (:|column_reference| (:|table_name| ("foo" 26 29)) ("." 29 30) (:|column_name| ("bar" 30 33)))) ("," 33 34) (:|expr| (:|column_reference| (:|table_name| ("foo" 35 38)) ("." 38 39) (:|column_name| ("baz" 39 42)))) (")" 42 43) ("WITH" 44 48) ("ORDINALITY" 49 59) ("AS" 60 62) (:|table_alias| ("bar" 63 66) ("(" 66 67) ("x" 67 68) ("," 68 69) ("y" 70 71) (")" 71 72))) (:|join_constraint|)))))))"###);
}
