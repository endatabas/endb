use crate::{Event, ParseErr, ParseErrorDescriptor, ParseResult, ParseState};
use endb_proc_macro::peg;

lazy_static::lazy_static! {
    static ref WHITESPACE: regex::Regex = regex::Regex::new("(\\s+|--[^\n\r]*)*").unwrap();
}

peg! {

    ident <- r"\b\p{XID_START}\p{XID_CONTINUE}*\b";

    numeric_literal <- r"\b(0[xX][0-9A-Fa-f]+|[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?)\b";
    string_literal <- r#"(?s)("(?:[^\\"]|\\.)*"|'(?:[^\\']|''|\\.)*')"#;
    blob_literal <- r#"(\b[xX]'[0-9A-Fa-f]*?'|[xX]"[0-9A-Fa-f]*?")"#;

    iso_time_literal <- r"\d{2}:\d{2}:\d{2}(:?\.\d+)?";
    iso_date_literal <- r"\d{4}-\d{2}-\d{2}";
    iso_timestamp_literal <- r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(:?\.\d+)?Z?";

    time_literal <- TIME r#"('\d{2}:\d{2}:\d{2}(:?\.\d+)?'|"\d{2}:\d{2}:\d{2}(:?\.\d+)?")"#;
    date_literal <- DATE r#"('\d{4}-\d{2}-\d{2}'|"\d{4}-\d{2}-\d{2}")"#;
    timestamp_literal <- TIMESTAMP r#"('\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(:?\.\d+)?Z?'|"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(:?\.\d+)?Z?")"#;

    iso_duration_literal <- r"P(\d+(:?[,.]\d+)?[YMD])+(T(\d+(:?[,.]\d+)?[HMS])+)?" / r"PT(\d+(:?[,.]\d+)?[HMS])+";

    datetime_field <- YEAR / MONTH / DAY / HOUR / MINUTE / SECOND;
    interval_literal <- INTERVAL r#"('\d+(:?-\d+)?'|"\d+(:?-\d+)?")|('(:?\d+ )?\d{2}(:?\:\d{2})?(:?\:\d{2})?(:?\.\d+)?'|"(:?\d+ )?\d{2}(:?\:\d{2})?(:?\:\d{2})?(:?\.\d+)?")"# datetime_field ( TO datetime_field )?;

    <literal> <-
        iso_timestamp_literal
        / iso_date_literal
        / iso_time_literal
        / iso_duration_literal
        / timestamp_literal
        / date_literal
        / time_literal
        / interval_literal
        / numeric_literal
        / string_literal
        / blob_literal
        / NULL
        / TRUE
        / FALSE
        / CURRENT_TIME
        / CURRENT_DATE
        / CURRENT_TIMESTAMP;

    bind_parameter <- r"(:?\?|:\p{XID_START}\p{XID_CONTINUE}*\b)";

    simple_func <- ident;
    type_name <- ident;
    column_name <- ident;

    expr_list <- expr ( "," expr )*;
    column_name_list <- "(" column_name ( "," column_name )* ")";

    all_distinct <- ALL / DISTINCT;

    subquery <- "(" select_stmt ")";
    scalar_subquery <- subquery;
    paren_expr <- "(" expr ")";
    extract_expr <- EXTRACT ^( "(" datetime_field FROM expr ")" );
    cast_expr <- CAST ^( "(" expr AS type_name ")" );
    filter_clause <- FILTER ^( "(" WHERE expr ")" );
    aggregate_func <- ARRAY_AGG / AVG / COUNT / GROUP_CONCAT / MIN / MAX / OBJECT_AGG / SUM / TOTAL;
    aggregate_function_invocation <- aggregate_func "(" all_distinct? ( star / expr_list order_by_clause? ) ")" filter_clause?;
    simple_function_invocation <- simple_func "(" expr_list? ")";
    exists_expr <- EXISTS ^subquery;
    case_when_then_expr <- WHEN expr THEN expr;
    case_else_expr <- ELSE expr;
    case_operand <- !WHEN expr;
    case_expr <- CASE ^( case_operand? case_when_then_expr+ case_else_expr? END );
    column_reference <- ( table_name "." !"." )? column_name;

    spread_expr <- ( "..." / ".." ) expr;
    array_element <- spread_expr / expr;
    array_expr <- ARRAY subquery / ARRAY? ( "[" "]" / "[" array_element ( "," array_element )* ","? "]" );
    computed_property <- "["  expr "]";
    shorthand_property <- column_reference / bind_parameter;
    object_key <- ident / string_literal / computed_property;
    object_key_value_pair <- ( object_key ( ":" / "=" ) expr ) / spread_expr / qualified_star / shorthand_property;
    object_key_value_list <- object_key_value_pair ( "," object_key_value_pair )* ","?;
    object_expr <- OBJECT "(" object_key_value_list? ")" / "{" object_key_value_list? "}";

    path_object_label <- "." ident;
    path_array_length <- "#";
    path_array_index <-( path_array_length "-" )? numeric_literal / path_array_length;
    path_array_access <- "[" path_array_index "]";
    path_element <- path_object_label / path_array_access;
    path_expr <- "$" path_element*;

    <atom> <-
        literal
        / bind_parameter
        / scalar_subquery
        / paren_expr
        / extract_expr
        / cast_expr
        / array_expr
        / object_expr
        / path_expr
        / case_expr
        / exists_expr
        / aggregate_function_invocation
        / simple_function_invocation
        / column_reference;

    paren_expr_list <- "(" expr_list ")";
    empty_list <- "(" ")";

    property_field_access <- "." ident;
    property_recursive_field_access <- ".." ident;
    property_bracket_access <- "[" expr "]";
    property_bracket_wildcard_access <- "[" star "]";
    property_recursive_bracket_access <- ".." ( property_bracket_access / property_bracket_wildcard_access );

    <property_access> <- property_field_access / property_recursive_field_access / property_bracket_access / property_bracket_wildcard_access / property_recursive_bracket_access;
    <access_expr> <- atom property_access*;
    <unary_expr> <- ("+" / "-" / "~" )* access_expr;
    <concat_expr> <- unary_expr ( "||" unary_expr )*;
    <mul_expr> <- concat_expr ( ( "*" / "/" / "%" ) concat_expr )*;
    <add_expr> <- mul_expr ( ( "+" / "-" ) mul_expr )*;
    <bit_expr> <- add_expr ( ( "<<" / ">>" / "&" / "|" ) add_expr )*;
    <rel_expr> <- bit_expr ( ( "<=" / "<" / ">=" / ">" ) bit_expr )*;
    <equal_expr> <-
        rel_expr (
            ( ( "==" / "=" / "!=" / "<>" / OVERLAPS / EQUALS / CONTAINS / IMMEDIATELY? PRECEDES / IMMEDIATELY? SUCCEEDS ) rel_expr )
                / NOT? ( LIKE ^( rel_expr ( ESCAPE rel_expr )? ) / ( GLOB / REGEXP / MATCH / "@>" ) ^rel_expr )
                / IS ^( NOT? rel_expr )
                / NOT NULL
                / NOT? BETWEEN ^( rel_expr AND rel_expr )
                / NOT? IN ^( subquery / paren_expr_list / empty_list / table_name )
        )*;

    <not_expr> <- NOT* equal_expr;
    <and_expr> <- not_expr ( AND not_expr )*;
    <or_expr> <- and_expr ( OR and_expr )*;

    <expr> <- or_expr;

    column_alias <- ident;
    table_name <- INFORMATION_SCHEMA "." ident / ident;

    qualified_star <- table_name "." star;
    star <- "*";
    invalid_column_alias <- FROM / WHERE / GROUP / HAVING / ORDER / LIMIT / UNION / INTERSECT / EXCEPT;
    result_column <- expr ( AS ^column_alias / !invalid_column_alias column_alias )? / qualified_star / star;

    table_alias <- ident column_name_list?;

    join_constraint <- ON expr;
    join_operator <- "," / ( LEFT OUTER? / INNER / CROSS )? JOIN;
    join_clause <- table_or_subquery ( join_operator table_or_subquery join_constraint? )*;
    system_time_clause <- FOR ^( SYSTEM_TIME ( ALL / AS OF atom / FROM atom TO atom / BETWEEN atom AND atom ) );
    invalid_table_alias <- LEFT / INNER / CROSS / JOIN / WHERE / GROUP / HAVING / ORDER / LIMIT / ON / UNION / INTERSECT / EXCEPT;
    with_ordinality <- WITH ORDINALITY;
    unnest_table_function <- UNNEST paren_expr_list with_ordinality?;
    not_indexed <- NOT INDEXED;
    table_or_subquery <-
        unnest_table_function AS? table_alias
        / table_name not_indexed? system_time_clause? ( AS ^table_alias / !invalid_table_alias table_alias )?
        / subquery AS? table_alias
        / "(" join_clause ")";

    from_clause <- FROM join_clause;
    where_clause <- WHERE expr;
    group_by_clause <- GROUP BY expr_list;
    having_clause <- HAVING expr;

    values_clause <- VALUES paren_expr_list ( "," paren_expr_list )*;
    objects_clause <- OBJECTS? object_expr ( "," object_expr)*;

    result_expr_list <- result_column ( "," result_column )*;

    select_core <-
        SELECT all_distinct? result_expr_list from_clause? where_clause? group_by_clause? having_clause?
        / values_clause
        / objects_clause;

    compound_operator <- UNION ALL? / INTERSECT / EXCEPT;

    common_table_expression <- table_name column_name_list? AS subquery;

    with_clause <- WITH RECURSIVE? common_table_expression ( "," common_table_expression )*;

    ordering_term <- expr ( ASC / DESC )?;
    order_by_clause <- ORDER BY ordering_term ( "," ordering_term )*;
    limit_offset_clause <- LIMIT expr ( ( "," / OFFSET ) expr )?;

    select_stmt <-
        with_clause?
        select_core ( compound_operator select_core )*
        order_by_clause?
        limit_offset_clause?;

    update_set_assignment <- ( column_name / path_expr ) "=" expr;
    update_set_clause <- SET update_set_assignment ( "," update_set_assignment )*;
    update_remove_clause <- ( UNSET / REMOVE) ( column_name / path_expr ) ( "," ( column_name / path_expr ) )*;
    update_patch_clause <- PATCH? object_expr;
    update_where_clause <- WHERE expr;
    update_clause <- update_set_clause? update_remove_clause? update_patch_clause? update_where_clause?;
    upsert_clause <- ON CONFLICT column_name_list DO ( NOTHING / UPDATE update_clause );

    or_replace <- OR REPLACE;
    insert_stmt <- INSERT or_replace? INTO table_name column_name_list? select_stmt upsert_clause?;

    delete_stmt <- DELETE FROM table_name ( WHERE expr )?;
    erase_stmt <- ERASE FROM table_name ( WHERE expr )?;
    update_stmt <- UPDATE table_name update_clause;

    index_name <- ident;
    indexed_column <- column_name ( ASC / DESC )?;
    create_index_stmt <- CREATE UNIQUE? INDEX index_name ON table_name "(" indexed_column ( "," indexed_column )* ")";

    view_name <- ident;
    create_view_stmt <- CREATE ( TEMPORARY / TEMP )? VIEW view_name column_name_list? AS select_stmt;

    signed_number <- ( "+" / "-" )? numeric_literal;
    column_constraint <- PRIMARY KEY / UNIQUE;
    column_def <- column_name !KEY type_name ( "(" signed_number ")" )? column_constraint?;
    foreign_key_clause <- REFERENCES table_name column_name_list;
    table_constraint <- PRIMARY KEY column_name_list / FOREIGN KEY column_name_list foreign_key_clause;
    create_table_stmt <- CREATE TABLE table_name "(" column_def ( "," column_def )*  ( "," table_constraint )* ")";

    assertion_name <- ident;
    create_assertion_stmt <- CREATE ASSERTION assertion_name CHECK "(" expr ")";

    if_exists <- IF EXISTS;
    drop_assertion_stmt <- DROP ASSERTION if_exists? assertion_name;
    drop_index_stmt <- DROP INDEX if_exists? index_name;
    drop_table_stmt <- DROP TABLE if_exists? table_name;
    drop_view_stmt <- DROP VIEW if_exists? view_name;

    <sql_stmt> <-
        select_stmt
        / insert_stmt
        / delete_stmt
        / erase_stmt
        / update_stmt
        / create_assertion_stmt
        / create_index_stmt
        / create_view_stmt
        / create_table_stmt
        / drop_assertion_stmt
        / drop_index_stmt
        / drop_table_stmt
        / drop_view_stmt;

    pub sql_stmt_list <- r"^" sql_stmt ( ";" sql_stmt )* ";"? !r".";

}
