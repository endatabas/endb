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

    literal <-
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

    function_name <- ident;
    type_name <- ident;
    column_name <- ident;

    expr_list <- expr ( "," expr )*;
    column_name_list <- column_name ( "," column_name )*;

    subquery <- "(" select_stmt ")";
    paren_expr <- "(" expr ")";
    extract_expr <- EXTRACT ^( "(" datetime_field FROM expr ")" );
    cast_expr <- CAST ^( "(" expr AS type_name ")" );
    filter_clause <- FILTER ^( "(" WHERE expr ")" );
    function_call_expr <- function_name "(" ( ALL / DISTINCT )? ( "*" / expr_list? order_by_clause? ) ")" filter_clause?;
    exists_expr <- EXISTS ^subquery;
    case_when_then_expr <- WHEN expr THEN expr;
    case_else_expr <- ELSE expr;
    case_expr <- CASE ^( ( !WHEN expr )? case_when_then_expr+ case_else_expr? END );
    column_reference <- ( table_name "." !"." )? column_name;

    spread_expr <- ( "..." / ".." ) expr;
    array_element <- spread_expr / expr;
    array_expr <- ARRAY subquery / ARRAY? ( "[" "]" / "[" array_element ( "," array_element )* ","? "]" );
    computed_property_name <- "["  expr "]";
    object_key_value_pair <- ( ( ident / string_literal / computed_property_name ) ( ":" / "=" ) expr ) / spread_expr / qualified_asterisk / column_reference / bind_parameter;
    object_key_value_list <- object_key_value_pair ( "," object_key_value_pair )* ","?;
    object_expr <- OBJECT "(" object_key_value_list? ")" / "{" object_key_value_list? "}";
    path_property_access <- ( "." ident ) / "[" ( "#" "-" )? expr "]" / "[" "#" "]";
    path_expr <- "$" path_property_access*;

    atom <-
        literal
        / bind_parameter
        / subquery
        / paren_expr
        / extract_expr
        / cast_expr
        / array_expr
        / object_expr
        / path_expr
        / case_expr
        / exists_expr
        / function_call_expr
        / column_reference;

    paren_expr_list <- "(" expr_list ")";
    empty_list <- "(" ")";

    property_access <- ( ( ".." / "." ) ident / ".."? "[" ( "*" / expr ) "]" );
    access_expr <- atom property_access*;
    unary_expr <- ("+" / "-" / "~" )* access_expr;
    concat_expr <- unary_expr ( "||" unary_expr )*;
    mul_expr <- concat_expr ( ( "*" / "/" / "%" ) concat_expr )*;
    add_expr <- mul_expr ( ( "+" / "-" ) mul_expr )*;
    bit_expr <- add_expr ( ( "<<" / ">>" / "&" / "|" ) add_expr )*;
    rel_expr <- bit_expr ( ( "<=" / "<" / ">=" / ">" ) bit_expr )*;
    equal_expr <-
        rel_expr (
            ( ( "==" / "=" / "!=" / "<>" / OVERLAPS / EQUALS / CONTAINS / IMMEDIATELY? PRECEDES / IMMEDIATELY? SUCCEEDS ) rel_expr )
                / NOT? ( LIKE ^( rel_expr ( ESCAPE rel_expr )? ) / ( GLOB / REGEXP / MATCH / "@>" ) ^rel_expr )
                / IS ^( NOT? rel_expr )
                / NOT NULL
                / NOT? BETWEEN ^( rel_expr AND rel_expr )
                / NOT? IN ^( subquery / paren_expr_list / empty_list / table_name )
        )*;

    not_expr <- NOT* equal_expr;
    and_expr <- not_expr ( AND not_expr )*;
    or_expr <- and_expr ( OR and_expr )*;

    expr <- or_expr;

    column_alias <- ident;
    table_name <- INFORMATION_SCHEMA "." ident / ident;

    qualified_asterisk <- table_name "." "*";
    asterisk <- "*";
    invalid_column_alias <- FROM / WHERE / GROUP / HAVING / ORDER / LIMIT / UNION / INTERSECT / EXCEPT;
    result_column <- expr ( AS ^column_alias / !invalid_column_alias column_alias )? / qualified_asterisk / asterisk;

    table_alias <- ident ( "(" column_name_list ")" )?;

    join_constraint <- ON expr;
    join_operator <- "," / ( LEFT OUTER? / INNER / CROSS )? JOIN;
    join_clause <- table_or_subquery ( join_operator table_or_subquery join_constraint? )*;
    system_time_clause <- FOR ^( SYSTEM_TIME ( ALL / AS OF atom / FROM atom TO atom / BETWEEN atom AND atom ) );
    invalid_table_alias <- LEFT / INNER / CROSS / JOIN / WHERE / GROUP / HAVING / ORDER / LIMIT / ON / UNION / INTERSECT / EXCEPT;
    table_or_subquery <-
        UNNEST paren_expr_list  ( WITH ORDINALITY )? AS? table_alias
        / table_name ( NOT INDEXED )? system_time_clause? ( AS ^table_alias / !invalid_table_alias table_alias )?
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
        SELECT ( ALL / DISTINCT )? result_expr_list from_clause? where_clause? group_by_clause? having_clause?
        / values_clause
        / objects_clause;

    compound_operator <- UNION ALL? / INTERSECT / EXCEPT;

    common_table_expression <- table_name ( "(" column_name_list ")" )? AS subquery;

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
    upsert_clause <- ON CONFLICT "(" column_name_list ")" DO ( NOTHING / UPDATE update_clause );

    insert_stmt <- INSERT ( OR REPLACE )? INTO table_name ( "(" column_name_list ")" )? select_stmt upsert_clause?;

    delete_stmt <- DELETE FROM table_name ( WHERE expr )?;
    erase_stmt <- ERASE FROM table_name ( WHERE expr )?;
    update_stmt <- UPDATE table_name update_clause;

    index_name <- ident;
    indexed_column <- column_name ( ASC / DESC )?;
    create_index_stmt <- CREATE UNIQUE? INDEX index_name ON table_name "(" indexed_column ( "," indexed_column )* ")";

    view_name <- ident;
    create_view_stmt <- CREATE ( TEMPORARY / TEMP )? VIEW view_name ( "(" column_name_list ")" )? AS select_stmt;

    signed_number <- ( "+" / "-" )? numeric_literal;
    column_constraint <- PRIMARY KEY / UNIQUE;
    column_def <- column_name !KEY type_name ( "(" signed_number ")" )? column_constraint?;
    foreign_key_clause <- REFERENCES table_name "(" column_name_list ")";
    table_constraint <- PRIMARY KEY "(" column_name_list ")" / FOREIGN KEY "(" column_name_list ")" foreign_key_clause;
    create_table_stmt <- CREATE TABLE table_name "(" column_def ( "," column_def )*  ( "," table_constraint )* ")";

    assertion_name <- ident;
    create_assertion_stmt <- CREATE ASSERTION assertion_name CHECK "(" expr ")";

    drop_assertion_stmt <- DROP ASSERTION ( IF EXISTS )? assertion_name;
    drop_index_stmt <- DROP INDEX ( IF EXISTS )? index_name;
    drop_table_stmt <- DROP TABLE ( IF EXISTS )? table_name;
    drop_view_stmt <- DROP VIEW ( IF EXISTS )? view_name;

    sql_stmt <-
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
