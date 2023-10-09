use crate::{Event, ParseErr, ParseErrorDescriptor, ParseResult, ParseState};
use endb_proc_macro::peg;

peg! {

    <whitespace> <- ~"(\\s+|--[^\n\r]*)*";

    <ident> <- #"\\b\\p{XID_START}\\p{XID_CONTINUE}*\\b";

    numeric_literal <- #"\\b(0[xX][0-9A-Fa-f]+|[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?)\\b";
    string_literal <- #"(\"(?:\\\"|[^\"])*\"|'(?:''|[^'])*')";
    blob_literal <- #"(\\b[xX]'[0-9A-Fa-f]*?'|[xX]\"[0-9A-Fa-f]*?\")";

    iso_time_literal <- #"\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?";
    iso_date_literal <- #"\\d{4}-\\d{2}-\\d{2}";
    iso_timestamp_literal <- #"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?Z?";

    time_literal <- TIME ^ (#"('\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?'|\"\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?\")");
    date_literal <- DATE ^ (#"('\\d{4}-\\d{2}-\\d{2}'|\"\\d{4}-\\d{2}-\\d{2}\")");
    timestamp_literal <- TIMESTAMP ^ (#"('\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?Z?'|\"\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(:?\\.\\d+)?Z?\")");

    iso_duration_literal <- #"P(\\d+(:?[,.]\\d+)?Y)?(:?\\d+(:?[,.]\\d+)?M)?(:?\\d+(:?[,.]\\d+)?D)?(T(\\d+(:?[,.]\\d+)?H)?(:?\\d+(:?[,.]\\d+)?M)?(:?\\d+(:?[,.]\\d+)?S)?)?";

    datetime_field <- YEAR / MONTH / DAY / HOUR / MINUTE / SECOND;
    interval_literal <- INTERVAL ^ (#"('\\d+(:?-\\d+)?'|\"\\d+(:?-\\d+)?\")|('(:?\\d+ )?\\d{2}(:?\\:\\d{2})?(:?\\:\\d{2})?(:?\\.\\d+)?'|\"(:?\\d+ )?\\d{2}(:?\\:\\d{2})?(:?\\:\\d{2})?(:?\\.\\d+)?\")") datetime_field ( TO ^datetime_field )?;

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

    bind_parameter <- #"(:?\\?|:\\p{XID_START}\\p{XID_CONTINUE}*\\b)";

    function_name <- ident;
    type_name <- ident;
    column_name <- ident;

    subquery <- "(" select_stmt ")";
    paren_expr <- "(" expr ")";
    extract_expr <- EXTRACT ^( "(" datetime_field FROM expr ")" );
    cast_expr <- CAST ^( "(" expr AS type_name ")" );
    function_call_expr <- function_name "(" ( DISTINCT? ( ( expr / "*" ) ( "," expr )* )? / "*" ) order_by_clause? ")" ( FILTER ^( "(" WHERE expr ")" ) )?;
    exists_expr <- EXISTS ^subquery;
    case_when_then_expr <- WHEN expr THEN expr;
    case_expr <- CASE ^( (!WHEN expr)? case_when_then_expr+ ( ELSE expr )? END );
    column_reference <- ( table_name "." )? column_name;

    array_expr <- ARRAY subquery / ARRAY? ( "[" "]" / "[" "..."? expr  ( "," "..."? expr )* ","? "]" );
    object_key_value_pair <- ( ( ident / string_literal / "["  expr "]" ) (#"[:=]") expr ) / "..." expr / ( table_name "." "*" ) / column_reference / bind_parameter;
    object_expr <- OBJECT "(" ( object_key_value_pair ( "," object_key_value_pair )* ","? )? ")" / "{" ( object_key_value_pair ( "," object_key_value_pair )* ","? )? "}";
    path_expr <- "$" ( ( "." ident ) / "[" ( "#" "-" )? expr "]" / "[" "#" "]" )* ;

    <atom> <-
        literal
        / bind_parameter
        / subquery
        / paren_expr
        / extract_expr
        / cast_expr
        / array_expr
        / object_expr
        / path_expr
        / function_call_expr
        / exists_expr
        / case_expr
        / column_reference;

    <access> <- atom ( ( ".." / "." ) ident / "[" ( expr / "*" ) "]" )*;
    <unary> <- ("+" / "-" / "~" )* access;
    <concat> <- unary ( "||" unary )*;
    <mul> <- concat ( ( "*" / "/" / "%" ) concat )*;
    <add> <- mul ( ( "+" / "-" ) mul )*;
    <bit> <- add ( ( "<<" / ">>" / "&" / "|" ) add )*;
    <comp> <- bit ( ( "<=" / "<" / ">=" / ">" ) bit )*;
    <equal> <-
        comp (
            ( ( "==" / "=" / "!=" / "<>" / OVERLAPS / EQUALS / CONTAINS / IMMEDIATELY? PRECEDES / IMMEDIATELY? SUCCEEDS ) comp )
                / NOT? ( LIKE ^( comp ( ESCAPE comp )? ) / ( GLOB / REGEXP / MATCH / "@>" ) ^comp )
                / IS ^( NOT? comp )
                / NOT NULL
                / NOT? BETWEEN ^( comp AND comp )
                / NOT? IN ^( "(" select_stmt ")" / "(" expr ( "," expr )* ")" / "(" ")" )
        )*;

    <not> <- NOT* equal;
    <and> <- not ( AND not )*;
    <or> <- and ( OR and )*;

    expr <- or;

    column_alias <- ident;
    table_name <- INFORMATION_SCHEMA "." ident / ident;

    qualified_asterisk <- table_name "." ^"*";
    asterisk <- "*";
    invalid_column_alias <- FROM / WHERE / GROUP / HAVING / ORDER / LIMIT / UNION / INTERSECT / EXCEPT;
    result_column <- expr ( AS ^column_alias / !invalid_column_alias column_alias )? / qualified_asterisk / asterisk;

    table_alias <- ident ( "(" ident ("," ident)* ")" )?;

    join_constraint <- ( ON expr )?;
    join_operator <- "," / ( LEFT OUTER? / INNER / CROSS )? JOIN;
    join_clause <- table_or_subquery ( join_operator table_or_subquery join_constraint )*;
    invalid_table_alias <- LEFT / INNER / CROSS / JOIN / WHERE / GROUP / HAVING / ORDER / LIMIT / ON / UNION / INTERSECT / EXCEPT;
    table_or_subquery <-
        UNNEST "(" expr ( "," expr )* ")" ( WITH ^ORDINALITY )? AS table_alias
        / table_name ( NOT ^INDEXED )?  ( FOR ^( SYSTEM_TIME ( ALL / AS OF atom / FROM atom TO atom / BETWEEN atom AND atom ) ) )? ( AS ^table_alias / !invalid_table_alias table_alias )?
        / "(" select_stmt ")" AS table_alias
        / "(" join_clause ")";

    from_clause <- FROM join_clause;
    where_clause <- WHERE expr;
    group_by_clause <- GROUP BY expr ( "," expr )*;
    having_clause <- HAVING expr;

    select_core <-
        SELECT ( ALL / DISTINCT )? ( result_column ( "," result_column )* )
        from_clause? where_clause? group_by_clause? having_clause?
        / VALUES ( "(" expr ( "," expr)* ")" ) ( "," ( "(" expr ( "," expr)* ")" ) )*
        / OBJECTS object_expr ( "," object_expr)*;

    compound_operator <- UNION ALL? / INTERSECT / EXCEPT;
    common_table_expression <- table_name ( "(" column_name ( "," column_name )* ")" )? AS "(" select_stmt ")";

    with_clause <- WITH RECURSIVE? common_table_expression ( "," common_table_expression )*;

    ordering_term <- expr ( ASC / DESC )?;
    order_by_clause <- ORDER BY ordering_term ( "," ordering_term )*;
    limit_offset_clause <- LIMIT expr ( ( "," / OFFSET ) expr )?;

    select_stmt <-
        with_clause?
        select_core ( compound_operator select_core )*
        order_by_clause?
        limit_offset_clause?;

    update_body <- ( SET ( column_name / path_expr ) "=" expr ( "," ( column_name / path_expr ) "=" expr )* )? ( ( UNSET / REMOVE) ( column_name / path_expr ) ( "," ( column_name / path_expr ) )* )? ( PATCH? object_expr )? ( WHERE expr )?;

    insert_stmt <- INSERT ( OR ^REPLACE )? INTO table_name ( ( "(" column_name ( "," column_name)* ")" )? select_stmt / OBJECTS? object_expr ( "," object_expr)* )
        ( ON ^CONFLICT "(" column_name ("," column_name )* ")" DO ( NOTHING / UPDATE update_body ) )?;

    delete_stmt <- DELETE FROM table_name ( WHERE expr )?;
    erase_stmt <- ERASE FROM table_name ( WHERE expr )?;
    update_stmt <- UPDATE table_name update_body;

    create_index_stmt <- CREATE UNIQUE? INDEX ident ON table_name "(" ordering_term ( "," ordering_term )* ")";
    create_view_stmt <- CREATE ( TEMPORARY / TEMP )? VIEW ident ( "(" column_name ( "," column_name )* ")" )? AS select_stmt;

    column_definition <- PRIMARY KEY "(" column_name ( "," column_name )* ")" / FOREIGN KEY "(" column_name ( "," column_name )* ")" REFERENCES table_name "(" column_name ( "," column_name )* ")" / column_name type_name ( "(" numeric_literal ")" )? ( PRIMARY KEY )? UNIQUE?;
    create_table_stmt <- CREATE TABLE table_name "(" column_definition ( "," column_definition )* ")";

    create_assertion_stmt <- CREATE ASSERTION ident CHECK "(" expr ")";
    ddl_drop_stmt <- DROP ^( INDEX / VIEW / TABLE / ASSERTION ) ( IF EXISTS )? ident;

    sql_stmt <- select_stmt / insert_stmt / delete_stmt / erase_stmt / update_stmt / create_index_stmt / create_view_stmt / create_table_stmt /  create_assertion_stmt / ddl_drop_stmt;
    sql_stmt_list <- whitespace sql_stmt ( ";" sql_stmt )* ";"? !(~".");

}
