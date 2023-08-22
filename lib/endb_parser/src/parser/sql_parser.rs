use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::Rich;
use chumsky::extra::{Default, Err, ParserExtra};
use chumsky::prelude::*;

use std::string::String;

use super::ast::Ast;
use super::expr_parser::*;
use super::util::*;

fn sql_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    let id = id_ast_parser_no_pad().then_ignore(text::whitespace());

    let id_list = id
        .clone()
        .separated_by(pad(','))
        .at_least(1)
        .collect()
        .map(List);

    let id_list_parens = id_list.clone().delimited_by(pad('('), pad(')'));

    let col_ref = col_ref_ast_parser_no_pad().then_ignore(text::whitespace());

    let col_ref_list = col_ref
        .clone()
        .separated_by(pad(','))
        .at_least(1)
        .collect()
        .map(List);

    let positive_integer = just('0')
        .not()
        .ignore_then(text::int(10).slice().from_str().unwrapped().map(Integer))
        .then_ignore(text::whitespace());

    let non_negative_integer = text::int(10)
        .slice()
        .from_str()
        .unwrapped()
        .map(Integer)
        .then_ignore(text::whitespace());

    let select_stmt = recursive(|query| {
        let expr = expr_ast_parser(query.clone());
        let subquery = query.delimited_by(pad('('), pad(')'));

        let order_by_list = expr
            .clone()
            .then(
                choice((kw("ASC").to(Asc), kw("DESC").to(Desc)))
                    .or_not()
                    .map(|dir| KW(dir.unwrap_or(Asc))),
            )
            .map(|(var, dir)| List(vec![var, dir]))
            .separated_by(pad(','))
            .at_least(1)
            .collect()
            .map(List);

        let information_schema_table_name = kw_no_pad("INFORMATION_SCHEMA")
            .then(just('.'))
            .then(text::ident())
            .map_with_span(|_, span: SimpleSpan<_>| Id {
                start: span.start() as i32,
                end: span.end() as i32,
            })
            .then_ignore(text::whitespace());

        let table_alias = choice((
            kw("AS").ignore_then(id.clone()),
            id.clone().and_is(
                choice((
                    kw("CROSS"),
                    kw("LEFT"),
                    kw("JOIN"),
                    kw("WHERE"),
                    kw("GROUP"),
                    kw("HAVING"),
                    kw("ORDER"),
                    kw("LIMIT"),
                    kw("ON"),
                    kw("UNION"),
                    kw("EXCEPT"),
                    kw("INTERSECT"),
                ))
                .not(),
            ),
        ))
        .then(id_list_parens.clone().or_not());

        let system_time_atom = atom_ast_parser();

        let table = choice((
            subquery
                .clone()
                .map(|x| (x, None))
                .then(table_alias.clone().map(Some)),
            kw("UNNEST")
                .ignore_then(
                    expr.clone()
                        .separated_by(pad(','))
                        .at_least(1)
                        .collect()
                        .map(List)
                        .delimited_by(pad('('), pad(')')),
                )
                .then(
                    kw("WITH")
                        .ignore_then(kw("ORDINALITY"))
                        .to(WithOrdinality)
                        .map(KW)
                        .or_not(),
                )
                .map(|(exprs, ordinality)| {
                    let mut acc = vec![KW(Unnest), exprs];
                    add_clause(&mut acc, WithOrdinality, ordinality);
                    (List(acc), None)
                })
                .then(table_alias.clone().map(Some))
                .boxed(),
            choice((
                information_schema_table_name,
                id.clone()
                    .then_ignore(kw("NOT").then_ignore(kw("INDEXED")).or_not()),
            ))
            .then(
                kw("FOR")
                    .ignore_then(kw("SYSTEM_TIME"))
                    .ignore_then(choice((
                        kw("AS")
                            .ignore_then(kw("OF"))
                            .ignore_then(system_time_atom.clone())
                            .map(|as_of| List(vec![KW(AsOf), as_of])),
                        kw("FROM")
                            .ignore_then(system_time_atom.clone())
                            .then_ignore(kw("TO"))
                            .then(system_time_atom.clone())
                            .map(|(start, end)| List(vec![KW(From), start, end])),
                        kw("BETWEEN")
                            .ignore_then(system_time_atom.clone())
                            .then_ignore(kw("AND"))
                            .then(system_time_atom)
                            .map(|(start, end)| List(vec![KW(Between), start, end])),
                    )))
                    .or_not(),
            )
            .then(table_alias.or_not()),
        ))
        .map(|((table, temporal), alias)| match (temporal, alias) {
            (Some(temporal), Some((alias, column_names))) => List(vec![
                table,
                alias,
                column_names.unwrap_or_else(|| List(vec![])),
                temporal,
            ]),
            (Some(temporal), None) => List(vec![table.clone(), table, List(vec![]), temporal]),
            (None, Some((alias, column_names))) => {
                let mut acc = vec![table, alias];
                if let Some(column_names) = column_names {
                    acc.push(column_names)
                }
                List(acc)
            }
            (None, None) => List(vec![table]),
        })
        .boxed();

        let select_clause = kw("SELECT")
            .ignore_then(
                choice((kw("DISTINCT").to(Distinct), kw("ALL").to(All)))
                    .map(KW)
                    .or_not(),
            )
            .then(
                choice((
                    pad('*').map(|_| List(vec![KW(Mul)])),
                    id.clone()
                        .then_ignore(pad('.'))
                        .then_ignore(pad('*'))
                        .map(|id| List(vec![List(vec![KW(Mul), id])])),
                    expr.clone()
                        .then(
                            choice((
                                kw("AS").ignore_then(id.clone()),
                                id.clone().and_is(
                                    choice((
                                        kw("FROM"),
                                        kw("WHERE"),
                                        kw("GROUP"),
                                        kw("HAVING"),
                                        kw("ORDER"),
                                        kw("LIMIT"),
                                        kw("UNION"),
                                        kw("EXCEPT"),
                                        kw("INTERSECT"),
                                    ))
                                    .not(),
                                ),
                            ))
                            .or_not(),
                        )
                        .map(|(expr, id)| match id {
                            Some(id) => List(vec![expr, id]),
                            None => List(vec![expr]),
                        }),
                ))
                .separated_by(pad(','))
                .at_least(1)
                .collect()
                .map(List),
            );

        let table_list_element = choice((
            table.clone().foldl(
                choice((
                    kw("LEFT").to(Left).then_ignore(kw("OUTER").or_not()),
                    kw("INNER").to(Inner),
                ))
                .or_not()
                .then_ignore(kw("JOIN"))
                .then(table.clone())
                .then_ignore(kw("ON"))
                .then(expr.clone())
                .repeated(),
                |lhs, ((join_type, rhs), on)| {
                    List(vec![
                        KW(Join),
                        lhs,
                        rhs,
                        KW(On),
                        on,
                        KW(Type),
                        KW(join_type.unwrap_or(Inner)),
                    ])
                },
            ),
            table
                .clone()
                .then_ignore(kw("CROSS").ignore_then(kw("JOIN")))
                .then(table)
                .delimited_by(pad('('), pad(')'))
                .map(|(lhs, rhs)| {
                    List(vec![
                        KW(Join),
                        lhs,
                        rhs,
                        KW(On),
                        KW(True),
                        KW(Type),
                        KW(Inner),
                    ])
                }),
        ));

        let from_clause = kw("FROM")
            .ignore_then(
                table_list_element
                    .separated_by(choice((
                        pad(','),
                        kw("CROSS").ignore_then(kw("JOIN")).ignored(),
                    )))
                    .at_least(1)
                    .collect()
                    .map(List),
            )
            .or_not();

        let where_clause = kw("WHERE").ignore_then(expr.clone()).or_not();

        let group_by_clause = kw("GROUP")
            .ignore_then(kw("BY"))
            .ignore_then(col_ref_list.clone())
            .or_not();

        let having_clause = kw("HAVING").ignore_then(expr.clone()).or_not();

        let select_stmt = select_clause
            .then(from_clause)
            .then(where_clause)
            .then(group_by_clause)
            .then(having_clause)
            .map(
                |(((((distinct, select_list), from), where_clause), group_by), having)| {
                    let mut acc = vec![KW(Select), select_list];

                    add_clause(&mut acc, Distinct, distinct);
                    add_clause(&mut acc, From, from);
                    add_clause(&mut acc, Where, where_clause);
                    add_clause(&mut acc, GroupBy, group_by);
                    add_clause(&mut acc, Having, having);

                    List(acc)
                },
            );

        let values_stmt = kw("VALUES").ignore_then(
            expr.clone()
                .separated_by(pad(','))
                .at_least(1)
                .collect()
                .map(List)
                .delimited_by(pad('('), pad(')'))
                .separated_by(pad(','))
                .at_least(1)
                .collect()
                .map(|values| List(vec![KW(Values), List(values)])),
        );

        let select_core = choice((select_stmt, values_stmt));

        let compound_select_stmt = select_core.clone().foldl(
            choice((
                kw("EXCEPT").to(Except),
                kw("INTERSECT").to(Intersect),
                kw("UNION").then_ignore(kw("ALL")).to(UnionAll),
                kw("UNION").to(Union),
            ))
            .padded()
            .then(select_core)
            .repeated(),
            |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]),
        );

        let order_by = kw("ORDER")
            .ignore_then(kw("BY"))
            .ignore_then(order_by_list.clone())
            .or_not();

        let limit_clause = kw("LIMIT")
            .ignore_then(non_negative_integer)
            .then(
                choice((kw("OFFSET").ignored(), pad(',')))
                    .ignore_then(non_negative_integer)
                    .or_not(),
            )
            .or_not();

        let full_select = compound_select_stmt.then(order_by).then(limit_clause).map(
            |((query, order_by), limit_offset)| {
                let mut acc = match query {
                    List(x) => x,
                    _ => unreachable!(),
                };

                add_clause(&mut acc, OrderBy, order_by);

                if let Some((limit, offset)) = limit_offset {
                    add_clause(&mut acc, Limit, Some(limit));
                    add_clause(&mut acc, Offset, offset);
                }

                List(acc)
            },
        );

        let with_element = id
            .clone()
            .then(id_list_parens.clone().or_not())
            .then_ignore(kw("AS"))
            .then(subquery.clone())
            .map(|((id, id_list), query)| match id_list {
                Some(id_list) => List(vec![id, query, id_list]),
                None => List(vec![id, query]),
            });

        kw("WITH")
            .ignore_then(
                kw("RECURSIVE").to(Recursive).map(KW).or_not().then(
                    with_element
                        .separated_by(pad(','))
                        .at_least(1)
                        .collect()
                        .map(List),
                ),
            )
            .or_not()
            .then(full_select)
            .map(|(with_list, full_select)| match with_list {
                None => full_select,
                Some((recursive, with_list)) => {
                    let mut acc = vec![KW(With), with_list, full_select];
                    add_clause(&mut acc, Recursive, recursive);
                    List(acc)
                }
            })
    });

    let expr = expr_ast_parser(select_stmt.clone());

    let update_body = kw("SET")
        .ignore_then(
            id.clone()
                .then_ignore(pad('='))
                .then(expr.clone())
                .map(|(id, expr)| List(vec![id, expr]))
                .separated_by(pad(','))
                .collect()
                .map(List),
        )
        .or_not()
        .then(kw("UNSET").ignore_then(id_list.clone()).or_not())
        .then(kw("WHERE").ignore_then(expr.clone()).or_not())
        .map(|((updates, unsets), expr)| {
            let mut acc = vec![updates.unwrap_or_else(|| List(vec![]))];
            add_clause(&mut acc, Unset, unsets);
            add_clause(&mut acc, Where, expr);
            List(acc)
        });

    let insert_stmt = kw("INSERT")
        .ignore_then(kw("OR").then_ignore(kw("REPLACE")).or_not())
        .ignore_then(kw("INTO"))
        .ignore_then(choice((
            id.clone()
                .then(id_list_parens.clone().or_not())
                .then(select_stmt.clone())
                .map(|((id, id_list), query)| {
                    let mut acc = vec![KW(Insert), id, query];
                    add_clause(&mut acc, ColumnNames, id_list);
                    List(acc)
                }),
            id.clone()
                .then_ignore(kw("OBJECTS").or_not())
                .then(
                    object_ast_parser(expr.clone())
                        .separated_by(pad(','))
                        .at_least(1)
                        .collect()
                        .map(List),
                )
                .map(|(id, object_list)| List(vec![KW(InsertObjects), id, object_list])),
        )))
        .then(
            kw("ON")
                .ignore_then(kw("CONFLICT"))
                .ignore_then(id_list_parens.clone())
                .then_ignore(kw("DO"))
                .then(choice((
                    kw("NOTHING").to(None),
                    kw("UPDATE").ignore_then(update_body.clone().map(Some)),
                )))
                .or_not(),
        )
        .map(|(insert, conflict)| {
            let mut acc = match insert {
                List(x) => x,
                _ => unreachable!(),
            };

            if let Some((ids, update)) = conflict {
                add_clause(&mut acc, OnConflict, Some(ids));
                add_clause(&mut acc, Update, update);
            }

            List(acc)
        });

    let delete_stmt = kw("DELETE")
        .ignore_then(kw("FROM"))
        .ignore_then(id.clone())
        .then(kw("WHERE").ignore_then(expr.clone()).or_not())
        .map(|(id, expr)| {
            let mut acc = vec![KW(Delete), id];
            add_clause(&mut acc, Where, expr);
            List(acc)
        });

    let update_stmt = kw("UPDATE")
        .ignore_then(id.clone())
        .then(update_body)
        .map(|(id, update)| {
            let mut acc = vec![KW(Update), id];
            match update {
                List(mut x) => acc.append(&mut x),
                _ => unreachable!(),
            };
            List(acc)
        });

    let order_by_list = id
        .clone()
        .then(choice((kw("ASC"), kw("DESC"))).or_not())
        .separated_by(pad(','))
        .at_least(1);

    let create_index_stmt = kw("CREATE")
        .ignore_then(kw("UNIQUE").or_not())
        .ignore_then(kw("INDEX"))
        .ignore_then(id.clone())
        .then_ignore(kw("ON"))
        .then(id.clone())
        .then_ignore(order_by_list.delimited_by(pad('('), pad(')')))
        .map(|(index, table)| List(vec![KW(CreateIndex), index, table]));

    let create_view_stmt = kw("CREATE")
        .ignore_then(choice((kw("TEMPORARY"), kw("TEMP"))).or_not())
        .ignore_then(kw("VIEW"))
        .ignore_then(id.clone())
        .then(id_list_parens.clone().or_not())
        .then_ignore(kw("AS"))
        .then(select_stmt.clone())
        .map(|((id, id_list), query)| {
            let mut acc = vec![KW(CreateView), id, query];
            add_clause(&mut acc, ColumnNames, id_list);
            List(acc)
        });

    let col_def = choice((
        kw("PRIMARY")
            .then(kw("KEY"))
            .then(id_list_parens.clone())
            .map(|_| None),
        kw("FOREIGN")
            .then(kw("KEY"))
            .then(id_list.delimited_by(pad('('), pad(')')).clone())
            .then(kw("REFERENCES"))
            .then(id.clone())
            .then(id_list_parens)
            .map(|_| None),
        id.clone()
            .then_ignore(
                id.clone()
                    .then_ignore(positive_integer.delimited_by(pad('('), pad(')')).or_not())
                    .then_ignore(kw("PRIMARY").then_ignore(kw("KEY")).or_not())
                    .then_ignore(kw("UNIQUE").or_not()),
            )
            .map(Some),
    ));

    let create_table_stmt = kw("CREATE")
        .ignore_then(kw("TABLE"))
        .ignore_then(id.clone())
        .then(
            col_def
                .separated_by(pad(','))
                .at_least(1)
                .collect::<Vec<Option<Ast>>>()
                .map(|col_defs| List(col_defs.into_iter().flatten().collect()))
                .delimited_by(pad('('), pad(')')),
        )
        .map(|(id, columns)| List(vec![KW(CreateTable), id, columns]));

    let ddl_drop_stmt = kw("DROP")
        .ignore_then(
            choice((
                kw("INDEX").to(DropIndex),
                kw("VIEW").to(DropView),
                kw("TABLE").to(DropTable),
            ))
            .map(KW),
        )
        .then(
            kw("IF")
                .ignore_then(kw("EXISTS"))
                .to(IfExists)
                .map(KW)
                .or_not(),
        )
        .then(id.clone())
        .map(|((op, if_exists), id)| {
            let mut acc = vec![op, id];
            add_clause(&mut acc, IfExists, if_exists);
            List(acc)
        });

    let sql_stmt = choice((
        select_stmt,
        insert_stmt,
        delete_stmt,
        update_stmt,
        create_index_stmt,
        create_view_stmt,
        create_table_stmt,
        ddl_drop_stmt,
    ));

    let multiple_stmts = sql_stmt
        .clone()
        .then_ignore(pad(';'))
        .repeated()
        .at_least(1)
        .collect()
        .map(|stmts| List(vec![KW(MultipleStatements), List(stmts)]));

    choice((multiple_stmts, sql_stmt))
        .padded()
        .then_ignore(end())
}

pub fn sql_ast_parser_no_errors<'input>() -> impl Parser<'input, &'input str, Ast, Default> + Clone
{
    sql_ast_parser::<Default>()
}

pub fn sql_ast_parser_with_errors<'input>(
) -> impl Parser<'input, &'input str, Ast, Err<Rich<'input, char>>> + Clone {
    sql_ast_parser::<Err<Rich<'input, char>>>()
}

pub fn parse_errors_to_string(src: &str, errs: Vec<Rich<char>>) -> String {
    let mut buf = Vec::new();
    errs.into_iter().for_each(|e| {
        Report::build(ReportKind::Error, (), e.span().start)
            .with_message(e.to_string())
            .with_label(
                Label::new(e.span().into_range())
                    .with_message(e.reason().to_string())
                    .with_color(Color::Red),
            )
            .finish()
            .write(Source::from(&src), &mut buf)
            .unwrap()
    });
    String::from_utf8(buf).unwrap()
}

pub fn annotate_input_with_error(src: &str, message: &str, start: usize, end: usize) -> String {
    parse_errors_to_string(
        src,
        vec![Rich::custom(SimpleSpan::new(start, end), message)],
    )
}

#[cfg(test)]
mod tests {
    use crate::{parser::ast::Ast, sql_parser::sql_ast_parser_with_errors};
    use chumsky::Parser;
    use insta::assert_yaml_snapshot;

    fn parse(src: &str) -> Result<Ast, Vec<String>> {
        match sql_ast_parser_with_errors().parse(src).into_result() {
            Ok(ast) => Ok(ast),
            Err(errors) => Err(errors.into_iter().map(|e| e.to_string()).collect()),
        }
    }

    #[test]
    fn identifier_expr() {
        assert_yaml_snapshot!(parse("SELECT foo"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Id:
                        start: 7
                        end: 10
        "###);

        assert_yaml_snapshot!(parse("SELECT x.y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Id:
                        start: 7
                        end: 10
        "###)
    }

    #[test]
    fn parameter_expr() {
        assert_yaml_snapshot!(parse("SELECT ?"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Parameter
        "###);
        assert_yaml_snapshot!(parse("SELECT :name"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Parameter
                        - Id:
                            start: 8
                            end: 12
        "###);
    }

    #[test]
    fn number_expr() {
        assert_yaml_snapshot!(parse("SELECT 2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 2
        "###);
        assert_yaml_snapshot!(parse("SELECT 2.2e2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Float: 220
        "###);
        assert_yaml_snapshot!(parse("SELECT 2e-2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Float: 0.02
        "###);
        assert_yaml_snapshot!(parse("SELECT 9223372036854775808"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 9223372036854775808
        "###);
        assert_yaml_snapshot!(parse("SELECT 170141183460469231731687303715884105727, -170141183460469231731687303715884105727"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 170141183460469231731687303715884105727
                - List:
                    - List:
                        - KW: Minus
                        - Integer: 170141183460469231731687303715884105727
        "###);
        assert_yaml_snapshot!(parse("SELECT 170141183460469231731687303715884105728, -170141183460469231731687303715884105728"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Float: 170141183460469230000000000000000000000
                - List:
                    - List:
                        - KW: Minus
                        - Float: 170141183460469230000000000000000000000
        "###)
    }

    #[test]
    fn operator_expr() {
        assert_yaml_snapshot!(parse("SELECT 2 < x"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Lt
                        - Integer: 2
                        - Id:
                            start: 11
                            end: 12
        "###);
        assert_yaml_snapshot!(parse("SELECT 3 > 2.1"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Gt
                        - Integer: 3
                        - Float: 2.1
        "###);
        assert_yaml_snapshot!(parse("SELECT x>=y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Ge
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 10
                            end: 11
        "###);
        assert_yaml_snapshot!(parse("SELECT x<>y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Ne
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 10
                            end: 11
        "###);
        assert_yaml_snapshot!(parse("SELECT x AND y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: And
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 13
                            end: 14
        "###);
        assert_yaml_snapshot!(parse("SELECT x and y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: And
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 13
                            end: 14
        "###);
        assert_yaml_snapshot!(parse("SELECT x IS NOT y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Not
                        - List:
                            - KW: Is
                            - Id:
                                start: 7
                                end: 8
                            - Id:
                                start: 16
                                end: 17
        "###);
        assert_yaml_snapshot!(parse("SELECT x IS y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Is
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 12
                            end: 13
        "###);
        assert_yaml_snapshot!(parse("SELECT x BETWEEN y AND 2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Between
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 17
                            end: 18
                        - Integer: 2
        "###);
        assert_yaml_snapshot!(parse("SELECT x NOT BETWEEN y AND 2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Not
                        - List:
                            - KW: Between
                            - Id:
                                start: 7
                                end: 8
                            - Id:
                                start: 21
                                end: 22
                            - Integer: 2
        "###);
        assert_yaml_snapshot!(parse("SELECT x NOT NULL"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Not
                        - List:
                            - KW: Is
                            - Id:
                                start: 7
                                end: 8
                            - KW: "Null"
        "###);

        assert_yaml_snapshot!(parse("SELECT x || 'foo'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Concat
                        - Id:
                            start: 7
                            end: 8
                        - String:
                            start: 13
                            end: 16
        "###);

        assert_yaml_snapshot!(parse("SELECT x IMMEDIATELY PRECEDES y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: ImmediatelyPrecedes
                        - Id:
                            start: 7
                            end: 8
                        - Id:
                            start: 30
                            end: 31
        "###);
    }

    #[test]
    fn case_expr() {
        assert_yaml_snapshot!(parse("SELECT CASE WHEN 2 THEN 1 END"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Case
                        - List:
                            - List:
                                - Integer: 2
                                - Integer: 1
        "###);
        assert_yaml_snapshot!(parse("SELECT CASE 3 WHEN 2 THEN 1 END"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Case
                        - Integer: 3
                        - List:
                            - List:
                                - Integer: 2
                                - Integer: 1
        "###);
        assert_yaml_snapshot!(parse("SELECT CASE WHEN 2 THEN 1 ELSE 0 END"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Case
                        - List:
                            - List:
                                - Integer: 2
                                - Integer: 1
                            - List:
                                - KW: Else
                                - Integer: 0
        "###);
    }

    #[test]
    fn string_expr() {
        assert_yaml_snapshot!(parse("SELECT 'foo'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - String:
                        start: 8
                        end: 11
        "###);

        assert_yaml_snapshot!(parse("SELECT \"foo\""), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - String:
                        start: 8
                        end: 11
        "###);
        assert_yaml_snapshot!(parse("SELECT \"josé\""), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - String:
                        start: 8
                        end: 13
        "###);
        assert_yaml_snapshot!(parse("SELECT \"f\\noo\""), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - String:
                        start: 8
                        end: 13
        "###);
        assert_yaml_snapshot!(parse("SELECT \"f\\\"oo\""), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - String:
                        start: 8
                        end: 13
        "###);
    }

    #[test]
    fn binary_expr() {
        assert_yaml_snapshot!(parse("SELECT X'AF01'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Blob
                        - String:
                            start: 9
                            end: 13
        "###);
        assert_yaml_snapshot!(parse("SELECT x'AF01'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Blob
                        - String:
                            start: 9
                            end: 13
        "###);
    }

    #[test]
    fn function_expr() {
        assert_yaml_snapshot!(parse("SELECT foo(2, y)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Function
                        - Id:
                            start: 7
                            end: 10
                        - List:
                            - Integer: 2
                            - Id:
                                start: 14
                                end: 15
        "###);
        assert_yaml_snapshot!(parse("SELECT count(y)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: AggregateFunction
                        - KW: Count
                        - List:
                            - Id:
                                start: 13
                                end: 14
        "###);
        assert_yaml_snapshot!(parse("SELECT TOTAL(y)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: AggregateFunction
                        - KW: Total
                        - List:
                            - Id:
                                start: 13
                                end: 14
        "###);
        assert_yaml_snapshot!(parse("SELECT group_concat(DISTINCT y, ':'"), @r###"
        ---
        Err:
          - "found '(' expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', or ','"
        "###);
        assert_yaml_snapshot!(parse("SELECT count(*)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: AggregateFunction
                        - KW: CountStar
                        - List: []
        "###);

        assert_yaml_snapshot!(parse("SELECT ARRAY_AGG(x ORDER BY y DESC)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: AggregateFunction
                        - KW: ArrayAgg
                        - List:
                            - Id:
                                start: 17
                                end: 18
                        - KW: OrderBy
                        - List:
                            - List:
                                - Id:
                                    start: 28
                                    end: 29
                                - KW: Desc
        "###);

        assert_yaml_snapshot!(parse("SELECT count(*) FILTER (WHERE x > 2)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: AggregateFunction
                        - KW: CountStar
                        - List: []
                        - KW: Where
                        - List:
                            - KW: Gt
                            - Id:
                                start: 30
                                end: 31
                            - Integer: 2
        "###);

        assert_yaml_snapshot!(parse("SELECT CAST ( - 69 AS INTEGER )"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Cast
                        - List:
                            - KW: Minus
                            - Integer: 69
                        - Id:
                            start: 22
                            end: 29
        "###);
    }

    #[test]
    fn error() {
        assert_yaml_snapshot!(parse("SELEC"), @r###"
        ---
        Err:
          - found end of input expected something else
        "###);

        assert_yaml_snapshot!(parse("SELECT x 2"), @r###"
        ---
        Err:
          - "found '2' expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', or ','"
        "###);
    }

    #[test]
    fn simple_select() {
        assert_yaml_snapshot!(parse("SELECT 123"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 123
        "###);
        assert_yaml_snapshot!(parse("SELECT *"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
        "###);

        assert_yaml_snapshot!(parse("SELECT x.* FROM x"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Mul
                        - Id:
                            start: 7
                            end: 8
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 16
                        end: 17
        "###);
    }

    #[test]
    fn select_as() {
        assert_yaml_snapshot!(parse("SELECT 1 AS x, 2 y FROM z, w AS foo, (SELECT bar) baz WHERE FALSE"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
                    - Id:
                        start: 12
                        end: 13
                - List:
                    - Integer: 2
                    - Id:
                        start: 17
                        end: 18
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 24
                        end: 25
                - List:
                    - Id:
                        start: 27
                        end: 28
                    - Id:
                        start: 32
                        end: 35
                - List:
                    - List:
                        - KW: Select
                        - List:
                            - List:
                                - Id:
                                    start: 45
                                    end: 48
                    - Id:
                        start: 50
                        end: 53
            - KW: Where
            - KW: "False"
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 AS from"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
                    - Id:
                        start: 12
                        end: 16
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 from"), @r###"
        ---
        Err:
          - found end of input expected something else
        "###);
    }

    #[test]
    fn select() {
        assert_yaml_snapshot!(parse("SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Id:
                        start: 7
                        end: 8
                - List:
                    - Id:
                        start: 10
                        end: 11
                - List:
                    - Integer: 123
                - List:
                    - List:
                        - KW: Function
                        - Id:
                            start: 18
                            end: 24
                        - List:
                            - Id:
                                start: 25
                                end: 26
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 33
                        end: 40
            - KW: Where
            - List:
                - KW: And
                - List:
                    - KW: Gt
                    - Id:
                        start: 47
                        end: 48
                    - Id:
                        start: 51
                        end: 52
                - List:
                    - KW: Lt
                    - Id:
                        start: 57
                        end: 58
                    - Integer: 100
            - KW: OrderBy
            - List:
                - List:
                    - Id:
                        start: 74
                        end: 75
                    - KW: Desc
                - List:
                    - Id:
                        start: 82
                        end: 83
                    - KW: Asc
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 FROM x ORDER BY x.a + 1"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
            - KW: OrderBy
            - List:
                - List:
                    - List:
                        - KW: Plus
                        - Id:
                            start: 25
                            end: 28
                        - Integer: 1
                    - KW: Asc
        "###);

        assert_yaml_snapshot!(parse("SELECT 1 FROM x CROSS JOIN y"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
                - List:
                    - Id:
                        start: 27
                        end: 28
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 FROM (x CROSS JOIN y)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - KW: Join
                    - List:
                        - Id:
                            start: 15
                            end: 16
                    - List:
                        - Id:
                            start: 28
                            end: 29
                    - KW: "On"
                    - KW: "True"
                    - KW: Type
                    - KW: Inner
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 FROM x LEFT JOIN y ON TRUE)"), @r###"
        ---
        Err:
          - "found ')' expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', or ','"
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 INTERSECT SELECT 2 UNION SELECT 3"), @r###"
        ---
        Ok:
          List:
            - KW: Union
            - List:
                - KW: Intersect
                - List:
                    - KW: Select
                    - List:
                        - List:
                            - Integer: 1
                - List:
                    - KW: Select
                    - List:
                        - List:
                            - Integer: 2
            - List:
                - KW: Select
                - List:
                    - List:
                        - Integer: 3
        "###);
        assert_yaml_snapshot!(parse("VALUES (1, 2), (3, 4)"), @r###"
        ---
        Ok:
          List:
            - KW: Values
            - List:
                - List:
                    - Integer: 1
                    - Integer: 2
                - List:
                    - Integer: 3
                    - Integer: 4
        "###);

        assert_yaml_snapshot!(parse("SELECT * FROM (VALUES (1, 2), (3, 4)) AS foo(a, b)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - List:
                        - KW: Values
                        - List:
                            - List:
                                - Integer: 1
                                - Integer: 2
                            - List:
                                - Integer: 3
                                - Integer: 4
                    - Id:
                        start: 41
                        end: 44
                    - List:
                        - Id:
                            start: 45
                            end: 46
                        - Id:
                            start: 48
                            end: 49
        "###);
    }

    #[test]
    fn group_by_having() {
        assert_yaml_snapshot!(parse("SELECT 1 FROM x GROUP BY y HAVING TRUE"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
            - KW: GroupBy
            - List:
                - Id:
                    start: 25
                    end: 26
            - KW: Having
            - KW: "True"
        "###);
    }

    #[test]
    fn select_distinct() {
        assert_yaml_snapshot!(parse("SELECT DISTINCT 1 FROM x"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: Distinct
            - KW: Distinct
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 23
                        end: 24
        "###);
    }

    #[test]
    fn select_limit_offset() {
        assert_yaml_snapshot!(parse("SELECT 1 FROM x LIMIT 1 OFFSET 2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
            - KW: Limit
            - Integer: 1
            - KW: Offset
            - Integer: 2
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 FROM x LIMIT 1, 2"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
            - KW: Limit
            - Integer: 1
            - KW: Offset
            - Integer: 2
        "###);
        assert_yaml_snapshot!(parse("SELECT 1 FROM x LIMIT 1"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - Integer: 1
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 15
            - KW: Limit
            - Integer: 1
        "###);
    }

    #[test]
    fn dml() {
        assert_yaml_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2)"), @r###"
        ---
        Ok:
          List:
            - KW: Insert
            - Id:
                start: 12
                end: 15
            - List:
                - KW: Values
                - List:
                    - List:
                        - Integer: 1
                    - List:
                        - Integer: 2
            - KW: ColumnNames
            - List:
                - Id:
                    start: 17
                    end: 18
        "###);

        assert_yaml_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2) ON CONFLICT (x) DO NOTHING"), @r###"
        ---
        Ok:
          List:
            - KW: Insert
            - Id:
                start: 12
                end: 15
            - List:
                - KW: Values
                - List:
                    - List:
                        - Integer: 1
                    - List:
                        - Integer: 2
            - KW: ColumnNames
            - List:
                - Id:
                    start: 17
                    end: 18
            - KW: OnConflict
            - List:
                - Id:
                    start: 49
                    end: 50
        "###);
        assert_yaml_snapshot!(parse("INSERT INTO foo (x) VALUES (1), (2) ON CONFLICT (x) DO UPDATE SET x = 2"), @r###"
        ---
        Ok:
          List:
            - KW: Insert
            - Id:
                start: 12
                end: 15
            - List:
                - KW: Values
                - List:
                    - List:
                        - Integer: 1
                    - List:
                        - Integer: 2
            - KW: ColumnNames
            - List:
                - Id:
                    start: 17
                    end: 18
            - KW: OnConflict
            - List:
                - Id:
                    start: 49
                    end: 50
            - KW: Update
            - List:
                - List:
                    - List:
                        - Id:
                            start: 66
                            end: 67
                        - Integer: 2
        "###);

        assert_yaml_snapshot!(parse("DELETE FROM foo WHERE FALSE"), @r###"
        ---
        Ok:
          List:
            - KW: Delete
            - Id:
                start: 12
                end: 15
            - KW: Where
            - KW: "False"
        "###);
        assert_yaml_snapshot!(parse("DELETE FROM foo"), @r###"
        ---
        Ok:
          List:
            - KW: Delete
            - Id:
                start: 12
                end: 15
        "###);
        assert_yaml_snapshot!(parse("UPDATE foo SET x = 1, y = 2 WHERE NULL"), @r###"
        ---
        Ok:
          List:
            - KW: Update
            - Id:
                start: 7
                end: 10
            - List:
                - List:
                    - Id:
                        start: 15
                        end: 16
                    - Integer: 1
                - List:
                    - Id:
                        start: 22
                        end: 23
                    - Integer: 2
            - KW: Where
            - KW: "Null"
        "###);

        assert_yaml_snapshot!(parse("UPDATE foo UNSET z, w WHERE FALSE"), @r###"
        ---
        Ok:
          List:
            - KW: Update
            - Id:
                start: 7
                end: 10
            - List: []
            - KW: Unset
            - List:
                - Id:
                    start: 17
                    end: 18
                - Id:
                    start: 20
                    end: 21
            - KW: Where
            - KW: "False"
        "###);
        assert_yaml_snapshot!(parse("UPDATE foo SET x = 1, y = 2 UNSET z, w WHERE FALSE"), @r###"
        ---
        Ok:
          List:
            - KW: Update
            - Id:
                start: 7
                end: 10
            - List:
                - List:
                    - Id:
                        start: 15
                        end: 16
                    - Integer: 1
                - List:
                    - Id:
                        start: 22
                        end: 23
                    - Integer: 2
            - KW: Unset
            - List:
                - Id:
                    start: 34
                    end: 35
                - Id:
                    start: 37
                    end: 38
            - KW: Where
            - KW: "False"
        "###);
    }

    #[test]
    fn ddl() {
        assert_yaml_snapshot!(parse("CREATE UNIQUE INDEX foo ON t1(a1,b1)"), @r###"
        ---
        Ok:
          List:
            - KW: CreateIndex
            - Id:
                start: 20
                end: 23
            - Id:
                start: 27
                end: 29
        "###);
        assert_yaml_snapshot!(parse("DROP INDEX foo"), @r###"
        ---
        Ok:
          List:
            - KW: DropIndex
            - Id:
                start: 11
                end: 14
        "###);
        assert_yaml_snapshot!(parse("CREATE TEMP VIEW foo AS SELECT 1"), @r###"
        ---
        Ok:
          List:
            - KW: CreateView
            - Id:
                start: 17
                end: 20
            - List:
                - KW: Select
                - List:
                    - List:
                        - Integer: 1
        "###);
        assert_yaml_snapshot!(parse("CREATE TEMP VIEW foo(bar) AS SELECT 1"), @r###"
        ---
        Ok:
          List:
            - KW: CreateView
            - Id:
                start: 17
                end: 20
            - List:
                - KW: Select
                - List:
                    - List:
                        - Integer: 1
            - KW: ColumnNames
            - List:
                - Id:
                    start: 21
                    end: 24
        "###);
        assert_yaml_snapshot!(parse("DROP VIEW IF EXISTS foo"), @r###"
        ---
        Ok:
          List:
            - KW: DropView
            - Id:
                start: 20
                end: 23
            - KW: IfExists
            - KW: IfExists
        "###);
        assert_yaml_snapshot!(parse("CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INTEGER, x1 VARCHAR(40), FOREIGN KEY (y1) REFERENCES t2(z1), PRIMARY KEY(a1, b2))"), @r###"
        ---
        Ok:
          List:
            - KW: CreateTable
            - Id:
                start: 13
                end: 15
            - List:
                - Id:
                    start: 16
                    end: 18
                - Id:
                    start: 40
                    end: 42
                - Id:
                    start: 52
                    end: 54
        "###);
        assert_yaml_snapshot!(parse("DROP TABLE foo"), @r###"
        ---
        Ok:
          List:
            - KW: DropTable
            - Id:
                start: 11
                end: 14
        "###);
    }

    #[test]
    fn multiple() {
        assert_yaml_snapshot!(parse("SELECT 1; SELECT 1;"), @r###"
        ---
        Ok:
          List:
            - KW: MultipleStatements
            - List:
                - List:
                    - KW: Select
                    - List:
                        - List:
                            - Integer: 1
                - List:
                    - KW: Select
                    - List:
                        - List:
                            - Integer: 1
        "###);
        assert_yaml_snapshot!(parse("SELECT 1; SELECT 1"), @r###"
        ---
        Err:
          - "found end of input expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', ',', or ';'"
        "###);
    }

    #[test]
    fn with() {
        assert_yaml_snapshot!(parse("WITH foo(a) AS (SELECT 1) SELECT * FROM foo"), @r###"
        ---
        Ok:
          List:
            - KW: With
            - List:
                - List:
                    - Id:
                        start: 5
                        end: 8
                    - List:
                        - KW: Select
                        - List:
                            - List:
                                - Integer: 1
                    - List:
                        - Id:
                            start: 9
                            end: 10
            - List:
                - KW: Select
                - List:
                    - List:
                        - KW: Mul
                - KW: From
                - List:
                    - List:
                        - Id:
                            start: 40
                            end: 43
        "###);
        assert_yaml_snapshot!(parse("WITH foo AS (SELECT 1), bar(a, b) AS (SELECT 1, 2) SELECT * FROM foo, bar"), @r###"
        ---
        Ok:
          List:
            - KW: With
            - List:
                - List:
                    - Id:
                        start: 5
                        end: 8
                    - List:
                        - KW: Select
                        - List:
                            - List:
                                - Integer: 1
                - List:
                    - Id:
                        start: 24
                        end: 27
                    - List:
                        - KW: Select
                        - List:
                            - List:
                                - Integer: 1
                            - List:
                                - Integer: 2
                    - List:
                        - Id:
                            start: 28
                            end: 29
                        - Id:
                            start: 31
                            end: 32
            - List:
                - KW: Select
                - List:
                    - List:
                        - KW: Mul
                - KW: From
                - List:
                    - List:
                        - Id:
                            start: 65
                            end: 68
                    - List:
                        - Id:
                            start: 70
                            end: 73
        "###);
        assert_yaml_snapshot!(parse("WITH RECURSIVE foo(a) AS (SELECT 1) SELECT * FROM foo"), @r###"
        ---
        Ok:
          List:
            - KW: With
            - List:
                - List:
                    - Id:
                        start: 15
                        end: 18
                    - List:
                        - KW: Select
                        - List:
                            - List:
                                - Integer: 1
                    - List:
                        - Id:
                            start: 19
                            end: 20
            - List:
                - KW: Select
                - List:
                    - List:
                        - KW: Mul
                - KW: From
                - List:
                    - List:
                        - Id:
                            start: 50
                            end: 53
            - KW: Recursive
            - KW: Recursive
        "###);
    }

    #[test]
    fn information_schema() {
        assert_yaml_snapshot!(parse("SELECT * FROM information_schema.tables"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 39
        "###);

        assert_yaml_snapshot!(parse("SELECT * FROM information_schema.columns AS c"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 40
                    - Id:
                        start: 44
                        end: 45
        "###);
    }

    #[test]
    fn system_time() {
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF ?"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: AsOf
                        - List:
                            - KW: Parameter
        "###);

        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF TIMESTAMP '2011-01-02 00:00:00'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: AsOf
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 51
                                end: 70
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME AS OF 2011-01-02 AS e"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 54
                        end: 55
                    - List: []
                    - List:
                        - KW: AsOf
                        - List:
                            - KW: Date
                            - String:
                                start: 40
                                end: 50
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME FROM TIMESTAMP '2011-01-02 00:00:00' TO TIMESTAMP '2011-12-31 00:00:00'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: From
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 50
                                end: 69
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 85
                                end: 104
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME FROM 2011-01-02T00:00:00 TO 2011-12-31T00:00:00"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: From
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 39
                                end: 58
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 62
                                end: 81
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME BETWEEN TIMESTAMP '2011-01-02 00:00:00' AND TIMESTAMP '2011-12-31 00:00:00'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: Between
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 53
                                end: 72
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 89
                                end: 108
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM Emp FOR SYSTEM_TIME BETWEEN 2011-01-02T00:00:00 AND 2011-12-31T00:00:00"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                    - Id:
                        start: 14
                        end: 17
                    - List: []
                    - List:
                        - KW: Between
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 42
                                end: 61
                        - List:
                            - KW: Timestamp
                            - String:
                                start: 66
                                end: 85
        "###);
    }

    #[test]
    fn temporal_scalars() {
        assert_yaml_snapshot!(parse("SELECT 2001-01-01"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Date
                        - String:
                            start: 7
                            end: 17
        "###);
        assert_yaml_snapshot!(parse("SELECT 2001-01"), @r###"
        ---
        Err:
          - "found '1' expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', or ','"
        "###);
        assert_yaml_snapshot!(parse("SELECT DATE '2001-01-01'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Date
                        - String:
                            start: 13
                            end: 23
        "###);

        assert_yaml_snapshot!(parse("SELECT 12:01:20"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Time
                        - String:
                            start: 7
                            end: 15
        "###);
        assert_yaml_snapshot!(parse("SELECT 12:"), @r###"
        ---
        Err:
          - "found ':' expected '.', '[', '|', '*', '/', '%', '+', '-', '<', '>', '=', or ','"
        "###);
        assert_yaml_snapshot!(parse("SELECT TIME '12:01:20'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Time
                        - String:
                            start: 13
                            end: 21
        "###);

        assert_yaml_snapshot!(parse("SELECT 2023-05-16T14:43:39.970062Z"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Timestamp
                        - String:
                            start: 7
                            end: 34
        "###);
        assert_yaml_snapshot!(parse("SELECT TIMESTAMP '2023-05-16 14:43:39'"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Timestamp
                        - String:
                            start: 18
                            end: 37
        "###);
        assert_yaml_snapshot!(parse("SELECT P3Y6M4DT12H30M5S"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Duration
                        - String:
                            start: 7
                            end: 23
        "###);
        assert_yaml_snapshot!(parse("SELECT P3Y6M"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Duration
                        - String:
                            start: 7
                            end: 12
        "###);
        assert_yaml_snapshot!(parse("SELECT PT12H30M5S"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Duration
                        - String:
                            start: 7
                            end: 17
        "###);

        assert_yaml_snapshot!(parse("SELECT CURRENT_DATE"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: CurrentDate
        "###);

        assert_yaml_snapshot!(parse("SELECT INTERVAL '1' HOUR"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Interval
                        - String:
                            start: 17
                            end: 18
                        - KW: Hour
        "###);
        assert_yaml_snapshot!(parse("SELECT INTERVAL '01:40' MINUTE TO SECOND"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Interval
                        - String:
                            start: 17
                            end: 22
                        - KW: Minute
                        - KW: Second
        "###);
        assert_yaml_snapshot!(parse("SELECT INTERVAL '2-3' YEAR TO MONTH"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Interval
                        - String:
                            start: 17
                            end: 20
                        - KW: Year
                        - KW: Month
        "###);
    }

    #[test]
    fn semi_structured() {
        assert_yaml_snapshot!(parse("SELECT []"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Array
                        - List: []
        "###);
        assert_yaml_snapshot!(parse("SELECT ARRAY []"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Array
                        - List: []
        "###);
        assert_yaml_snapshot!(parse("SELECT [1, 2]"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Array
                        - List:
                            - Integer: 1
                            - Integer: 2
        "###);

        assert_yaml_snapshot!(parse("SELECT ARRAY [1, 2,]"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Array
                        - List:
                            - Integer: 1
                            - Integer: 2
        "###);

        assert_yaml_snapshot!(parse("SELECT ARRAY (SELECT 1)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: ArrayQuery
                        - List:
                            - KW: Select
                            - List:
                                - List:
                                    - Integer: 1
        "###);

        assert_yaml_snapshot!(parse("SELECT {}"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List: []
        "###);
        assert_yaml_snapshot!(parse("SELECT OBJECT()"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List: []
        "###);
        assert_yaml_snapshot!(parse("SELECT {foo: 2, bar: 'baz'}"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - Id:
                                    start: 8
                                    end: 11
                                - Integer: 2
                            - List:
                                - Id:
                                    start: 16
                                    end: 19
                                - String:
                                    start: 22
                                    end: 25
        "###);
        assert_yaml_snapshot!(parse("SELECT {\"foo\": 2, 'bar': 'baz',}"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - String:
                                    start: 9
                                    end: 12
                                - Integer: 2
                            - List:
                                - String:
                                    start: 19
                                    end: 22
                                - String:
                                    start: 26
                                    end: 29
        "###);
        assert_yaml_snapshot!(parse("SELECT {foo = 2, bar = 'baz'}"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - Id:
                                    start: 8
                                    end: 11
                                - Integer: 2
                            - List:
                                - Id:
                                    start: 17
                                    end: 20
                                - String:
                                    start: 24
                                    end: 27
        "###);
        assert_yaml_snapshot!(parse("SELECT OBJECT(foo: 2, bar: 'baz')"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - Id:
                                    start: 14
                                    end: 17
                                - Integer: 2
                            - List:
                                - Id:
                                    start: 22
                                    end: 25
                                - String:
                                    start: 28
                                    end: 31
        "###);

        assert_yaml_snapshot!(parse("SELECT {address: {street: 'Street', number: 42}, friends: [1, 2]}"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - Id:
                                    start: 8
                                    end: 15
                                - List:
                                    - KW: Object
                                    - List:
                                        - List:
                                            - Id:
                                                start: 18
                                                end: 24
                                            - String:
                                                start: 27
                                                end: 33
                                        - List:
                                            - Id:
                                                start: 36
                                                end: 42
                                            - Integer: 42
                            - List:
                                - Id:
                                    start: 49
                                    end: 56
                                - List:
                                    - KW: Array
                                    - List:
                                        - Integer: 1
                                        - Integer: 2
        "###);

        assert_yaml_snapshot!(parse("SELECT { :baz, foo, ...bar, ['baz' || 'boz']: 42 }"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Object
                        - List:
                            - List:
                                - KW: ShorthandProperty
                                - List:
                                    - KW: Parameter
                                    - Id:
                                        start: 10
                                        end: 13
                            - List:
                                - KW: ShorthandProperty
                                - Id:
                                    start: 15
                                    end: 18
                            - List:
                                - KW: SpreadProperty
                                - Id:
                                    start: 23
                                    end: 26
                            - List:
                                - KW: ComputedProperty
                                - List:
                                    - KW: Concat
                                    - String:
                                        start: 30
                                        end: 33
                                    - String:
                                        start: 39
                                        end: 42
                                - Integer: 42
        "###);
        assert_yaml_snapshot!(parse("SELECT [ 1, 2, ... bar, 4 ]"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Array
                        - List:
                            - Integer: 1
                            - Integer: 2
                            - List:
                                - KW: SpreadProperty
                                - Id:
                                    start: 19
                                    end: 22
                            - Integer: 4
        "###);

        assert_yaml_snapshot!(parse("INSERT INTO users {foo: 2, bar: 'baz'}, {foo: 3}"), @r###"
        ---
        Ok:
          List:
            - KW: InsertObjects
            - Id:
                start: 12
                end: 17
            - List:
                - List:
                    - KW: Object
                    - List:
                        - List:
                            - Id:
                                start: 19
                                end: 22
                            - Integer: 2
                        - List:
                            - Id:
                                start: 27
                                end: 30
                            - String:
                                start: 33
                                end: 36
                - List:
                    - KW: Object
                    - List:
                        - List:
                            - Id:
                                start: 41
                                end: 44
                            - Integer: 3
        "###);

        assert_yaml_snapshot!(parse("INSERT INTO users OBJECTS {foo: 3}"), @r###"
        ---
        Ok:
          List:
            - KW: InsertObjects
            - Id:
                start: 12
                end: 17
            - List:
                - List:
                    - KW: Object
                    - List:
                        - List:
                            - Id:
                                start: 27
                                end: 30
                            - Integer: 3
        "###);
    }

    #[test]
    fn nested_access() {
        assert_yaml_snapshot!(parse("SELECT [][1]"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Array
                            - List: []
                        - Integer: 1
        "###);
        assert_yaml_snapshot!(parse("SELECT {}.bar"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Object
                            - List: []
                        - Id:
                            start: 10
                            end: 13
        "###);

        assert_yaml_snapshot!(parse("SELECT {}.bar[x].baz"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Access
                            - List:
                                - KW: Access
                                - List:
                                    - KW: Object
                                    - List: []
                                - Id:
                                    start: 10
                                    end: 13
                            - Id:
                                start: 14
                                end: 15
                        - Id:
                            start: 17
                            end: 20
        "###);

        assert_yaml_snapshot!(parse("SELECT foo.bar.baz"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - Id:
                            start: 7
                            end: 14
                        - Id:
                            start: 15
                            end: 18
        "###);

        assert_yaml_snapshot!(parse("SELECT foo['bar'].baz"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Access
                            - Id:
                                start: 7
                                end: 10
                            - String:
                                start: 12
                                end: 15
                        - Id:
                            start: 18
                            end: 21
        "###);

        assert_yaml_snapshot!(parse("SELECT {}.bar[*].baz"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Access
                            - List:
                                - KW: Access
                                - List:
                                    - KW: Object
                                    - List: []
                                - Id:
                                    start: 10
                                    end: 13
                            - KW: Mul
                        - Id:
                            start: 17
                            end: 20
        "###);

        assert_yaml_snapshot!(parse("SELECT {}.bar..baz"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - List:
                        - KW: Access
                        - List:
                            - KW: Access
                            - List:
                                - KW: Object
                                - List: []
                            - Id:
                                start: 10
                                end: 13
                        - Id:
                            start: 15
                            end: 18
                        - KW: Recursive
                        - KW: Recursive
        "###);

        assert_yaml_snapshot!(parse("SELECT * FROM foo, UNNEST(foo.bar)"), @r###"
        ---
        Err:
          - "found '(' expected ','"
        "###);
        assert_yaml_snapshot!(parse("SELECT * FROM foo, UNNEST(foo.bar) AS bar"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                - List:
                    - List:
                        - KW: Unnest
                        - List:
                            - Id:
                                start: 26
                                end: 33
                    - Id:
                        start: 38
                        end: 41
        "###);

        assert_yaml_snapshot!(parse("SELECT * FROM foo, UNNEST(foo.bar, foo.baz) WITH ORDINALITY AS bar(x, y)"), @r###"
        ---
        Ok:
          List:
            - KW: Select
            - List:
                - List:
                    - KW: Mul
            - KW: From
            - List:
                - List:
                    - Id:
                        start: 14
                        end: 17
                - List:
                    - List:
                        - KW: Unnest
                        - List:
                            - Id:
                                start: 26
                                end: 33
                            - Id:
                                start: 35
                                end: 42
                        - KW: WithOrdinality
                        - KW: WithOrdinality
                    - Id:
                        start: 63
                        end: 66
                    - List:
                        - Id:
                            start: 67
                            end: 68
                        - Id:
                            start: 70
                            end: 71
        "###);
    }
}
