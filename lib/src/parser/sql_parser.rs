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

    let order_by_list = choice((id.clone(), positive_integer))
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

    let select_stmt = recursive(|query| {
        let expr = expr_ast_parser(query.clone());
        let subquery = query.delimited_by(pad('('), pad(')'));

        let table = choice((
            subquery,
            id.clone()
                .then_ignore(kw("NOT").then_ignore(kw("INDEXED")).or_not()),
        ))
        .then(
            choice((
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
            .or_not(),
        )
        .map(|(id, alias)| match alias {
            Some(alias) => List(vec![id, alias]),
            None => List(vec![id]),
        });

        let select_clause = kw("SELECT")
            .ignore_then(
                choice((kw("DISTINCT").to(Distinct), kw("ALL").to(All)))
                    .map(KW)
                    .or_not(),
            )
            .then(
                choice((
                    pad('*').map(|_| List(vec![KW(Mul)])),
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

        compound_select_stmt.then(order_by).then(limit_clause).map(
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
        )
    });

    let expr = expr_ast_parser(select_stmt.clone());

    let insert_stmt = kw("INSERT")
        .ignore_then(kw("OR").then_ignore(kw("REPLACE")).or_not())
        .ignore_then(kw("INTO"))
        .ignore_then(id.clone())
        .then(id_list_parens.clone().or_not())
        .then(select_stmt.clone())
        .map(|((id, id_list), query)| {
            let mut acc = vec![KW(Insert), id, query];
            add_clause(&mut acc, ColumnNames, id_list);
            List(acc)
        });

    let delete_stmt = kw("DELETE")
        .ignore_then(kw("FROM"))
        .ignore_then(id.clone())
        .then_ignore(kw("WHERE"))
        .then(expr.clone())
        .map(|(id, expr)| List(vec![KW(Delete), id, expr]));

    let update_stmt = kw("UPDATE")
        .ignore_then(id.clone())
        .then_ignore(kw("SET"))
        .then(
            id.clone()
                .then_ignore(pad('='))
                .then(expr.clone())
                .map(|(id, expr)| List(vec![id, expr]))
                .separated_by(pad(','))
                .collect()
                .map(List),
        )
        .then(kw("WHERE").ignore_then(expr.clone()).or_not())
        .map(|((id, updates), expr)| {
            let mut acc = vec![KW(Update), id, updates];
            add_clause(&mut acc, Where, expr);
            List(acc)
        });

    let create_index_stmt = kw("CREATE")
        .ignore_then(kw("UNIQUE").or_not())
        .ignore_then(kw("INDEX"))
        .ignore_then(id.clone())
        .then_ignore(kw("ON"))
        .then(id.clone())
        .then_ignore(order_by_list.clone().delimited_by(pad('('), pad(')')))
        .map(|(index, table)| List(vec![KW(CreateIndex), index, table]));

    let create_view_stmt = kw("CREATE")
        .ignore_then(choice((kw("TEMPORARY"), kw("TEMP"))).or_not())
        .ignore_then(kw("VIEW"))
        .ignore_then(id.clone())
        .then_ignore(kw("AS"))
        .then(select_stmt.clone())
        .map(|(id, query)| List(vec![KW(CreateView), id, query]));

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

    choice((
        select_stmt,
        insert_stmt,
        delete_stmt,
        update_stmt,
        create_index_stmt,
        create_view_stmt,
        create_table_stmt,
        ddl_drop_stmt,
    ))
    .padded()
    .then_ignore(pad(';').or_not())
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

#[cfg(test)]
mod tests {
    use crate::{
        parser::ast::Ast, parser::ast::Ast::*, parser::ast::Keyword::*,
        sql_parser::sql_ast_parser_no_errors,
    };
    use chumsky::Parser;

    fn parse(src: &str) -> Ast {
        match sql_ast_parser_no_errors().parse(src).into_result() {
            Ok(ast) => ast,
            Err(_) => List(vec![]),
        }
    }

    #[test]
    fn identifier_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Id { start: 7, end: 10 }])])
            ]),
            parse("SELECT foo")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Id { start: 7, end: 10 }])])
            ]),
            parse("SELECT x.y")
        );
    }

    #[test]
    fn number_expr() {
        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![Integer(2)])])]),
            parse("SELECT 2")
        );
    }

    #[test]
    fn operator_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Lt),
                    Integer(2),
                    Id { start: 11, end: 12 }
                ])])])
            ]),
            parse("SELECT 2 < x")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![KW(Gt), Integer(3), Float(2.1)])])])
            ]),
            parse("SELECT 3 > 2.1")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Ge),
                    Id { start: 7, end: 8 },
                    Id { start: 10, end: 11 }
                ])])])
            ]),
            parse("SELECT x>=y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Ne),
                    Id { start: 7, end: 8 },
                    Id { start: 10, end: 11 }
                ])])])
            ]),
            parse("SELECT x<>y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(And),
                    Id { start: 7, end: 8 },
                    Id { start: 13, end: 14 }
                ])])])
            ]),
            parse("SELECT x AND y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(And),
                    Id { start: 7, end: 8 },
                    Id { start: 13, end: 14 }
                ])])])
            ]),
            parse("SELECT x and y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Not),
                    List(vec![
                        KW(Is),
                        Id { start: 7, end: 8 },
                        Id { start: 16, end: 17 }
                    ])
                ])])])
            ]),
            parse("SELECT x IS NOT y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Is),
                    Id { start: 7, end: 8 },
                    Id { start: 12, end: 13 }
                ])])])
            ]),
            parse("SELECT x IS y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Between),
                    Id { start: 7, end: 8 },
                    Id { start: 17, end: 18 },
                    Integer(2)
                ])])])
            ]),
            parse("SELECT x BETWEEN y AND 2")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Not),
                    List(vec![
                        KW(Between),
                        Id { start: 7, end: 8 },
                        Id { start: 21, end: 22 },
                        Integer(2)
                    ])
                ])])])
            ]),
            parse("SELECT x NOT BETWEEN y AND 2")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Not),
                    List(vec![KW(Is), Id { start: 7, end: 8 }, KW(Null)])
                ])])])
            ]),
            parse("SELECT x NOT NULL")
        );
    }

    #[test]
    fn case_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Case),
                    List(vec![List(vec![Integer(2), Integer(1)])])
                ])])])
            ]),
            parse("SELECT CASE WHEN 2 THEN 1 END")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Case),
                    Integer(3),
                    List(vec![List(vec![Integer(2), Integer(1)])])
                ])])])
            ]),
            parse("SELECT CASE 3 WHEN 2 THEN 1 END")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Case),
                    List(vec![
                        List(vec![Integer(2), Integer(1)]),
                        List(vec![KW(Else), Integer(0)])
                    ])
                ])])])
            ]),
            parse("SELECT CASE WHEN 2 THEN 1 ELSE 0 END")
        );
    }

    #[test]
    fn string_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![String { start: 8, end: 11 }])])
            ]),
            parse("SELECT 'foo'")
        );
    }

    #[test]
    fn binary_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Binary { start: 9, end: 13 }])])
            ]),
            parse("SELECT X'AF01'")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Binary { start: 9, end: 13 }])])
            ]),
            parse("SELECT x'AF01'")
        );
    }

    #[test]
    fn function_expr() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Function),
                    Id { start: 7, end: 10 },
                    List(vec![Integer(2), Id { start: 14, end: 15 }])
                ])])])
            ]),
            parse("SELECT foo(2, y)")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(Count),
                    List(vec![Id { start: 13, end: 14 }])
                ])])])
            ]),
            parse("SELECT count(y)")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(Total),
                    List(vec![Id { start: 13, end: 14 }])
                ])])])
            ]),
            parse("SELECT TOTAL(y)")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(GroupConcat),
                    List(vec![
                        Id { start: 29, end: 30 },
                        String { start: 33, end: 34 }
                    ]),
                    KW(Distinct),
                    KW(Distinct)
                ])])])
            ]),
            parse("SELECT group_concat(DISTINCT y, ':')")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(CountStar),
                    List(vec![])
                ])])])
            ]),
            parse("SELECT count(*)")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Cast),
                    List(vec![KW(Minus), Integer(69)]),
                    Id { start: 22, end: 29 }
                ])])])
            ]),
            parse("SELECT CAST ( - 69 AS INTEGER )")
        );
    }

    #[test]
    fn error() {
        let src = "SELECT x 2";
        let result = sql_ast_parser_no_errors()
            .then_ignore(chumsky::prelude::end())
            .parse(src);
        assert_eq!(1, result.into_errors().len());
    }

    #[test]
    fn simple_select() {
        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![Integer(123)])])]),
            parse("SELECT 123")
        );

        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![KW(Mul)])])]),
            parse("SELECT *")
        );
    }
    #[test]
    fn select_as() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![
                    List(vec![Integer(1), Id { start: 12, end: 13 }]),
                    List(vec![Integer(2), Id { start: 17, end: 18 }])
                ]),
                KW(From),
                List(vec![
                    List(vec![Id { start: 24, end: 25 }]),
                    List(vec![Id { start: 27, end: 28 }, Id { start: 32, end: 35 }]),
                    List(vec![
                        List(vec![
                            KW(Select),
                            List(vec![List(vec![Id { start: 45, end: 48 }])])
                        ]),
                        Id { start: 50, end: 53 }
                    ])
                ]),
                KW(Where),
                KW(False)
            ]),
            parse("SELECT 1 AS x, 2 y FROM z, w AS foo, (SELECT bar) baz WHERE FALSE")
        );

        let src = "SELECT 1 AS from";
        assert_eq!(false, sql_ast_parser_no_errors().parse(src).has_errors());

        let src = "SELECT 1 from";
        assert_eq!(true, sql_ast_parser_no_errors().parse(src).has_errors());
    }

    #[test]
    fn select() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![
                    List(vec![Id { start: 7, end: 8 }]),
                    List(vec![Id { start: 10, end: 11 }]),
                    List(vec![Integer(123)]),
                    List(vec![List(vec![
                        KW(Function),
                        Id { start: 18, end: 24 },
                        List(vec![Id { start: 25, end: 26 }])
                    ])])
                ]),
                KW(From),
                List(vec![List(vec![Id { start: 33, end: 40 }])]),
                KW(Where),
                List(vec![
                    KW(And),
                    List(vec![
                        KW(Gt),
                        Id { start: 47, end: 48 },
                        Id { start: 51, end: 52 }
                    ]),
                    List(vec![KW(Lt), Id { start: 57, end: 58 }, Integer(100)])
                ]),
                KW(OrderBy),
                List(vec![
                    List(vec![Id { start: 74, end: 75 }, KW(Desc)]),
                    List(vec![Id { start: 82, end: 83 }, KW(Asc)])
                ])
            ]),
            parse("SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![
                    List(vec![Id { start: 14, end: 15 }]),
                    List(vec![Id { start: 27, end: 28 }])
                ])
            ]),
            parse("SELECT 1 FROM x CROSS JOIN y")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![
                    KW(Join),
                    List(vec![Id { start: 15, end: 16 }]),
                    List(vec![Id { start: 28, end: 29 }]),
                    KW(On),
                    KW(True),
                    KW(Type),
                    KW(Inner)
                ])])
            ]),
            parse("SELECT 1 FROM (x CROSS JOIN y)")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![
                    KW(Join),
                    List(vec![Id { start: 14, end: 15 }]),
                    List(vec![Id { start: 26, end: 27 }]),
                    KW(On),
                    KW(True),
                    KW(Type),
                    KW(Left)
                ])])
            ]),
            parse("SELECT 1 FROM x LEFT JOIN y ON TRUE")
        );

        assert_eq!(
            List(vec![
                KW(Union),
                List(vec![
                    KW(Intersect),
                    List(vec![KW(Select), List(vec![List(vec![Integer(1)])])]),
                    List(vec![KW(Select), List(vec![List(vec![Integer(2)])])])
                ]),
                List(vec![KW(Select), List(vec![List(vec![Integer(3)])])])
            ]),
            parse("SELECT 1 INTERSECT SELECT 2 UNION SELECT 3")
        );

        assert_eq!(
            List(vec![
                KW(Values),
                List(vec![
                    List(vec![Integer(1), Integer(2)]),
                    List(vec![Integer(3), Integer(4)])
                ])
            ]),
            parse("VALUES (1, 2), (3, 4)")
        );
    }

    #[test]
    fn group_by_having() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![Id { start: 14, end: 15 }])]),
                KW(GroupBy),
                List(vec![Id { start: 25, end: 26 }]),
                KW(Having),
                KW(True)
            ]),
            parse("SELECT 1 FROM x GROUP BY y HAVING TRUE")
        );
    }

    #[test]
    fn select_distinct() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(Distinct),
                KW(Distinct),
                KW(From),
                List(vec![List(vec![Id { start: 23, end: 24 }])])
            ]),
            parse("SELECT DISTINCT 1 FROM x")
        );
    }

    #[test]
    fn select_limit_offset() {
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![Id { start: 14, end: 15 }])]),
                KW(Limit),
                Integer(1),
                KW(Offset),
                Integer(2)
            ]),
            parse("SELECT 1 FROM x LIMIT 1 OFFSET 2")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![Id { start: 14, end: 15 }])]),
                KW(Limit),
                Integer(1),
                KW(Offset),
                Integer(2)
            ]),
            parse("SELECT 1 FROM x LIMIT 1, 2")
        );

        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![Id { start: 14, end: 15 }])]),
                KW(Limit),
                Integer(1),
            ]),
            parse("SELECT 1 FROM x LIMIT 1")
        );
    }

    #[test]
    fn dml() {
        assert_eq!(
            List(vec![
                KW(Insert),
                Id { start: 12, end: 15 },
                List(vec![
                    KW(Values),
                    List(vec![List(vec![Integer(1)]), List(vec![Integer(2)])])
                ]),
                KW(ColumnNames),
                List(vec![Id { start: 17, end: 18 }])
            ]),
            parse("INSERT INTO foo (x) VALUES (1), (2)")
        );

        assert_eq!(
            List(vec![KW(Delete), Id { start: 12, end: 15 }, KW(False)]),
            parse("DELETE FROM foo WHERE FALSE")
        );

        assert_eq!(
            List(vec![
                KW(Update),
                Id { start: 7, end: 10 },
                List(vec![
                    List(vec![Id { start: 15, end: 16 }, Integer(1)]),
                    List(vec![Id { start: 22, end: 23 }, Integer(2)])
                ]),
                KW(Where),
                KW(Null)
            ]),
            parse("UPDATE foo SET x = 1, y = 2 WHERE NULL")
        );
    }

    #[test]
    fn ddl() {
        assert_eq!(
            List(vec![
                KW(CreateIndex),
                Id { start: 20, end: 23 },
                Id { start: 27, end: 29 }
            ]),
            parse("CREATE UNIQUE INDEX foo ON t1(a1,b1)")
        );

        assert_eq!(
            List(vec![KW(DropIndex), Id { start: 11, end: 14 }]),
            parse("DROP INDEX foo")
        );

        assert_eq!(
            List(vec![
                KW(CreateView),
                Id { start: 17, end: 20 },
                List(vec![KW(Select), List(vec![List(vec![Integer(1)])])])
            ]),
            parse("CREATE TEMP VIEW foo AS SELECT 1")
        );

        assert_eq!(
            List(vec![
                KW(DropView),
                Id { start: 20, end: 23 },
                KW(IfExists),
                KW(IfExists)
            ]),
            parse("DROP VIEW IF EXISTS foo")
        );

        assert_eq!(
            List(vec![
                KW(CreateTable),
                Id { start: 13, end: 15 },
                List(vec![
                    Id { start: 16, end: 18 },
                    Id { start: 40, end: 42 },
                    Id { start: 52, end: 54 }
                ])
            ]),
            parse("CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INTEGER, x1 VARCHAR(40), FOREIGN KEY (y1) REFERENCES t2(z1), PRIMARY KEY(a1, b2))")
        );

        assert_eq!(
            List(vec![KW(DropTable), Id { start: 11, end: 14 }]),
            parse("DROP TABLE foo")
        );
    }
}
