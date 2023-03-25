use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::Rich;
use chumsky::extra::{Err, ParserExtra};
use chumsky::input::{StrInput, ValueInput};
use chumsky::prelude::*;
use chumsky::text::Char;

#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub enum Keyword {
    Select,
    From,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Lt,
    Gt,
    And,
    Function,
    Asc,
    Desc,
    Distinct,
    All,
    True,
    False,
    Null,
    Limit,
    Offset,
    Join,
    Type,
    Left,
    Inner,
    On,
    Except,
    Intersect,
    Union,
    UnionAll,
    Values,
    Insert,
    ColumnNames,
    Delete,
    Update,
    CreateIndex,
    DropIndex,
    CreateView,
    DropView,
    IfExists,
    CreateTable,
    DropTable,
}

#[derive(PartialEq, Debug)]
#[repr(C)]
pub enum Ast {
    List(Vec<Ast>),
    KW(Keyword),
    Integer(i64),
    Float(f64),
    Id { start: usize, end: usize },
    String { start: usize, end: usize },
    Binary { start: usize, end: usize },
}

fn id_ast_parser<'input>() -> impl Parser<'input, &'input str, Ast, Err<Rich<'input, char>>> + Clone
{
    use Ast::*;

    text::ident()
        .map_with_span(|_, span: SimpleSpan<usize>| Id {
            start: span.start(),
            end: span.end(),
        })
        .padded()
}

fn atom_ast_parser<'input>(
) -> impl Parser<'input, &'input str, Ast, Err<Rich<'input, char>>> + Clone {
    use Ast::*;
    use Keyword::*;

    let number = text::int(10)
        .then(just('.').then(text::digits(10)).or_not())
        .map_slice(|s: &str| match s.find('.') {
            Some(_) => Float(s.parse().unwrap()),
            None => Integer(s.parse().unwrap()),
        });

    let string = none_of('\'')
        .repeated()
        .map_with_span(|_, span: SimpleSpan<usize>| String {
            start: span.start(),
            end: span.end(),
        })
        .padded_by(just('\''));

    let binary = choice((just('X'), just('x'))).ignore_then(
        text::int(16)
            .map_with_span(|_, span: SimpleSpan<usize>| Binary {
                start: span.start(),
                end: span.end(),
            })
            .padded_by(just('\'')),
    );

    let boolean = choice((
        keyword_ignore_case("TRUE").to(True),
        keyword_ignore_case("FALSE").to(False),
        keyword_ignore_case("NULL").to(Null),
    ))
    .map(KW);

    choice((number, binary, string, boolean, id_ast_parser())).padded()
}

fn expr_ast_parser<'input>(
) -> impl Parser<'input, &'input str, Ast, Err<Rich<'input, char>>> + Clone {
    use Ast::*;
    use Keyword::*;

    let op = |c| just(c).padded();

    recursive(|expr| {
        let fun = id_ast_parser()
            .then(
                expr.separated_by(just(','))
                    .collect::<Vec<Ast>>()
                    .delimited_by(just('('), just(')')),
            )
            .map(|(f, exprs)| List(vec![KW(Function), f, List(exprs)]));

        let atom = choice((fun, atom_ast_parser()));

        let rel = atom.clone().foldl(
            choice((op("<").to(Lt), op(">").to(Gt)))
                .then(atom)
                .repeated(),
            |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]),
        );

        rel.clone().foldl(
            keyword_ignore_case("AND").to(And).then(rel).repeated(),
            |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]),
        )
    })
}

fn add_clause(acc: &mut Vec<Ast>, kw: Keyword, c: Option<Ast>) {
    c.map(|c| {
        acc.push(Ast::KW(kw));
        acc.push(c);
    });
}

pub fn sql_ast_parser<'input>(
) -> impl Parser<'input, &'input str, Ast, Err<Rich<'input, char>>> + Clone {
    use Ast::*;
    use Keyword::*;

    let expr = expr_ast_parser();
    let id = id_ast_parser();

    let positive_integer = just('0')
        .not()
        .ignore_then(text::int(10).slice().from_str().unwrapped().map(Integer));

    let order_by_list = expr
        .clone()
        .then(
            choice((
                keyword_ignore_case("ASC").to(Asc),
                keyword_ignore_case("DESC").to(Desc),
            ))
            .or_not()
            .map(|dir| KW(dir.unwrap_or(Asc))),
        )
        .map(|(var, dir)| List(vec![var, dir]))
        .separated_by(just(','))
        .at_least(1)
        .collect()
        .map(List);

    let select_stmt = recursive(|query| {
        let subquery = query.delimited_by(just('('), just(')')).padded();

        let table = choice((subquery, id.clone()))
            .then(
                choice((
                    keyword_ignore_case("AS").ignore_then(id.clone()),
                    id.clone().and_is(
                        choice((
                            keyword_ignore_case("CROSS"),
                            keyword_ignore_case("LEFT"),
                            keyword_ignore_case("JOIN"),
                            keyword_ignore_case("WHERE"),
                            keyword_ignore_case("GROUP"),
                            keyword_ignore_case("HAVING"),
                            keyword_ignore_case("ORDER"),
                            keyword_ignore_case("LIMIT"),
                            keyword_ignore_case("ON"),
                            keyword_ignore_case("UNION"),
                            keyword_ignore_case("EXCEPT"),
                            keyword_ignore_case("INTERSECT"),
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

        let table_list_element = choice((
            table.clone().foldl(
                choice((
                    keyword_ignore_case("LEFT")
                        .to(Left)
                        .then_ignore(keyword_ignore_case("OUTER").or_not()),
                    keyword_ignore_case("INNER").to(Inner),
                ))
                .or_not()
                .then_ignore(keyword_ignore_case("JOIN"))
                .then(table.clone())
                .then_ignore(keyword_ignore_case("ON"))
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
                .then_ignore(keyword_ignore_case("CROSS").ignore_then(keyword_ignore_case("JOIN")))
                .then(table)
                .delimited_by(just('('), just(')'))
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

        let select_clause = keyword_ignore_case("SELECT")
            .ignore_then(
                choice((
                    keyword_ignore_case("DISTINCT").to(Distinct),
                    keyword_ignore_case("ALL").to(All),
                ))
                .map(KW)
                .or_not(),
            )
            .then(
                expr.clone()
                    .then(
                        choice((
                            keyword_ignore_case("AS").ignore_then(id.clone()),
                            id.clone().and_is(
                                choice((
                                    keyword_ignore_case("FROM"),
                                    keyword_ignore_case("WHERE"),
                                    keyword_ignore_case("GROUP"),
                                    keyword_ignore_case("HAVING"),
                                    keyword_ignore_case("ORDER"),
                                    keyword_ignore_case("LIMIT"),
                                    keyword_ignore_case("UNION"),
                                    keyword_ignore_case("EXCEPT"),
                                    keyword_ignore_case("INTERSECT"),
                                ))
                                .not(),
                            ),
                        ))
                        .or_not(),
                    )
                    .map(|(expr, id)| match id {
                        Some(id) => List(vec![expr, id]),
                        None => List(vec![expr]),
                    })
                    .separated_by(just(','))
                    .at_least(1)
                    .collect()
                    .map(List),
            );

        let from_clause = keyword_ignore_case("FROM")
            .ignore_then(
                table_list_element
                    .separated_by(choice((
                        just(","),
                        keyword_ignore_case("CROSS").ignore_then(keyword_ignore_case("JOIN")),
                    )))
                    .at_least(1)
                    .collect()
                    .map(List),
            )
            .or_not();

        let where_clause = keyword_ignore_case("WHERE")
            .ignore_then(expr.clone())
            .or_not();

        let group_by_clause = keyword_ignore_case("GROUP")
            .ignore_then(keyword_ignore_case("BY"))
            .ignore_then(
                id_ast_parser()
                    .separated_by(just(','))
                    .at_least(1)
                    .collect()
                    .map(List),
            )
            .or_not();

        let having_clause = keyword_ignore_case("HAVING")
            .ignore_then(expr.clone())
            .or_not();

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

        let values_stmt = keyword_ignore_case("VALUES").ignore_then(
            expr.clone()
                .separated_by(just(','))
                .at_least(1)
                .collect()
                .map(List)
                .delimited_by(just('('), just(')'))
                .padded()
                .separated_by(just(','))
                .at_least(1)
                .collect()
                .map(|values| List(vec![KW(Values), List(values)])),
        );

        let select_core = choice((select_stmt, values_stmt));

        let compound_select_stmt = select_core.clone().foldl(
            choice((
                keyword_ignore_case("EXCEPT").to(Except),
                keyword_ignore_case("INTERSECT").to(Intersect),
                keyword_ignore_case("UNION")
                    .then_ignore(keyword_ignore_case("ALL"))
                    .to(UnionAll),
                keyword_ignore_case("UNION").to(Union),
            ))
            .then(select_core)
            .repeated(),
            |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]),
        );

        let order_by = keyword_ignore_case("ORDER")
            .ignore_then(keyword_ignore_case("BY"))
            .ignore_then(order_by_list.clone())
            .or_not();

        let limit_clause = keyword_ignore_case("LIMIT")
            .ignore_then(positive_integer)
            .then(
                choice((keyword_ignore_case("OFFSET"), just(",").padded()))
                    .ignore_then(positive_integer)
                    .or_not(),
            )
            .or_not();

        compound_select_stmt.then(order_by).then(limit_clause).map(
            |((query, order_by), limit_offset)| {
                let mut acc = match query {
                    List(x) => x,
                    _ => vec![],
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

    let id_list = id
        .clone()
        .separated_by(just(','))
        .at_least(1)
        .collect()
        .map(List)
        .delimited_by(just('('), just(')'));

    let insert_stmt = keyword_ignore_case("INSERT")
        .ignore_then(
            keyword_ignore_case("OR")
                .then_ignore(keyword_ignore_case("REPLACE"))
                .or_not(),
        )
        .ignore_then(keyword_ignore_case("INTO"))
        .ignore_then(id.clone())
        .then(id_list.clone().or_not())
        .then(select_stmt.clone())
        .map(|((id, id_list), query)| {
            let mut acc = vec![KW(Insert), id, query];
            add_clause(&mut acc, ColumnNames, id_list);
            List(acc)
        });

    let delete_stmt = keyword_ignore_case("DELETE")
        .ignore_then(keyword_ignore_case("FROM"))
        .ignore_then(id.clone())
        .then_ignore(keyword_ignore_case("WHERE"))
        .then(expr.clone())
        .map(|(id, expr)| List(vec![KW(Delete), id, expr]));

    let update_stmt = keyword_ignore_case("UPDATE")
        .ignore_then(id.clone())
        .then_ignore(keyword_ignore_case("SET"))
        .then(
            id.clone()
                .then_ignore(just('='))
                .then(expr.clone())
                .map(|(id, expr)| List(vec![id, expr]))
                .separated_by(just(','))
                .collect()
                .map(List),
        )
        .then(
            keyword_ignore_case("WHERE")
                .ignore_then(expr.clone())
                .or_not(),
        )
        .map(|((id, updates), expr)| {
            let mut acc = vec![KW(Update), id, updates];
            add_clause(&mut acc, Where, expr);
            List(acc)
        });

    let create_index_stmt = keyword_ignore_case("CREATE")
        .ignore_then(keyword_ignore_case("UNIQUE").or_not())
        .ignore_then(keyword_ignore_case("INDEX"))
        .ignore_then(id.clone())
        .then_ignore(keyword_ignore_case("ON"))
        .then(id.clone())
        .then_ignore(order_by_list.clone().delimited_by(just('('), just(')')))
        .map(|(index, table)| List(vec![KW(CreateIndex), index, table]));

    let create_view_stmt = keyword_ignore_case("CREATE")
        .ignore_then(keyword_ignore_case("VIEW"))
        .ignore_then(
            choice((
                keyword_ignore_case("TEMPORARY"),
                keyword_ignore_case("TEMP"),
            ))
            .or_not(),
        )
        .ignore_then(id.clone())
        .then_ignore(keyword_ignore_case("AS"))
        .then(select_stmt.clone())
        .map(|(id, query)| List(vec![KW(CreateView), id, query]));

    let col_def = choice((
        keyword_ignore_case("PRIMARY")
            .then(keyword_ignore_case("KEY"))
            .then(id_list.clone())
            .map(|_| None),
        keyword_ignore_case("FOREIGN")
            .then(keyword_ignore_case("KEY"))
            .then(id_list.clone())
            .then(keyword_ignore_case("REFERENCES"))
            .then(id.clone())
            .then(id_list)
            .map(|_| None),
        id.clone()
            .then_ignore(
                id.clone()
                    .then_ignore(positive_integer.delimited_by(just('('), just(')')).or_not())
                    .then_ignore(
                        keyword_ignore_case("PRIMARY")
                            .then_ignore(keyword_ignore_case("KEY"))
                            .or_not(),
                    )
                    .then_ignore(keyword_ignore_case("UNIQUE").or_not()),
            )
            .map(Some),
    ));

    let create_table_stmt = keyword_ignore_case("CREATE")
        .ignore_then(keyword_ignore_case("TABLE"))
        .ignore_then(id.clone())
        .then(
            col_def
                .separated_by(just(','))
                .at_least(1)
                .collect::<Vec<Option<Ast>>>()
                .map(|col_defs| List(col_defs.into_iter().flatten().collect()))
                .delimited_by(just('('), just(')')),
        )
        .map(|(id, columns)| List(vec![KW(CreateTable), id, columns]));

    let ddl_drop_stmt = keyword_ignore_case("DROP")
        .ignore_then(
            choice((
                keyword_ignore_case("INDEX").to(DropIndex),
                keyword_ignore_case("VIEW").to(DropView),
                keyword_ignore_case("TABLE").to(DropTable),
            ))
            .map(KW),
        )
        .then(
            keyword_ignore_case("IF")
                .ignore_then(keyword_ignore_case("EXISTS"))
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
    .then_ignore(end())
}

pub fn parse_errors_to_string<'input>(src: &str, errs: Vec<Rich<'input, char>>) -> String {
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

fn keyword_ignore_case<
    'a,
    I: ValueInput<'a> + StrInput<'a, C>,
    C: Char<Str = str> + 'a,
    E: ParserExtra<'a, I> + 'a,
>(
    keyword: &'a str,
) -> impl Parser<'a, I, &'a C::Str, E> + Clone + 'a
where
    C::Str: PartialEq,
{
    text::ident()
        .try_map(|s: &C::Str, span| {
            if keyword.eq_ignore_ascii_case(s) {
                Ok(())
            } else {
                Err(E::Error::expected_found(None, None, span))
            }
        })
        .slice()
        .padded()
}

#[cfg(test)]
mod tests {
    use crate::{
        parser::expr_ast_parser, parser::sql_ast_parser, parser::Ast::*, parser::Keyword::*,
    };
    use chumsky::Parser;

    #[test]
    fn identifier() {
        let src = "foo";
        let ast = expr_ast_parser().parse(src);
        assert_eq!(Id { start: 0, end: 3 }, ast.into_output().unwrap());
    }

    #[test]
    fn number() {
        let src = "2";
        let ast = expr_ast_parser().parse(src);
        assert_eq!(Integer(2), ast.into_output().unwrap());
    }

    #[test]
    fn lt_expr() {
        let src = "2 < x";
        let ast = expr_ast_parser().parse(src);
        assert_eq!(
            List(vec![KW(Lt), Integer(2), Id { start: 4, end: 5 }]),
            ast.into_output().unwrap()
        );
    }

    #[test]
    fn gt_expr() {
        let src = "3 > 2.1";
        let ast = expr_ast_parser().parse(src);
        assert_eq!(
            List(vec![KW(Gt), Integer(3), Float(2.1)]),
            ast.into_output().unwrap()
        );
    }

    #[test]
    fn and_expr() {
        let src = "x AND y";
        let ast = expr_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(And),
                Id { start: 0, end: 1 },
                Id { start: 6, end: 7 },
            ]),
            ast
        );
        let src = "x and y";
        let ast = expr_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(And),
                Id { start: 0, end: 1 },
                Id { start: 6, end: 7 },
            ]),
            ast
        );
    }

    #[test]
    fn string_expr() {
        let src = "'foo'";
        let ast = expr_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(String { start: 1, end: 4 }, ast);
    }

    #[test]
    fn binary_expr() {
        let src = "X'AF01'";
        let ast = expr_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(Binary { start: 2, end: 6 }, ast);
        let src = "x'AF01'";
        let ast = expr_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(Binary { start: 2, end: 6 }, ast);
    }

    #[test]
    fn fun_expr() {
        let src = "foo(2, y)";
        let ast = expr_ast_parser().parse(src);
        assert_eq!(
            List(vec!(
                KW(Function),
                Id { start: 0, end: 3 },
                List(vec![Integer(2), Id { start: 7, end: 8 }])
            )),
            ast.into_output().unwrap()
        );
    }

    #[test]
    fn error() {
        let src = "2x";
        let result = expr_ast_parser()
            .then_ignore(chumsky::prelude::end())
            .parse(src);
        assert_eq!(1, result.into_errors().len());
    }

    #[test]
    fn simple_select() {
        let src = "SELECT 123";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![Integer(123)])])]),
            ast
        );
    }

    #[test]
    fn select_as() {
        let src = "SELECT 1 AS x, 2 y FROM z, w AS foo, (SELECT bar) baz WHERE FALSE";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );
        let src = "SELECT 1 AS from";
        assert_eq!(false, sql_ast_parser().parse(src).has_errors());
        let src = "SELECT 1 from";
        assert_eq!(true, sql_ast_parser().parse(src).has_errors());
    }

    #[test]
    fn select() {
        let src =
            "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );
        let src = "SELECT 1 FROM x CROSS JOIN y";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT 1 FROM (x CROSS JOIN y)";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT 1 FROM x LEFT JOIN y ON TRUE";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT 1 INTERSECT SELECT 2 UNION SELECT 3";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "VALUES (1, 2), (3, 4)";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Values),
                List(vec![
                    List(vec![Integer(1), Integer(2)]),
                    List(vec![Integer(3), Integer(4)])
                ])
            ]),
            ast
        );
    }

    #[test]
    fn group_by_having() {
        let src = "SELECT 1 FROM x GROUP BY y HAVING TRUE";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );
    }

    #[test]
    fn select_distinct() {
        let src = "SELECT DISTINCT 1 FROM x";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(Distinct),
                KW(Distinct),
                KW(From),
                List(vec![List(vec![Id { start: 23, end: 24 }])])
            ]),
            ast
        );
    }

    #[test]
    fn select_limit_offset() {
        let src = "SELECT 1 FROM x LIMIT 1 OFFSET 2";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT 1 FROM x LIMIT 1, 2";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT 1 FROM x LIMIT 1";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Integer(1)])]),
                KW(From),
                List(vec![List(vec![Id { start: 14, end: 15 }])]),
                KW(Limit),
                Integer(1),
            ]),
            ast
        );
        let src = "SELECT 1 FROM x LIMIT 0";
        let errs = sql_ast_parser().parse(src).into_errors();
        assert_eq!(1, errs.len());
    }

    #[test]
    fn dml() {
        let src = "INSERT INTO foo (x) VALUES (1), (2)";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "DELETE FROM foo WHERE FALSE";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![KW(Delete), Id { start: 12, end: 15 }, KW(False)]),
            ast
        );

        let src = "UPDATE foo SET x = 1, y = 2 WHERE NULL";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );
    }

    #[test]
    fn ddl() {
        let src = "CREATE UNIQUE INDEX foo ON t1(a1,b1)";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(CreateIndex),
                Id { start: 20, end: 23 },
                Id { start: 27, end: 29 }
            ]),
            ast
        );

        let src = "DROP INDEX foo";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(List(vec![KW(DropIndex), Id { start: 11, end: 14 }]), ast);

        let src = "CREATE VIEW foo AS SELECT 1";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(CreateView),
                Id { start: 12, end: 15 },
                List(vec![KW(Select), List(vec![List(vec![Integer(1)])])])
            ]),
            ast
        );

        let src = "DROP VIEW IF EXISTS foo";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(DropView),
                Id { start: 20, end: 23 },
                KW(IfExists),
                KW(IfExists)
            ]),
            ast
        );

        let src = "CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INTEGER, x1 VARCHAR(40), FOREIGN KEY (y1) REFERENCES t2(z1), PRIMARY KEY(a1, b2))";

        let ast = sql_ast_parser().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "DROP TABLE foo";
        let ast = sql_ast_parser().parse(src).into_output().unwrap();
        assert_eq!(List(vec![KW(DropTable), Id { start: 11, end: 14 }]), ast);
    }
}
