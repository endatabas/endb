use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::Rich;
use chumsky::extra::{Default, Err, ParserExtra};
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
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
    Is,
    In,
    Between,
    Like,
    Case,
    Exists,
    ScalarSubquery,
    Else,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    Lsh,
    Rsh,
    And,
    Or,
    Not,
    Function,
    AggregateFunction,
    Count,
    CountStar,
    Avg,
    Sum,
    Min,
    Max,
    Total,
    GroupConcat,
    Cast,
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
    Id { start: i32, end: i32 },
    String { start: i32, end: i32 },
    Binary { start: i32, end: i32 },
}

fn id_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use Ast::*;

    text::ident().map_with_span(|_, span: SimpleSpan<_>| Id {
        start: span.start() as i32,
        end: span.end() as i32,
    })
}

fn atom_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
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
        .map_with_span(|_, span: SimpleSpan<_>| String {
            start: span.start() as i32,
            end: span.end() as i32,
        })
        .padded_by(just('\''));

    let binary = one_of("Xx").ignore_then(
        text::int(16)
            .map_with_span(|_, span: SimpleSpan<_>| Binary {
                start: span.start() as i32,
                end: span.end() as i32,
            })
            .padded_by(just('\'')),
    );

    let boolean = choice((
        keyword_ignore_case_no_pad("TRUE").to(True),
        keyword_ignore_case_no_pad("FALSE").to(False),
        keyword_ignore_case_no_pad("NULL").to(Null),
    ))
    .map(KW);

    choice((number, binary, string, boolean, id_ast_parser())).padded()
}

fn expr_ast_parser<'input, E>(
    query: impl Parser<'input, &'input str, Ast, E> + Clone + 'input,
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use Ast::*;
    use Keyword::*;

    let op = |c| just(c).padded();
    let unary_op = |op, rhs| List(vec![KW(op), rhs]);
    let bin_op = |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]);

    let subquery = query.delimited_by(just('('), just(')')).padded();

    recursive(|expr| {
        let id = id_ast_parser().padded();

        let all_distinct = choice((
            keyword_ignore_case("DISTINCT").to(Distinct),
            keyword_ignore_case("ALL").to(All),
        ))
        .map(KW)
        .or_not();

        let count_star = keyword_ignore_case("COUNT")
            .ignore_then(
                all_distinct
                    .clone()
                    .then_ignore(just('*'))
                    .delimited_by(just('('), just(')')),
            )
            .map(|distinct| {
                let mut acc = vec![KW(AggregateFunction), KW(CountStar)];
                add_clause(&mut acc, Distinct, distinct);
                List(acc)
            });

        let aggregate_function = choice((
            keyword_ignore_case("COUNT").to(Count),
            keyword_ignore_case("AVG").to(Avg),
            keyword_ignore_case("SUM").to(Sum),
            keyword_ignore_case("MIN").to(Min),
            keyword_ignore_case("MAX").to(Max),
            keyword_ignore_case("TOTAL").to(Total),
            keyword_ignore_case("GROUP_CONCAT").to(GroupConcat),
        ))
        .map(KW)
        .then(
            all_distinct
                .then(
                    expr.clone()
                        .separated_by(just(','))
                        .at_least(1)
                        .collect()
                        .map(List),
                )
                .delimited_by(just('('), just(')')),
        )
        .map(|(f, (distinct, expr_list))| {
            let mut acc = vec![KW(AggregateFunction), f, expr_list];
            add_clause(&mut acc, Distinct, distinct);
            List(acc)
        });

        let cast = keyword_ignore_case("CAST")
            .ignore_then(
                expr.clone()
                    .then_ignore(keyword_ignore_case("AS"))
                    .then(id.clone())
                    .delimited_by(just('('), just(')')),
            )
            .map(|(expr, id)| List(vec![KW(Cast), expr, id]));

        let exists = keyword_ignore_case("EXISTS")
            .ignore_then(subquery.clone())
            .map(|query| List(vec![KW(Exists), query]));

        let case = keyword_ignore_case("CASE")
            .ignore_then(
                expr.clone()
                    .or_not()
                    .then(
                        keyword_ignore_case("WHEN")
                            .ignore_then(expr.clone())
                            .then_ignore(keyword_ignore_case("THEN"))
                            .then(expr.clone())
                            .map(|(lhs, rhs)| List(vec![lhs, rhs]))
                            .repeated()
                            .at_least(1)
                            .collect()
                            .map(List),
                    )
                    .then(
                        keyword_ignore_case("ELSE")
                            .ignore_then(expr.clone())
                            .map(|else_clause| List(vec![KW(Else), else_clause]))
                            .or_not(),
                    )
                    .then_ignore(keyword_ignore_case("END")),
            )
            .map(|((base_expr, whens), else_clause)| {
                let mut acc = vec![KW(Case)];
                if let Some(base_expr) = base_expr {
                    acc.push(base_expr);
                }
                let whens = match (whens, else_clause) {
                    (List(mut whens), Some(else_clause)) => {
                        whens.push(else_clause);
                        List(whens)
                    }
                    (whens, _) => whens,
                };
                acc.push(whens);
                List(acc)
            });

        let function = id
            .clone()
            .then(
                expr.clone()
                    .separated_by(just(','))
                    .collect()
                    .map(List)
                    .delimited_by(just('('), just(')')),
            )
            .map(|(f, exprs)| List(vec![KW(Function), f, exprs]));

        let scalar_subquery = subquery
            .clone()
            .map(|query| List(vec![KW(ScalarSubquery), query]));

        let atom = choice((
            count_star,
            aggregate_function,
            cast,
            case,
            exists,
            function,
            scalar_subquery,
            atom_ast_parser(),
            expr.clone().delimited_by(just('('), just(')')),
        ))
        .boxed();

        let unary = choice((op("+").to(Plus), op("-").to(Minus)))
            .repeated()
            .foldr(atom, unary_op);

        let mul = unary.clone().foldl(
            choice((op("*").to(Mul), op("/").to(Div), op("%").to(Mod)))
                .then(unary)
                .repeated(),
            bin_op,
        );

        let add = mul.clone().foldl(
            choice((op("+").to(Plus), op("-").to(Minus)))
                .then(mul)
                .repeated(),
            bin_op,
        );

        let shift = add.clone().foldl(
            choice((op("<<").to(Lsh), op(">>").to(Rsh)))
                .then(add)
                .repeated(),
            bin_op,
        );

        let comp = shift.clone().foldl(
            choice((
                op("<").to(Lt),
                op("<=").to(Le),
                op(">").to(Gt),
                op(">=").to(Ge),
            ))
            .then(shift)
            .repeated(),
            bin_op,
        );

        let equal = comp
            .clone()
            .foldl(
                choice((
                    op("=").to((None, Some(Eq))).then(comp.clone()),
                    op("<>").to((None, Some(Ne))).then(comp.clone()),
                    keyword_ignore_case("IS")
                        .to(Some(Is))
                        .then(keyword_ignore_case("NOT").to(Not).or_not())
                        .then(comp.clone()),
                    keyword_ignore_case("NOT")
                        .to(Not)
                        .or_not()
                        .then(keyword_ignore_case("BETWEEN").to(Some(Between)))
                        .then(
                            comp.clone()
                                .then(keyword_ignore_case("AND").ignore_then(comp.clone()))
                                .map(|(lhs, rhs)| List(vec![KW(And), lhs, rhs])),
                        ),
                    keyword_ignore_case("NOT")
                        .to(Not)
                        .or_not()
                        .then(keyword_ignore_case("LIKE").to(Some(Like)))
                        .then(comp.clone()),
                    keyword_ignore_case("NOT")
                        .to(Not)
                        .or_not()
                        .then(keyword_ignore_case("IN").to(Some(In)))
                        .then(choice((
                            subquery.clone(),
                            id.clone(),
                            expr.separated_by(just(','))
                                .collect()
                                .map(List)
                                .delimited_by(just('('), just(')')),
                        ))),
                    keyword_ignore_case("NOT")
                        .to((Some(Is), Some(Not)))
                        .then(keyword_ignore_case("NULL").to(Null).map(KW)),
                ))
                .repeated(),
                |lhs, (op, rhs)| match op {
                    (not, Some(Between)) => {
                        let mut acc = vec![KW(Between), lhs];
                        if let List(mut rhs) = rhs {
                            let rhs_rhs = rhs.pop().unwrap();
                            let rhs_lhs = rhs.pop().unwrap();
                            acc.push(rhs_lhs);
                            acc.push(rhs_rhs);
                        };
                        let acc = List(acc);
                        match not {
                            Some(Not) => List(vec![KW(Not), acc]),
                            _ => acc,
                        }
                    }
                    (None, Some(op)) => List(vec![KW(op), lhs, rhs]),
                    (Some(Not), Some(op)) => List(vec![KW(Not), List(vec![KW(op), lhs, rhs])]),
                    (Some(op), Some(Not)) => List(vec![KW(Not), List(vec![KW(op), lhs, rhs])]),
                    _ => KW(False),
                },
            )
            .boxed();

        let not = op("NOT").to(Not).repeated().foldr(equal, unary_op);

        let and = not.clone().foldl(
            keyword_ignore_case("AND").to(And).then(not).repeated(),
            bin_op,
        );

        and.clone().foldl(
            keyword_ignore_case("OR").to(Or).then(and).repeated(),
            bin_op,
        )
    })
}

fn add_clause(acc: &mut Vec<Ast>, kw: Keyword, c: Option<Ast>) {
    if let Some(c) = c {
        acc.push(Ast::KW(kw));
        acc.push(c);
    }
}

fn sql_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use Ast::*;
    use Keyword::*;

    let id = id_ast_parser().padded();

    let positive_integer = just('0')
        .not()
        .ignore_then(text::int(10).slice().from_str().unwrapped().map(Integer))
        .padded();

    let order_by_list = choice((id.clone(), positive_integer))
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
        let expr = expr_ast_parser(query.clone());
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
                choice((keyword_ignore_case("OFFSET"), just(",")))
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

    let expr = expr_ast_parser(select_stmt.clone());

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
    keyword_ignore_case_no_pad(keyword).padded()
}

fn keyword_ignore_case_no_pad<
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
}

#[cfg(test)]
mod tests {
    use crate::{parser::sql_ast_parser_no_errors, parser::Ast::*, parser::Keyword::*};
    use chumsky::Parser;

    #[test]
    fn identifier_expr() {
        let src = "SELECT foo";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Id { start: 7, end: 10 }])])
            ]),
            ast.into_output().unwrap()
        );
    }

    #[test]
    fn number_expr() {
        let src = "SELECT 2";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![Integer(2)])])]),
            ast.into_output().unwrap()
        );
    }

    #[test]
    fn operator_expr() {
        let src = "SELECT 2 < x";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Lt),
                    Integer(2),
                    Id { start: 11, end: 12 }
                ])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT 3 > 2.1";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![KW(Gt), Integer(3), Float(2.1)])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT x AND y";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(And),
                    Id { start: 7, end: 8 },
                    Id { start: 13, end: 14 }
                ])])])
            ]),
            ast
        );

        let src = "SELECT x and y";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(And),
                    Id { start: 7, end: 8 },
                    Id { start: 13, end: 14 }
                ])])])
            ]),
            ast
        );

        let src = "SELECT x IS NOT y";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT x BETWEEN y AND 2";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
            ast
        );

        let src = "SELECT x NOT NULL";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Not),
                    List(vec![KW(Is), Id { start: 7, end: 8 }, KW(Null)])
                ])])])
            ]),
            ast
        );
    }

    #[test]
    fn string_expr() {
        let src = "SELECT 'foo'";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![String { start: 8, end: 11 }])])
            ]),
            ast
        );
    }

    #[test]
    fn binary_expr() {
        let src = "SELECT X'AF01'";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Binary { start: 9, end: 13 }])])
            ]),
            ast
        );

        let src = "SELECT x'AF01'";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![Binary { start: 9, end: 13 }])])
            ]),
            ast
        );
    }

    #[test]
    fn function_expr() {
        let src = "SELECT foo(2, y)";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Function),
                    Id { start: 7, end: 10 },
                    List(vec![Integer(2), Id { start: 14, end: 15 }])
                ])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT count(y)";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(Count),
                    List(vec![Id { start: 13, end: 14 }])
                ])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT TOTAL(y)";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(Total),
                    List(vec![Id { start: 13, end: 14 }])
                ])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT group_concat(DISTINCT y, ':')";
        let ast = sql_ast_parser_no_errors().parse(src);
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
            ast.into_output().unwrap()
        );

        let src = "SELECT count(*)";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(AggregateFunction),
                    KW(CountStar)
                ])])])
            ]),
            ast.into_output().unwrap()
        );

        let src = "SELECT CAST ( - 69 AS INTEGER )";
        let ast = sql_ast_parser_no_errors().parse(src);
        assert_eq!(
            List(vec![
                KW(Select),
                List(vec![List(vec![List(vec![
                    KW(Cast),
                    List(vec![KW(Minus), Integer(69)]),
                    Id { start: 22, end: 29 }
                ])])])
            ]),
            ast.into_output().unwrap()
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
        let src = "SELECT 123";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![KW(Select), List(vec![List(vec![Integer(123)])])]),
            ast
        );
    }
    #[test]
    fn select_as() {
        let src = "SELECT 1 AS x, 2 y FROM z, w AS foo, (SELECT bar) baz WHERE FALSE";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        assert_eq!(false, sql_ast_parser_no_errors().parse(src).has_errors());
        let src = "SELECT 1 from";
        assert_eq!(true, sql_ast_parser_no_errors().parse(src).has_errors());
    }

    #[test]
    fn select() {
        let src =
            "SELECT a, b, 123, myfunc(b) FROM table_1 WHERE a > b AND b < 100 ORDER BY a DESC, b";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let errs = sql_ast_parser_no_errors().parse(src).into_errors();
        assert_eq!(1, errs.len());
    }

    #[test]
    fn dml() {
        let src = "INSERT INTO foo (x) VALUES (1), (2)";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![KW(Delete), Id { start: 12, end: 15 }, KW(False)]),
            ast
        );

        let src = "UPDATE foo SET x = 1, y = 2 WHERE NULL";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(CreateIndex),
                Id { start: 20, end: 23 },
                Id { start: 27, end: 29 }
            ]),
            ast
        );

        let src = "DROP INDEX foo";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(List(vec![KW(DropIndex), Id { start: 11, end: 14 }]), ast);

        let src = "CREATE VIEW foo AS SELECT 1";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(
            List(vec![
                KW(CreateView),
                Id { start: 12, end: 15 },
                List(vec![KW(Select), List(vec![List(vec![Integer(1)])])])
            ]),
            ast
        );

        let src = "DROP VIEW IF EXISTS foo";
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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

        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
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
        let ast = sql_ast_parser_no_errors().parse(src).into_output().unwrap();
        assert_eq!(List(vec![KW(DropTable), Id { start: 11, end: 14 }]), ast);
    }
}
