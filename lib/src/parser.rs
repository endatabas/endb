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
        kw_no_pad("TRUE").to(True),
        kw_no_pad("FALSE").to(False),
        kw_no_pad("NULL").to(Null),
    ))
    .map(KW);

    choice((number, binary, string, boolean, id_ast_parser())).then_ignore(text::whitespace())
}

fn expr_ast_parser<'input, E>(
    query: impl Parser<'input, &'input str, Ast, E> + Clone + 'input,
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use Ast::*;
    use Keyword::*;

    recursive(|expr| {
        let pad = |c| just(c).then_ignore(text::whitespace()).ignored();
        let pad_str = |c| just(c).then_ignore(text::whitespace()).ignored();
        let unary_op = |op, rhs| List(vec![KW(op), rhs]);
        let bin_op = |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]);

        let subquery = query.delimited_by(pad('('), pad(')'));

        let id = id_ast_parser().then_ignore(text::whitespace());

        let expr_list = expr
            .clone()
            .separated_by(pad(','))
            .at_least(1)
            .collect()
            .map(List);

        let all_distinct = choice((kw("DISTINCT").to(Distinct), kw("ALL").to(All)))
            .map(KW)
            .or_not();

        let count_star = kw("COUNT")
            .ignore_then(
                all_distinct
                    .clone()
                    .then_ignore(pad('*'))
                    .delimited_by(pad('('), pad(')')),
            )
            .map(|distinct| {
                let mut acc = vec![KW(AggregateFunction), KW(CountStar)];
                add_clause(&mut acc, Distinct, distinct);
                List(acc)
            });

        let aggregate_function = choice((
            kw("COUNT").to(Count),
            kw("AVG").to(Avg),
            kw("SUM").to(Sum),
            kw("MIN").to(Min),
            kw("MAX").to(Max),
            kw("TOTAL").to(Total),
            kw("GROUP_CONCAT").to(GroupConcat),
        ))
        .map(KW)
        .then(
            all_distinct
                .then(expr_list.clone())
                .delimited_by(pad('('), pad(')')),
        )
        .map(|(f, (distinct, expr_list))| {
            let mut acc = vec![KW(AggregateFunction), f, expr_list];
            add_clause(&mut acc, Distinct, distinct);
            List(acc)
        });

        let cast = kw("CAST")
            .ignore_then(
                expr.clone()
                    .then_ignore(kw("AS"))
                    .then(id.clone())
                    .delimited_by(pad('('), pad(')')),
            )
            .map(|(expr, id)| List(vec![KW(Cast), expr, id]));

        let exists = kw("EXISTS")
            .ignore_then(subquery.clone())
            .map(|query| List(vec![KW(Exists), query]));

        let case = kw("CASE")
            .ignore_then(
                expr.clone()
                    .or_not()
                    .then(
                        kw("WHEN")
                            .ignore_then(expr.clone())
                            .then_ignore(kw("THEN"))
                            .then(expr.clone())
                            .map(|(lhs, rhs)| List(vec![lhs, rhs]))
                            .repeated()
                            .at_least(1)
                            .collect()
                            .map(List),
                    )
                    .then(
                        kw("ELSE")
                            .ignore_then(expr.clone())
                            .map(|else_clause| List(vec![KW(Else), else_clause]))
                            .or_not(),
                    )
                    .then_ignore(kw("END")),
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
            .then(expr_list.clone().delimited_by(pad('('), pad(')')))
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
            expr.clone().delimited_by(pad('('), pad(')')),
        ))
        .boxed();

        let unary = choice((pad('+').to(Plus), pad('-').to(Minus)))
            .repeated()
            .foldr(atom, unary_op);

        let mul = unary.clone().foldl(
            choice((pad('*').to(Mul), pad('/').to(Div), pad('%').to(Mod)))
                .then(unary)
                .repeated(),
            bin_op,
        );

        let add = mul.clone().foldl(
            choice((pad('+').to(Plus), pad('-').to(Minus)))
                .then(mul)
                .repeated(),
            bin_op,
        );

        let shift = add.clone().foldl(
            choice((pad_str("<<").to(Lsh), pad_str(">>").to(Rsh)))
                .then(add)
                .repeated(),
            bin_op,
        );

        let comp = shift.clone().foldl(
            choice((
                pad('<').to(Lt),
                pad_str("<=").to(Le),
                pad('>').to(Gt),
                pad_str(">=").to(Ge),
            ))
            .then(shift)
            .repeated(),
            bin_op,
        );

        let equal = comp
            .clone()
            .foldl(
                choice((
                    choice((pad('=').to(Some(Eq)), pad_str("<>").to(Some(Ne))))
                        .then(comp.clone())
                        .map(|x| (None, x)),
                    kw("IS")
                        .to(Some(Is))
                        .then(kw("NOT").to(Not).or_not().then(comp.clone())),
                    kw("NOT").ignore_then(kw("NULL").map(|_| (Some(Not), (Some(Is), KW(Null))))),
                    kw("NOT").to(Not).or_not().then(choice((
                        kw("BETWEEN").to(Some(Between)).then(
                            comp.clone()
                                .then(kw("AND").ignore_then(comp.clone()))
                                .map(|(lhs, rhs)| List(vec![KW(And), lhs, rhs])),
                        ),
                        kw("LIKE").to(Some(Like)).then(comp.clone()),
                        kw("IN").to(Some(In)).then(choice((
                            subquery.clone(),
                            id.clone(),
                            expr_list.delimited_by(pad('('), pad(')')),
                        ))),
                    ))),
                ))
                .repeated(),
                |lhs, (op_1, (op_2, rhs))| match (op_1, op_2) {
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
                    _ => unreachable!(),
                },
            )
            .boxed();

        let not = kw("NOT").to(Not).repeated().foldr(equal, unary_op);

        let and = not
            .clone()
            .foldl(kw("AND").to(And).then(not).repeated(), bin_op);

        and.clone()
            .foldl(kw("OR").to(Or).then(and).repeated(), bin_op)
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

    let pad = |c| just(c).then_ignore(text::whitespace()).ignored();

    let id = id_ast_parser().then_ignore(text::whitespace());

    let id_list = id
        .clone()
        .separated_by(pad(','))
        .at_least(1)
        .collect()
        .map(List);

    let id_list_parens = id_list.clone().delimited_by(pad('('), pad(')'));

    let positive_integer = just('0')
        .not()
        .ignore_then(text::int(10).slice().from_str().unwrapped().map(Integer))
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

        let table = choice((subquery, id.clone()))
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
                    })
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
            .ignore_then(id_list.clone())
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
            .ignore_then(positive_integer)
            .then(
                choice((kw("OFFSET").ignored(), pad(',')))
                    .ignore_then(positive_integer)
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
        .ignore_then(kw("VIEW"))
        .ignore_then(choice((kw("TEMPORARY"), kw("TEMP"))).or_not())
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

fn kw<
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
    kw_no_pad(keyword).then_ignore(text::whitespace())
}

fn kw_no_pad<
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
