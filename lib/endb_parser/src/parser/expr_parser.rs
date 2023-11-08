use chumsky::extra::ParserExtra;
use chumsky::prelude::*;

use super::ast::{Ast, Keyword};
use super::util::*;

pub fn id_ast_parser_no_pad<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::Ast::*;

    text::ident().map_with(|_, e| Id {
        start: (e.span() as SimpleSpan).start() as i32,
        end: (e.span() as SimpleSpan).end() as i32,
    })
}

pub fn col_ref_ast_parser_no_pad<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::Ast::*;

    text::ident()
        .then(just('.').then(text::ident()).or_not())
        .map_with(|_, e| Id {
            start: (e.span() as SimpleSpan).start() as i32,
            end: (e.span() as SimpleSpan).end() as i32,
        })
}

pub fn named_parameter_ast_parser_no_pad<'input, E>(
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    one_of(":$")
        .ignore_then(id_ast_parser_no_pad())
        .map(|id| List(vec![KW(Parameter), id]))
}

pub fn string_ast_parser_no_pad<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::Ast::*;

    let escape = just('\\').ignore_then(choice((
        just('0'),
        just('\\'),
        just('/'),
        just('\''),
        just('"'),
        just('b'),
        just('f'),
        just('n'),
        just('r'),
        just('t'),
        just('v'),
        one_of("\n\r\u{2028}\u{2029}"),
        just('u').then_ignore(text::digits(16).exactly(4)),
    )));

    let single_quoted_string = choice((
        just('\'').ignore_then(just('\'')),
        choice((none_of("\\'"), escape)),
    ))
    .repeated()
    .map_with(|_, e| String {
        start: (e.span() as SimpleSpan).start() as i32,
        end: (e.span() as SimpleSpan).end() as i32,
    })
    .padded_by(just('\''));

    let double_quoted_string = choice((none_of("\\\""), escape))
        .repeated()
        .map_with(|_, e| String {
            start: (e.span() as SimpleSpan).start() as i32,
            end: (e.span() as SimpleSpan).end() as i32,
        })
        .padded_by(just('"'));

    choice((single_quoted_string, double_quoted_string))
}

fn kw_literal(kw: Keyword, span: &SimpleSpan<usize>) -> Ast {
    use super::ast::Ast::*;

    List(vec![
        KW(kw),
        String {
            start: span.start as i32,
            end: span.end as i32,
        },
    ])
}

pub fn datetime_field_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    choice((
        kw("YEAR").to(Year),
        kw("MONTH").to(Month),
        kw("DAY").to(Day),
        kw("HOUR").to(Hour),
        kw("MINUTE").to(Minute),
        kw("SECOND").to(Second),
    ))
    .map(KW)
}

pub fn atom_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    let iso_time = digits(2, 2)
        .then(just(':'))
        .then(digits(2, 2))
        .then(just(':'))
        .then(digits(2, 2))
        .then(just('.').then(digits(1, 6)).or_not());

    let time = choice((
        kw("TIME").ignore_then(choice((
            iso_time
                .clone()
                .map_with(|_, e| kw_literal(Time, &e.span()))
                .padded_by(just('\'')),
            iso_time
                .clone()
                .map_with(|_, e| kw_literal(Time, &e.span()))
                .padded_by(just('"')),
        ))),
        iso_time
            .clone()
            .map_with(|_, e| kw_literal(Time, &e.span())),
    ));

    let iso_date = digits(4, 4)
        .then(just('-'))
        .then(digits(2, 2))
        .then(just('-'))
        .then(digits(2, 2));

    let date = choice((
        kw("DATE").ignore_then(choice((
            iso_date
                .clone()
                .map_with(|_, e| kw_literal(Date, &e.span()))
                .padded_by(just('\'')),
            iso_date
                .clone()
                .map_with(|_, e| kw_literal(Date, &e.span()))
                .padded_by(just('"')),
        ))),
        iso_date
            .clone()
            .map_with(|_, e| kw_literal(Date, &e.span())),
    ));

    let iso_timestamp = iso_date
        .clone()
        .then(one_of(" T"))
        .then(iso_time.clone())
        .then(just('Z').or_not());

    let timestamp = choice((
        kw("TIMESTAMP").ignore_then(choice((
            iso_timestamp
                .clone()
                .map_with(|_, e| kw_literal(Timestamp, &e.span()))
                .padded_by(just('\'')),
            iso_timestamp
                .clone()
                .map_with(|_, e| kw_literal(Timestamp, &e.span()))
                .padded_by(just('"')),
        ))),
        iso_timestamp.map_with(|_, e| kw_literal(Timestamp, &e.span())),
    ));

    let iso_quantity = text::int(10).then(one_of(",.").then(text::digits(10)).or_not());

    let iso_duration = just('P')
        .then(iso_quantity.then(just('Y')).or_not())
        .then(iso_quantity.then(just('M')).or_not())
        .then(iso_quantity.then(just('D')).or_not())
        .then(
            just('T')
                .then(iso_quantity.then(just('H')).or_not())
                .then(iso_quantity.then(just('M')).or_not())
                .then(iso_quantity.then(just('S')).or_not())
                .or_not(),
        )
        .map_with(|_, e| kw_literal(Duration, &e.span()));

    let interval_field = datetime_field_ast_parser();

    let interval_year_month_or_single = text::int(10)
        .then(just('-').then(text::int(10)).or_not())
        .map_with(|_, e| kw_literal(Interval, &e.span()));
    let interval_time = digits(2, 2)
        .separated_by(just(':'))
        .at_least(1)
        .at_most(3)
        .then(just('.').then(text::digits(10)).or_not());
    let interval_day_time = text::int(10)
        .then(just(' '))
        .then(interval_time.clone())
        .map_with(|_, e| kw_literal(Interval, &e.span()));

    let unquoted_interval_string = choice((
        interval_time.map_with(|_, e| kw_literal(Interval, &e.span())),
        interval_day_time,
        interval_year_month_or_single,
    ));

    let interval_string = choice((
        unquoted_interval_string.clone().padded_by(just('\'')),
        unquoted_interval_string.padded_by(just('"')),
    ));

    let interval = kw("INTERVAL")
        .ignore_then(interval_string.padded_by(ws()))
        .then(interval_field.clone())
        .then(kw("TO").ignore_then(interval_field).or_not())
        .map(|((interval, start), end)| {
            let mut acc = match interval {
                List(x) => x,
                _ => unreachable!(),
            };
            acc.push(start);
            if let Some(end) = end {
                acc.push(end);
            }
            List(acc)
        });

    let interval = choice((iso_duration, interval));

    let frac = just('.').then(text::digits(10));
    let exp = one_of("eE")
        .then(one_of("+-").or_not())
        .then(text::digits(10));

    let number = text::int(10)
        .then(frac.or_not())
        .then(exp.or_not())
        .map_with(|_, e| match (e.slice() as &str).parse::<i128>() {
            Ok(x) => Integer(x),
            Err(_) => Float((e.slice() as &str).parse().unwrap()),
        })
        .boxed();

    let hex =
        just("0x").ignore_then(text::digits(16).map_with(|_, e| {
            match i128::from_str_radix(e.slice(), 16) {
                Ok(x) => Integer(x),
                Err(_) => Float(f64::NAN),
            }
        }));

    let binary = one_of("Xx").ignore_then(choice((
        text::digits(16)
            .map_with(|_, e| kw_literal(Blob, &e.span()))
            .padded_by(just('\'')),
        text::digits(16)
            .map_with(|_, e| kw_literal(Blob, &e.span()))
            .padded_by(just('"')),
    )));

    let string = string_ast_parser_no_pad();

    let boolean = choice((
        kw_no_pad("TRUE").to(True),
        kw_no_pad("FALSE").to(False),
        kw_no_pad("NULL").to(Null),
    ))
    .map(KW);

    let current_literals = choice((
        kw_no_pad("CURRENT_DATE").to(CurrentDate),
        kw_no_pad("CURRENT_TIME").to(CurrentTime),
        kw_no_pad("CURRENT_TIMESTAMP").to(CurrentTimestamp),
    ))
    .map(KW);

    let parameter = choice((
        pad('?').to(Parameter).map(|_| List(vec![KW(Parameter)])),
        named_parameter_ast_parser_no_pad(),
    ));

    choice((
        timestamp,
        date,
        time,
        interval,
        hex,
        number,
        binary,
        string,
        boolean,
        current_literals,
        parameter,
        col_ref_ast_parser_no_pad(),
    ))
    .then_ignore(ws())
    .boxed()
}

pub fn object_ast_parser<'input, E>(
    expr: impl Parser<'input, &'input str, Ast, E> + Clone + 'input,
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    let ws = ws();
    let id = id_ast_parser_no_pad().padded_by(ws.clone());
    let string = string_ast_parser_no_pad().padded_by(ws.clone());
    let col_ref = col_ref_ast_parser_no_pad().padded_by(ws.clone());
    let named_parameter = named_parameter_ast_parser_no_pad().padded_by(ws.clone());

    let kw_pair = choice((id.clone(), string))
        .clone()
        .then_ignore(one_of(":=").padded_by(ws.clone()))
        .then(expr.clone())
        .map(|(k, v)| List(vec![k, v]));

    let shorthand_property = choice((col_ref, named_parameter))
        .clone()
        .map(|col_ref| List(vec![KW(ShorthandProperty), col_ref]));

    let spread_property = pad("...")
        .ignore_then(expr.clone())
        .map(|expr| List(vec![KW(SpreadProperty), expr]));

    let computed_property = expr
        .clone()
        .delimited_by(pad('['), pad(']'))
        .then_ignore(one_of(":=").padded_by(ws.clone()))
        .then(expr.clone())
        .map(|(expr, v)| List(vec![KW(ComputedProperty), expr, v]));

    let row_property = id
        .clone()
        .then_ignore(pad('.'))
        .then_ignore(pad('*'))
        .map(|id| List(vec![KW(Mul), id]));

    let kws = choice((
        spread_property,
        computed_property,
        row_property,
        kw_pair,
        shorthand_property,
    ))
    .separated_by(pad(','))
    .allow_trailing()
    .collect()
    .map(List)
    .map(|kws| List(vec![KW(Object), kws]));

    choice((
        kw("OBJECT").ignore_then(kws.clone().delimited_by(pad('('), pad(')'))),
        kws.delimited_by(pad('{'), pad('}')),
    ))
    .boxed()
}

pub fn path_ast_parser<'input, E>(
    expr: impl Parser<'input, &'input str, Ast, E> + Clone + 'input,
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    let id = id_ast_parser_no_pad().padded_by(ws());
    pad('$').ignore_then(
        choice((
            pad('.').ignore_then(id.clone()),
            choice((
                expr.clone(),
                pad('#')
                    .ignore_then(pad('-').ignore_then(expr.clone()).or_not())
                    .map(|expr| {
                        if let Some(expr) = expr {
                            List(vec![KW(Minus), expr])
                        } else {
                            KW(Hash)
                        }
                    }),
            ))
            .delimited_by(pad('['), pad(']')),
        ))
        .repeated()
        .collect()
        .map(List)
        .map(|path_elements| List(vec![KW(Path), path_elements])),
    )
}

pub fn expr_ast_parser<'input, E>(
    query: impl Parser<'input, &'input str, Ast, E> + Clone + 'input,
) -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    recursive(|expr| {
        let unary_op = |op, rhs| List(vec![KW(op), rhs]);
        let bin_op = |lhs, (op, rhs)| List(vec![KW(op), lhs, rhs]);

        let subquery = query.delimited_by(pad('('), pad(')'));

        let id = id_ast_parser_no_pad().then_ignore(ws());

        let opt_expr_list = expr.clone().separated_by(pad(',')).collect().map(List);

        let expr_list = expr
            .clone()
            .separated_by(pad(','))
            .at_least(1)
            .collect()
            .map(List);

        let all_distinct = choice((kw("DISTINCT").to(Distinct), kw("ALL").to(All)))
            .map(KW)
            .or_not();

        let aggregate_filter = kw("FILTER")
            .ignore_then(
                kw("WHERE")
                    .ignore_then(expr.clone())
                    .delimited_by(pad('('), pad(')')),
            )
            .or_not();

        let count_star = kw("COUNT")
            .ignore_then(
                all_distinct
                    .clone()
                    .then_ignore(pad('*'))
                    .delimited_by(pad('('), pad(')')),
            )
            .then(aggregate_filter.clone())
            .map(|(distinct, filter)| {
                let mut acc = vec![KW(AggregateFunction), KW(CountStar), List(vec![])];
                add_clause(&mut acc, Distinct, distinct);
                add_clause(&mut acc, Where, filter);
                List(acc)
            });

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

        let order_by = kw("ORDER")
            .ignore_then(kw("BY"))
            .ignore_then(order_by_list)
            .or_not();

        let array_agg = kw("ARRAY_AGG")
            .to(ArrayAgg)
            .map(KW)
            .then(
                expr_list
                    .clone()
                    .then(order_by)
                    .delimited_by(pad('('), pad(')'))
                    .then(aggregate_filter.clone()),
            )
            .map(|(f, ((expr_list, order_by), filter))| {
                let mut acc = vec![KW(AggregateFunction), f, expr_list];
                add_clause(&mut acc, OrderBy, order_by);
                add_clause(&mut acc, Where, filter);
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
            kw("OBJECT_AGG").to(ObjectAgg),
        ))
        .map(KW)
        .then(
            all_distinct
                .then(expr_list.clone())
                .delimited_by(pad('('), pad(')'))
                .then(aggregate_filter),
        )
        .map(|(f, ((distinct, expr_list), filter))| {
            let mut acc = vec![KW(AggregateFunction), f, expr_list];
            add_clause(&mut acc, Distinct, distinct);
            add_clause(&mut acc, Where, filter);
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

        let extract = kw("EXTRACT")
            .ignore_then(
                datetime_field_ast_parser()
                    .then_ignore(kw("FROM"))
                    .then(expr.clone())
                    .delimited_by(pad('('), pad(')')),
            )
            .map(|(field, expr)| List(vec![KW(Extract), field, expr]));

        let exists = kw("EXISTS")
            .ignore_then(subquery.clone())
            .map(|query| List(vec![KW(Exists), query]));

        let case = kw("CASE")
            .ignore_then(
                expr.clone()
                    .and_is(kw("WHEN").not())
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
            .then(opt_expr_list.clone().delimited_by(pad('('), pad(')')))
            .map(|(f, exprs)| List(vec![KW(Function), f, exprs]));

        let scalar_subquery = subquery
            .clone()
            .map(|query| List(vec![KW(ScalarSubquery), query]));

        let array = choice((
            kw("ARRAY")
                .ignore_then(subquery.clone())
                .map(|subquery| List(vec![KW(ArrayQuery), subquery])),
            kw("ARRAY").or_not().ignore_then(
                pad("...")
                    .or_not()
                    .then(expr.clone())
                    .map(|(spread, expr)| match spread {
                        Some(_) => List(vec![KW(SpreadProperty), expr]),
                        None => expr,
                    })
                    .separated_by(pad(','))
                    .allow_trailing()
                    .collect()
                    .map(List)
                    .delimited_by(pad('['), pad(']'))
                    .map(|exprs| List(vec![KW(Array), exprs])),
            ),
        ))
        .boxed();

        let atom = choice((
            count_star,
            array_agg,
            aggregate_function,
            cast,
            extract,
            case,
            exists,
            array,
            object_ast_parser(expr.clone()),
            path_ast_parser(expr.clone()),
            function,
            scalar_subquery,
            atom_ast_parser(),
            expr.clone().delimited_by(pad('('), pad(')')),
        ))
        .boxed();

        let bracketed_path =
            choice((pad('*').to(Mul).map(KW), expr.clone())).delimited_by(pad('['), pad(']'));

        let access = atom
            .foldl(
                choice((
                    pad("..")
                        .to(Recursive)
                        .map(KW)
                        .then(choice((id.clone(), bracketed_path.clone())))
                        .map(|(recursive, path)| (Some(recursive), path)),
                    choice((pad('.').ignore_then(id.clone()), bracketed_path))
                        .map(|path| (None, path)),
                ))
                .repeated(),
                |lhs, (recursive, rhs)| {
                    let mut acc = vec![KW(Access), lhs, rhs];
                    add_clause(&mut acc, Recursive, recursive);
                    List(acc)
                },
            )
            .boxed();

        let unary = choice((pad('+').to(Plus), pad('-').to(Minus), pad('~').to(BitNot)))
            .repeated()
            .foldr(access, unary_op);

        let concat = unary
            .clone()
            .foldl(pad("||").to(Concat).then(unary).repeated(), bin_op);

        let mul = concat.clone().foldl(
            choice((pad('*').to(Mul), pad('/').to(Div), pad('%').to(Mod)))
                .then(concat)
                .repeated(),
            bin_op,
        );

        let add = mul.clone().foldl(
            choice((pad('+').to(Plus), pad('-').to(Minus)))
                .then(mul)
                .repeated(),
            bin_op,
        );

        let shift = add
            .clone()
            .foldl(
                choice((
                    pad("<<").to(Lsh),
                    pad(">>").to(Rsh),
                    pad("&").to(BitAnd),
                    pad("|").to(BitOr),
                ))
                .then(add)
                .repeated(),
                bin_op,
            )
            .boxed();

        let comp = shift.clone().foldl(
            choice((
                pad("<=").to(Le),
                pad('<').to(Lt),
                pad(">=").to(Ge),
                pad('>').to(Gt),
            ))
            .then(shift)
            .repeated(),
            bin_op,
        );

        let equal = comp
            .clone()
            .foldl(
                choice((
                    choice((
                        pad("==").to(Some(Eq)),
                        pad('=').to(Some(Eq)),
                        pad("<>").to(Some(Ne)),
                        pad("!=").to(Some(Ne)),
                        kw("OVERLAPS").to(Some(Overlaps)),
                        kw("EQUALS").to(Some(Eq)),
                        kw("CONTAINS").to(Some(Contains)),
                        kw("PRECEDES").to(Some(Precedes)),
                        kw("SUCCEEDS").to(Some(Succeeds)),
                        kw("IMMEDIATELY").ignore_then(choice((
                            kw("PRECEDES").to(Some(ImmediatelyPrecedes)),
                            kw("SUCCEEDS").to(Some(ImmediatelySucceeds)),
                        ))),
                    ))
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
                                .map(|(lhs, rhs)| List(vec![lhs, rhs])),
                        ),
                        kw("LIKE").to(Some(Like)).then(
                            comp.clone()
                                .then(kw("ESCAPE").ignore_then(comp.clone()).or_not())
                                .map(|(lhs, rhs)| {
                                    if let Some(rhs) = rhs {
                                        List(vec![lhs, rhs])
                                    } else {
                                        List(vec![lhs])
                                    }
                                }),
                        ),
                        kw("GLOB").to(Some(Glob)).then(comp.clone()),
                        kw("REGEXP").to(Some(Regexp)).then(comp.clone()),
                        choice((kw("MATCH").to(Some(Match)), pad("@>").to(Some(Match))))
                            .then(comp.clone()),
                        kw("IN").ignore_then(choice((
                            subquery.clone().map(|query| (Some(InQuery), query)),
                            id.clone().map(|id| (Some(InQuery), id)),
                            opt_expr_list
                                .delimited_by(pad('('), pad(')'))
                                .map(|expr_list| (Some(In), expr_list)),
                        ))),
                    ))),
                ))
                .repeated(),
                |lhs, (op_1, (op_2, rhs))| match (op_1, op_2) {
                    (not, Some(op @ Between | op @ Like)) => {
                        let mut acc = vec![KW(op), lhs];
                        if let List(mut rhs) = rhs {
                            let rhs_rhs = rhs.pop().unwrap();
                            if let Some(rhs_lhs) = rhs.pop() {
                                acc.push(rhs_lhs);
                            }
                            acc.push(rhs_rhs);
                        };
                        let acc = List(acc);
                        match not {
                            Some(Not) => List(vec![KW(Not), acc]),
                            _ => acc,
                        }
                    }
                    (None, Some(op)) | (Some(op), None) => List(vec![KW(op), lhs, rhs]),
                    (Some(Not), Some(op)) | (Some(op), Some(Not)) => {
                        List(vec![KW(Not), List(vec![KW(op), lhs, rhs])])
                    }
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
