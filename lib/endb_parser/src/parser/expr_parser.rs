use chumsky::extra::ParserExtra;
use chumsky::prelude::*;

use super::ast::{Ast, Keyword};
use super::util::*;

pub fn id_ast_parser_no_pad<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::Ast::*;

    text::ident().map_with_span(|_, span: SimpleSpan<_>| Id {
        start: span.start() as i32,
        end: span.end() as i32,
    })
}

pub fn col_ref_ast_parser_no_pad<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::Ast::*;

    text::ident()
        .then(just('.').then(text::ident()).or_not())
        .map_with_span(|_, span: SimpleSpan<_>| Id {
            start: span.start() as i32,
            end: span.end() as i32,
        })
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

pub fn atom_ast_parser<'input, E>() -> impl Parser<'input, &'input str, Ast, E> + Clone
where
    E: ParserExtra<'input, &'input str>,
{
    use super::ast::{Ast::*, Keyword::*};

    let iso_date = digits(4, 4)
        .then_ignore(just('-'))
        .then(digits(2, 2))
        .then_ignore(just('-'))
        .then(digits(2, 2));

    let iso_time = digits(2, 2)
        .then_ignore(just(':'))
        .then(digits(2, 2))
        .then_ignore(just(':'))
        .then(digits(2, 2))
        .then(just('.').ignore_then(digits(1, 6)).or_not());

    let date_ctor = |_, span: SimpleSpan<_>| kw_literal(Date, &span);

    let date = choice((
        kw("DATE").ignore_then(
            iso_date
                .clone()
                .map_with_span(date_ctor)
                .padded_by(just('\'')),
        ),
        iso_date.clone().map_with_span(date_ctor),
    ));

    let time_ctor = |_, span: SimpleSpan<_>| kw_literal(Time, &span);

    let time = choice((
        kw("TIME").ignore_then(
            iso_time
                .clone()
                .map_with_span(time_ctor)
                .padded_by(just('\'')),
        ),
        iso_time.clone().map_with_span(time_ctor),
    ));

    let timestamp_ctor = |_, span: SimpleSpan<_>| kw_literal(Timestamp, &span);

    let timestamp = choice((
        kw("TIMESTAMP").ignore_then(
            iso_date
                .clone()
                .then(choice((just(' '), just('T'))))
                .then(iso_time.clone())
                .then(just('Z').or_not())
                .map_with_span(timestamp_ctor)
                .padded_by(just('\'')),
        ),
        iso_date
            .clone()
            .then(just('T'))
            .then(iso_time.clone())
            .then(just('Z').or_not())
            .map_with_span(timestamp_ctor),
    ));

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

    choice((
        timestamp,
        date,
        time,
        number,
        binary,
        string,
        boolean,
        col_ref_ast_parser_no_pad(),
    ))
    .then_ignore(text::whitespace())
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

        let id = id_ast_parser_no_pad().then_ignore(text::whitespace());

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

        let count_star = kw("COUNT")
            .ignore_then(
                all_distinct
                    .clone()
                    .then_ignore(pad('*'))
                    .delimited_by(pad('('), pad(')')),
            )
            .map(|distinct| {
                let mut acc = vec![KW(AggregateFunction), KW(CountStar), List(vec![])];
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
            choice((pad("<<").to(Lsh), pad(">>").to(Rsh)))
                .then(add)
                .repeated(),
            bin_op,
        );

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
                    choice((pad('=').to(Some(Eq)), pad("<>").to(Some(Ne))))
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
