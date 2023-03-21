// https://app.gather.town/app/5ta6dFBl7lzTrEMF/endatabas

use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::error::Rich;
use chumsky::extra::{Err, ParserExtra};
use chumsky::input::{StrInput, ValueInput};
use chumsky::prelude::*;
use chumsky::text::Char;

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

pub fn expr_ast_parser(
) -> Recursive<dyn Parser<'static, &'static str, Ast, Err<Rich<'static, char>>>> {
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

pub fn sql_ast_parser(
) -> Recursive<dyn Parser<'static, &'static str, Ast, Err<Rich<'static, char>>>> {
    use Ast::*;
    use Keyword::*;

    let expr = expr_ast_parser();
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
                .map(|expr| List(vec![expr]))
                .separated_by(just(','))
                .at_least(1)
                .collect()
                .map(Ast::List),
        );

    let from_clause = keyword_ignore_case("FROM")
        .ignore_then(
            expr.clone()
                .map(|expr| List(vec![expr]))
                .separated_by(just(','))
                .at_least(1)
                .collect()
                .map(Ast::List),
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
                .map(Ast::List),
        )
        .or_not();

    let having_clause = keyword_ignore_case("HAVING")
        .ignore_then(expr.clone())
        .or_not();

    let order_by = keyword_ignore_case("ORDER")
        .ignore_then(keyword_ignore_case("BY"))
        .ignore_then(
            expr.then(
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
            .map(Ast::List),
        )
        .or_not();

    let positive_integer = just('0')
        .not()
        .ignore_then(text::int(10).slice().from_str().unwrapped().map(Integer));

    let limit_clause = keyword_ignore_case("LIMIT")
        .ignore_then(positive_integer)
        .then(
            choice((keyword_ignore_case("OFFSET"), just(",").padded()))
                .ignore_then(positive_integer)
                .or_not(),
        )
        .or_not();

    recursive(|_| {
        select_clause
            .then(from_clause)
            .then(where_clause)
            .then(group_by_clause)
            .then(having_clause)
            .then(order_by)
            .then(limit_clause)
            .map(
                |(
                    (
                        (((((distinct, select_list), from), where_clause), group_by), having),
                        order_by,
                    ),
                    limit_offset,
                )| {
                    let mut acc = vec![KW(Select), select_list];

                    let mut clause = |kw, c: Option<Ast>| {
                        c.map(|c| {
                            acc.push(KW(kw));
                            acc.push(c);
                        });
                    };

                    clause(Distinct, distinct);
                    clause(From, from);
                    clause(Where, where_clause);
                    clause(GroupBy, group_by);
                    clause(Having, having);
                    clause(OrderBy, order_by);

                    match limit_offset {
                        Some((limit, offset)) => {
                            clause(Limit, Some(limit));
                            clause(Offset, offset);
                        }
                        None => {}
                    }

                    List(acc)
                },
            )
            .then_ignore(end())
    })
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
}
