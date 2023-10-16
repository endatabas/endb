use chumsky::container::OrderedSeq;
use chumsky::extra::ParserExtra;
use chumsky::input::{StrInput, ValueInput};
use chumsky::prelude::*;
use chumsky::text::Char;

use super::ast::{Ast, Keyword};

pub fn ws<'input, I, E, C>() -> impl Parser<'input, I, (), E> + Clone
where
    I: StrInput<'input, C>,
    C: Char,
    E: ParserExtra<'input, I> + 'input,
{
    let hyphen: C = Char::from_ascii(b'-');

    choice((
        text::whitespace().at_least(1),
        just(hyphen)
            .ignore_then(just(hyphen))
            .ignore_then(any().and_is(text::newline().not()).repeated().ignored()),
    ))
    .repeated()
    .ignored()
    .boxed()
}

pub fn pad<'input, T, I, E, C>(seq: T) -> impl Parser<'input, I, (), E> + Clone
where
    I: StrInput<'input, C>,
    E: ParserExtra<'input, I>,
    C: Char,
    T: OrderedSeq<'input, C> + Clone,
{
    just(seq).then_ignore(ws()).ignored()
}

pub fn digits<'a, I: StrInput<'a, C>, C: Char<Str = str> + 'a, E: ParserExtra<'a, I> + 'a>(
    min: usize,
    max: usize,
) -> impl Parser<'a, I, &'a C::Str, E> + Clone + 'a {
    any()
        .filter(|c: &C| c.is_digit(10))
        .repeated()
        .at_least(min)
        .at_most(max)
        .to_slice()
}

pub fn kw<
    'a,
    I: ValueInput<'a> + StrInput<'a, C>,
    C: Char<Str = str> + 'a,
    E: ParserExtra<'a, I> + 'a,
>(
    keyword: &'a C::Str,
) -> impl Parser<'a, I, &'a C::Str, E> + Clone + 'a
where
    C::Str: PartialEq,
{
    kw_no_pad(keyword).then_ignore(ws())
}

pub fn kw_no_pad<
    'a,
    I: ValueInput<'a> + StrInput<'a, C>,
    C: Char<Str = str> + 'a,
    E: ParserExtra<'a, I> + 'a,
>(
    keyword: &'a C::Str,
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
        .to_slice()
}

pub fn add_clause(acc: &mut Vec<Ast>, kw: Keyword, c: Option<Ast>) {
    if let Some(c) = c {
        acc.push(Ast::KW(kw));
        acc.push(c);
    }
}
