use chumsky::container::OrderedSeq;
use chumsky::extra::ParserExtra;
use chumsky::input::{StrInput, ValueInput};
use chumsky::prelude::*;
use chumsky::text::Char;

use super::ast::{Ast, Keyword};

pub fn pad<'input, T, I, E, C>(seq: T) -> impl Parser<'input, I, (), E> + Clone
where
    I: StrInput<'input, C>,
    E: ParserExtra<'input, I>,
    C: Char,
    T: OrderedSeq<'input, C> + Clone,
{
    just(seq).then_ignore(text::whitespace()).ignored()
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
        .slice()
}

pub fn kw<
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

pub fn kw_no_pad<
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

pub fn add_clause(acc: &mut Vec<Ast>, kw: Keyword, c: Option<Ast>) {
    if let Some(c) = c {
        acc.push(Ast::KW(kw));
        acc.push(c);
    }
}
