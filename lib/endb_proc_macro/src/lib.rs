use proc_macro2::{Ident, Literal, TokenStream, TokenTree};
use quote::{quote, quote_spanned};

fn build_private_rule(id: Ident, body: TokenStream) -> TokenStream {
    quote_spanned! {id.span()=>
       #[allow(clippy::redundant_closure_call)]
        fn #id<'a, 'b: 'a>(input: &'a str, pos: usize, state: &mut ParseState<'b>) -> ParseResult {
            (#body)(input, pos, state)
        }
    }
}

fn build_pub_rule(id: Ident, body: TokenStream) -> TokenStream {
    let id_str = Literal::string(&id.to_string());

    quote_spanned! {id.span()=>
       #[allow(clippy::redundant_closure_call)]
        pub fn #id<'a, 'b: 'a>(input: &'a str, pos: usize, state: &mut ParseState<'b>) -> ParseResult {
            state.events.push(Event::Open { label: #id_str, pos });
            if state.track_errors {
                state.errors.push(Event::Open { label: #id_str, pos });
            }

            let result = (#body)(input, pos, state);

            state.events.push(Event::Close);
            if state.track_errors {
                state.errors.push(Event::Close);
            }

            result
        }
    }
}

fn build_nt(nt: Ident) -> TokenStream {
    TokenTree::Ident(nt).into()
}

fn build_pattern(pattern: Literal) -> TokenStream {
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            lazy_static::lazy_static! {
                static ref RE: regex::Regex = regex::Regex::new(#pattern).unwrap();
            }
            match RE.find_at(input, pos) {
                Some(m) if m.range().start == pos => {
                    state.events.push(Event::Token {
                        range: m.range(),
                        trivia: false,
                    });
                    whitespace(input, m.range().end, state)
                }
                _ => {
                    if state.track_errors {
                        state.errors.push(Event::Error {
                            descriptor: ParseErrorDescriptor::ExpectedPattern(#pattern),
                            range: pos..pos,
                        });
                    }
                    Err(ParseErr::Fail)
                }
            }
        }
    }
}

fn build_trivia(pattern: Literal) -> TokenStream {
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            lazy_static::lazy_static! {
                static ref RE: regex::Regex = regex::Regex::new(#pattern).unwrap();
            }
            match RE.find_at(input, pos) {
                Some(m) if m.range().start == pos => {
                    Ok(m.range().end)
                }
                _ => {
                    if state.track_errors {
                        state.errors.push(Event::Error {
                            descriptor: ParseErrorDescriptor::ExpectedPattern(#pattern),
                            range: pos..pos,
                        });
                    }
                    Err(ParseErr::Fail)
                }
            }
        }
    }
}

fn build_literal(literal: Literal) -> TokenStream {
    let literal_str = literal.to_string();
    let punctuation = literal_str[1..literal_str.len() - 1]
        .chars()
        .all(|c| c.is_ascii_punctuation());
    let valid_next_char = if punctuation {
        quote! {true}
    } else {
        quote! {
            input[range.end..]
                .chars()
                .next()
                .map_or(true, |c| !c.is_alphanumeric())
        }
    };
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            let range = pos..(pos + #literal.len()).min(input.len());
            if input[range.clone()].eq_ignore_ascii_case(#literal)
                && #valid_next_char {
                state.events.push(Event::Token {
                    range: range.clone(),
                    trivia: false,
                });
                whitespace(input, range.end, state)
            } else {
                if state.track_errors {
                    state.errors.push(Event::Error {
                        descriptor: ParseErrorDescriptor::ExpectedLiteral(#literal),
                        range,
                    });
                }
                Err(ParseErr::Fail)
            }
        }
    }
}

fn build_neg(neg: TokenStream) -> TokenStream {
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            let idx = state.events.len();
            let err_idx = state.errors.len();
            let result = (#neg)(input, pos, state);
            state.events.truncate(idx);
            if state.track_errors {
                state.errors.truncate(err_idx);
            }
            match result {
                Err(_) => Ok(pos),
                Ok(new_pos) => {
                    if state.track_errors {
                        state.errors.push(Event::Error {
                            descriptor: ParseErrorDescriptor::Unexpected,
                            range: pos..new_pos,
                        });
                    }
                    Err(ParseErr::Fail)
                }
            }
        }
    }
}

fn build_look(look: TokenStream) -> TokenStream {
    build_neg(build_neg(look))
}

fn build_cut(cut: TokenStream) -> TokenStream {
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            (#cut)(input, pos, state).or(Err(ParseErr::Error))
        }
    }
}

fn build_star(star: TokenStream) -> TokenStream {
    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            let mut pos = pos;
            loop {
                let idx = state.events.len();
                match (#star)(input, pos, state) {
                    Ok(new_pos) => {
                        pos = new_pos;
                    }
                    Err(ParseErr::Error) => {
                        state.events.truncate(idx);
                        return Err(ParseErr::Error);
                    }
                    Err(_) => {
                        state.events.truncate(idx);
                        return Ok(pos);
                    }
                }
            }
        }
    }
}

fn build_plus(plus: TokenStream) -> TokenStream {
    build_seq(&[plus.clone(), build_star(plus)])
}

fn build_opt(opt: TokenStream) -> TokenStream {
    build_ord(&[opt, build_trivia(Literal::string(""))])
}

fn build_seq(seq: &[TokenStream]) -> TokenStream {
    if let [tt] = seq {
        return tt.clone();
    }

    let mut body = TokenStream::new();

    for tt in seq {
        body.extend(quote! {
            match (#tt)(input, pos, state) {
                Ok(new_pos) => {
                    pos = new_pos;
                }
                Err(err) => {
                    state.events.truncate(idx);
                    return Err(err);
                }
            };
        });
    }

    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            let mut pos = pos;
            let idx = state.events.len();

            #body

            Ok(pos)
        }
    }
}

fn build_ord(ord: &[TokenStream]) -> TokenStream {
    if let [tt] = ord {
        return tt.clone();
    }

    let mut body = TokenStream::new();

    for tt in ord {
        body.extend(quote! {
            match (#tt)(input, pos, state) {
                Ok(pos) => return Ok(pos),
                Err(ParseErr::Error) => {
                    state.events.truncate(idx);
                    return Err(ParseErr::Error);
                }
                Err(_) => {
                    state.events.truncate(idx);
                }
            };
        });
    }

    quote! {
        |input: &str, pos: usize, state: &mut ParseState| {
            let idx = state.events.len();

            #body

            Err(ParseErr::Fail)
        }
    }
}

fn parse_seq(i: &mut impl Iterator<Item = TokenTree>) -> TokenStream {
    let mut seq = vec![];
    let mut i = i.peekable();

    while i.peek().is_some() {
        let prefix = i
            .next_if(|i| matches!(i, TokenTree::Punct(punct) if "!&^#~".contains(punct.as_char())));
        let token = i.next().expect("token");

        let parser = match token {
            TokenTree::Ident(id) => {
                if id.to_string().chars().all(|c| c.is_uppercase() || c == '_') {
                    build_literal(Literal::string(&id.to_string()))
                } else {
                    build_nt(id)
                }
            }
            TokenTree::Group(group) => parse_ord(&mut group.stream().into_iter()),
            TokenTree::Literal(literal) => match prefix {
                Some(TokenTree::Punct(ref punct)) if '#' == punct.as_char() => {
                    build_pattern(literal)
                }
                Some(TokenTree::Punct(ref punct)) if '~' == punct.as_char() => {
                    build_trivia(literal)
                }
                _ => build_literal(literal),
            },
            TokenTree::Punct(punct) => {
                unreachable!("unexpected punct: {}", punct.as_char())
            }
        };

        let suffix =
            i.next_if(|i| matches!(i, TokenTree::Punct(punct) if "?+*".contains(punct.as_char())));

        let parser = match suffix {
            Some(TokenTree::Punct(ref punct)) => match punct.as_char() {
                '*' => build_star(parser),
                '+' => build_plus(parser),
                '?' => build_opt(parser),
                _ => unreachable!("unexpected suffix: {}", punct.as_char()),
            },

            _ => parser,
        };

        let parser = match prefix {
            Some(TokenTree::Punct(punct)) => match punct.as_char() {
                '!' => build_neg(parser),
                '&' => build_look(parser),
                '^' => build_cut(parser),
                '#' | '~' => parser,
                _ => unreachable!("unexpected prefix: {}", punct.as_char()),
            },
            _ => parser,
        };

        seq.push(parser);
    }

    build_seq(&seq)
}

fn parse_ord(i: &mut impl Iterator<Item = TokenTree>) -> TokenStream {
    let mut ord = vec![];

    for seq in i
        .collect::<Vec<_>>()
        .split(|item| matches!(item, TokenTree::Punct(punct) if '/' == punct.as_char()))
    {
        ord.push(parse_seq(&mut seq.iter().cloned()));
    }

    build_ord(&ord)
}

fn read_rule_arrow(i: &mut impl Iterator<Item = TokenTree>) {
    assert!(matches!(i.next(), Some(TokenTree::Punct(punct)) if '<' == punct.as_char()));
    assert!(matches!(i.next(), Some(TokenTree::Punct(punct)) if '-' == punct.as_char()));
}

fn parse_rule(i: &mut impl Iterator<Item = TokenTree>) -> TokenStream {
    let token = i.next();

    match token {
        Some(TokenTree::Punct(punct)) if '<' == punct.as_char() => {
            let token = i.next();
            let Some(TokenTree::Ident(id)) = token else {
                unreachable!("unexpected {:?}", token)
            };
            assert!(matches!(i.next(), Some(TokenTree::Punct(punct)) if '>' == punct.as_char()));
            read_rule_arrow(i);

            let body = parse_ord(i);
            build_private_rule(id.clone(), body)
        }
        Some(TokenTree::Ident(id)) => {
            read_rule_arrow(i);

            let body = parse_ord(i);
            build_pub_rule(id, body)
        }
        None => TokenStream::new(),
        _ => unreachable!("unexpected: {:?}", token),
    }
}

fn parse_grammar(i: &mut impl Iterator<Item = TokenTree>) -> TokenStream {
    let mut grammar = TokenStream::new();

    for rule in i
        .collect::<Vec<_>>()
        .split(|item| matches!(item, TokenTree::Punct(punct) if ';' == punct.as_char()))
    {
        grammar.extend(parse_rule(&mut rule.iter().cloned()));
    }

    grammar
}

#[proc_macro]
pub fn peg(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro::TokenStream::from(parse_grammar(&mut TokenStream::from(input).into_iter()))
}
