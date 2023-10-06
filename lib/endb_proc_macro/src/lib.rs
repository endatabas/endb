use proc_macro2::TokenStream;
use quote::{quote, quote_spanned, ToTokens};
use syn::parse::{Parse, ParseStream, Result};
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, token, Ident, LitStr, Token};

#[derive(Clone)]
enum PegParser {
    Seq(Vec<PegParser>),
    Ord(Vec<PegParser>),
    Pattern(LitStr),
    Trivia(LitStr),
    Literal(LitStr),
    NonTerminal(Ident),
    Neg(Box<PegParser>),
    Look(Box<PegParser>),
    Cut(Box<PegParser>),
    Star(Box<PegParser>),
    Plus(Box<PegParser>),
    Opt(Box<PegParser>),
}

impl Parse for PegParser {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut seq = vec![];

        while !(input.is_empty() || input.peek(token::Semi) || input.peek(token::Slash)) {
            let prefix_span = input.span();
            let prefix_error = input.error("invalid prefix");
            let mut prefix = if input.peek(Token![~])
                || input.peek(Token![#])
                || input.peek(Token![!])
                || input.peek(Token![&])
                || input.peek(Token![^])
            {
                input.step(|cursor| cursor.token_tree().ok_or(cursor.error("missing prefix")))?;
                prefix_span.source_text()
            } else {
                None
            };

            let parser = if let Ok(literal) = input.parse::<LitStr>() {
                match prefix.as_deref() {
                    Some("~") => {
                        prefix.take();
                        PegParser::Trivia(literal)
                    }
                    Some("#") => {
                        prefix.take();
                        PegParser::Pattern(literal)
                    }
                    _ => PegParser::Literal(literal),
                }
            } else if let Ok(id) = input.parse::<Ident>() {
                if id.to_string().chars().all(|c| c.is_uppercase() || c == '_') {
                    PegParser::Literal(LitStr::new(&id.to_string(), id.span()))
                } else {
                    PegParser::NonTerminal(id)
                }
            } else if input.peek(token::Paren) {
                let content;
                syn::parenthesized!(content in input);
                content.call(parse_ord)?
            } else {
                return Err(input.error("unknown parser"));
            };

            let parser = match prefix.as_deref() {
                Some("!") => PegParser::Neg(parser.into()),
                Some("&") => PegParser::Look(parser.into()),
                Some("^") => PegParser::Cut(parser.into()),
                Some(_) => return Err(prefix_error),
                _ => parser,
            };

            let parser = if input.parse::<Token![*]>().is_ok() {
                PegParser::Star(parser.into())
            } else if input.parse::<Token![+]>().is_ok() {
                PegParser::Plus(parser.into())
            } else if input.parse::<Token![?]>().is_ok() {
                PegParser::Opt(parser.into())
            } else {
                parser
            };

            seq.push(parser);
        }

        if seq.len() == 1 {
            Ok(seq[0].clone())
        } else {
            Ok(PegParser::Seq(seq))
        }
    }
}

impl ToTokens for PegParser {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        match self {
            PegParser::Pattern(pattern) => quote! {
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
            },
            PegParser::Trivia(pattern) => quote! {
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
            },
            PegParser::Literal(literal) => {
                let literal_str = literal.value();
                let literal_len = literal_str.len();
                let punctuation = literal_str.chars().all(|c| c.is_ascii_punctuation());
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
                        let range = pos..(pos + #literal_len).min(input.len());
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
            PegParser::Seq(parsers) => quote! {
                |input: &str, pos: usize, state: &mut ParseState| {
                    let mut pos = pos;
                    let idx = state.events.len();

                    #(
                        match (#parsers)(input, pos, state) {
                            Ok(new_pos) => {
                                pos = new_pos;
                            }
                            Err(err) => {
                                state.events.truncate(idx);
                                return Err(err);
                            }
                        };

                    )*

                    Ok(pos)
                }
            },
            PegParser::Ord(parsers) => quote! {
                |input: &str, pos: usize, state: &mut ParseState| {
                    let idx = state.events.len();

                    #(
                        match (#parsers)(input, pos, state) {
                            Ok(pos) => return Ok(pos),
                            Err(ParseErr::Error) => {
                                state.events.truncate(idx);
                                return Err(ParseErr::Error);
                            }
                            Err(_) => {
                                state.events.truncate(idx);
                            }
                        };
                    )*

                    Err(ParseErr::Fail)
                }
            },
            PegParser::Star(parser) => quote! {
                |input: &str, pos: usize, state: &mut ParseState| {
                    let mut pos = pos;
                    loop {
                        let idx = state.events.len();
                        match (#parser)(input, pos, state) {
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
            },
            PegParser::Neg(parser) => quote! {
                |input: &str, pos: usize, state: &mut ParseState| {
                    let idx = state.events.len();
                    let err_idx = state.errors.len();
                    let result = (#parser)(input, pos, state);
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
            },
            PegParser::Cut(parser) => quote! {
                |input: &str, pos: usize, state: &mut ParseState| {
                    (#parser)(input, pos, state).or(Err(ParseErr::Error))
                }
            },
            PegParser::NonTerminal(id) => id.to_token_stream(),
            PegParser::Look(parser) => {
                PegParser::Neg(PegParser::Neg(parser.clone()).into()).to_token_stream()
            }
            PegParser::Plus(parser) => {
                PegParser::Seq(vec![*parser.clone(), PegParser::Star(parser.clone())])
                    .to_token_stream()
            }
            PegParser::Opt(parser) => PegParser::Ord(vec![
                *parser.clone(),
                PegParser::Trivia(LitStr::new("", proc_macro2::Span::mixed_site())),
            ])
            .to_token_stream(),
        }
        .to_tokens(tokens);
    }
}

fn parse_ord(input: ParseStream) -> Result<PegParser> {
    let ord = Punctuated::<PegParser, Token![/]>::parse_separated_nonempty(input)?
        .iter()
        .cloned()
        .collect::<Vec<_>>();

    if ord.len() == 1 {
        Ok(ord[0].clone())
    } else {
        Ok(PegParser::Ord(ord))
    }
}

#[derive(Clone)]
struct Rule {
    id: Ident,
    hidden: bool,
    body: PegParser,
}

impl ToTokens for Rule {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let id = &self.id;
        let body = &self.body;
        if self.hidden {
            quote_spanned! {id.span()=>
               #[allow(clippy::redundant_closure_call)]
                fn #id<'a, 'b: 'a>(input: &'a str, pos: usize, state: &mut ParseState<'b>) -> ParseResult {
                    (#body)(input, pos, state)
                }
            }
        } else {
            let id_string = id.to_string();

            quote_spanned! {id.span()=>
               #[allow(clippy::redundant_closure_call)]
                pub fn #id<'a, 'b: 'a>(input: &'a str, pos: usize, state: &mut ParseState<'b>) -> ParseResult {
                    state.events.push(Event::Open { label: #id_string, pos });
                    if state.track_errors {
                        state.errors.push(Event::Open { label: #id_string, pos });
                    }

                    let result = (#body)(input, pos, state);

                    state.events.push(Event::Close);
                    if state.track_errors {
                        state.errors.push(Event::Close);
                    }

                    result
                }
            }
        }.to_tokens(tokens);
    }
}

impl Parse for Rule {
    fn parse(input: ParseStream) -> Result<Self> {
        let hidden = input.peek(Token![<]);
        let id: Ident = if hidden {
            input.parse::<Token![<]>()?;
            let id = input.parse()?;
            input.parse::<Token![>]>()?;
            id
        } else {
            input.parse()?
        };
        input.parse::<Token![<]>()?;
        input.parse::<Token![-]>()?;

        let body = input.call(parse_ord)?;
        Ok(Rule { id, hidden, body })
    }
}

#[derive(Clone)]
struct Grammar {
    rules: Vec<Rule>,
}

impl ToTokens for Grammar {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        for rule in &self.rules {
            rule.to_tokens(tokens);
        }
    }
}

impl Parse for Grammar {
    fn parse(input: ParseStream) -> Result<Self> {
        let rules = Punctuated::<Rule, Token![;]>::parse_terminated(input)?;
        Ok(Grammar {
            rules: rules.iter().cloned().collect::<Vec<_>>(),
        })
    }
}

#[proc_macro]
pub fn peg(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let grammar = parse_macro_input!(input as Grammar);
    proc_macro::TokenStream::from(grammar.to_token_stream())
}
