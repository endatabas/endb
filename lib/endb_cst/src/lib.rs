use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::ops::Range;
use std::rc::Rc;

use ariadne::{sources, Color, Label, Report, ReportKind};

#[derive(Clone, PartialEq, Debug)]
pub enum ParseErrorDescriptor<'a> {
    ExpectedLiteral(&'a str),
    ExpectedPattern(&'a str),
    Labeled(&'a str),
    Unexpected,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Event<'a> {
    Open {
        label: &'a str,
        pos: usize,
    },
    Close,
    Token {
        range: Range<usize>,
        trivia: bool,
    },
    Error {
        range: Range<usize>,
        descriptor: ParseErrorDescriptor<'a>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub struct ParseError<'a> {
    descriptor: ParseErrorDescriptor<'a>,
    range: Range<usize>,
    context: ParseContext<'a>,
}

#[derive(Clone, PartialEq, Debug, Default)]
pub struct ParseState<'a> {
    pub events: Vec<Event<'a>>,
    pub errors: Vec<Event<'a>>,
    pub track_errors: bool,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ParseErr {
    Fail,
    Error,
    Cut,
}

type ParseContextEntry<'a> = (&'a str, usize);
type ParseContext<'a> = Vec<ParseContextEntry<'a>>;
type ParseResult<'a> = Result<usize, ParseErr>;
type ParseFn<'a, 'b> = dyn Fn(&str, usize, &mut ParseState<'a>) -> ParseResult<'a> + 'b;
type Parser<'a, 'b> = Rc<ParseFn<'a, 'b>>;
type ParserGrammar<'a, 'b> = Rc<RefCell<HashMap<&'a str, Parser<'a, 'b>>>>;

#[derive(Clone, PartialEq, Debug)]
#[repr(C)]
pub enum Node<'a> {
    Branch {
        id: &'a str,
        children: Vec<Node<'a>>,
    },
    Leaf {
        range: Range<usize>,
    },
}

pub fn label<'a: 'b, 'b>(label: &'a str, parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    Rc::new(move |input, pos, state| {
        state.events.push(Event::Open { label, pos });
        if state.track_errors {
            state.errors.push(Event::Open { label, pos });
        }

        let result = parser(input, pos, state);
        state.events.push(Event::Close);
        if state.track_errors {
            state.errors.push(Event::Close);
        }

        result
    })
}

pub fn throw(message: &str) -> Parser<'_, '_> {
    Rc::new(move |_, pos, state| {
        if state.track_errors {
            state.errors.push(Event::Error {
                descriptor: ParseErrorDescriptor::Labeled(message),
                range: pos..pos,
            });
        }
        Err(ParseErr::Error)
    })
}

pub fn cut<'a: 'b, 'b>() -> Parser<'a, 'b> {
    Rc::new(|_, _, _| Err(ParseErr::Cut))
}

fn terminal(re: Regex, descriptor: ParseErrorDescriptor<'_>, trivia: bool) -> Parser<'_, '_> {
    Rc::new(move |input, pos, state| match re.find_at(input, pos) {
        Some(m) if m.range().start == pos => {
            state.events.push(Event::Token {
                range: m.range(),
                trivia,
            });
            Ok(m.range().end)
        }
        _ => {
            if state.track_errors {
                state.errors.push(Event::Error {
                    descriptor: descriptor.clone(),
                    range: pos..pos,
                });
            }
            Err(ParseErr::Fail)
        }
    })
}

pub fn trivia(pattern: &str) -> Parser<'_, '_> {
    terminal(
        Regex::new(pattern).unwrap(),
        ParseErrorDescriptor::ExpectedPattern(pattern),
        true,
    )
}

pub fn re(pattern: &str) -> Parser<'_, '_> {
    terminal(
        Regex::new(pattern).unwrap(),
        ParseErrorDescriptor::ExpectedPattern(pattern),
        false,
    )
}

pub fn string(literal: &str) -> Parser<'_, '_> {
    let is_punctuation = literal.chars().all(|c| c.is_ascii_punctuation());
    Rc::new(move |input, pos, state| {
        let range = pos..(pos + literal.len()).min(input.len());
        if input[range.clone()].eq_ignore_ascii_case(literal)
            && (is_punctuation
                || input[range.end..]
                    .chars()
                    .next()
                    .map_or(true, |c| !c.is_alphanumeric()))
        {
            state.events.push(Event::Token {
                range: range.clone(),
                trivia: false,
            });
            Ok(range.end)
        } else {
            if state.track_errors {
                state.errors.push(Event::Error {
                    descriptor: ParseErrorDescriptor::ExpectedLiteral(literal),
                    range,
                });
            }
            Err(ParseErr::Fail)
        }
    })
}

pub fn eof<'a: 'b, 'b>() -> Parser<'a, 'b> {
    trivia("$")
}

pub fn epsilon<'a: 'b, 'b>() -> Parser<'a, 'b> {
    trivia("")
}

pub fn seq<'a: 'b, 'b>(parsers: Vec<Parser<'a, 'b>>) -> Parser<'a, 'b> {
    Rc::new(move |input, pos, state| {
        let mut pos = pos;
        let mut cut = ParseErr::Fail;
        for parser in &parsers {
            let idx = state.events.len();
            match parser(input, pos, state) {
                Ok(new_pos) => {
                    pos = new_pos;
                }
                Err(ParseErr::Cut) => {
                    state.events.truncate(idx);
                    cut = ParseErr::Error;
                }
                Err(ParseErr::Error) => {
                    state.events.truncate(idx);
                    return Err(ParseErr::Error);
                }
                Err(ParseErr::Fail) => {
                    state.events.truncate(idx);
                    return Err(cut);
                }
            }
        }
        Ok(pos)
    })
}

pub fn star<'a: 'b, 'b>(parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    Rc::new(move |input, pos, state| {
        let mut pos = pos;
        loop {
            let idx = state.events.len();
            match parser(input, pos, state) {
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
    })
}

pub fn plus<'a: 'b, 'b>(parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    seq(vec![parser.clone(), star(parser)])
}

pub fn opt<'a: 'b, 'b>(parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    ord(vec![parser, epsilon()])
}

pub fn neg<'a: 'b, 'b>(parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    Rc::new(move |input, pos, state| {
        let idx = state.events.len();
        let err_idx = state.errors.len();
        let result = parser(input, pos, state);
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
    })
}

pub fn look<'a: 'b, 'b>(parser: Parser<'a, 'b>) -> Parser<'a, 'b> {
    neg(neg(parser))
}

pub fn ord<'a: 'b, 'b>(parsers: Vec<Parser<'a, 'b>>) -> Parser<'a, 'b> {
    Rc::new(move |input, pos, state| {
        let idx = state.events.len();
        for parser in &parsers {
            match parser(input, pos, state) {
                Ok(pos) => return Ok(pos),
                Err(ParseErr::Error) => {
                    state.events.truncate(idx);
                    return Err(ParseErr::Error);
                }
                Err(_) => {
                    state.events.truncate(idx);
                }
            }
        }
        Err(ParseErr::Fail)
    })
}

pub fn nt<'a: 'b, 'b>(rules: ParserGrammar<'a, 'b>, rule_name: &'b str) -> Parser<'a, 'b> {
    let cache = RefCell::<Option<Parser>>::default();
    Rc::new(move |input, pos, state| {
        if let Some(parser) = cache.borrow().as_deref() {
            return parser(input, pos, state);
        }
        match rules.borrow().get(rule_name) {
            Some(parser) => {
                cache.replace(Some(parser.clone()));
                parser(input, pos, state)
            }
            None => {
                unreachable!("unknown rule {}", rule_name)
            }
        }
    })
}

pub fn peg_meta_parser<'a: 'b, 'b>() -> Parser<'a, 'b> {
    let rules = ParserGrammar::default();

    let spacing = trivia("\\s*");
    let end_of_file = eof();

    let identifier = label(
        "identifier",
        seq(vec![
            re("\\b\\p{XID_START}\\p{XID_CONTINUE}*\\b"),
            spacing.clone(),
        ]),
    );
    let string_literal = label("string", seq(vec![re("'[^']*?'"), spacing.clone()]));
    let regex_literal = label(
        "regex",
        seq(vec![re("\"(?:[^\\\\\"]|\\\\.)*\""), spacing.clone()]),
    );

    let token = |s| seq(vec![label(s, string(s)), spacing.clone()]);
    let hide = |s| seq(vec![trivia(s), spacing.clone()]);

    let left_arrow = hide("<-");
    let open = hide("\\(");
    let close = hide("\\)");
    let question = token("?");
    let and = token("&");
    let not = token("!");
    let star_ = token("*");
    let plus_ = token("+");
    let slash = hide("/");
    let caret = token("^");

    let throw = seq(vec![
        label("%", re("\\%\\p{XID_START}\\p{XID_CONTINUE}*\\b")),
        spacing.clone(),
    ]);

    let primary = label(
        "primary",
        ord(vec![
            seq(vec![identifier.clone(), neg(left_arrow.clone())]),
            seq(vec![open, nt(rules.clone(), "expression"), close]),
            string_literal,
            regex_literal,
            throw,
            caret,
        ]),
    );
    let labeled = seq(vec![
        label("^", re("\\^\\p{XID_START}\\p{XID_CONTINUE}*\\b")),
        spacing.clone(),
    ]);
    let suffix = label(
        "suffix",
        seq(vec![
            primary,
            opt(ord(vec![question, star_, plus_, labeled])),
        ]),
    );
    let prefix = label("prefix", seq(vec![opt(ord(vec![and, not])), suffix]));
    let sequence = label("sequence", star(prefix));
    let expression = label(
        "expression",
        seq(vec![sequence.clone(), star(seq(vec![slash, sequence]))]),
    );

    rules.borrow_mut().insert("expression", expression.clone());

    let hidden_identifier = label(
        "hidden_identifier",
        seq(vec![
            re("<\\p{XID_START}\\p{XID_CONTINUE}*>"),
            spacing.clone(),
        ]),
    );
    let definition = label(
        "definition",
        seq(vec![
            ord(vec![hidden_identifier, identifier]),
            left_arrow,
            expression,
        ]),
    );

    let grammar = label("grammar", seq(vec![spacing, plus(definition), end_of_file]));

    grammar
}

pub fn build_cst_tree(events: Vec<Event>) -> Node {
    let mut idx = 0;
    let mut stack = vec![];
    while idx < events.len() {
        match events[idx] {
            Event::Open { label, .. } => {
                stack.push(Node::Branch {
                    id: label,
                    children: Vec::new(),
                });
            }
            Event::Close => {
                let node = stack.pop().expect("unbalanced tree");
                if let Some(Node::Branch { children, .. }) = stack.last_mut() {
                    children.push(node);
                } else {
                    assert!(idx + 1 == events.len());
                    return node;
                }
            }
            Event::Token {
                ref range,
                trivia: false,
            } => {
                if let Some(Node::Branch { children, .. }) = stack.last_mut() {
                    children.push(Node::Leaf {
                        range: range.clone(),
                    });
                } else {
                    unreachable!("invalid parent");
                };
            }
            Event::Token { trivia: true, .. } => {}
            Event::Error { .. } => {}
        }
        idx += 1;
    }
    unreachable!("invalid tree");
}

pub fn peg_cst_to_parser<'a: 'b, 'b>(
    src: &'a str,
    start_rule: &'a str,
    whitespace: &'a str,
    node: Node<'a>,
) -> Parser<'a, 'b> {
    use Node::*;

    struct WalkEnv<'a, 'b> {
        src: &'a str,
        whitespace: Parser<'a, 'b>,
        rules: ParserGrammar<'a, 'b>,
    }

    fn walk<'a: 'b, 'b>(env: &WalkEnv<'a, 'b>, node: &Node<'a>) -> Parser<'a, 'b> {
        match node {
            Branch {
                id: "grammar",
                children,
            } => {
                for c in children {
                    walk(env, c);
                }
                epsilon()
            }
            Branch {
                id: "definition",
                children,
            } => match children.as_slice() {
                [Branch {
                    id: "identifier",
                    children,
                }, expression] => match children.as_slice() {
                    [Leaf { range }] => {
                        let rule_name = &env.src[range.clone()];
                        let parser = label(rule_name, walk(env, expression));
                        env.rules.borrow_mut().insert(rule_name, parser);
                        epsilon()
                    }
                    _ => unreachable!("{:?}", node),
                },
                [Branch {
                    id: "hidden_identifier",
                    children,
                }, expression] => match children.as_slice() {
                    [Leaf { range }] => {
                        let rule_name = &env.src[(range.start + 1)..(range.end - 1)];
                        let parser = walk(env, expression);
                        env.rules.borrow_mut().insert(rule_name, parser);
                        epsilon()
                    }
                    _ => unreachable!("{:?}", node),
                },
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "expression",
                children,
            } => {
                if children.len() == 1 {
                    walk(env, children.first().unwrap())
                } else {
                    ord(children.iter().map(|c| walk(env, c)).collect())
                }
            }
            Branch {
                id: "sequence",
                children,
            } => {
                if children.len() == 1 {
                    walk(env, children.first().unwrap())
                } else {
                    seq(children.iter().map(|c| walk(env, c)).collect())
                }
            }
            Branch {
                id: "prefix",
                children,
            } => match children.as_slice() {
                [Branch { id: "!", .. }, node] => neg(walk(env, node)),
                [Branch { id: "&", .. }, node] => look(walk(env, node)),
                [node] => walk(env, node),
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "suffix",
                children,
            } => match children.as_slice() {
                [node, Branch { id: "?", .. }] => opt(walk(env, node)),
                [node, Branch { id: "*", .. }] => star(walk(env, node)),
                [node, Branch { id: "+", .. }] => plus(walk(env, node)),
                [node, Branch { id: "^", children }] => match children.as_slice() {
                    [Leaf { range }] => {
                        let labeled = &env.src[(range.start + 1)..range.end];
                        ord(vec![label(labeled, walk(env, node)), throw(labeled)])
                    }
                    _ => unreachable!("{:?}", node),
                },
                [node] => walk(env, node),
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "primary",
                children,
            } => match children.as_slice() {
                [Branch { id: "%", children }] => match children.as_slice() {
                    [Leaf { range }] => {
                        let labeled = &env.src[(range.start + 1)..range.end];
                        throw(labeled)
                    }
                    _ => unreachable!("{:?}", node),
                },
                [Branch { id: "^", .. }] => cut(),
                [node] => walk(env, node),
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "identifier",
                children,
            } => match children.as_slice() {
                [Leaf { range }] => nt(env.rules.clone(), &env.src[range.clone()]),
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "regex",
                children,
            } => match children.as_slice() {
                [Leaf { range }] => {
                    let literal = &env.src[(range.start + 1)..(range.end - 1)];
                    let pattern = literal.replace("\\\\", "\\");
                    let re = terminal(
                        Regex::new(&pattern).unwrap(),
                        ParseErrorDescriptor::ExpectedPattern(literal),
                        false,
                    );
                    seq(vec![re, env.whitespace.clone()])
                }
                _ => unreachable!("{:?}", node),
            },
            Branch {
                id: "string",
                children,
            } => match children.as_slice() {
                [Leaf { range }] => {
                    let literal = &env.src[(range.start + 1)..(range.end - 1)];
                    seq(vec![string(literal), env.whitespace.clone()])
                }
                _ => unreachable!("{:?}", node),
            },
            _ => unreachable!("{:?}", node),
        }
    }

    let rules = ParserGrammar::default();
    let whitespace = trivia(whitespace);
    let env = WalkEnv {
        src,
        whitespace: whitespace.clone(),
        rules,
    };

    walk(&env, &node);

    seq(vec![whitespace, nt(env.rules.clone(), start_rule)])
}

pub fn build_peg_parser<'a: 'b, 'b>(
    src: &'a str,
    start_rule: &'a str,
    whitespace: &'a str,
) -> Result<Parser<'a, 'b>, Vec<Event<'a>>> {
    let peg_meta_parser = peg_meta_parser();
    let mut state = ParseState::default();
    match peg_meta_parser(src, 0, &mut state) {
        Ok(_) => Ok(peg_cst_to_parser(
            src,
            start_rule,
            whitespace,
            build_cst_tree(state.events),
        )),
        _ => Err(state.errors),
    }
}

fn events_to_sexp_into(
    out: &mut String,
    src: &str,
    events: &[Event],
) -> Result<(), Box<dyn Error>> {
    use std::fmt::Write;

    for e in events {
        match e {
            Event::Open { label, .. } => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str("(:|");
                out.push_str(label);
                out.push('|');
            }
            Event::Token {
                ref range,
                trivia: false,
            } => {
                out.push_str(" (\"");
                out.push_str(&src[range.clone()]);
                out.push_str("\" ");
                write!(out, "{}", range.start)?;
                out.push(' ');
                write!(out, "{}", range.end)?;
                out.push(')');
            }
            Event::Token { trivia: true, .. } => {}
            Event::Close => out.push(')'),
            Event::Error { .. } => {}
        }
    }
    Ok(())
}

pub fn events_to_sexp(src: &str, events: &[Event]) -> Result<String, Box<dyn Error>> {
    let mut out = String::new();
    events_to_sexp_into(&mut out, src, events)?;
    Ok(out)
}

pub fn events_to_errors<'a>(events: &'a [Event<'a>]) -> Vec<ParseError<'a>> {
    let mut context = vec![];
    let mut errors = vec![];
    let mut max_error_pos = 0;
    for e in events {
        match e {
            Event::Open { label, pos } => {
                context.push((*label, *pos));
            }
            Event::Close => {
                context.pop().expect("unbalanced tree");
            }
            Event::Token { .. } => {}
            Event::Error {
                descriptor, range, ..
            } => {
                if range.start > max_error_pos {
                    max_error_pos = range.start;
                    errors.clear();
                }
                if range.start == max_error_pos {
                    errors.push(ParseError {
                        descriptor: descriptor.clone(),
                        range: range.clone(),
                        context: context.clone(),
                    });
                }
            }
        }
    }
    errors
}

pub fn parse_errors_to_string<'a>(
    filename: &'static str,
    src: &'a str,
    errors: &'a [ParseError<'a>],
) -> Result<String, Box<dyn Error>> {
    let start = errors.first().map(|e| e.range.start).unwrap_or(0);
    let punctuation = src
        .chars()
        .nth(start)
        .map_or(false, |c| c.is_ascii_punctuation());
    let end = start
        + src[start..]
            .chars()
            .enumerate()
            .take_while(|(_, c)| {
                if punctuation {
                    c.is_ascii_punctuation()
                } else {
                    c.is_alphanumeric()
                }
            })
            .map(|(idx, _)| idx + 1)
            .last()
            .unwrap_or(0);
    let range = start..end;

    let errors_by_context = errors.iter().fold(
        HashMap::<Vec<&'a str>, Vec<ParseError<'a>>>::new(),
        |mut acc, e| {
            let last_idx = if let ParseErrorDescriptor::ExpectedPattern(_) = e.descriptor {
                e.context.len() - 1
            } else {
                e.context.len()
            };
            let context = e.context[0..last_idx]
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>();
            acc.entry(context).or_default().push(e.clone());
            acc
        },
    );

    let mut target_sizes_by_context = HashMap::<Vec<&'a str>, usize>::new();
    for (context, errs) in errors_by_context.iter() {
        if errs.len() == 1 {
            target_sizes_by_context
                .entry(context[0..context.len() - 1].to_vec())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        } else {
            target_sizes_by_context
                .entry(context.to_vec())
                .and_modify(|c| *c += errs.len())
                .or_insert(errs.len());
        }
    }

    let errors_by_context = errors_by_context.iter().fold(
        HashMap::<Vec<&'a str>, Vec<ParseError<'a>>>::new(),
        |mut acc, (context, errs)| {
            let parent_path = &context[0..context.len() - 1];
            match target_sizes_by_context.get(parent_path) {
                Some(target_size) if *target_size > 1 && errs.len() == 1 => {
                    acc.entry(parent_path.to_vec())
                        .or_default()
                        .append(&mut errs.to_vec());
                }

                _ => {
                    acc.entry(context.to_vec())
                        .or_default()
                        .append(&mut errs.to_vec());
                }
            }
            acc
        },
    );

    let mut errors_sorted_by_context_len = errors_by_context.iter().collect::<Vec<_>>();
    errors_sorted_by_context_len
        .sort_by_key(|(context, _)| context.iter().map(|label| label.len()).collect::<Vec<_>>());
    errors_sorted_by_context_len.dedup();

    let mut report = ParseReport {
        kind: ParseReportKind::Error,
        location: (filename.to_string(), range.start),
        source: src.to_string(),
        ..ParseReport::default()
    };
    let found = &src[range.clone()].trim();
    let unexpected_message = format!(
        "unexpected {}",
        if found.is_empty() { "EOF" } else { found }
    );
    report.msg = Some(unexpected_message.clone());
    let mut label_sets = vec![];
    for (context, errs) in errors_sorted_by_context_len {
        let mut labels = vec![];
        for e in errs {
            if let ParseErrorDescriptor::Labeled(label) = e.descriptor {
                labels.push(ParseReportLabel {
                    span: (filename.to_string(), e.range.clone()),
                    msg: Some(label.to_string()),
                    color: Some(ParseReportColor::Yellow),
                    ..ParseReportLabel::default()
                });
            }
        }

        let mut expected = errs
            .iter()
            .flat_map(|e| match e.descriptor {
                ParseErrorDescriptor::ExpectedLiteral(literal) => {
                    if "," == literal {
                        vec!["',' (comma)".to_string()]
                    } else {
                        vec![literal.to_string()]
                    }
                }
                ParseErrorDescriptor::ExpectedPattern(_) => {
                    vec![e
                        .context
                        .last()
                        .map(|(label, _)| *label)
                        .unwrap_or("")
                        .to_string()]
                }
                ParseErrorDescriptor::Labeled(_) => vec![],
                ParseErrorDescriptor::Unexpected => vec![],
            })
            .collect::<Vec<_>>();
        expected.sort();
        expected.dedup();

        if let Some(e) = expected
            .iter()
            .find(|e| found.len() > 1 && e.starts_with(found))
        {
            report.help = Some(format!("did you mean {}?", e));
        }

        if expected.is_empty()
            && errs
                .iter()
                .any(|e| e.descriptor == ParseErrorDescriptor::Unexpected)
        {
            labels.push(ParseReportLabel {
                span: (filename.to_string(), range.clone()),
                msg: Some(unexpected_message.to_string()),
                color: Some(ParseReportColor::Red),
                ..ParseReportLabel::default()
            });
        }

        let expected_parts = expected.chunks(5);
        let expected_parts_len = expected_parts.len();
        for (idx, expected) in expected_parts.enumerate() {
            let message = format!(
                "{} {}{}{}",
                if idx == 0 { "expected" } else { " " },
                if idx == 0 && expected.len() > 1 {
                    "one of "
                } else {
                    ""
                },
                expected.join(", "),
                if idx == expected_parts_len - 1 {
                    ""
                } else {
                    ","
                }
            );

            labels.push(ParseReportLabel {
                span: (filename.to_string(), range.clone()),
                msg: Some(message.to_string()),
                color: Some(ParseReportColor::Red),
                ..ParseReportLabel::default()
            });
        }

        if let Some(label) = context.iter().last() {
            let msg = Some(format!(" in {}", label));
            labels.push(ParseReportLabel {
                span: (
                    filename.to_string(),
                    errs[0]
                        .context
                        .iter()
                        .find(|(e_label, _)| e_label == label)
                        .map(|(_, pos)| *pos)
                        .unwrap_or(0)..range.start,
                ),
                color: Some(ParseReportColor::Blue),
                msg,
                ..ParseReportLabel::default()
            });
        }
        if !label_sets.iter().any(|s| *s == labels) {
            label_sets.push(labels);
        }
    }
    for (idx, label) in label_sets.iter_mut().flatten().enumerate() {
        label.order = idx as i32;
        report.labels.push(label.clone());
    }

    parse_report_to_string(&report)
}

pub const SQL_PEG: &str = include_str!("sql.peg");

std::thread_local! {
    pub static SQL_CST_PARSER: Parser<'static, 'static> = build_peg_parser(SQL_PEG, "sql_stmt_list", "(\\s|--[^\n\r]*?)*").unwrap();
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default)]
pub enum ParseReportKind {
    #[default]
    Error,
    Warning,
    Advice,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default, Hash, Eq)]
pub enum ParseReportColor {
    #[default]
    Unset,
    Default,
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default, Hash, Eq)]
pub struct ParseReportLabel {
    span: (String, Range<usize>),
    msg: Option<String>,
    color: Option<ParseReportColor>,
    order: i32,
    priority: i32,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default)]
pub struct ParseReport {
    kind: ParseReportKind,
    code: Option<String>,
    msg: Option<String>,
    note: Option<String>,
    help: Option<String>,
    location: (String, usize),
    labels: Vec<ParseReportLabel>,
    source: String,
}

fn parse_report_to_string(report: &ParseReport) -> Result<String, Box<dyn Error>> {
    let mut buf = vec![];
    let (filename, pos) = &report.location;
    let report_kind = match &report.kind {
        ParseReportKind::Error => ReportKind::Error,
        ParseReportKind::Warning => ReportKind::Warning,
        ParseReportKind::Advice => ReportKind::Advice,
    };
    let mut builder = Report::build(report_kind, filename.clone(), *pos);

    if let Some(code) = &report.code {
        builder = builder.with_code(code);
    }
    if let Some(msg) = &report.msg {
        builder = builder.with_message(msg);
    }
    if let Some(note) = &report.note {
        builder = builder.with_note(note);
    }
    if let Some(help) = &report.help {
        builder = builder.with_help(help);
    }

    for label in &report.labels {
        let (filename, range) = &label.span;
        let label_color = match &label.color {
            Some(ParseReportColor::Unset) | None => Color::Unset,
            Some(ParseReportColor::Default) => Color::Default,
            Some(ParseReportColor::Black) => Color::Black,
            Some(ParseReportColor::Red) => Color::Red,
            Some(ParseReportColor::Green) => Color::Green,
            Some(ParseReportColor::Yellow) => Color::Yellow,
            Some(ParseReportColor::Blue) => Color::Blue,
            Some(ParseReportColor::Magenta) => Color::Magenta,
            Some(ParseReportColor::Cyan) => Color::Cyan,
            Some(ParseReportColor::White) => Color::White,
        };
        let mut report_label = Label::new((filename.clone(), range.clone()))
            .with_color(label_color)
            .with_order(label.order)
            .with_priority(label.priority);
        if let Some(msg) = &label.msg {
            report_label = report_label.with_message(msg);
        }
        builder = builder.with_label(report_label);
    }

    builder.finish().write(
        sources([(filename.clone(), report.source.clone())]),
        &mut buf,
    )?;

    Ok(String::from_utf8(buf)?)
}

pub fn json_error_report_to_string(report_json: &str) -> Result<String, Box<dyn Error>> {
    let report: ParseReport = serde_json::from_str(report_json)?;
    parse_report_to_string(&report)
}
