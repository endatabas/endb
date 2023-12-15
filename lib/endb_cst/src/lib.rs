use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::ops::Range;

use ariadne::{sources, Color, Label, Report, ReportKind};

pub mod sql;

#[cfg(test)]
mod tests;

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
        pos: u32,
        hide: bool,
    },
    Close {
        open_idx: u32,
        hide: bool,
    },
    Literal {
        literal: &'a str,
        range: Range<u32>,
    },
    Pattern {
        range: Range<u32>,
    },
    Error {
        range: Range<u32>,
        descriptor: ParseErrorDescriptor<'a>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub struct ParseError<'a> {
    descriptor: ParseErrorDescriptor<'a>,
    range: Range<u32>,
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
}

type ParseContextEntry<'a> = (&'a str, u32);
type ParseContext<'a> = Vec<ParseContextEntry<'a>>;
pub type ParseResult = Result<u32, ParseErr>;

fn events_to_sexp_into(
    out: &mut String,
    src: &str,
    events: &[Event],
) -> Result<(), Box<dyn Error>> {
    use std::fmt::Write;

    for e in events {
        match e {
            Event::Open {
                label, hide: false, ..
            } => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str("(:|");
                out.push_str(label);
                out.push('|');
            }
            Event::Open { hide: true, .. } => {}
            Event::Pattern { ref range } | Event::Literal { ref range, .. } => {
                out.push_str(" (\"");
                out.push_str(
                    &src[(range.start as usize)..(range.end as usize)].replace('"', "\\\""),
                );
                out.push_str("\" ");
                write!(out, "{}", range.start)?;
                out.push(' ');
                write!(out, "{}", range.end)?;
                out.push(')');
            }
            Event::Close { hide: false, .. } => out.push(')'),
            Event::Close { hide: true, .. } => {}
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
            Event::Open {
                hide: false,
                label,
                pos,
                ..
            } => {
                context.push((*label, *pos));
            }
            Event::Close { hide: false, .. } => {
                context.pop().expect("unbalanced tree");
            }
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
            _ => {}
        }
    }
    errors
}

pub fn parse_errors_to_string<'a>(
    filename: &'static str,
    src: &'a str,
    errors: &'a [ParseError<'a>],
) -> Result<String, Box<dyn Error>> {
    let start = errors.first().map(|e| e.range.start).unwrap_or(0) as usize;
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
                    span: (
                        filename.to_string(),
                        (e.range.start as usize)..(e.range.end as usize),
                    ),
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
            .find(|e| found.len() > 1 && e.starts_with(&found.to_uppercase()))
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
                    (errs[0]
                        .context
                        .iter()
                        .find(|(e_label, _)| e_label == label)
                        .map(|(_, pos)| *pos)
                        .unwrap_or(0) as usize)..range.start,
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
