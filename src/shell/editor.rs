//! Rustyline integration; Tree-sitter assists editing, while QuickJS executes code.
use rustyline::{
    completion::{Completer, Pair},
    highlight::{CmdKind, Highlighter},
    hint::{Hint, Hinter},
    history::DefaultHistory,
    validate::{ValidationContext, ValidationResult, Validator},
    Cmd, CompletionType, ConditionalEventHandler, Config, Context, Editor, Event, EventContext,
    EventHandler, Helper, KeyEvent, RepeatCount,
};
use std::{
    borrow::Cow,
    ops::Range,
    sync::{Arc, Mutex},
};
use tree_sitter::{Node, Parser, Query, QueryCursor, StreamingIterator, Tree};

const MAX_EDIT_BYTES: usize = 256 * 1024;
const GLOBALS: &[&str] = &[
    "ObjectId",
    "ISODate",
    "Date",
    "NumberInt",
    "NumberLong",
    "NumberDouble",
    "NumberDecimal",
    "BinData",
    "db",
    "use",
    "print",
    "console",
];
const DATABASE_METHODS: &[&str] = &[
    "getName",
    "getSiblingDB",
    "getCollection",
    "runCommand",
    "listCollections",
];
const COLLECTION_METHODS: &[&str] = &[
    "find",
    "findOne",
    "insertOne",
    "insertMany",
    "updateOne",
    "updateMany",
    "replaceOne",
    "deleteOne",
    "deleteMany",
    "aggregate",
];

pub(super) struct Syntax {
    // Rustyline requires contextual key handlers to be Send + Sync.
    parser: Mutex<Parser>,
    query: Query,
    colors: Vec<&'static str>,
}

impl Syntax {
    pub fn new() -> eyre::Result<Self> {
        let language = tree_sitter_javascript::LANGUAGE.into();
        let mut parser = Parser::new();
        parser.set_language(&language)?;
        let query = Query::new(&language, tree_sitter_javascript::HIGHLIGHT_QUERY)?;
        let colors = query
            .capture_names()
            .iter()
            .map(|name| match name.split('.').next().unwrap_or("") {
                "string" => "\x1b[32m",
                "number" | "constant" => "\x1b[33m",
                "keyword" => "\x1b[35m",
                "function" | "constructor" => "\x1b[36m",
                "comment" => "\x1b[90m",
                "property" => "\x1b[34m",
                _ => "",
            })
            .collect();
        Ok(Self {
            parser: Mutex::new(parser),
            query,
            colors,
        })
    }

    fn parse(&self, source: &str) -> Option<Tree> {
        if source.len() > MAX_EDIT_BYTES {
            return None;
        }
        self.parser.lock().ok()?.parse(source, None)
    }

    pub fn highlight(&self, source: &str, position: Option<usize>) -> String {
        let Some(tree) = self.parse(source) else {
            return source.into();
        };
        let mut colors = vec![""; source.len()];
        let mut cursor = QueryCursor::new();
        let mut captures = cursor.captures(&self.query, tree.root_node(), source.as_bytes());
        while let Some((found, index)) = captures.next() {
            let capture = &found.captures[*index];
            let color = self.colors[capture.index as usize];
            if !color.is_empty() {
                colors[capture.node.byte_range()].fill(color);
            }
        }
        if let Some(pos) = position {
            for (left, right) in brackets(tree.root_node()).pairs {
                if [left.start, right.start]
                    .iter()
                    .any(|&p| pos == p || pos == p + 1)
                {
                    colors[left].fill("\x1b[7m");
                    colors[right].fill("\x1b[7m");
                    break;
                }
            }
        }
        let mut output = String::with_capacity(source.len());
        let mut current = "";
        for (offset, ch) in source.char_indices() {
            if colors[offset] != current {
                output.push_str("\x1b[0m");
                current = colors[offset];
                output.push_str(current);
            }
            output.push(ch);
        }
        if !current.is_empty() {
            output.push_str("\x1b[0m");
        }
        output
    }

    fn incomplete(&self, source: &str) -> bool {
        let trimmed = source.trim();
        if trimmed.is_empty() || is_shell_command(trimmed) {
            return false;
        }
        // A second Enter on a blank continuation line submits even invalid input.
        if source.contains('\n') && source.rsplit('\n').next().unwrap_or("").trim().is_empty() {
            return false;
        }
        let Some(tree) = self.parse(source) else {
            return false;
        };
        let root = tree.root_node();
        if !root.has_error() {
            return false;
        }
        let delimiters = brackets(root);
        if delimiters.mismatched {
            return false;
        }
        if !delimiters.open.is_empty() || !delimiters.template_quotes.is_multiple_of(2) {
            return true;
        }
        // Continue a missing expression/body at EOF, not every syntax error.
        let mut cursor = root.walk();
        loop {
            let node = cursor.node();
            if node.is_missing() && node.start_byte() >= source.trim_end().len() {
                return true;
            }
            // An unfinished block comment is parsed as an error/regex token.
            if node.is_error()
                && node.end_byte() == source.trim_end().len()
                && source[node.byte_range()].starts_with("/*")
                && !trimmed.ends_with("*/")
            {
                return true;
            }
            if cursor.goto_first_child() {
                continue;
            }
            while !cursor.goto_next_sibling() {
                if !cursor.goto_parent() {
                    return trimmed.ends_with([
                        '=', '+', '-', '*', '/', ',', '.', ':', '&', '|', '?', '!', '%', '^', '~',
                        '<', '>',
                    ]) || trimmed.ends_with("=>");
                }
            }
        }
    }

    fn newline(&self, line: &str, pos: usize) -> Option<String> {
        if pos != line.len() || !self.incomplete(line) {
            return None;
        }
        let last = line.rsplit('\n').next().unwrap_or(line);
        let indent = last.chars().take_while(|c| *c == ' ').count();
        let extra = usize::from(last.trim_end().ends_with(['{', '[', '('])) * 2;
        Some(format!("\n{}", " ".repeat((indent + extra).min(80))))
    }

    fn in_literal(&self, source: &str, pos: usize) -> bool {
        let Some(tree) = self.parse(source) else {
            return true;
        };
        let Some(mut node) = tree.root_node().descendant_for_byte_range(pos, pos) else {
            return false;
        };
        loop {
            if matches!(
                node.kind(),
                "string" | "string_fragment" | "comment" | "regex"
            ) {
                return true;
            }
            let Some(parent) = node.parent() else {
                return false;
            };
            node = parent;
        }
    }

    // Only inspect syntax: calling a receiver to discover its methods could write data.
    fn literal_collection_method(&self, source: &str) -> Option<(usize, String, String)> {
        let tree = self.parse(source)?;
        let pos = source.char_indices().next_back()?.0;
        let mut node = tree.root_node().descendant_for_byte_range(pos, pos)?;
        while node.kind() != "member_expression" {
            node = node.parent()?;
        }
        let call = node.child_by_field_name("object")?;
        let property = node.child_by_field_name("property")?;
        if call.kind() != "call_expression" || property.end_byte() != source.len() {
            return None;
        }
        let function = call.child_by_field_name("function")?;
        let arguments = call.child_by_field_name("arguments")?;
        if &source[function.byte_range()] != "db.getCollection"
            || arguments.named_child_count() != 1
            || arguments.named_child(0)?.kind() != "string"
            || arguments.has_error()
        {
            return None;
        }
        Some((
            node.start_byte(),
            source[call.byte_range()].into(),
            source[property.byte_range()].into(),
        ))
    }
}

#[derive(Default)]
struct Brackets {
    open: Vec<(char, Range<usize>)>,
    pairs: Vec<(Range<usize>, Range<usize>)>,
    mismatched: bool,
    template_quotes: usize,
}

fn brackets(root: Node<'_>) -> Brackets {
    let mut result = Brackets::default();
    let mut cursor = root.walk();
    loop {
        let node = cursor.node();
        let opaque = matches!(
            node.kind(),
            "string" | "comment" | "regex" | "string_fragment"
        );
        if !node.is_missing() {
            match node.kind() {
                "`" => result.template_quotes += 1,
                "(" | "[" | "{" => result
                    .open
                    .push((node.kind().chars().next().unwrap(), node.byte_range())),
                concat!("$", "{") => result.open.push(('{', node.byte_range())),
                ")" | "]" | "}" => {
                    let expected = match node.kind() {
                        ")" => '(',
                        "]" => '[',
                        _ => '{',
                    };
                    if let Some((open, range)) = result.open.pop() {
                        if open == expected {
                            result.pairs.push((range, node.byte_range()));
                        } else {
                            result.mismatched = true;
                        }
                    } else {
                        result.mismatched = true;
                    }
                }
                _ => {}
            }
        }
        if !opaque && cursor.goto_first_child() {
            continue;
        }
        while !cursor.goto_next_sibling() {
            if !cursor.goto_parent() {
                return result;
            }
        }
    }
}

fn is_shell_command(source: &str) -> bool {
    !source.contains('\n')
        && (source.starts_with("use ") || source.starts_with("show ") || source == "exit")
}

pub(super) struct ShellHelper {
    pub syntax: Arc<Syntax>,
    pub color: bool,
    database: String,
    collections: Vec<String>,
}
impl ShellHelper {
    fn new(syntax: Arc<Syntax>, color: bool) -> Self {
        Self {
            syntax,
            color,
            database: String::new(),
            collections: Vec::new(),
        }
    }
    pub fn refresh(&mut self, database: &str, collections: Option<Vec<String>>) {
        if database != self.database {
            self.collections.clear();
        }
        self.database = database.into();
        if let Some(mut names) = collections {
            names.sort();
            names.dedup();
            names.truncate(1000);
            self.collections = names;
        }
    }
}
impl Helper for ShellHelper {}
impl Validator for ShellHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        Ok(if self.syntax.incomplete(ctx.input()) {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Valid(None)
        })
    }
}
pub(super) struct ContinuationHint;
impl Hint for ContinuationHint {
    fn display(&self) -> &str {
        "  …"
    }
    fn completion(&self) -> Option<&str> {
        None
    }
}
impl Hinter for ShellHelper {
    type Hint = ContinuationHint;
    fn hint(&self, line: &str, pos: usize, _: &Context<'_>) -> Option<Self::Hint> {
        (pos == line.len() && self.syntax.incomplete(line)).then_some(ContinuationHint)
    }
}
impl Highlighter for ShellHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        if self.color {
            Cow::Owned(self.syntax.highlight(line, Some(pos)))
        } else {
            Cow::Borrowed(line)
        }
    }
    fn highlight_char(&self, _: &str, _: usize, _: CmdKind) -> bool {
        self.color
    }
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if self.color {
            Cow::Owned(format!("\x1b[90m{hint}\x1b[0m"))
        } else {
            Cow::Borrowed(hint)
        }
    }
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().enumerate().all(|(i, c)| {
            c.is_ascii_alphabetic() || c == b'_' || c == b'$' || (i > 0 && c.is_ascii_digit())
        })
}
impl Completer for ShellHelper {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];
        let mut start = prefix
            .char_indices()
            .rev()
            .find(|(_, c)| !(c.is_ascii_alphanumeric() || "_$.".contains(*c)))
            .map_or(0, |(i, c)| i + c.len_utf8());
        let word = &prefix[start..];
        if self.syntax.in_literal(line, pos.saturating_sub(1)) {
            return Ok((pos, Vec::new()));
        }
        let mut options: Vec<String> = Vec::new();
        if let Some((offset, receiver, method)) = self.syntax.literal_collection_method(prefix) {
            start = offset;
            options.extend(
                COLLECTION_METHODS
                    .iter()
                    .filter(|name| name.starts_with(&method))
                    .map(|name| format!("{receiver}.{name}")),
            );
        } else if let Some(tail) = word.strip_prefix("db.") {
            if let Some((collection, method)) = tail.split_once('.') {
                if identifier(collection) && !method.contains('.') {
                    options.extend(
                        COLLECTION_METHODS
                            .iter()
                            .filter(|name| name.starts_with(method))
                            .map(|name| format!("db.{collection}.{name}")),
                    );
                }
            } else {
                options.extend(
                    DATABASE_METHODS
                        .iter()
                        .filter(|name| name.starts_with(tail))
                        .map(|name| format!("db.{name}")),
                );
                options.extend(
                    self.collections
                        .iter()
                        .filter(|name| name.starts_with(tail))
                        .map(|name| {
                            if identifier(name)
                                && !DATABASE_METHODS.contains(&name.as_str())
                                && ![
                                    "name",
                                    "constructor",
                                    "__proto__",
                                    "toString",
                                    "valueOf",
                                    "hasOwnProperty",
                                    "isPrototypeOf",
                                    "propertyIsEnumerable",
                                    "toLocaleString",
                                    "__defineGetter__",
                                    "__defineSetter__",
                                    "__lookupGetter__",
                                    "__lookupSetter__",
                                ]
                                .contains(&name.as_str())
                            {
                                format!("db.{name}")
                            } else {
                                format!(
                                    "db.getCollection({})",
                                    serde_json::to_string(name).unwrap()
                                )
                            }
                        }),
                );
            }
        } else if !word.is_empty() && !word.contains('.') {
            options.extend(
                GLOBALS
                    .iter()
                    .filter(|name| name.starts_with(word))
                    .map(|name| (*name).into()),
            );
        }
        options.sort();
        options.dedup();
        Ok((
            start,
            options
                .into_iter()
                .map(|value| Pair {
                    display: value.clone(),
                    replacement: value,
                })
                .collect(),
        ))
    }
}
struct Enter(Arc<Syntax>);
impl ConditionalEventHandler for Enter {
    fn handle(&self, _: &Event, _: RepeatCount, _: bool, ctx: &EventContext<'_>) -> Option<Cmd> {
        self.0
            .newline(ctx.line(), ctx.pos())
            .map(|text| Cmd::Insert(1, text))
    }
}
pub(super) fn editor(
    syntax: Arc<Syntax>,
    color: bool,
) -> rustyline::Result<Editor<ShellHelper, DefaultHistory>> {
    let config = Config::builder()
        .completion_type(CompletionType::List)
        .build();
    let mut editor = Editor::with_config(config)?;
    editor.set_helper(Some(ShellHelper::new(syntax.clone(), color)));
    editor.bind_sequence(
        KeyEvent::from('\r'),
        EventHandler::Conditional(Box::new(Enter(syntax.clone()))),
    );
    editor.bind_sequence(
        KeyEvent::ctrl('J'),
        EventHandler::Conditional(Box::new(Enter(syntax))),
    );
    Ok(editor)
}

#[cfg(test)]
mod tests;
