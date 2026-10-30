//! Readers for values stored in serialized forms, as RedisInsight's formatters: MessagePack,
//! PHP `serialize`, Python pickle, Java serialization, and float vectors. They only read the
//! structure: no class is looked up, imported or instantiated and no code runs, so a hostile
//! value can at worst be shown oddly. Depth, size and output are bounded.

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decoder {
    Msgpack,
    Php,
    Pickle,
    Java,
    Float32Vector,
    Float64Vector,
}

/// Nesting deeper than this is not read.
const MAX_DEPTH: usize = 64;
/// Values read from one input; past this the input is not read further.
const MAX_NODES: usize = 200_000;
/// Text shown for one value; past this it is cut and says so.
const MAX_OUTPUT: usize = 1_000_000;
/// Entries all containers of one input may hold together, references included, so a few memo
/// references cannot stand for unbounded work.
const MAX_ENTRIES: usize = 1_000_000;
/// Values visited while showing one input; a value shared many times is shown each time, so
/// this bounds the work, not just the text.
const MAX_VISITS: usize = 1_000_000;

type Id = usize;

/// One value read from the input. Containers refer to their items by id, so a value shared or
/// referring back to itself (pickle memo, PHP `r:`, Java handles) is read once.
#[derive(Clone, Debug)]
enum Node {
    Null,
    Bool(bool),
    Int(String),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    List(Vec<Id>),
    Tuple(Vec<Id>),
    Set(Vec<Id>),
    Map(Vec<(Id, Id)>),
    /// A class instance as recorded: its class name and what was stored for it. Never built.
    Object {
        class: String,
        fields: Vec<(String, Id)>,
    },
    /// A name the value refers to (a pickle global, a Java class or enum constant).
    Name(String),
    /// Raw data a class wrote for itself (Java `writeObject`, PHP `C:`).
    Custom(String, Vec<u8>),
}

#[derive(Default)]
struct Arena {
    nodes: Vec<Node>,
    entries: usize,
}

impl Arena {
    fn add(&mut self, node: Node) -> Result<Id, String> {
        if self.nodes.len() >= MAX_NODES {
            return Err(format!("more than {MAX_NODES} values; not read further"));
        }
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Counts entries added to containers against `MAX_ENTRIES`.
    fn entries(&mut self, count: usize) -> Result<(), String> {
        self.entries = self.entries.saturating_add(count);
        if self.entries > MAX_ENTRIES {
            return Err(format!("more than {MAX_ENTRIES} entries; not read further"));
        }
        Ok(())
    }

    fn get_mut(&mut self, id: Id) -> Result<&mut Node, String> {
        self.nodes
            .get_mut(id)
            .ok_or_else(|| "a reference to a value that does not exist".to_string())
    }
}

/// The value as indented text, or why it could not be read.
pub fn decode(decoder: Decoder, bytes: &[u8]) -> Result<String, String> {
    let mut arena = Arena::default();
    let root = match decoder {
        Decoder::Msgpack => msgpack(&mut arena, bytes)?,
        Decoder::Php => Php {
            bytes,
            at: 0,
            arena: &mut arena,
            values: Vec::new(),
        }
        .read_all()?,
        Decoder::Pickle => pickle(&mut arena, bytes)?,
        Decoder::Java => Java {
            bytes,
            at: 0,
            arena: &mut arena,
            handles: Vec::new(),
        }
        .read_all()?,
        Decoder::Float32Vector => return floats(bytes, 4),
        Decoder::Float64Vector => return floats(bytes, 8),
    };
    let mut out = Output::default();
    render(&arena, root, 0, &mut BTreeSet::new(), &mut out);
    Ok(out.finish())
}

fn floats(bytes: &[u8], width: usize) -> Result<String, String> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(width) {
        return Err(format!(
            "{} bytes is not a whole number of {}-bit floats",
            bytes.len(),
            width * 8
        ));
    }
    let mut out = Output::default();
    out.push("[");
    for (index, chunk) in bytes.chunks_exact(width).enumerate() {
        if out.cut {
            break;
        }
        if index > 0 {
            out.push(", ");
        }
        let value = match chunk {
            [a, b, c, d] => f64::from(f32::from_le_bytes([*a, *b, *c, *d])),
            [a, b, c, d, e, f, g, h] => f64::from_le_bytes([*a, *b, *c, *d, *e, *f, *g, *h]),
            _ => f64::NAN,
        };
        out.push(&format_float(value));
    }
    out.push("]");
    Ok(out.finish())
}

fn format_float(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

#[derive(Default)]
struct Output {
    text: String,
    cut: bool,
    visits: usize,
}

impl Output {
    fn push(&mut self, text: &str) {
        if self.cut {
            return;
        }
        if self.text.len() + text.len() > MAX_OUTPUT {
            self.cut = true;
            return;
        }
        self.text.push_str(text);
    }

    fn finish(mut self) -> String {
        if self.cut {
            self.text
                .push_str(&format!("\n… cut at {} KB of text", MAX_OUTPUT / 1000));
        }
        self.text
    }
}

fn quoted(text: &str) -> String {
    serde_json::Value::String(text.to_string()).to_string()
}

fn render(arena: &Arena, id: Id, depth: usize, path: &mut BTreeSet<Id>, out: &mut Output) {
    out.visits += 1;
    if out.cut || out.visits > MAX_VISITS {
        out.cut = true;
        return;
    }
    let Some(node) = arena.nodes.get(id) else {
        out.push("<missing>");
        return;
    };
    if path.contains(&id) {
        out.push("<refers back to an enclosing value>");
        return;
    }
    if depth >= MAX_DEPTH {
        out.push("<nested too deeply>");
        return;
    }
    let indent = "  ".repeat(depth + 1);
    let close = "  ".repeat(depth);
    path.insert(id);
    let mut items = |open: &str, ids: &[Id], end: &str, out: &mut Output| {
        if ids.is_empty() {
            out.push(open);
            out.push(end);
            return;
        }
        out.push(open);
        out.push("\n");
        for item in ids {
            out.push(&indent);
            render(arena, *item, depth + 1, path, out);
            out.push(",\n");
        }
        out.push(&close);
        out.push(end);
    };
    match node {
        Node::Null => out.push("null"),
        Node::Bool(flag) => out.push(if *flag { "true" } else { "false" }),
        Node::Int(number) => out.push(number),
        Node::Float(value) => out.push(&format_float(*value)),
        Node::Text(text) => out.push(&quoted(text)),
        Node::Bytes(bytes) => out.push(&format!("b{}", crate::redis::quote_bytes(bytes))),
        Node::List(ids) => items("[", ids, "]", out),
        Node::Tuple(ids) => items("(", ids, ")", out),
        Node::Set(ids) => items("{", ids, "}", out),
        Node::Name(name) => out.push(name),
        Node::Custom(label, bytes) => out.push(&format!(
            "<{label}: {} bytes {}>",
            bytes.len(),
            crate::redis::quote_bytes(bytes)
        )),
        Node::Map(pairs) => {
            if pairs.is_empty() {
                out.push("{}");
            } else {
                out.push("{\n");
                for (key, value) in pairs {
                    out.push(&indent);
                    render(arena, *key, depth + 1, path, out);
                    out.push(": ");
                    render(arena, *value, depth + 1, path, out);
                    out.push(",\n");
                }
                out.push(&close);
                out.push("}");
            }
        }
        Node::Object { class, fields } => {
            out.push(class);
            if fields.is_empty() {
                out.push(" {}");
            } else {
                out.push(" {\n");
                for (name, value) in fields {
                    out.push(&indent);
                    out.push(name);
                    out.push(": ");
                    render(arena, *value, depth + 1, path, out);
                    out.push(",\n");
                }
                out.push(&close);
                out.push("}");
            }
        }
    }
    path.remove(&id);
}

/// MessagePack, read by its specification. A reserved byte (0xc1) is refused rather than shown
/// as nil.
fn msgpack(arena: &mut Arena, bytes: &[u8]) -> Result<Id, String> {
    let mut at = 0;
    let root = msgpack_value(arena, bytes, &mut at, 0)?;
    if at != bytes.len() {
        return Err(format!(
            "not one MessagePack value: {} bytes follow it",
            bytes.len() - at
        ));
    }
    Ok(root)
}

fn msgpack_value(
    arena: &mut Arena,
    bytes: &[u8],
    at: &mut usize,
    depth: usize,
) -> Result<Id, String> {
    if depth > MAX_DEPTH {
        return Err("not MessagePack that Vilo reads: nested too deeply".into());
    }
    let fail = |at: usize, what: &str| Err(format!("not MessagePack: {what} at byte {at}"));
    let mut take = |count: usize| -> Result<&[u8], String> {
        let end = at.checked_add(count).filter(|end| *end <= bytes.len());
        match end {
            Some(end) => {
                let slice = &bytes[*at..end];
                *at = end;
                Ok(slice)
            }
            None => Err(format!("not MessagePack: ends early at byte {}", *at)),
        }
    };
    let marker = take(1)?[0];
    let unsigned = |slice: &[u8]| {
        slice
            .iter()
            .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte))
    };
    let mut take_length = |width: usize| -> Result<usize, String> {
        let length = unsigned(take(width)?);
        usize::try_from(length).map_err(|_| "not MessagePack: a length too large".to_string())
    };
    let node = match marker {
        0x00..=0x7f => Node::Int(marker.to_string()),
        0xe0..=0xff => Node::Int((marker as i8).to_string()),
        0xc0 => Node::Null,
        0xc2 => Node::Bool(false),
        0xc3 => Node::Bool(true),
        0xcc..=0xcf => {
            let width = 1 << (marker - 0xcc);
            Node::Int(unsigned(take(width)?).to_string())
        }
        0xd0..=0xd3 => {
            let width = 1 << (marker - 0xd0);
            let raw = unsigned(take(width)?);
            let shift = 64 - width * 8;
            Node::Int((((raw << shift) as i64) >> shift).to_string())
        }
        0xca => Node::Float(f64::from(f32::from_bits(unsigned(take(4)?) as u32))),
        0xcb => Node::Float(f64::from_bits(unsigned(take(8)?))),
        0xa0..=0xbf | 0xd9..=0xdb => {
            let length = match marker {
                0xd9 => take_length(1)?,
                0xda => take_length(2)?,
                0xdb => take_length(4)?,
                _ => usize::from(marker - 0xa0),
            };
            let text = take(length)?;
            match std::str::from_utf8(text) {
                Ok(text) => Node::Text(text.to_string()),
                Err(_) => Node::Bytes(text.to_vec()),
            }
        }
        0xc4..=0xc6 => {
            let length = take_length(1 << (marker - 0xc4))?;
            Node::Bytes(take(length)?.to_vec())
        }
        0xc7..=0xc9 | 0xd4..=0xd8 => {
            let length = match marker {
                0xc7 => take_length(1)?,
                0xc8 => take_length(2)?,
                0xc9 => take_length(4)?,
                _ => 1 << (marker - 0xd4),
            };
            let kind = take(1)?[0] as i8;
            Node::Custom(format!("extension {kind}"), take(length)?.to_vec())
        }
        0x90..=0x9f | 0xdc | 0xdd => {
            let length = match marker {
                0xdc => take_length(2)?,
                0xdd => take_length(4)?,
                _ => usize::from(marker - 0x90),
            };
            if length > MAX_NODES {
                return fail(*at, "an array too long to read");
            }
            let mut items = Vec::with_capacity(length.min(1024));
            for _ in 0..length {
                items.push(msgpack_value(arena, bytes, at, depth + 1)?);
            }
            Node::List(items)
        }
        0x80..=0x8f | 0xde | 0xdf => {
            let length = match marker {
                0xde => take_length(2)?,
                0xdf => take_length(4)?,
                _ => usize::from(marker - 0x80),
            };
            if length > MAX_NODES {
                return fail(*at, "a map too long to read");
            }
            let mut pairs = Vec::with_capacity(length.min(1024));
            for _ in 0..length {
                let key = msgpack_value(arena, bytes, at, depth + 1)?;
                let value = msgpack_value(arena, bytes, at, depth + 1)?;
                pairs.push((key, value));
            }
            Node::Map(pairs)
        }
        0xc1 => return fail(*at - 1, "the reserved byte 0xc1"),
    };
    arena.add(node)
}

/// PHP `serialize()` output. Objects are shown with their class name and properties; protected
/// and private property names are marked as such.
struct Php<'a> {
    bytes: &'a [u8],
    at: usize,
    arena: &'a mut Arena,
    /// Every value in order, for `r:`/`R:` references (numbered from 1).
    values: Vec<Id>,
}

impl Php<'_> {
    fn read_all(mut self) -> Result<Id, String> {
        let root = self.value(0)?;
        if self.at != self.bytes.len() {
            return Err(format!(
                "not PHP serialized data: {} bytes follow the value",
                self.bytes.len() - self.at
            ));
        }
        Ok(root)
    }

    fn error<T>(&self, what: &str) -> Result<T, String> {
        Err(format!(
            "not PHP serialized data: {what} at byte {}",
            self.at
        ))
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        if self.bytes.get(self.at) == Some(&byte) {
            self.at += 1;
            Ok(())
        } else {
            self.error(&format!("expected '{}'", byte as char))
        }
    }

    fn until(&mut self, end: u8) -> Result<&[u8], String> {
        let start = self.at;
        let Some(length) = self.bytes[start..].iter().position(|byte| *byte == end) else {
            return self.error(&format!("missing '{}'", end as char));
        };
        self.at = start + length + 1;
        Ok(&self.bytes[start..start + length])
    }

    fn number(&mut self, end: u8) -> Result<String, String> {
        let text = String::from_utf8_lossy(self.until(end)?).to_string();
        Ok(text)
    }

    fn count(&mut self, end: u8) -> Result<usize, String> {
        let text = self.number(end)?;
        text.parse()
            .or_else(|_| self.error(&format!("bad length {text}")))
    }

    /// `LEN:"…"` with exactly LEN bytes between the quotes.
    fn sized(&mut self) -> Result<Vec<u8>, String> {
        let length = self.count(b':')?;
        self.expect(b'"')?;
        let Some(text) = self
            .at
            .checked_add(length)
            .and_then(|end| self.bytes.get(self.at..end))
        else {
            return self.error("a string runs past the end");
        };
        let text = text.to_vec();
        self.at += length;
        self.expect(b'"')?;
        Ok(text)
    }

    fn remember(&mut self, node: Node) -> Result<Id, String> {
        let id = self.arena.add(node)?;
        self.values.push(id);
        Ok(id)
    }

    fn value(&mut self, depth: usize) -> Result<Id, String> {
        if depth > MAX_DEPTH {
            return self.error("nested too deeply");
        }
        let Some(&kind) = self.bytes.get(self.at) else {
            return self.error("ends early");
        };
        self.at += 1;
        if kind == b'N' {
            self.expect(b';')?;
            return self.remember(Node::Null);
        }
        self.expect(b':')?;
        match kind {
            b'b' => {
                let flag = self.number(b';')? == "1";
                self.remember(Node::Bool(flag))
            }
            b'i' => {
                let number = self.number(b';')?;
                self.remember(Node::Int(number))
            }
            b'd' => {
                let text = self.number(b';')?;
                let value = match text.as_str() {
                    "INF" => f64::INFINITY,
                    "-INF" => f64::NEG_INFINITY,
                    "NAN" => f64::NAN,
                    text => text.parse().or_else(|_| self.error("bad float"))?,
                };
                self.remember(Node::Float(value))
            }
            b's' => {
                let text = self.sized()?;
                self.expect(b';')?;
                self.remember(match String::from_utf8(text) {
                    Ok(text) => Node::Text(text),
                    Err(error) => Node::Bytes(error.into_bytes()),
                })
            }
            b'E' => {
                let text = self.sized()?;
                self.expect(b';')?;
                self.remember(Node::Name(String::from_utf8_lossy(&text).to_string()))
            }
            b'r' | b'R' => {
                let index = self.count(b';')?;
                let Some(&target) = index
                    .checked_sub(1)
                    .and_then(|index| self.values.get(index))
                else {
                    return self.error("a reference to a value that does not exist");
                };
                // A reference is not a value of its own for later references' numbering, but
                // `r:` counts as one in PHP; both are kept as the value they point at.
                if kind == b'r' {
                    self.values.push(target);
                }
                Ok(target)
            }
            b'a' => {
                let length = self.count(b':')?;
                self.expect(b'{')?;
                let id = self.remember(Node::Map(Vec::new()))?;
                let mut pairs = Vec::new();
                for _ in 0..length {
                    let key = self.key()?;
                    let value = self.value(depth + 1)?;
                    pairs.push((key, value));
                }
                self.expect(b'}')?;
                *self.arena.get_mut(id)? = Node::Map(pairs);
                Ok(id)
            }
            b'O' => {
                let class = String::from_utf8_lossy(&self.sized()?).to_string();
                self.expect(b':')?;
                let length = self.count(b':')?;
                self.expect(b'{')?;
                let id = self.remember(Node::Object {
                    class: class.clone(),
                    fields: Vec::new(),
                })?;
                let mut fields = Vec::new();
                for _ in 0..length {
                    let name = self.property_name()?;
                    let value = self.value(depth + 1)?;
                    fields.push((name, value));
                }
                self.expect(b'}')?;
                *self.arena.get_mut(id)? = Node::Object { class, fields };
                Ok(id)
            }
            b'C' => {
                let class = String::from_utf8_lossy(&self.sized()?).to_string();
                self.expect(b':')?;
                let length = self.count(b':')?;
                self.expect(b'{')?;
                let Some(data) = self
                    .at
                    .checked_add(length)
                    .and_then(|end| self.bytes.get(self.at..end))
                else {
                    return self.error("custom data runs past the end");
                };
                let data = data.to_vec();
                self.at += length;
                self.expect(b'}')?;
                self.remember(Node::Custom(format!("{class} custom data"), data))
            }
            other => self.error(&format!("unknown type '{}'", other as char)),
        }
    }

    /// Array keys are integers or strings and are not numbered for references.
    fn key(&mut self) -> Result<Id, String> {
        match self.bytes.get(self.at) {
            Some(b'i') => {
                self.at += 1;
                self.expect(b':')?;
                let number = self.number(b';')?;
                self.arena.add(Node::Int(number))
            }
            Some(b's') => {
                self.at += 1;
                self.expect(b':')?;
                let text = self.sized()?;
                self.expect(b';')?;
                self.arena
                    .add(Node::Text(String::from_utf8_lossy(&text).to_string()))
            }
            _ => self.error("an array key must be an integer or a string"),
        }
    }

    /// `\0*\0name` is protected, `\0Class\0name` private to Class.
    fn property_name(&mut self) -> Result<String, String> {
        if self.bytes.get(self.at) != Some(&b's') {
            return self.error("a property name must be a string");
        }
        self.at += 1;
        self.expect(b':')?;
        let name = self.sized()?;
        self.expect(b';')?;
        let name = String::from_utf8_lossy(&name).to_string();
        Ok(
            match name
                .strip_prefix('\0')
                .and_then(|rest| rest.split_once('\0'))
            {
                Some(("*", property)) => format!("{property} (protected)"),
                Some((class, property)) => format!("{property} (private to {class})"),
                None => name,
            },
        )
    }
}

/// Python pickle, read opcode by opcode. Globals are shown by name and calls as
/// `name(arguments)`; nothing is imported or called.
fn pickle(arena: &mut Arena, bytes: &[u8]) -> Result<Id, String> {
    enum Slot {
        Value(Id),
        Mark,
    }
    let mut stack: Vec<Slot> = Vec::new();
    let mut memo: std::collections::HashMap<u64, Id> = Default::default();
    let mut at = 0;
    let fail = |at: usize, what: &str| Err(format!("not a pickle: {what} at byte {at}"));
    macro_rules! take {
        ($count:expr) => {{
            let count: usize = $count;
            match bytes.get(at..at.saturating_add(count)) {
                Some(slice) if at.checked_add(count).is_some() => {
                    at += count;
                    slice
                }
                _ => return fail(at, "ends early"),
            }
        }};
    }
    macro_rules! line {
        () => {{
            let Some(length) = bytes
                .get(at..)
                .and_then(|rest| rest.iter().position(|b| *b == b'\n'))
            else {
                return fail(at, "a line without its end");
            };
            let text = String::from_utf8_lossy(&bytes[at..at + length]).to_string();
            at += length + 1;
            text
        }};
    }
    let pop = |stack: &mut Vec<Slot>, at: usize| -> Result<Id, String> {
        match stack.pop() {
            Some(Slot::Value(id)) => Ok(id),
            _ => Err(format!("not a pickle: the stack is empty at byte {at}")),
        }
    };
    let pop_mark = |stack: &mut Vec<Slot>, at: usize| -> Result<Vec<Id>, String> {
        let mut items = Vec::new();
        loop {
            match stack.pop() {
                Some(Slot::Value(id)) => items.push(id),
                Some(Slot::Mark) => break,
                None => return Err(format!("not a pickle: no mark at byte {at}")),
            }
        }
        items.reverse();
        Ok(items)
    };
    let top = |stack: &[Slot], at: usize| -> Result<Id, String> {
        match stack.last() {
            Some(Slot::Value(id)) => Ok(*id),
            _ => Err(format!("not a pickle: nothing to change at byte {at}")),
        }
    };
    let name_of = |arena: &Arena, id: Id| -> String {
        match arena.nodes.get(id) {
            Some(Node::Name(name)) => name.clone(),
            Some(Node::Text(text)) => text.clone(),
            _ => "<callable>".to_string(),
        }
    };
    let small = |bytes: &[u8]| -> String {
        // Little-endian two's complement of any length.
        let negative = bytes.last().is_some_and(|byte| byte & 0x80 != 0);
        let mut value: i128 = if negative { -1 } else { 0 };
        if bytes.len() <= 15 {
            for byte in bytes.iter().rev() {
                value = (value << 8) | i128::from(*byte);
            }
            value.to_string()
        } else {
            format!("<{}-byte integer>", bytes.len())
        }
    };
    loop {
        if stack.len() > MAX_NODES {
            return fail(at, "too many values");
        }
        let opcode = take!(1)[0];
        let push = |arena: &mut Arena, stack: &mut Vec<Slot>, node: Node| -> Result<(), String> {
            stack.push(Slot::Value(arena.add(node)?));
            Ok(())
        };
        match opcode {
            0x80 => {
                take!(1);
            }
            0x95 => {
                take!(8);
            }
            b'.' => return pop(&mut stack, at),
            b'(' => stack.push(Slot::Mark),
            b'0' => {
                stack.pop();
            }
            b'1' => {
                pop_mark(&mut stack, at)?;
            }
            b'2' => {
                let id = top(&stack, at)?;
                stack.push(Slot::Value(id));
            }
            b'N' => push(arena, &mut stack, Node::Null)?,
            0x88 => push(arena, &mut stack, Node::Bool(true))?,
            0x89 => push(arena, &mut stack, Node::Bool(false))?,
            b'I' => {
                let text = line!();
                let node = match text.as_str() {
                    "00" => Node::Bool(false),
                    "01" => Node::Bool(true),
                    _ => Node::Int(text),
                };
                push(arena, &mut stack, node)?
            }
            b'L' => {
                let text = line!();
                push(
                    arena,
                    &mut stack,
                    Node::Int(text.trim_end_matches('L').to_string()),
                )?
            }
            b'J' => {
                let value = i32::from_le_bytes(take!(4).try_into().map_err(|_| "ends early")?);
                push(arena, &mut stack, Node::Int(value.to_string()))?
            }
            b'K' => {
                let value = take!(1)[0];
                push(arena, &mut stack, Node::Int(value.to_string()))?
            }
            b'M' => {
                let value = u16::from_le_bytes(take!(2).try_into().map_err(|_| "ends early")?);
                push(arena, &mut stack, Node::Int(value.to_string()))?
            }
            0x8a => {
                let length = usize::from(take!(1)[0]);
                let digits = small(take!(length));
                push(arena, &mut stack, Node::Int(digits))?
            }
            0x8b => {
                let length = u32::from_le_bytes(take!(4).try_into().map_err(|_| "ends early")?);
                let digits = small(take!(length as usize));
                push(arena, &mut stack, Node::Int(digits))?
            }
            b'F' => {
                let text = line!();
                let value = match text.trim() {
                    "nan" => f64::NAN,
                    "inf" => f64::INFINITY,
                    "-inf" => f64::NEG_INFINITY,
                    text => match text.parse() {
                        Ok(value) => value,
                        Err(_) => return fail(at, "a float that does not read"),
                    },
                };
                push(arena, &mut stack, Node::Float(value))?
            }
            b'G' => {
                let value = f64::from_be_bytes(take!(8).try_into().map_err(|_| "ends early")?);
                push(arena, &mut stack, Node::Float(value))?
            }
            b'S' => {
                let text = line!();
                let Some(inner) = text
                    .strip_prefix('\'')
                    .and_then(|rest| rest.strip_suffix('\''))
                    .or_else(|| {
                        text.strip_prefix('"')
                            .and_then(|rest| rest.strip_suffix('"'))
                    })
                else {
                    return fail(at, "a STRING that is not quoted");
                };
                let Some(bytes) = python_escapes(inner) else {
                    return fail(at, "a STRING with a bad escape");
                };
                let node = match String::from_utf8(bytes) {
                    Ok(text) => Node::Text(text),
                    Err(error) => Node::Bytes(error.into_bytes()),
                };
                push(arena, &mut stack, node)?
            }
            b'V' => {
                let Some(length) = bytes
                    .get(at..)
                    .and_then(|rest| rest.iter().position(|b| *b == b'\n'))
                else {
                    return fail(at, "a line without its end");
                };
                let raw = &bytes[at..at + length];
                at += length + 1;
                let Some(text) = raw_unicode_escape(raw) else {
                    return fail(at, "a UNICODE with a bad escape");
                };
                push(arena, &mut stack, Node::Text(text))?
            }
            b'T' | b'X' | b'B' => {
                let length = u32::from_le_bytes(take!(4).try_into().map_err(|_| "ends early")?);
                let data = take!(length as usize).to_vec();
                let node = match opcode {
                    b'X' => Node::Text(String::from_utf8_lossy(&data).to_string()),
                    b'B' => Node::Bytes(data),
                    _ => match String::from_utf8(data) {
                        Ok(text) => Node::Text(text),
                        Err(error) => Node::Bytes(error.into_bytes()),
                    },
                };
                push(arena, &mut stack, node)?
            }
            b'U' | b'C' | 0x8c => {
                let length = usize::from(take!(1)[0]);
                let data = take!(length).to_vec();
                let node = match opcode {
                    b'C' => Node::Bytes(data),
                    0x8c => Node::Text(String::from_utf8_lossy(&data).to_string()),
                    _ => match String::from_utf8(data) {
                        Ok(text) => Node::Text(text),
                        Err(error) => Node::Bytes(error.into_bytes()),
                    },
                };
                push(arena, &mut stack, node)?
            }
            0x8d | 0x8e | 0x96 => {
                let length = u64::from_le_bytes(take!(8).try_into().map_err(|_| "ends early")?);
                let length = usize::try_from(length).map_err(|_| "a value too long")?;
                let data = take!(length).to_vec();
                let node = if opcode == 0x8d {
                    Node::Text(String::from_utf8_lossy(&data).to_string())
                } else {
                    Node::Bytes(data)
                };
                push(arena, &mut stack, node)?
            }
            b']' => push(arena, &mut stack, Node::List(Vec::new()))?,
            b')' => push(arena, &mut stack, Node::Tuple(Vec::new()))?,
            b'}' => push(arena, &mut stack, Node::Map(Vec::new()))?,
            0x8f => push(arena, &mut stack, Node::Set(Vec::new()))?,
            b'l' => {
                let items = pop_mark(&mut stack, at)?;
                push(arena, &mut stack, Node::List(items))?
            }
            b't' => {
                let items = pop_mark(&mut stack, at)?;
                push(arena, &mut stack, Node::Tuple(items))?
            }
            0x91 => {
                let items = pop_mark(&mut stack, at)?;
                push(arena, &mut stack, Node::Set(items))?
            }
            0x85..=0x87 => {
                let count = usize::from(opcode - 0x84);
                let mut items = (0..count)
                    .map(|_| pop(&mut stack, at))
                    .collect::<Result<Vec<_>, _>>()?;
                items.reverse();
                push(arena, &mut stack, Node::Tuple(items))?
            }
            b'd' => {
                let items = pop_mark(&mut stack, at)?;
                let Some(pairs) = pairs_of(&items) else {
                    return fail(at, "a dict with a key and no value");
                };
                push(arena, &mut stack, Node::Map(pairs))?
            }
            b'a' | b'e' | 0x90 => {
                let items = if opcode == b'a' {
                    vec![pop(&mut stack, at)?]
                } else {
                    pop_mark(&mut stack, at)?
                };
                let target = top(&stack, at)?;
                arena.entries(items.len())?;
                match arena.get_mut(target)? {
                    Node::List(list) | Node::Set(list) => list.extend(items),
                    Node::Object { fields, .. } => {
                        fields.extend(items.into_iter().map(|item| ("item".to_string(), item)))
                    }
                    _ => return fail(at, "items added to something that is not a list"),
                }
            }
            b's' | b'u' => {
                let items = if opcode == b's' {
                    let value = pop(&mut stack, at)?;
                    let key = pop(&mut stack, at)?;
                    vec![key, value]
                } else {
                    pop_mark(&mut stack, at)?
                };
                let target = top(&stack, at)?;
                let Some(pairs) = pairs_of(&items) else {
                    return fail(at, "a key with no value");
                };
                arena.entries(pairs.len())?;
                match arena.get_mut(target)? {
                    Node::Map(map) => map.extend(pairs),
                    Node::Object { fields, .. } => fields.extend(
                        pairs
                            .into_iter()
                            .map(|(_, value)| ("item".to_string(), value)),
                    ),
                    _ => return fail(at, "entries set on something that is not a dict"),
                }
            }
            b'c' => {
                let module = line!();
                let name = line!();
                push(arena, &mut stack, Node::Name(format!("{module}.{name}")))?
            }
            0x93 => {
                let name = pop(&mut stack, at)?;
                let module = pop(&mut stack, at)?;
                let name = format!("{}.{}", name_of(arena, module), name_of(arena, name));
                push(arena, &mut stack, Node::Name(name))?
            }
            b'R' | 0x81 | 0x92 => {
                let keywords = if opcode == 0x92 {
                    Some(pop(&mut stack, at)?)
                } else {
                    None
                };
                let arguments = pop(&mut stack, at)?;
                let callable = pop(&mut stack, at)?;
                let mut fields = vec![("arguments".to_string(), arguments)];
                if let Some(keywords) = keywords {
                    fields.push(("keywords".to_string(), keywords));
                }
                let class = name_of(arena, callable);
                push(arena, &mut stack, Node::Object { class, fields })?
            }
            b'i' => {
                let module = line!();
                let name = line!();
                let arguments = pop_mark(&mut stack, at)?;
                let arguments = arena.add(Node::Tuple(arguments))?;
                let class = format!("{module}.{name}");
                push(
                    arena,
                    &mut stack,
                    Node::Object {
                        class,
                        fields: vec![("arguments".to_string(), arguments)],
                    },
                )?
            }
            b'o' => {
                let mut items = pop_mark(&mut stack, at)?;
                if items.is_empty() {
                    return fail(at, "an object without a class");
                }
                let callable = items.remove(0);
                let arguments = arena.add(Node::Tuple(items))?;
                let class = name_of(arena, callable);
                push(
                    arena,
                    &mut stack,
                    Node::Object {
                        class,
                        fields: vec![("arguments".to_string(), arguments)],
                    },
                )?
            }
            b'b' => {
                let state = pop(&mut stack, at)?;
                let target = top(&stack, at)?;
                match arena.get_mut(target)? {
                    Node::Object { fields, .. } => fields.push(("state".to_string(), state)),
                    _ => return fail(at, "state set on something that is not an object"),
                }
            }
            b'P' => {
                let name = line!();
                push(
                    arena,
                    &mut stack,
                    Node::Name(format!("<persistent id {name}>")),
                )?
            }
            b'Q' => {
                let id = pop(&mut stack, at)?;
                let object = arena.add(Node::Object {
                    class: "<persistent object>".into(),
                    fields: vec![("id".to_string(), id)],
                })?;
                stack.push(Slot::Value(object));
            }
            0x82..=0x84 => {
                let width = [1, 2, 4][usize::from(opcode - 0x82)];
                let code = small(take!(width));
                push(arena, &mut stack, Node::Name(format!("<extension {code}>")))?
            }
            0x97 => push(arena, &mut stack, Node::Name("<out-of-band buffer>".into()))?,
            0x98 => {}
            b'p' | b'q' | b'r' | 0x94 => {
                let key = match opcode {
                    b'p' => line!().trim().parse().map_err(|_| "bad memo index")?,
                    b'q' => u64::from(take!(1)[0]),
                    b'r' => u64::from(u32::from_le_bytes(
                        take!(4).try_into().map_err(|_| "ends early")?,
                    )),
                    _ => memo.len() as u64,
                };
                if memo.len() >= MAX_NODES && !memo.contains_key(&key) {
                    return fail(at, "too many memo entries");
                }
                memo.insert(key, top(&stack, at)?);
            }
            b'g' | b'h' | b'j' => {
                let key = match opcode {
                    b'g' => line!().trim().parse().map_err(|_| "bad memo index")?,
                    b'h' => u64::from(take!(1)[0]),
                    _ => u64::from(u32::from_le_bytes(
                        take!(4).try_into().map_err(|_| "ends early")?,
                    )),
                };
                let Some(&id) = memo.get(&key) else {
                    return fail(at, "a memo entry that does not exist");
                };
                arena.entries(1)?;
                stack.push(Slot::Value(id));
            }
            other => return fail(at - 1, &format!("unknown opcode 0x{other:02x}")),
        }
    }
}

/// Java serialization (`ObjectOutputStream`). Objects are shown by class with their fields,
/// superclass fields first; data a class wrote for itself is shown as it was recorded.
struct Java<'a> {
    bytes: &'a [u8],
    at: usize,
    arena: &'a mut Arena,
    /// What each handle (numbered from 0x7E0000) stands for: a value or a class description.
    handles: Vec<Handle>,
}

#[derive(Clone)]
enum Handle {
    Value(Id),
    Class(std::rc::Rc<ClassDesc>),
}

struct ClassDesc {
    name: String,
    flags: u8,
    /// Type code and name of each serialized field.
    fields: Vec<(u8, String)>,
    parent: Option<std::rc::Rc<ClassDesc>>,
}

const SC_WRITE_METHOD: u8 = 0x01;
const SC_SERIALIZABLE: u8 = 0x02;
const SC_EXTERNALIZABLE: u8 = 0x04;
const SC_BLOCK_DATA: u8 = 0x08;

impl Java<'_> {
    fn error<T>(&self, what: &str) -> Result<T, String> {
        Err(format!(
            "not Java serialized data: {what} at byte {}",
            self.at
        ))
    }

    fn take(&mut self, count: usize) -> Result<&[u8], String> {
        let Some(slice) = self
            .at
            .checked_add(count)
            .and_then(|end| self.bytes.get(self.at..end))
        else {
            return self.error("ends early");
        };
        self.at += count;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let slice = self.take(N)?;
        let mut array = [0; N];
        array.copy_from_slice(slice);
        Ok(array)
    }

    fn utf(&mut self, long: bool) -> Result<String, String> {
        let length = if long {
            usize::try_from(u64::from_be_bytes(self.array()?))
                .or_else(|_| self.error("too long"))?
        } else {
            usize::from(u16::from_be_bytes(self.array()?))
        };
        Ok(modified_utf8(self.take(length)?))
    }

    fn read_all(mut self) -> Result<Id, String> {
        if self.take(4)? != [0xAC, 0xED, 0x00, 0x05] {
            return Err("not Java serialized data: it does not start with AC ED 00 05".into());
        }
        let mut contents = Vec::new();
        while self.at < self.bytes.len() {
            contents.push(self.content(0)?);
        }
        match contents.as_slice() {
            [one] => Ok(*one),
            _ => self.arena.add(Node::List(contents)),
        }
    }

    fn handle(&mut self, handle: Handle) -> usize {
        self.handles.push(handle);
        self.handles.len() - 1
    }

    fn reference(&mut self) -> Result<Handle, String> {
        let handle = u32::from_be_bytes(self.array()?);
        let index = handle.checked_sub(0x7E_0000).map(|index| index as usize);
        match index.and_then(|index| self.handles.get(index)) {
            Some(handle) => Ok(handle.clone()),
            None => self.error("a reference to a handle that does not exist"),
        }
    }

    fn class_desc(&mut self, depth: usize) -> Result<Option<std::rc::Rc<ClassDesc>>, String> {
        if depth > MAX_DEPTH {
            return self.error("nested too deeply");
        }
        match self.u8()? {
            0x70 => Ok(None),
            0x71 => match self.reference()? {
                Handle::Class(class) => Ok(Some(class)),
                Handle::Value(_) => self.error("a class reference to a value"),
            },
            0x72 => {
                let name = self.utf(false)?;
                self.take(8)?;
                // The handle is taken before the fields, as Java numbers them.
                let slot = self.handle(Handle::Value(0));
                let flags = self.u8()?;
                let count = u16::from_be_bytes(self.array()?);
                let mut fields = Vec::new();
                for _ in 0..count {
                    let code = self.u8()?;
                    let field = self.utf(false)?;
                    if code == b'L' || code == b'[' {
                        // The field's class name, as a string object.
                        self.content(depth + 1)?;
                    }
                    fields.push((code, field));
                }
                self.annotations(depth + 1)?;
                let parent = self.class_desc(depth + 1)?;
                let class = std::rc::Rc::new(ClassDesc {
                    name,
                    flags,
                    fields,
                    parent,
                });
                if let Some(handle) = self.handles.get_mut(slot) {
                    *handle = Handle::Class(class.clone());
                }
                Ok(Some(class))
            }
            0x7D => {
                let slot = self.handle(Handle::Value(0));
                let count = u32::from_be_bytes(self.array()?);
                let mut names = Vec::new();
                if count as usize > MAX_NODES {
                    return self.error("a proxy with too many interfaces");
                }
                for _ in 0..count {
                    names.push(self.utf(false)?);
                }
                self.annotations(depth + 1)?;
                let parent = self.class_desc(depth + 1)?;
                let class = std::rc::Rc::new(ClassDesc {
                    name: format!("proxy for {}", names.join(", ")),
                    flags: SC_SERIALIZABLE,
                    fields: Vec::new(),
                    parent,
                });
                if let Some(handle) = self.handles.get_mut(slot) {
                    *handle = Handle::Class(class.clone());
                }
                Ok(Some(class))
            }
            other => self.error(&format!(
                "expected a class description, found 0x{other:02x}"
            )),
        }
    }

    /// Contents up to the end-of-block marker, which a class wrote for itself.
    fn annotations(&mut self, depth: usize) -> Result<Vec<Id>, String> {
        let mut items = Vec::new();
        loop {
            if self.bytes.get(self.at) == Some(&0x78) {
                self.at += 1;
                return Ok(items);
            }
            items.push(self.content(depth)?);
        }
    }

    fn primitive(&mut self, code: u8) -> Result<Node, String> {
        Ok(match code {
            b'B' => Node::Int((self.u8()? as i8).to_string()),
            b'C' => Node::Text(
                char::from_u32(u32::from(u16::from_be_bytes(self.array()?)))
                    .unwrap_or('\u{FFFD}')
                    .to_string(),
            ),
            b'D' => Node::Float(f64::from_be_bytes(self.array()?)),
            b'F' => Node::Float(f64::from(f32::from_be_bytes(self.array()?))),
            b'I' => Node::Int(i32::from_be_bytes(self.array()?).to_string()),
            b'J' => Node::Int(i64::from_be_bytes(self.array()?).to_string()),
            b'S' => Node::Int(i16::from_be_bytes(self.array()?).to_string()),
            b'Z' => Node::Bool(self.u8()? != 0),
            other => return self.error(&format!("unknown field type '{}'", other as char)),
        })
    }

    fn content(&mut self, depth: usize) -> Result<Id, String> {
        if depth > MAX_DEPTH {
            return self.error("nested too deeply");
        }
        let code = self.u8()?;
        match code {
            0x70 => self.arena.add(Node::Null),
            0x71 => match self.reference()? {
                Handle::Value(id) => Ok(id),
                Handle::Class(class) => self.arena.add(Node::Name(format!("class {}", class.name))),
            },
            0x74 | 0x7C => {
                let text = self.utf(code == 0x7C)?;
                let id = self.arena.add(Node::Text(text))?;
                self.handle(Handle::Value(id));
                Ok(id)
            }
            0x72 | 0x7D => {
                self.at -= 1;
                let class = self.class_desc(depth + 1)?;
                self.arena.add(Node::Name(format!(
                    "class {}",
                    class.map_or("<none>".to_string(), |class| class.name.clone())
                )))
            }
            0x76 => {
                let class = self.class_desc(depth + 1)?;
                let id = self.arena.add(Node::Name(format!(
                    "class {}",
                    class.map_or("<none>".to_string(), |class| class.name.clone())
                )))?;
                self.handle(Handle::Value(id));
                Ok(id)
            }
            0x7E => {
                let class = self.class_desc(depth + 1)?;
                let slot = self.handle(Handle::Value(0));
                let constant = self.content(depth + 1)?;
                let constant = match self.arena.nodes.get(constant) {
                    Some(Node::Text(name)) => name.clone(),
                    _ => "<constant>".to_string(),
                };
                let class = class.map_or("<enum>".to_string(), |class| class.name.clone());
                let id = self.arena.add(Node::Name(format!("{class}.{constant}")))?;
                if let Some(handle) = self.handles.get_mut(slot) {
                    *handle = Handle::Value(id);
                }
                Ok(id)
            }
            0x75 => {
                let class = self.class_desc(depth + 1)?;
                let id = self.arena.add(Node::List(Vec::new()))?;
                self.handle(Handle::Value(id));
                let length = i32::from_be_bytes(self.array()?);
                let length =
                    usize::try_from(length).or_else(|_| self.error("a negative array length"))?;
                if length > MAX_NODES {
                    return self.error("an array too long to read");
                }
                // An array's class name is "[" and its element type: "[J", "[Ljava.lang.String;".
                let Some(element) = class
                    .as_ref()
                    .and_then(|class| class.name.strip_prefix('['))
                    .and_then(|element| element.bytes().next())
                else {
                    return self.error("an array whose class is not an array");
                };
                let mut items = Vec::new();
                for _ in 0..length {
                    items.push(if element == b'L' || element == b'[' {
                        self.content(depth + 1)?
                    } else {
                        let node = self.primitive(element)?;
                        self.arena.add(node)?
                    });
                }
                *self.arena.get_mut(id)? = Node::List(items);
                Ok(id)
            }
            0x73 => {
                let class = self.class_desc(depth + 1)?;
                let Some(class) = class else {
                    return self.error("an object without a class");
                };
                let id = self.arena.add(Node::Object {
                    class: class.name.clone(),
                    fields: Vec::new(),
                })?;
                self.handle(Handle::Value(id));
                let mut chain = Vec::new();
                let mut current = Some(class.clone());
                while let Some(class) = current {
                    current = class.parent.clone();
                    chain.push(class);
                }
                let mut fields = Vec::new();
                for class in chain.iter().rev() {
                    if class.flags & SC_EXTERNALIZABLE != 0 {
                        if class.flags & SC_BLOCK_DATA == 0 {
                            return self.error("old-style externalizable data cannot be read");
                        }
                        let items = self.annotations(depth + 1)?;
                        let custom = self.arena.add(Node::List(items))?;
                        fields.push((format!("{} external data", class.name), custom));
                        continue;
                    }
                    for (code, name) in &class.fields {
                        let value = if *code == b'L' || *code == b'[' {
                            self.content(depth + 1)?
                        } else {
                            let node = self.primitive(*code)?;
                            self.arena.add(node)?
                        };
                        fields.push((name.clone(), value));
                    }
                    if class.flags & SC_WRITE_METHOD != 0 {
                        let items = self.annotations(depth + 1)?;
                        if !items.is_empty() {
                            let custom = self.arena.add(Node::List(items))?;
                            fields.push((format!("{} custom data", class.name), custom));
                        }
                    }
                }
                *self.arena.get_mut(id)? = Node::Object {
                    class: class.name.clone(),
                    fields,
                };
                Ok(id)
            }
            0x77 => {
                let length = usize::from(self.u8()?);
                let data = self.take(length)?.to_vec();
                self.arena.add(Node::Custom("block data".into(), data))
            }
            0x7A => {
                let length = usize::try_from(u32::from_be_bytes(self.array()?))
                    .or_else(|_| self.error("block data too long"))?;
                let data = self.take(length)?.to_vec();
                self.arena.add(Node::Custom("block data".into(), data))
            }
            0x79 => {
                // Resets are skipped in a loop, so a run of them cannot nest calls.
                self.handles.clear();
                while self.bytes.get(self.at) == Some(&0x79) {
                    self.at += 1;
                }
                self.content(depth)
            }
            other => self.error(&format!("unknown content 0x{other:02x}")),
        }
    }
}

/// Key and value pairs from a flat run of items; `None` when one is left over.
fn pairs_of(items: &[Id]) -> Option<Vec<(Id, Id)>> {
    if !items.len().is_multiple_of(2) {
        return None;
    }
    Some(
        items
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect(),
    )
}

/// The bytes of a Python string literal's inside (protocol 0 STRING): \\, \', \", \n, \r, \t,
/// \xHH and octal escapes.
fn python_escapes(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\\' {
            let mut buffer = [0; 4];
            out.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        match chars.next()? {
            '\\' => out.push(b'\\'),
            '\'' => out.push(b'\''),
            '"' => out.push(b'"'),
            'n' => out.push(b'\n'),
            'r' => out.push(b'\r'),
            't' => out.push(b'\t'),
            'x' => {
                let high = chars.next()?.to_digit(16)?;
                let low = chars.next()?.to_digit(16)?;
                out.push((high * 16 + low) as u8);
            }
            digit @ '0'..='7' => {
                let mut value = digit.to_digit(8)?;
                for _ in 0..2 {
                    match chars.peek().and_then(|next| next.to_digit(8)) {
                        Some(next) => {
                            value = value * 8 + next;
                            chars.next();
                        }
                        None => break,
                    }
                }
                out.push(u8::try_from(value).ok()?);
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Python's raw-unicode-escape (protocol 0 UNICODE): bytes are Latin-1 characters except
/// \uXXXX and \UXXXXXXXX.
fn raw_unicode_escape(bytes: &[u8]) -> Option<String> {
    let mut out = String::new();
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let width = match (byte, bytes.get(at + 1)) {
            (b'\\', Some(b'u')) => 4,
            (b'\\', Some(b'U')) => 8,
            _ => 0,
        };
        if width == 0 {
            out.push(char::from(byte));
            at += 1;
            continue;
        }
        let digits = std::str::from_utf8(bytes.get(at + 2..at + 2 + width)?).ok()?;
        out.push(char::from_u32(u32::from_str_radix(digits, 16).ok()?)?);
        at += 2 + width;
    }
    Some(out)
}

/// Java's modified UTF-8: NUL as C0 80, characters outside the BMP as two 3-byte surrogates.
/// A lone surrogate or a broken sequence is kept visible as \u{…} rather than replaced.
fn modified_utf8(bytes: &[u8]) -> String {
    let mut units = Vec::new();
    let mut at = 0;
    while let Some(&first) = bytes.get(at) {
        let continuation = |index: usize| {
            bytes
                .get(index)
                .filter(|byte| *byte & 0xC0 == 0x80)
                .copied()
        };
        let unit = match first {
            0x01..=0x7F => {
                at += 1;
                Some(u16::from(first))
            }
            0xC0..=0xDF => continuation(at + 1).map(|second| {
                at += 2;
                (u16::from(first & 0x1F) << 6) | u16::from(second & 0x3F)
            }),
            0xE0..=0xEF => continuation(at + 1)
                .zip(continuation(at + 2))
                .map(|(second, third)| {
                    at += 3;
                    (u16::from(first & 0x0F) << 12)
                        | (u16::from(second & 0x3F) << 6)
                        | u16::from(third & 0x3F)
                }),
            _ => None,
        };
        match unit {
            Some(unit) => units.push(unit),
            None => {
                units.push(0xFFFD);
                at += 1;
            }
        }
    }
    char::decode_utf16(units.iter().copied())
        .map(|decoded| match decoded {
            Ok(character) => character.to_string(),
            Err(error) => format!("\\u{{{:x}}}", error.unpaired_surrogate()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_msgpack() {
        let mut bytes = Vec::new();
        let value = rmpv::Value::Map(vec![
            (rmpv::Value::from("name"), rmpv::Value::from("Ann")),
            (rmpv::Value::from("n"), rmpv::Value::from(-7)),
            (
                rmpv::Value::from("list"),
                rmpv::Value::Array(vec![rmpv::Value::Nil, rmpv::Value::from(1.5)]),
            ),
            (rmpv::Value::from("bin"), rmpv::Value::Binary(vec![0, 255])),
        ]);
        rmpv::encode::write_value(&mut bytes, &value).unwrap();
        let text = decode(Decoder::Msgpack, &bytes).unwrap();
        assert!(text.contains(r#""name": "Ann""#), "{text}");
        assert!(text.contains(r#""n": -7"#));
        assert!(text.contains("null") && text.contains("1.5"));
        assert!(text.contains(r#"b"\x00\xff""#), "{text}");
        assert!(
            decode(Decoder::Msgpack, b"\xc1").is_err(),
            "never-used byte"
        );
        assert!(
            decode(Decoder::Msgpack, &[0x01, 0x02]).is_err(),
            "trailing bytes"
        );
    }

    #[test]
    fn test_msgpack_widths() {
        let values = [
            rmpv::Value::from(200_u8),
            rmpv::Value::from(70_000_u32),
            rmpv::Value::from(u64::MAX),
            rmpv::Value::from(-100_i8),
            rmpv::Value::from(-40_000_i32),
            rmpv::Value::from(i64::MIN),
            rmpv::Value::F32(0.25),
            rmpv::Value::Ext(5, vec![1, 2, 3, 4]),
            rmpv::Value::Array(vec![rmpv::Value::Nil; 20]),
            rmpv::Value::from("x".repeat(40)),
        ];
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &rmpv::Value::Array(values.to_vec())).unwrap();
        let text = decode(Decoder::Msgpack, &bytes).unwrap();
        for expected in [
            "200",
            "70000",
            "18446744073709551615",
            "-100",
            "-40000",
            "-9223372036854775808",
            "0.25",
            "extension 5: 4 bytes",
        ] {
            assert!(text.contains(expected), "{expected} in {text}");
        }
        assert_eq!(text.matches("null").count(), 20);
    }

    #[test]
    fn test_php_from_real_serialize() {
        // php 8.3: serialize(new User) with protected, private and a reference to itself.
        let object = "O:4:\"User\":5:{s:4:\"name\";s:3:\"Ann\";s:5:\"\0*\0id\";i:7;s:12:\"\0User\0secret\";s:1:\"s\";s:4:\"tags\";a:2:{s:1:\"x\";i:1;i:2;a:3:{i:0;b:1;i:1;N;i:2;d:1.5;}}s:4:\"self\";r:1;}";
        let text = decode(Decoder::Php, object.as_bytes()).unwrap();
        assert!(text.starts_with("User {"), "{text}");
        assert!(text.contains("id (protected): 7"));
        assert!(text.contains(r#"secret (private to User): "s""#));
        assert!(
            text.contains("self: <refers back to an enclosing value>"),
            "{text}"
        );
        assert!(text.contains("true") && text.contains("null") && text.contains("1.5"));
        let array = "a:3:{i:0;i:1;s:3:\"two\";s:3:\"dos\";s:6:\"nested\";a:1:{i:0;s:1:\"a\";}}";
        assert!(
            decode(Decoder::Php, array.as_bytes())
                .unwrap()
                .contains(r#""two": "dos""#)
        );
        assert!(
            decode(Decoder::Php, b"s:5:\"ab\";").is_err(),
            "length past the end"
        );
        assert!(decode(Decoder::Php, b"x:1;").is_err());
    }

    #[test]
    fn test_pickle_from_real_python() {
        // python3 pickle.dumps of a dict with str, int, float, bytes, tuple, set, None, a big int,
        // a list and an OrderedDict, in protocols 0, 2 and 5.
        let samples = include_str!("../data/pickle_samples.txt");
        for line in samples.lines() {
            let (label, data) = line.split_once(' ').unwrap();
            let text = decode(Decoder::Pickle, &hex(data))
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            if label == "obj" {
                assert!(
                    text.starts_with("copyreg.__reduce_ex__") || text.contains("__main__.Point"),
                    "{label}: {text}"
                );
                assert!(
                    text.contains("state") || text.contains("arguments"),
                    "{text}"
                );
            } else if label == "dt" {
                assert!(text.contains("datetime.date"), "{text}");
            } else {
                assert!(text.contains(r#""name": "Ann""#), "{label}: {text}");
                assert!(
                    text.contains("1180591620717411303424"),
                    "2**70 in {label}: {text}"
                );
                assert!(text.contains("collections.OrderedDict"), "{label}: {text}");
                // Protocols 0 and 2 store bytes as a call to _codecs.encode, shown as such.
                if label == "0" || label == "2" {
                    assert!(text.contains("_codecs.encode"), "{label}: {text}");
                } else {
                    assert!(text.contains(r#"b"\x00\x01""#), "{label}: {text}");
                }
            }
        }
        assert!(
            decode(Decoder::Pickle, b"\x80\x04\xff").is_err(),
            "unknown opcode"
        );
        assert!(decode(Decoder::Pickle, b"").is_err());
    }

    #[test]
    fn test_java_from_real_object_stream() {
        // Java 21 ObjectOutputStream of a User (extends Base) with a long[], a HashMap, an
        // ArrayList holding the same string twice, an enum and a reference to itself.
        let bytes = hex(include_str!("../data/java_sample.hex").trim());
        let text = decode(Decoder::Java, &bytes).unwrap();
        assert!(text.starts_with("Make$User {"), "{text}");
        assert!(text.contains("id: 7"), "superclass fields: {text}");
        assert!(text.contains(r#"name: "Ann""#));
        assert!(text.contains("Make$Kind.ADMIN"), "{text}");
        assert!(
            text.contains("self: <refers back to an enclosing value>"),
            "{text}"
        );
        assert!(text.contains("java.util.HashMap"), "{text}");
        assert!(text.contains("java.util.ArrayList"));
        assert!(
            text.matches(r#""r1""#).count() >= 2,
            "a shared string shown where used: {text}"
        );
        assert!(
            decode(Decoder::Java, b"\xac\xed\x00\x05\x73").is_err(),
            "ends early"
        );
        assert!(decode(Decoder::Java, b"plain").is_err());
    }

    #[test]
    fn test_hostile_and_exact_inputs() {
        // Python 3: pickle.dumps("é😀\n", protocol=0) uses raw-unicode-escape.
        let text = decode(
            Decoder::Pickle,
            &hex("56e95c5530303031663630305c75303030610a70300a2e"),
        )
        .unwrap();
        assert_eq!(text, quoted("é😀\n"), "{text}");
        assert_eq!(
            decode(Decoder::Pickle, b"S'a\\n\\x41\\'b'\n.").unwrap(),
            quoted("a\nA'b"),
            "protocol 0 STRING escapes"
        );
        assert!(
            decode(Decoder::Pickle, b"(K\x01d.").is_err(),
            "a dict key without a value"
        );
        assert!(decode(Decoder::Pickle, b"Fnot-a-float\n.").is_err());
        // Successive lists that each hold the previous one twice grow exponentially when shown;
        // the work stops at the visit limit.
        let mut doubling = b"\x80\x04]\x94".to_vec();
        for index in 0..40_u8 {
            doubling.extend_from_slice(&[b'h', index, b'h', index, 0x86, 0x94]);
        }
        doubling.push(b'.');
        let started = std::time::Instant::now();
        let text = decode(Decoder::Pickle, &doubling).unwrap();
        assert!(
            text.ends_with("KB of text"),
            "{}",
            &text[text.len().saturating_sub(80)..]
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        assert!(
            decode(Decoder::Php, b"s:18446744073709551615:\"\";").is_err(),
            "no overflow"
        );
        assert!(decode(Decoder::Php, b"C:1:\"A\":18446744073709551615:{}").is_err());
        // A long run of Java resets does not nest calls.
        let mut resets = b"\xac\xed\x00\x05".to_vec();
        resets.extend(std::iter::repeat_n(0x79, 200_000));
        resets.push(0x70);
        assert_eq!(decode(Decoder::Java, &resets).unwrap(), "null");
        // Java's modified UTF-8: NUL as C0 80 and 😀 as two surrogates.
        assert_eq!(modified_utf8(b"a\xc0\x80\xed\xa0\xbd\xed\xb8\x80"), "a\0😀");
        assert_eq!(
            modified_utf8(b"\xed\xa0\xbd"),
            "\\u{d83d}",
            "a lone surrogate stays visible"
        );
        // An array class must name an array.
        let not_array = b"\xac\xed\x00\x05\x75\x72\x00\x01X\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x78\x70\x00\x00\x00\x01";
        assert!(decode(Decoder::Java, not_array).is_err());
        // Recorded custom data is shown whole.
        let long = format!("C:1:\"A\":100:{{{}}}", "z".repeat(100));
        assert!(
            decode(Decoder::Php, long.as_bytes())
                .unwrap()
                .contains(&"z".repeat(100))
        );
    }

    #[test]
    fn test_vectors_and_limits() {
        let bytes = [1.0_f32, -2.5]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(
            decode(Decoder::Float32Vector, &bytes).unwrap(),
            "[1.0, -2.5]"
        );
        assert_eq!(
            decode(Decoder::Float64Vector, &0.5_f64.to_le_bytes()).unwrap(),
            "[0.5]"
        );
        assert!(decode(Decoder::Float32Vector, &[0, 1, 2]).is_err());
        // Deep nesting is refused instead of overflowing the stack.
        let deep = "a:1:{i:0;".repeat(100) + "N;" + &"}".repeat(100);
        assert!(decode(Decoder::Php, deep.as_bytes()).is_err());
        let mut nested = vec![0x91_u8; 200];
        nested.push(0xc0);
        assert!(decode(Decoder::Msgpack, &nested).is_err());
        // A huge output is cut, and says so.
        let wide = rmpv::Value::Array(vec![rmpv::Value::from("x".repeat(1000)); 2000]);
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &wide).unwrap();
        assert!(
            decode(Decoder::Msgpack, &bytes)
                .unwrap()
                .ends_with("KB of text")
        );
    }
}
