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
/// Bytes of text all values of one input may hold together; class and field names copied into
/// each object count too, so a long name shared by many objects cannot multiply memory.
const MAX_BYTES: usize = 64 * 1024 * 1024;
/// Reading steps for one Java input, never given back when a guess is rolled back, so nested
/// guesses cannot multiply the work.
const MAX_STEPS: usize = 2_000_000;
/// Bytes a pickle global's name may have; names are built by joining others, so unbounded
/// joining could grow a tiny input into a huge allocation.
const MAX_NAME: usize = 4096;
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
    /// Text holding a lone surrogate (Python and Java allow them), kept as UTF-16 units so it
    /// never equals ordinary text that merely spells the escape out.
    Utf16(Vec<u16>),
    /// Python text holding a lone surrogate, kept as code points: a Python str is a sequence of
    /// code points, so a surrogate pair written as two of them is not the character they encode.
    CodePoints(Vec<u32>),
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
    bytes: usize,
}

fn text_size(node: &Node) -> usize {
    match node {
        Node::Text(text) | Node::Int(text) | Node::Name(text) => text.len(),
        Node::Bytes(bytes) => bytes.len(),
        Node::Utf16(units) => units.len() * 2,
        Node::CodePoints(points) => points.len() * 4,
        Node::Custom(label, bytes) => label.len() + bytes.len(),
        Node::Object { class, fields } => {
            class.len() + fields.iter().map(|(name, _)| name.len()).sum::<usize>()
        }
        _ => 0,
    }
}

impl Arena {
    /// Refuses before a copy of `bytes` that would go past `MAX_BYTES` (counted when added).
    fn fits(&self, bytes: usize) -> Result<(), String> {
        if self.bytes.saturating_add(bytes) > MAX_BYTES {
            return Err(format!(
                "more than {} MB of text; not read further",
                MAX_BYTES / (1024 * 1024)
            ));
        }
        Ok(())
    }

    /// Counts text about to be held against `MAX_BYTES`; called before copying it.
    fn charge(&mut self, bytes: usize) -> Result<(), String> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > MAX_BYTES {
            return Err(format!(
                "more than {} MB of text; not read further",
                MAX_BYTES / (1024 * 1024)
            ));
        }
        Ok(())
    }

    /// Replaces a placeholder with the value it became, counting its text.
    fn replace(&mut self, id: Id, node: Node) -> Result<(), String> {
        // The placeholder's own text was counted when it was added.
        let before = self.nodes.get(id).map_or(0, text_size);
        self.bytes = self.bytes.saturating_sub(before);
        self.charge(text_size(&node))?;
        *self.get_mut(id)? = node;
        Ok(())
    }

    fn add(&mut self, node: Node) -> Result<Id, String> {
        self.charge(text_size(&node))?;
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
            steps: 0,
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
    /// Appends what still fits; a piece that does not fit whole is cut where the limit is.
    fn push(&mut self, text: &str) {
        if self.cut {
            return;
        }
        let room = MAX_OUTPUT.saturating_sub(self.text.len());
        if text.len() <= room {
            self.text.push_str(text);
            return;
        }
        let end = (0..=room)
            .rev()
            .find(|end| text.is_char_boundary(*end))
            .unwrap_or(0);
        self.text.push_str(&text[..end]);
        self.cut = true;
    }

    /// A JSON-style quoted string, escaped piece by piece so the limit cuts it where it is
    /// reached instead of dropping it whole.
    fn push_text(&mut self, text: &str) {
        self.push("\"");
        for character in text.chars() {
            if self.cut {
                return;
            }
            self.push_char(character);
        }
        self.push("\"");
    }

    /// Like `push_text`, with each lone surrogate shown as \u{…}.
    fn push_units(&mut self, units: &[u16]) {
        self.push("\"");
        for decoded in char::decode_utf16(units.iter().copied()) {
            if self.cut {
                return;
            }
            match decoded {
                Ok(character) => self.push_char(character),
                Err(error) => self.push(&format!("\\u{{{:x}}}", error.unpaired_surrogate())),
            }
        }
        self.push("\"");
    }

    /// Like `push_text`, with each surrogate code point shown as \u{…}.
    fn push_points(&mut self, points: &[u32]) {
        self.push("\"");
        for point in points {
            if self.cut {
                return;
            }
            match char::from_u32(*point) {
                Some(character) => self.push_char(character),
                None => self.push(&format!("\\u{{{point:x}}}")),
            }
        }
        self.push("\"");
    }

    fn push_char(&mut self, character: char) {
        let mut buffer = [0; 4];
        match character {
            '"' => self.push("\\\""),
            '\\' => self.push("\\\\"),
            '\n' => self.push("\\n"),
            '\r' => self.push("\\r"),
            '\t' => self.push("\\t"),
            control if u32::from(control) < 0x20 => {
                self.push(&format!("\\u{:04x}", u32::from(control)))
            }
            character => self.push(character.encode_utf8(&mut buffer)),
        }
    }

    /// Bytes quoted as `quote_bytes` writes them, escaped piece by piece like `push_text`.
    fn push_bytes(&mut self, bytes: &[u8]) {
        self.push("\"");
        for byte in bytes {
            if self.cut {
                return;
            }
            match byte {
                b'"' => self.push("\\\""),
                b'\\' => self.push("\\\\"),
                b'\n' => self.push("\\n"),
                b' '..=b'~' => {
                    let printable = [*byte];
                    self.push(std::str::from_utf8(&printable).unwrap_or("?"))
                }
                _ => self.push(&format!("\\x{byte:02x}")),
            }
        }
        self.push("\"");
    }

    fn finish(mut self) -> String {
        if self.cut {
            self.text
                .push_str(&format!("\n… cut at {} KB of text", MAX_OUTPUT / 1000));
        }
        self.text
    }
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
        Node::Text(text) => out.push_text(text),
        Node::Utf16(units) => out.push_units(units),
        Node::CodePoints(points) => out.push_points(points),
        Node::Bytes(bytes) => {
            out.push("b");
            out.push_bytes(bytes);
        }
        Node::List(ids) => items("[", ids, "]", out),
        Node::Tuple(ids) => items("(", ids, ")", out),
        Node::Set(ids) => items("{", ids, "}", out),
        Node::Name(name) => out.push(name),
        Node::Custom(label, bytes) => {
            out.push(&format!("<{label}: {} bytes ", bytes.len()));
            out.push_bytes(bytes);
            out.push(">");
        }
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
            arena.fits(length)?;
            let text = take(length)?;
            match std::str::from_utf8(text) {
                Ok(text) => Node::Text(text.to_string()),
                Err(_) => Node::Bytes(text.to_vec()),
            }
        }
        0xc4..=0xc6 => {
            let length = take_length(1 << (marker - 0xc4))?;
            arena.fits(length)?;
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
            arena.fits(length)?;
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
        self.arena.fits(length)?;
        self.at = start + length + 1;
        Ok(&self.bytes[start..start + length])
    }

    fn number(&mut self, end: u8) -> Result<String, String> {
        let text = String::from_utf8_lossy(self.until(end)?).to_string();
        Ok(text)
    }

    fn count(&mut self, end: u8) -> Result<usize, String> {
        let text = self.number(end)?;
        text.parse().or_else(|_| {
            let shown = text.chars().take(40).collect::<String>();
            self.error(&format!("bad length {shown}"))
        })
    }

    /// `LEN:"…"` with exactly LEN bytes between the quotes.
    /// A sized class or enum name as text. Names are bounded before they are converted, as a
    /// name that is not UTF-8 grows when shown.
    fn name(&mut self) -> Result<String, String> {
        let name = self.sized()?;
        if name.len() > MAX_NAME {
            return self.error("a name too long");
        }
        Ok(String::from_utf8_lossy(&name).to_string())
    }

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
        self.arena.fits(length)?;
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
                let flag = match self.number(b';')?.as_str() {
                    "0" => false,
                    "1" => true,
                    _ => return self.error("a boolean that is not 0 or 1"),
                };
                self.remember(Node::Bool(flag))
            }
            b'i' => {
                let number = self.number(b';')?;
                if !is_integer(&number) {
                    return self.error("an integer that is not a number");
                }
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
                let text = self.name()?;
                self.expect(b';')?;
                self.remember(Node::Name(text))
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
                    self.arena.entries(1)?;
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
                    self.arena.entries(1)?;
                    let key = self.key()?;
                    let value = self.value(depth + 1)?;
                    pairs.push((key, value));
                }
                self.expect(b'}')?;
                self.arena.replace(id, Node::Map(pairs))?;
                Ok(id)
            }
            b'O' => {
                let class = self.name()?;
                self.expect(b':')?;
                let length = self.count(b':')?;
                self.expect(b'{')?;
                let id = self.remember(Node::Object {
                    class: class.clone(),
                    fields: Vec::new(),
                })?;
                let mut fields = Vec::new();
                for _ in 0..length {
                    self.arena.entries(1)?;
                    let name = self.property_name()?;
                    self.arena.charge(name.len())?;
                    let value = self.value(depth + 1)?;
                    fields.push((name, value));
                }
                self.expect(b'}')?;
                self.arena.replace(id, Node::Object { class, fields })?;
                Ok(id)
            }
            b'C' => {
                let class = self.name()?;
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
                self.arena.fits(length)?;
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
                if !is_integer(&number) {
                    return self.error("an integer that is not a number");
                }
                self.arena.add(Node::Int(number))
            }
            Some(b's') => {
                self.at += 1;
                self.expect(b':')?;
                let text = self.sized()?;
                self.expect(b';')?;
                // PHP strings are bytes; a key that is not text is kept, and shown escaped.
                self.arena.add(match String::from_utf8(text) {
                    Ok(text) => Node::Text(text),
                    Err(error) => Node::Bytes(error.into_bytes()),
                })
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
        if name.len() > MAX_NAME {
            return self.error("a property name too long");
        }
        let name = String::from_utf8(name)
            .unwrap_or_else(|error| crate::redis::quote_bytes(error.as_bytes()));
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
    let mut steps = 0_usize;
    let mut items_of = std::collections::HashMap::new();
    let mut keys_of = std::collections::HashMap::new();
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
            arena.fits(length)?;
            let text = String::from_utf8_lossy(&bytes[at..at + length]).to_string();
            at += length + 1;
            text
        }};
    }
    // A module or class name line, bounded before it is converted.
    macro_rules! name_line {
        () => {{
            let Some(length) = bytes
                .get(at..)
                .and_then(|rest| rest.iter().position(|b| *b == b'\n'))
            else {
                return fail(at, "a line without its end");
            };
            if length > MAX_NAME {
                return fail(at, "a global name too long");
            }
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
    let name_of = |arena: &Arena, id: Id| -> Result<String, String> {
        let name = match arena.nodes.get(id) {
            Some(Node::Name(name)) | Some(Node::Text(name)) => name.as_str(),
            _ => "<callable>",
        };
        if name.len() > MAX_NAME {
            return Err(format!("not a pickle: a name longer than {MAX_NAME} bytes"));
        }
        Ok(name.to_string())
    };
    let small = |bytes: &[u8]| -> Result<String, String> {
        decimal(bytes).ok_or_else(|| {
            format!(
                "not a pickle Vilo shows: a {}-byte integer is too large",
                bytes.len()
            )
        })
    };
    loop {
        if stack.len() > MAX_NODES {
            return fail(at, "too many values");
        }
        let opcode = take!(1)[0];
        steps += 1;
        if steps > MAX_STEPS {
            return fail(at, "too much to read");
        }
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
                if stack.pop().is_none() {
                    return fail(at, "POP with nothing to pop");
                }
            }
            b'1' => {
                pop_mark(&mut stack, at)?;
            }
            b'2' => {
                arena.entries(1)?;
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
                    text if is_integer(text) => Node::Int(text.to_string()),
                    _ => return fail(at, "an INT that is not a number"),
                };
                push(arena, &mut stack, node)?
            }
            b'L' => {
                let text = line!();
                let digits = text.trim_end_matches('L');
                if digits.is_empty()
                    || digits
                        .trim_start_matches('-')
                        .bytes()
                        .any(|byte| !byte.is_ascii_digit())
                {
                    return fail(at, "a LONG that is not a number");
                }
                push(arena, &mut stack, Node::Int(digits.to_string()))?
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
                let digits = small(take!(length))?;
                push(arena, &mut stack, Node::Int(digits))?
            }
            0x8b => {
                let length = u32::from_le_bytes(take!(4).try_into().map_err(|_| "ends early")?);
                let digits = small(take!(length as usize))?;
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
                // Code points take four bytes each.
                arena.fits(raw.len().saturating_mul(4))?;
                let Some(node) = raw_unicode_escape(raw) else {
                    return fail(at, "a UNICODE with a bad escape");
                };
                push(arena, &mut stack, node)?
            }
            b'T' | b'X' | b'B' => {
                let length = u32::from_le_bytes(take!(4).try_into().map_err(|_| "ends early")?);
                arena.fits(length as usize)?;
                let data = take!(length as usize).to_vec();
                let node = match opcode {
                    b'X' => match python_text(arena, &data)? {
                        Some(node) => node,
                        None => return fail(at, "text that is not UTF-8"),
                    },
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
                arena.fits(length)?;
                let data = take!(length).to_vec();
                let node = match opcode {
                    b'C' => Node::Bytes(data),
                    0x8c => match python_text(arena, &data)? {
                        Some(node) => node,
                        None => return fail(at, "text that is not UTF-8"),
                    },
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
                arena.fits(length)?;
                let data = take!(length).to_vec();
                let node = if opcode == 0x8d {
                    match python_text(arena, &data)? {
                        Some(node) => node,
                        None => return fail(at, "text that is not UTF-8"),
                    }
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
                arena.entries(items.len())?;
                push(arena, &mut stack, Node::List(items))?
            }
            b't' => {
                let items = pop_mark(&mut stack, at)?;
                arena.entries(items.len())?;
                push(arena, &mut stack, Node::Tuple(items))?
            }
            0x91 => {
                let items = pop_mark(&mut stack, at)?;
                arena.entries(items.len())?;
                let set = arena.add(Node::Set(Vec::new()))?;
                add_to_set(arena, &mut keys_of, set, items)?;
                stack.push(Slot::Value(set));
            }
            0x85..=0x87 => {
                let count = usize::from(opcode - 0x84);
                arena.entries(count)?;
                let mut items = (0..count)
                    .map(|_| pop(&mut stack, at))
                    .collect::<Result<Vec<_>, _>>()?;
                items.reverse();
                push(arena, &mut stack, Node::Tuple(items))?
            }
            b'd' => {
                let items = pop_mark(&mut stack, at)?;
                arena.entries(items.len())?;
                let Some(pairs) = pairs_of(&items) else {
                    return fail(at, "a dict with a key and no value");
                };
                let map = arena.add(Node::Map(Vec::new()))?;
                set_items(arena, &mut keys_of, map, pairs)?;
                stack.push(Slot::Value(map));
            }
            b'a' | b'e' | 0x90 => {
                let items = if opcode == b'a' {
                    vec![pop(&mut stack, at)?]
                } else {
                    pop_mark(&mut stack, at)?
                };
                let target = top(&stack, at)?;
                arena.entries(items.len())?;
                let target = match arena.nodes.get(target) {
                    Some(Node::Object { .. }) => object_items(arena, &mut items_of, target, false)?,
                    _ => target,
                };
                match arena.get_mut(target)? {
                    Node::List(list) => list.extend(items),
                    Node::Set(_) => add_to_set(arena, &mut keys_of, target, items)?,
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
                let target = match arena.nodes.get(target) {
                    Some(Node::Object { .. }) => object_items(arena, &mut items_of, target, true)?,
                    _ => target,
                };
                if !matches!(arena.nodes.get(target), Some(Node::Map(_))) {
                    return fail(at, "entries set on something that is not a dict");
                }
                set_items(arena, &mut keys_of, target, pairs)?;
            }
            b'c' => {
                let module = name_line!();
                let name = name_line!();
                if module.len() + name.len() > MAX_NAME {
                    return fail(at, "a global name too long");
                }
                push(arena, &mut stack, Node::Name(format!("{module}.{name}")))?
            }
            0x93 => {
                let name = pop(&mut stack, at)?;
                let module = pop(&mut stack, at)?;
                let name = format!("{}.{}", name_of(arena, module)?, name_of(arena, name)?);
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
                let class = name_of(arena, callable)?;
                push(arena, &mut stack, Node::Object { class, fields })?
            }
            b'i' => {
                let module = name_line!();
                let name = name_line!();
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
                let class = name_of(arena, callable)?;
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
                    Node::Object { fields, .. } => {
                        fields.push(("state".to_string(), state));
                        arena.entries(1)?;
                    }
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
                let code = small(take!(width))?;
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
    steps: usize,
}

#[derive(Clone)]
enum Handle {
    Value(Id),
    Class(std::rc::Rc<ClassDesc>),
}

struct ClassDesc {
    name: String,
    /// Text copied into every object of this class: its name and its field names.
    copied: usize,
    /// Classes above this one, which bounds the hierarchy however it was reached.
    ancestors: usize,
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

    /// A class, field or interface name; bounded before it is converted, as broken bytes grow
    /// into escapes when shown.
    fn utf(&mut self, long: bool) -> Result<String, String> {
        let length = if long {
            usize::try_from(u64::from_be_bytes(self.array()?))
                .or_else(|_| self.error("too long"))?
        } else {
            usize::from(u16::from_be_bytes(self.array()?))
        };
        if length > MAX_NAME {
            return self.error("a name too long");
        }
        let bytes = self.take(length)?;
        match modified_utf8(bytes) {
            Some(text) => Ok(text),
            None => self.error("a name that is not modified UTF-8"),
        }
    }

    /// A string object's text as UTF-16 units, as Java holds it.
    fn utf_units(&mut self, long: bool) -> Result<Vec<u16>, String> {
        let length = if long {
            usize::try_from(u64::from_be_bytes(self.array()?))
                .or_else(|_| self.error("too long"))?
        } else {
            usize::from(u16::from_be_bytes(self.array()?))
        };
        self.arena.fits(length.saturating_mul(2))?;
        let bytes = self.take(length)?;
        match modified_utf8_units(bytes) {
            Some(units) => Ok(units),
            None => self.error("a string that is not modified UTF-8"),
        }
    }

    fn read_all(mut self) -> Result<Id, String> {
        if self.take(4)? != [0xAC, 0xED, 0x00, 0x05] {
            return Err("not Java serialized data: it does not start with AC ED 00 05".into());
        }
        let mut contents = Vec::new();
        while self.at < self.bytes.len() {
            contents.push(self.content(0)?);
            self.arena.entries(1)?;
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
                self.arena.charge(name.len())?;
                self.take(8)?;
                // The handle is taken before the fields, as Java numbers them.
                let slot = self.handle(Handle::Value(0));
                let flags = self.u8()?;
                let count = u16::from_be_bytes(self.array()?);
                // The field list is kept with the class; counted before it is built.
                self.arena.entries(usize::from(count))?;
                let mut fields = Vec::new();
                for _ in 0..count {
                    let code = self.u8()?;
                    let field = self.utf(false)?;
                    if code == b'L' || code == b'[' {
                        // The field's class name, as a string object.
                        self.content(depth + 1)?;
                    }
                    self.arena.charge(field.len())?;
                    fields.push((code, field));
                }
                self.annotations(depth + 1)?;
                let parent = self.class_desc(depth + 1)?;
                let ancestors = self.ancestors(&parent)?;
                let copied =
                    name.len() + fields.iter().map(|(_, field)| field.len()).sum::<usize>();
                let class = std::rc::Rc::new(ClassDesc {
                    name,
                    copied,
                    ancestors,
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
                    let name = self.utf(false)?;
                    self.arena.charge(name.len() + 2)?;
                    names.push(name);
                }
                self.annotations(depth + 1)?;
                let parent = self.class_desc(depth + 1)?;
                let ancestors = self.ancestors(&parent)?;
                let name = format!("proxy for {}", names.join(", "));
                let class = std::rc::Rc::new(ClassDesc {
                    copied: name.len(),
                    name,
                    ancestors,
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
            b'C' => {
                let unit = u16::from_be_bytes(self.array()?);
                // A lone surrogate is a valid Java char but no character; it stays visible.
                match char::from_u32(u32::from(unit)) {
                    Some(character) => Node::Text(character.to_string()),
                    None => Node::Utf16(vec![unit]),
                }
            }
            b'D' => Node::Float(f64::from_be_bytes(self.array()?)),
            b'F' => Node::Float(f64::from(f32::from_be_bytes(self.array()?))),
            b'I' => Node::Int(i32::from_be_bytes(self.array()?).to_string()),
            b'J' => Node::Int(i64::from_be_bytes(self.array()?).to_string()),
            b'S' => Node::Int(i16::from_be_bytes(self.array()?).to_string()),
            b'Z' => Node::Bool(self.u8()? != 0),
            other => return self.error(&format!("unknown field type '{}'", other as char)),
        })
    }

    fn ancestors(&self, parent: &Option<std::rc::Rc<ClassDesc>>) -> Result<usize, String> {
        let ancestors = parent.as_ref().map_or(0, |parent| parent.ancestors + 1);
        if ancestors > MAX_DEPTH {
            return self.error("a class hierarchy too deep");
        }
        Ok(ancestors)
    }

    /// The values of a class's serialized fields, in their declared order.
    fn default_fields(
        &mut self,
        class: &ClassDesc,
        depth: usize,
    ) -> Result<Vec<(String, Id)>, String> {
        let mut fields = Vec::new();
        self.arena
            .charge(class.fields.iter().map(|(_, name)| name.len()).sum())?;
        for (code, name) in &class.fields {
            let value = if *code == b'L' || *code == b'[' {
                self.content(depth + 1)?
            } else {
                let node = self.primitive(*code)?;
                self.arena.add(node)?
            };
            fields.push((name.clone(), value));
        }
        Ok(fields)
    }

    fn content(&mut self, depth: usize) -> Result<Id, String> {
        if depth > MAX_DEPTH {
            return self.error("nested too deeply");
        }
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return self.error("too much to read");
        }
        let code = self.u8()?;
        match code {
            0x70 => self.arena.add(Node::Null),
            0x71 => match self.reference()? {
                Handle::Value(id) => {
                    // A reference is one more entry wherever it is used.
                    self.arena.entries(1)?;
                    Ok(id)
                }
                Handle::Class(class) => self.arena.add(Node::Name(format!("class {}", class.name))),
            },
            0x74 | 0x7C => {
                let units = self.utf_units(code == 0x7C)?;
                let node = match String::from_utf16(&units) {
                    Ok(text) => Node::Text(text),
                    Err(_) => Node::Utf16(units),
                };
                let id = self.arena.add(node)?;
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
                self.arena.replace(id, Node::List(items))?;
                Ok(id)
            }
            0x73 => {
                let class = self.class_desc(depth + 1)?;
                let Some(class) = class else {
                    return self.error("an object without a class");
                };
                // The class's text is copied into this object (and each placeholder); counted first.
                self.arena.charge(class.copied)?;
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
                    if class.flags & SC_WRITE_METHOD == 0 {
                        fields.extend(self.default_fields(class, depth)?);
                        continue;
                    }
                    // A class with its own writeObject may or may not have written its fields
                    // first (defaultWriteObject). Java writes primitive fields first, and an
                    // object field never starts with a block marker; so when the first field is
                    // primitive and the data starts with one, either reading could be right, and
                    // the data is shown as recorded with a note instead of guessed field values.
                    let first_primitive = class
                        .fields
                        .first()
                        .is_some_and(|(code, _)| *code != b'L' && *code != b'[');
                    let starts_with_block =
                        matches!(self.bytes.get(self.at), Some(0x77 | 0x7A | 0x78));
                    if first_primitive && starts_with_block {
                        let note = self.arena.add(Node::Name(
                            "<not read: the class wrote its own data, which may or may not hold them>"
                                .into(),
                        ))?;
                        fields.push((format!("{} fields", class.name), note));
                    }
                    let saved = (self.at, self.handles.len(), self.arena.nodes.len());
                    let attempt = if first_primitive && starts_with_block {
                        Err(String::new())
                    } else {
                        self.default_fields(class, depth)
                            .and_then(|defaults| Ok((defaults, self.annotations(depth + 1)?)))
                    };
                    let (defaults, items) = match attempt {
                        Ok(read) => read,
                        Err(_) => {
                            self.at = saved.0;
                            self.handles.truncate(saved.1);
                            self.arena.nodes.truncate(saved.2);
                            (Vec::new(), self.annotations(depth + 1)?)
                        }
                    };
                    // Fields read from a class with its own writeObject rest on the assumption that
                    // it called defaultWriteObject first; that is said, not asserted.
                    if !defaults.is_empty() {
                        let note = self.arena.add(Node::Name(
                            "<read assuming its writeObject called defaultWriteObject first>"
                                .into(),
                        ))?;
                        fields.push((format!("{} fields", class.name), note));
                    }
                    fields.extend(defaults);
                    if !items.is_empty() {
                        let custom = self.arena.add(Node::List(items))?;
                        fields.push((format!("{} custom data", class.name), custom));
                    }
                }
                self.arena.replace(
                    id,
                    Node::Object {
                        class: class.name.clone(),
                        fields,
                    },
                )?;
                Ok(id)
            }
            0x77 => {
                let length = usize::from(self.u8()?);
                self.arena.fits(length)?;
                let data = self.take(length)?.to_vec();
                self.arena.add(Node::Custom("block data".into(), data))
            }
            0x7A => {
                let length = usize::try_from(u32::from_be_bytes(self.array()?))
                    .or_else(|_| self.error("block data too long"))?;
                self.arena.fits(length)?;
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

/// The container an object built by a call keeps added items in (a map for SETITEM, a list
/// for APPEND), shown as its `items` field, so keys and values stay together. Remembered per
/// object, so adding items never searches the object's fields.
fn object_items(
    arena: &mut Arena,
    known: &mut std::collections::HashMap<(Id, bool), Id>,
    object: Id,
    map: bool,
) -> Result<Id, String> {
    if let Some(items) = known.get(&(object, map)) {
        return Ok(*items);
    }
    let items = arena.add(if map {
        Node::Map(Vec::new())
    } else {
        Node::List(Vec::new())
    })?;
    if let Node::Object { fields, .. } = arena.get_mut(object)? {
        fields.push(("items".to_string(), items));
    }
    known.insert((object, map), items);
    Ok(items)
}

fn is_integer(text: &str) -> bool {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// Python text as pickled: UTF-8 with surrogates passed through (Python allows lone
/// surrogates in a str), which stay visible as \u{…}. Anything else that is not UTF-8 is `None`.
fn python_text(arena: &Arena, bytes: &[u8]) -> Result<Option<Node>, String> {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return Ok(Some(Node::Text(text.to_string())));
    }
    // Code points take four bytes each; checked before they are built.
    arena.fits(bytes.len().saturating_mul(4))?;
    let mut points: Vec<u32> = Vec::new();
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(text) => {
                points.extend(text.chars().map(u32::from));
                return Ok(Some(Node::CodePoints(points)));
            }
            Err(error) => {
                let (valid, after) = rest.split_at(error.valid_up_to());
                let Ok(valid) = std::str::from_utf8(valid) else {
                    return Ok(None);
                };
                points.extend(valid.chars().map(u32::from));
                match after {
                    [0xED, second @ 0xA0..=0xBF, third @ 0x80..=0xBF, ..] => {
                        points.push(
                            0xD000 | (u32::from(second & 0x3F) << 6) | u32::from(third & 0x3F),
                        );
                        rest = &after[3..];
                    }
                    _ => return Ok(None),
                }
            }
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
fn raw_unicode_escape(bytes: &[u8]) -> Option<Node> {
    let mut points: Vec<u32> = Vec::new();
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        if byte != b'\\' {
            points.push(u32::from(byte));
            at += 1;
            continue;
        }
        // In a run of backslashes only an odd count escapes what follows; pairs stay literal.
        let run = bytes[at..]
            .iter()
            .take_while(|byte| **byte == b'\\')
            .count();
        at += run;
        let width = match bytes.get(at) {
            Some(b'u') if run % 2 == 1 => 4,
            Some(b'U') if run % 2 == 1 => 8,
            _ => 0,
        };
        let literal = if width == 0 { run } else { run - 1 };
        points.extend(std::iter::repeat_n(u32::from(b'\\'), literal));
        if width > 0 {
            let digits = std::str::from_utf8(bytes.get(at + 1..at + 1 + width)?).ok()?;
            let point = u32::from_str_radix(digits, 16).ok()?;
            // Surrogates are kept as code points, as Python does; past Unicode is not text.
            if point > 0x10FFFF {
                return None;
            }
            points.push(point);
            at += 1 + width;
        }
    }
    let text: Option<String> = points.iter().map(|point| char::from_u32(*point)).collect();
    Some(match text {
        Some(text) => Node::Text(text),
        None => Node::CodePoints(points),
    })
}

/// What makes two pickled dict keys the same key, as Python compares them: equal numbers are
/// one key whatever their type (1, 1.0 and True), text and bytes by content, tuples by their
/// items. Keys this cannot compare (objects) are never merged.
fn dict_key(arena: &Arena, id: Id, depth: usize) -> Option<Vec<u8>> {
    /// Keys larger than this are shown as they are but not compared with others; comparing
    /// nested shared tuples could otherwise take unbounded memory.
    const MAX_KEY: usize = 4096;
    if depth > 16 {
        return None;
    }
    let number = |text: &str| text.trim_start_matches('+').parse::<i128>().ok();
    Some(match arena.nodes.get(id)? {
        Node::Null => b"n".to_vec(),
        Node::Bool(flag) => format!("i{}", i128::from(*flag)).into_bytes(),
        Node::Int(text) => format!("i{}", number(text)?).into_bytes(),
        Node::Float(value) if value.fract() == 0.0 && value.abs() < 1e30 => {
            format!("i{}", *value as i128).into_bytes()
        }
        // NaN equals nothing, not even another NaN, so NaN keys are never merged.
        Node::Float(value) if value.is_nan() => return None,
        Node::Float(value) => format!("f{}", value.to_bits()).into_bytes(),
        Node::Text(text) if text.len() > MAX_KEY => return None,
        Node::Text(text) => [b"s".as_slice(), text.as_bytes()].concat(),
        Node::Utf16(units) if units.len() * 2 > MAX_KEY => return None,
        Node::CodePoints(points) if points.len() * 4 > MAX_KEY => return None,
        Node::CodePoints(points) => std::iter::once(b'p')
            .chain(points.iter().flat_map(|point| point.to_le_bytes()))
            .collect(),
        Node::Utf16(units) => std::iter::once(b'u')
            .chain(units.iter().flat_map(|unit| unit.to_le_bytes()))
            .collect(),
        Node::Bytes(bytes) if bytes.len() > MAX_KEY => return None,
        Node::Bytes(bytes) => [b"b".as_slice(), bytes].concat(),
        Node::Tuple(items) => {
            let mut key = b"(".to_vec();
            for item in items {
                let part = dict_key(arena, *item, depth + 1)?;
                key.extend_from_slice(&part.len().to_le_bytes());
                key.extend_from_slice(&part);
                if key.len() > MAX_KEY {
                    return None;
                }
            }
            key
        }
        _ => return None,
    })
}

/// Adds members the way Python's set does: one equal to a member already there is dropped.
fn add_to_set(
    arena: &mut Arena,
    keys_of: &mut std::collections::HashMap<(Id, Vec<u8>), usize>,
    set: Id,
    items: Vec<Id>,
) -> Result<(), String> {
    for item in items {
        let fingerprint = dict_key(arena, item, 0);
        if let Some(fingerprint) = &fingerprint {
            if keys_of.contains_key(&(set, fingerprint.clone())) {
                continue;
            }
            arena.charge(fingerprint.len())?;
        }
        let Node::Set(members) = arena.get_mut(set)? else {
            return Err("not a pickle: items added to something that is not a set".into());
        };
        if let Some(fingerprint) = fingerprint {
            keys_of.insert((set, fingerprint), members.len());
        }
        members.push(item);
    }
    Ok(())
}

/// A little-endian two's complement integer in decimal, for up to 4,096 bytes.
fn decimal(bytes: &[u8]) -> Option<String> {
    if bytes.len() > 4096 {
        return None;
    }
    let negative = bytes.last().is_some_and(|byte| byte & 0x80 != 0);
    // The magnitude, most significant byte first.
    let mut magnitude = bytes.iter().rev().copied().collect::<Vec<_>>();
    if negative {
        // Two's complement: invert, then add one.
        for byte in &mut magnitude {
            *byte = !*byte;
        }
        for byte in magnitude.iter_mut().rev() {
            let (sum, carry) = byte.overflowing_add(1);
            *byte = sum;
            if !carry {
                break;
            }
        }
    }
    let mut chunks = Vec::new();
    while magnitude.iter().any(|byte| *byte != 0) {
        // Divide by 10^9, keeping the remainder as the next nine digits.
        let mut remainder = 0_u64;
        for byte in &mut magnitude {
            let current = (remainder << 8) | u64::from(*byte);
            *byte = (current / 1_000_000_000) as u8;
            remainder = current % 1_000_000_000;
        }
        chunks.push(remainder);
    }
    let mut text = if negative {
        "-".to_string()
    } else {
        String::new()
    };
    match chunks.split_last() {
        None => text.push('0'),
        Some((first, rest)) => {
            text.push_str(&first.to_string());
            for chunk in rest.iter().rev() {
                text.push_str(&format!("{chunk:09}"));
            }
        }
    }
    Some(text)
}

/// Sets entries the way Python's dict does: a key already present keeps its place and takes
/// the new value.
fn set_items(
    arena: &mut Arena,
    keys_of: &mut std::collections::HashMap<(Id, Vec<u8>), usize>,
    map: Id,
    pairs: Vec<(Id, Id)>,
) -> Result<(), String> {
    for (key, value) in pairs {
        let fingerprint = dict_key(arena, key, 0);
        // Remembered fingerprints are held text too.
        if let Some(fingerprint) = &fingerprint {
            arena.charge(fingerprint.len())?;
        }
        let Node::Map(entries) = arena.get_mut(map)? else {
            return Err("not a pickle: entries set on something that is not a dict".into());
        };
        match fingerprint {
            Some(fingerprint) => match keys_of.get(&(map, fingerprint.clone())) {
                Some(&position) => {
                    if let Some(entry) = entries.get_mut(position) {
                        entry.1 = value;
                    }
                }
                None => {
                    keys_of.insert((map, fingerprint), entries.len());
                    entries.push((key, value));
                }
            },
            None => entries.push((key, value)),
        }
    }
    Ok(())
}

/// Java's modified UTF-8: NUL as C0 80, characters outside the BMP as two 3-byte surrogates.
/// A lone surrogate or a broken sequence is kept visible as \u{…} rather than replaced.
fn modified_utf8(bytes: &[u8]) -> Option<String> {
    Some(
        char::decode_utf16(modified_utf8_units(bytes)?)
            .map(|decoded| match decoded {
                Ok(character) => character.to_string(),
                Err(error) => format!("\\u{{{:x}}}", error.unpaired_surrogate()),
            })
            .collect(),
    )
}

/// Java's modified UTF-8 as UTF-16 units; `None` when it is malformed (a stray byte, a cut
/// sequence or an overlong form other than C0 80 for NUL).
fn modified_utf8_units(bytes: &[u8]) -> Option<Vec<u16>> {
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
                u16::from(first)
            }
            0xC0..=0xDF => {
                let second = continuation(at + 1)?;
                let unit = (u16::from(first & 0x1F) << 6) | u16::from(second & 0x3F);
                if unit != 0 && unit < 0x80 {
                    return None;
                }
                at += 2;
                unit
            }
            0xE0..=0xEF => {
                let second = continuation(at + 1)?;
                let third = continuation(at + 2)?;
                let unit = (u16::from(first & 0x0F) << 12)
                    | (u16::from(second & 0x3F) << 6)
                    | u16::from(third & 0x3F);
                if unit < 0x800 {
                    return None;
                }
                at += 3;
                unit
            }
            _ => return None,
        };
        units.push(unit);
    }
    Some(units)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quoted(text: &str) -> String {
        serde_json::Value::String(text.to_string()).to_string()
    }

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
        // php 8.3: r: takes a number for later references, R: does not.
        let refs = "a:4:{i:0;O:8:\"stdClass\":0:{}i:1;r:2;i:2;O:8:\"stdClass\":0:{}i:3;r:4;}";
        let text = decode(Decoder::Php, refs.as_bytes()).unwrap();
        assert_eq!(text.matches("stdClass {}").count(), 4, "{text}");
        let hard = "a:5:{i:0;s:1:\"x\";i:1;R:2;i:2;O:8:\"stdClass\":0:{}i:3;r:3;i:4;s:4:\"tail\";}";
        let text = decode(Decoder::Php, hard.as_bytes()).unwrap();
        assert_eq!(
            (
                text.matches(r#""x""#).count(),
                text.matches("stdClass {}").count()
            ),
            (2, 2),
            "{text}"
        );
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
        assert!(decode(Decoder::Pickle, b"L\n.").is_err(), "an empty LONG");
        assert_eq!(decode(Decoder::Pickle, b"L-12L\n.").unwrap(), "-12");
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
        assert_eq!(
            modified_utf8(b"a\xc0\x80\xed\xa0\xbd\xed\xb8\x80").as_deref(),
            Some("a\0😀")
        );
        assert_eq!(
            modified_utf8(b"\xed\xa0\xbd").as_deref(),
            Some("\\u{d83d}"),
            "a lone surrogate stays visible"
        );
        assert_eq!(modified_utf8(b"\xc1\x81"), None, "overlong");
        assert!(
            decode(Decoder::Java, b"\xac\xed\x00\x05\x74\x00\x01\xff").is_err(),
            "a stray byte"
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
    fn test_java_write_object_without_default_fields() {
        // Java 21: a class with `int x` whose writeObject only writes writeInt(42) and
        // writeUTF("hi"), never defaultWriteObject.
        let bytes = hex(
            "aced00057372000b4f776e24436f756e746572873d94ab468ea01903000149000178787077080000002a0002686978",
        );
        let text = decode(Decoder::Java, &bytes).unwrap();
        assert!(text.contains("Own$Counter custom data"), "{text}");
        assert!(text.contains(r#"\x00\x00\x00*\x00\x02hi"#), "{text}");
        assert!(!text.contains("x: "), "no made-up field value: {text}");
    }

    #[test]
    fn test_names_and_hierarchies_are_bounded() {
        let mut names = b"Vx\n".to_vec();
        for _ in 0..30 {
            names.extend_from_slice(b"2\x93");
        }
        names.push(b'.');
        assert!(
            decode(Decoder::Pickle, &names).is_err(),
            "names stop at the limit"
        );
        // Class descriptions each naming the previous one as superclass, reached by reference.
        let mut classes = b"\xac\xed\x00\x05".to_vec();
        for index in 0..100_u32 {
            classes.extend_from_slice(&[0x72, 0x00, 0x01, b'C']);
            classes.extend_from_slice(&[0; 8]);
            classes.extend_from_slice(&[0x02, 0x00, 0x00, 0x78]);
            if index == 0 {
                classes.push(0x70);
            } else {
                classes.push(0x71);
                classes.extend_from_slice(&(0x7E_0000 + index - 1).to_be_bytes());
            }
        }
        assert!(
            decode(Decoder::Java, &classes).is_err(),
            "hierarchy deeper than the limit"
        );
        // int x, writeObject writing only writeShort(42) and writeObject(null): ambiguous, so no
        // value is made up for x.
        let ambiguous = hex(
            "aced00057372000b4f776e24436f756e746572873d94ab468ea0190300014900017878707702002a7078",
        );
        let text = decode(Decoder::Java, &ambiguous).unwrap();
        assert!(!text.contains("x: "), "{text}");
        assert!(
            text.contains("may or may not hold them") && text.contains(r#"\x00*"#),
            "{text}"
        );
    }

    #[test]
    fn test_shared_class_text_is_counted() {
        // One class with a 60,000-byte name, then many objects of it by reference.
        let mut bytes = b"\xac\xed\x00\x05\x73\x72".to_vec();
        bytes.extend_from_slice(&60_000_u16.to_be_bytes());
        bytes.extend(std::iter::repeat_n(b'N', 60_000));
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&[0x02, 0x00, 0x00, 0x78, 0x70]);
        for _ in 0..5_000 {
            bytes.extend_from_slice(&[0x73, 0x71, 0x00, 0x7e, 0x00, 0x00]);
        }
        assert!(
            decode(Decoder::Java, &bytes).is_err(),
            "copies of the name are counted"
        );
        // A class with one Object field whose writeObject wrote only a string of its own:
        // whatever is shown says it rests on an assumption.
        let mut own = b"\xac\xed\x00\x05\x73\x72\x00\x01A".to_vec();
        own.extend_from_slice(&[0; 8]);
        own.extend_from_slice(&[0x03, 0x00, 0x01, b'L', 0x00, 0x01, b'x', 0x74, 0x00, 0x12]);
        own.extend_from_slice(b"Ljava/lang/Object;");
        own.extend_from_slice(&[0x78, 0x70, 0x74, 0x00, 0x06]);
        own.extend_from_slice(b"custom");
        own.push(0x78);
        let text = decode(Decoder::Java, &own).unwrap();
        assert!(
            text.contains("assuming its writeObject called defaultWriteObject"),
            "{text}"
        );
    }

    #[test]
    fn test_every_copy_is_counted() {
        // Many lists, each holding one value DUP'd 199,990 times: refused, not ~1.6 GB of ids.
        let mut lists = b"]".to_vec();
        for _ in 0..10 {
            lists.extend_from_slice(b"(N");
            lists.extend(std::iter::repeat_n(b'2', 199_990));
            lists.extend_from_slice(b"la");
        }
        lists.push(b'.');
        assert!(decode(Decoder::Pickle, &lists).is_err());
        // A string longer than the text budget is refused before it is copied.
        let mut huge = vec![0xdb];
        huge.extend_from_slice(&(70_u32 * 1024 * 1024).to_be_bytes());
        huge.extend(std::iter::repeat_n(b'a', 70 * 1024 * 1024));
        assert!(
            decode(Decoder::Msgpack, &huge)
                .unwrap_err()
                .contains("MB of text")
        );
        // A PHP length error does not quote the whole bad length.
        let long = format!("s:{}:\"\";", "9x".repeat(1_000_000));
        assert!(decode(Decoder::Php, long.as_bytes()).unwrap_err().len() < 200);
    }

    #[test]
    fn test_more_growth_is_bounded() {
        let mut properties = b"O:1:\"A\":1100000:{".to_vec();
        for _ in 0..1_100_000 {
            properties.extend_from_slice(b"s:0:\"\";r:1;");
        }
        properties.push(b'}');
        assert!(
            decode(Decoder::Php, &properties).is_err(),
            "properties are counted"
        );
        let mut unicode = b"V".to_vec();
        unicode.extend(std::iter::repeat_n(b'a', 70 * 1024 * 1024));
        unicode.extend_from_slice(b"\n.");
        assert!(
            decode(Decoder::Pickle, &unicode)
                .unwrap_err()
                .contains("MB of text")
        );
        // python3: pickle.dumps(collections.OrderedDict(secret=7), protocol=2) keeps its key.
        let ordered = hex(
            "800263636f6c6c656374696f6e730a4f726465726564446963740a710029527101580600000073656372657471024b07732e",
        );
        let text = decode(Decoder::Pickle, &ordered).unwrap();
        assert!(text.contains(r#""secret": 7"#), "{text}");
    }

    #[test]
    fn test_char_and_append_work() {
        // A class with one char field holding a lone high surrogate.
        let mut lone = b"\xac\xed\x00\x05\x73\x72\x00\x01A".to_vec();
        lone.extend_from_slice(&[0; 8]);
        lone.extend_from_slice(&[
            0x02, 0x00, 0x01, b'C', 0x00, 0x01, b'c', 0x78, 0x70, 0xd8, 0x3d,
        ]);
        assert!(
            decode(Decoder::Java, &lone)
                .unwrap()
                .contains(r#""\u{d83d}""#)
        );
        // One object with many BUILD states, then many APPENDs to it: no per-append field search.
        let mut work = b"cm\nC\n)R\x94".to_vec();
        for _ in 0..200_000 {
            work.extend_from_slice(b"2b");
        }
        for _ in 0..299_999 {
            work.extend_from_slice(b"h\x00a");
        }
        work.push(b'.');
        let started = std::time::Instant::now();
        assert!(decode(Decoder::Pickle, &work).is_ok(), "within the budgets");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_exact_scalars_and_escaped_cuts() {
        // python3: pickle.dumps(chr(0xd800), protocol=4)
        assert_eq!(
            decode(
                Decoder::Pickle,
                &hex("80049507000000000000008c03eda080942e")
            )
            .unwrap(),
            r#""\u{d800}""#
        );
        assert!(
            decode(Decoder::Pickle, b"\x8c\x01\xff.").is_err(),
            "not UTF-8"
        );
        assert!(decode(Decoder::Php, b"b:garbage;").is_err());
        assert!(decode(Decoder::Php, b"i:garbage;").is_err());
        assert!(decode(Decoder::Pickle, b"Igarbage\n.").is_err());
        assert_eq!(decode(Decoder::Php, b"i:-12;").unwrap(), "-12");
        // A value whose escapes are longer than the limit is cut where the limit is reached,
        // not dropped.
        let mut bin = vec![0xc6, 0x00, 0x04, 0x93, 0xe0];
        bin.extend(std::iter::repeat_n(0_u8, 300_000));
        let text = decode(Decoder::Msgpack, &bin).unwrap();
        assert!(
            text.starts_with(r#"b"\x00\x00"#) && text.ends_with("KB of text"),
            "{}",
            &text[..40]
        );
        // Descriptors with many fields count those fields.
        let mut fields = b"\xac\xed\x00\x05".to_vec();
        for _ in 0..20 {
            fields.extend_from_slice(&[0x72, 0x00, 0x01, b'D']);
            fields.extend_from_slice(&[0; 8]);
            fields.push(0x02);
            fields.extend_from_slice(&u16::MAX.to_be_bytes());
            for _ in 0..u16::MAX {
                fields.extend_from_slice(&[b'I', 0x00, 0x00]);
            }
            fields.extend_from_slice(&[0x78, 0x70]);
        }
        assert!(decode(Decoder::Java, &fields).is_err());
    }

    #[test]
    fn test_dict_and_text_semantics() {
        // Python: {'x': 1} then d['x'] = 2 is {'x': 2}; 1, 1.0 and True are one key.
        assert_eq!(
            decode(Decoder::Pickle, b"}Vx\nK\x01sVx\nK\x02s.").unwrap(),
            "{\n  \"x\": 2,\n}"
        );
        let numbers = decode(
            Decoder::Pickle,
            b"(K\x01K\x01G?\xf0\x00\x00\x00\x00\x00\x00K\x02\x88K\x03d.",
        )
        .unwrap();
        assert_eq!(numbers, "{\n  1: 3,\n}", "{numbers}");
        // raw-unicode-escape: two backslashes stay literal, one starts an escape.
        let text = |node: Option<Node>| match node {
            Some(Node::Text(text)) => Some(text),
            _ => None,
        };
        assert_eq!(
            text(raw_unicode_escape(b"\\\\u0041")).as_deref(),
            Some("\\\\u0041")
        );
        assert_eq!(
            text(raw_unicode_escape(b"\\\\\\u0041")).as_deref(),
            Some("\\\\A")
        );
        // python3: pickle.dumps(chr(0xd800), protocol=0) is b"V\\ud800\np0\n."
        assert_eq!(
            decode(Decoder::Pickle, b"V\\ud800\np0\n.").unwrap(),
            r#""\u{d800}""#
        );
        // PHP keys and property names are bytes; ones that are not text stay distinct.
        let keys = decode(Decoder::Php, b"a:2:{s:1:\"\xff\";i:1;s:1:\"\xfe\";i:2;}").unwrap();
        assert!(
            keys.contains(r#"b"\xff""#) && keys.contains(r#"b"\xfe""#),
            "{keys}"
        );
        let property = decode(Decoder::Php, b"O:1:\"A\":1:{s:1:\"\xff\";i:1;}").unwrap();
        assert!(property.contains(r#""\xff": 1"#), "{property}");
    }

    #[test]
    fn test_keys_python_keeps_apart() {
        // python3: {chr(0xd800): 1, "\\u{d800}": 2} has two keys, and so does
        // {float("nan"): 1, float("nan"): 2}.
        let surrogate = decode(
            Decoder::Pickle,
            &hex("8004951a000000000000007d94288c03eda080944b018c085c757b643830307d944b02752e"),
        )
        .unwrap();
        assert!(
            surrogate.contains(": 1") && surrogate.contains(": 2"),
            "{surrogate}"
        );
        let nan = decode(
            Decoder::Pickle,
            &hex("8004951b000000000000007d9428477ff80000000000004b01477ff80000000000004b02752e"),
        )
        .unwrap();
        assert!(nan.contains(": 1") && nan.contains(": 2"), "{nan}");
        // A key of nested shared tuples is not expanded to compare it.
        let mut nested = b"\x80\x04}\x94".to_vec();
        nested.push(0x8d);
        nested.extend_from_slice(&(1_000_000_u64).to_le_bytes());
        nested.extend(std::iter::repeat_n(b'a', 1_000_000));
        nested.extend_from_slice(&[0x94, b'0']);
        for index in 1..=15_u8 {
            nested.extend_from_slice(&[b'h', index, b'h', index, 0x86, 0x94, b'0']);
        }
        nested.extend_from_slice(b"h\x10K\x01s.");
        let started = std::time::Instant::now();
        assert!(decode(Decoder::Pickle, &nested).is_ok());
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_code_points_and_whole_cuts() {
        // python3: {chr(0xd800) + chr(0xdc00) + chr(0xd800): 1, chr(0x10000) + chr(0xd800): 2}
        // has two keys (the first is three code points, not a pair).
        let pair = decode(
            Decoder::Pickle,
            &hex(include_str!("../data/pickle_pair.hex").trim()),
        )
        .unwrap();
        assert!(pair.contains(": 1") && pair.contains(": 2"), "{pair}");
        assert!(
            pair.contains(r#""\u{d800}\u{dc00}\u{d800}""#) && pair.contains("𐀀"),
            "{pair}"
        );
        // A number longer than the limit is cut, not dropped.
        let mut long = b"L".to_vec();
        long.extend(std::iter::repeat_n(b'9', 2_000_000));
        long.extend_from_slice(b"\n.");
        let text = decode(Decoder::Pickle, &long).unwrap();
        assert!(text.starts_with("9999") && text.ends_with("KB of text"));
        assert!(
            decode(Decoder::Pickle, b"0N.").is_err(),
            "POP on an empty stack"
        );
    }

    #[test]
    fn test_fingerprints_and_names_are_bounded() {
        // {(s, i): s for i in range(99_000)} with a 4,000-character s: the remembered keys
        // count against the text budget.
        let mut keys = b"\x80\x04}\x94X\xa0\x0f\x00\x00".to_vec();
        keys.extend(std::iter::repeat_n(b'a', 4000));
        keys.extend_from_slice(b"\x940");
        for index in 0..99_000_i32 {
            keys.extend_from_slice(b"h\x01J");
            keys.extend_from_slice(&index.to_le_bytes());
            keys.extend_from_slice(b"\x86h\x01s");
        }
        keys.push(b'.');
        assert!(
            decode(Decoder::Pickle, &keys)
                .unwrap_err()
                .contains("MB of text")
        );
        let class = format!("O:5000:\"{}\":0:{{}}", "\u{ff}".repeat(2500));
        assert!(
            decode(Decoder::Php, class.as_bytes()).is_err(),
            "a name past the limit"
        );
        let mut global = b"c".to_vec();
        global.extend(std::iter::repeat_n(0xff, 5000));
        global.extend_from_slice(b"\nx\n.");
        assert!(decode(Decoder::Pickle, &global).is_err());
    }

    #[test]
    fn test_sets_ints_and_pop_like_python() {
        // Each expected value is what python3's pickle.loads returns.
        assert_eq!(
            decode(Decoder::Pickle, b"\x8f(K\x01K\x01\x90.").unwrap(),
            "{\n  1,\n}"
        );
        assert_eq!(decode(Decoder::Pickle, b"(K\x01(K\x020t.").unwrap(), "()");
        assert_eq!(
            decode(Decoder::Pickle, b"(K\x01(0t.").unwrap(),
            "(\n  1,\n)"
        );
        assert_eq!(
            decode(Decoder::Pickle, b"(K\x01(K\x021t.").unwrap(),
            "(\n  1,\n)"
        );
        assert_eq!(
            decode(
                Decoder::Pickle,
                &hex("80049513000000000000008a10000000000000000000000000000000012e")
            )
            .unwrap(),
            "1329227995784915872903807060280344576",
            "2**120"
        );
        assert_eq!(
            decode(
                Decoder::Pickle,
                &hex("80028a1a00000000000000000000000000000000000000000000000000ff2e")
            )
            .unwrap(),
            "-1606938044258990275541962092341162602522202993782792835301376",
            "-(2**200)"
        );
        assert_eq!(decimal(&[]).as_deref(), Some("0"));
        assert_eq!(decimal(&[0xff]).as_deref(), Some("-1"));
    }

    #[test]
    fn test_reference_growth_is_bounded() {
        // DUP and BUILD on one object: a few values, many fields.
        let mut build = b"cm\nC\n)R".to_vec();
        for _ in 0..1_100_000 {
            build.extend_from_slice(b"2b");
        }
        build.push(b'.');
        assert!(decode(Decoder::Pickle, &build).is_err());
        // A Java string, then many references to it.
        let mut references = b"\xac\xed\x00\x05\x74\x00\x01a".to_vec();
        for _ in 0..1_100_000 {
            references.extend_from_slice(&[0x71, 0x00, 0x7e, 0x00, 0x00]);
        }
        assert!(decode(Decoder::Java, &references).is_err());
        // A large binary value is cut without escaping all of it.
        let mut big = vec![0xc6, 0x00, 0x60, 0x00, 0x00];
        big.extend(std::iter::repeat_n(0_u8, 0x0060_0000));
        let started = std::time::Instant::now();
        let text = decode(Decoder::Msgpack, &big).unwrap();
        assert!(text.ends_with("KB of text") && text.len() < 1_100_000);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
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
