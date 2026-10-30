//! The query syntax Compass accepts: unquoted keys, single quotes, `ObjectId('…')`, `ISODate('…')`, `/regex/i` …
//! It is rewritten to Extended JSON and parsed as data; no JavaScript is ever run.

use anyhow::{Result, anyhow, bail};
use mongodb::bson::{Bson, Document};

/// Parses a filter, projection or sort as typed in Compass. Empty means `{}`.
pub fn parse_query(text: &str) -> Result<Document> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(Document::new());
    }
    let json = to_extended_json(text)?;
    let value: serde_json::Value = serde_json::from_str(&json).map_err(|error| anyhow!("Could not read the query: {error}"))?;
    match Bson::try_from(value).map_err(|error| anyhow!("Not a valid value: {error}"))? {
        Bson::Document(document) => Ok(document),
        _ => bail!("Expected an object like {{ field: 'value' }}"),
    }
}

fn json_string(text: &str) -> String {
    serde_json::Value::String(text.to_string()).to_string()
}

struct Reader<'a> {
    chars: Vec<char>,
    position: usize,
    output: String,
    _text: &'a str,
    /// Keys seen in each open object, so a key given twice is refused instead of silently keeping the last.
    keys: Vec<Vec<String>>,
}

impl Reader<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.position).copied()
    }

    fn next_significant(&self) -> Option<char> {
        self.chars[self.position..].iter().copied().find(|character| !character.is_whitespace())
    }

    /// Whether a value (not an operator or key) is expected here, which decides what `/` means.
    fn expects_value(&self) -> bool {
        matches!(self.output.trim_end().chars().last(), Some(':' | ',' | '[' | '(') | None)
    }

    fn note_key(&mut self, key: &str) -> Result<()> {
        if let Some(keys) = self.keys.last_mut() {
            if keys.iter().any(|seen| seen == key) {
                bail!("“{key}” appears twice in the same object");
            }
            keys.push(key.to_string());
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.position += 1;
        }
    }

    fn read_string(&mut self) -> Result<String> {
        let quote = self.peek().ok_or_else(|| anyhow!("Expected a string"))?;
        self.position += 1;
        let mut value = String::new();
        while let Some(character) = self.peek() {
            self.position += 1;
            match character {
                '\\' => {
                    let escaped = self.peek().ok_or_else(|| anyhow!("Unfinished escape in a string"))?;
                    self.position += 1;
                    value.push(match escaped {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        other => other,
                    });
                }
                character if character == quote => return Ok(value),
                character => value.push(character),
            }
        }
        bail!("A string is missing its closing quote")
    }

    fn read_identifier(&mut self) -> String {
        let start = self.position;
        while self.peek().is_some_and(|character| character.is_alphanumeric() || character == '_' || character == '$' || character == '.') {
            self.position += 1;
        }
        self.chars[start..self.position].iter().collect()
    }

    /// The single argument of `ObjectId('…')` and friends: a string or a bare number.
    fn read_argument(&mut self) -> Result<Option<String>> {
        self.skip_whitespace();
        if self.peek() != Some('(') {
            bail!("Expected ( after a type name");
        }
        self.position += 1;
        self.skip_whitespace();
        let argument = match self.peek() {
            Some(')') => None,
            Some('\'' | '"') => Some(self.read_string()?),
            Some(_) => {
                let start = self.position;
                while self.peek().is_some_and(|character| character != ')' && !character.is_whitespace()) {
                    self.position += 1;
                }
                Some(self.chars[start..self.position].iter().collect())
            }
            None => bail!("Unfinished call"),
        };
        self.skip_whitespace();
        if self.peek() != Some(')') {
            bail!("Expected ) to close the call");
        }
        self.position += 1;
        Ok(argument)
    }

    fn read_regex(&mut self) -> Result<String> {
        self.position += 1;
        let mut pattern = String::new();
        loop {
            match self.peek() {
                None => bail!("A /regex/ is missing its closing slash"),
                Some('\\') => {
                    pattern.push('\\');
                    self.position += 1;
                    if let Some(character) = self.peek() {
                        pattern.push(character);
                        self.position += 1;
                    }
                }
                Some('/') => {
                    self.position += 1;
                    break;
                }
                Some(character) => {
                    pattern.push(character);
                    self.position += 1;
                }
            }
        }
        let mut options = String::new();
        while self.peek().is_some_and(|character| character.is_ascii_alphabetic()) {
            options.extend(self.peek());
            self.position += 1;
        }
        let mut sorted = options.chars().collect::<Vec<_>>();
        sorted.sort_unstable();
        Ok(format!(
            r#"{{"$regularExpression":{{"pattern":{},"options":{}}}}}"#,
            json_string(&pattern),
            json_string(&sorted.into_iter().collect::<String>())
        ))
    }
}

fn date_value(text: &str) -> String {
    // Compass accepts a bare date; Extended JSON needs a full timestamp.
    if text.len() == 10 { format!("{text}T00:00:00Z") } else { text.to_string() }
}

/// Rewrites Compass query text into Extended JSON text.
pub fn to_extended_json(text: &str) -> Result<String> {
    let mut reader = Reader { chars: text.chars().collect(), position: 0, output: String::new(), _text: text, keys: Vec::new() };
    while let Some(character) = reader.peek() {
        match character {
            '\'' | '"' => {
                let value = reader.read_string()?;
                if reader.next_significant() == Some(':') {
                    reader.note_key(&value)?;
                }
                reader.output.push_str(&json_string(&value));
            }
            '{' | '}' => {
                if character == '{' {
                    reader.keys.push(Vec::new());
                } else {
                    reader.keys.pop();
                }
                reader.output.push(character);
                reader.position += 1;
            }
            '/' if reader.expects_value() => {
                let regex = reader.read_regex()?;
                reader.output.push_str(&regex);
            }
            character if character.is_alphabetic() || character == '_' || character == '$' => {
                let identifier = reader.read_identifier();
                // Only a key is followed by a colon.
                if reader.next_significant() == Some(':') {
                    reader.note_key(&identifier)?;
                    reader.output.push_str(&json_string(&identifier));
                    continue;
                }
                let constructed = match identifier.as_str() {
                    "true" | "false" | "null" => identifier.clone(),
                    "new" => continue,
                    "ObjectId" => {
                        let id = reader.read_argument()?.ok_or_else(|| anyhow!("ObjectId() needs an id"))?;
                        format!(r#"{{"$oid":{}}}"#, json_string(&id))
                    }
                    "ISODate" | "Date" => {
                        let date = reader.read_argument()?.ok_or_else(|| anyhow!("{identifier}() needs a date"))?;
                        format!(r#"{{"$date":{}}}"#, json_string(&date_value(&date)))
                    }
                    "NumberLong" | "Long" => {
                        let number = reader.read_argument()?.ok_or_else(|| anyhow!("{identifier}() needs a number"))?;
                        format!(r#"{{"$numberLong":{}}}"#, json_string(&number))
                    }
                    "NumberInt" | "Int32" => {
                        let number = reader.read_argument()?.ok_or_else(|| anyhow!("{identifier}() needs a number"))?;
                        format!(r#"{{"$numberInt":{}}}"#, json_string(&number))
                    }
                    "UUID" => {
                        let uuid = reader.read_argument()?.ok_or_else(|| anyhow!("UUID() needs a value"))?;
                        format!(r#"{{"$uuid":{}}}"#, json_string(&uuid))
                    }
                    "NumberDecimal" | "Decimal128" => {
                        let number = reader.read_argument()?.ok_or_else(|| anyhow!("{identifier}() needs a number"))?;
                        format!(r#"{{"$numberDecimal":{}}}"#, json_string(&number))
                    }
                    other => bail!("Vilo does not understand “{other}” here. Use a value, or ObjectId(), ISODate(), NumberLong(), NumberDecimal()."),
                };
                reader.output.push_str(&constructed);
            }
            character => {
                reader.output.push(character);
                reader.position += 1;
            }
        }
    }
    Ok(reader.output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{Regex, doc, oid::ObjectId};

    #[test]
    fn test_compass_syntax() {
        let id = ObjectId::parse_str("6ae41e934acd77bafc90923e").unwrap();
        assert_eq!(parse_query("{_id: ObjectId('6ae41e934acd77bafc90923e')}").unwrap(), doc! { "_id": id });
        assert_eq!(parse_query(r#"{"data.playerId": "1205591845"}"#).unwrap(), doc! { "data.playerId": "1205591845" });
        assert_eq!(parse_query("{data.playerId: '1205591845', status: 'Success'}").unwrap(), doc! { "data.playerId": "1205591845", "status": "Success" });
        assert_eq!(
            parse_query("{name: /^vip\\d+/i}").unwrap(),
            doc! { "name": Bson::RegularExpression(Regex { pattern: "^vip\\d+".into(), options: "i".into() }) }
        );
        let created = parse_query("{createTime: {$gte: ISODate('2026-10-30T04:34:55.412+00:00'), $lt: new Date('2026-10-31')}}").unwrap();
        let range = created.get_document("createTime").unwrap();
        assert!(matches!(range.get("$gte"), Some(Bson::DateTime(_))));
        assert!(matches!(range.get("$lt"), Some(Bson::DateTime(_))));
        assert_eq!(parse_query("{n: NumberLong('9007199254740993')}").unwrap(), doc! { "n": 9_007_199_254_740_993_i64 });
        assert!(matches!(parse_query("{a: NumberDecimal('0.10')}").unwrap().get("a"), Some(Bson::Decimal128(_))));
        assert_eq!(parse_query("{ok: true, gone: null, n: 3}").unwrap(), doc! { "ok": true, "gone": Bson::Null, "n": 3 });
        assert_eq!(parse_query("{status: {$in: ['a', \"b\"]}}").unwrap(), doc! { "status": { "$in": ["a", "b"] } });
        assert_eq!(parse_query("  ").unwrap(), Document::new());
        // A quote inside a string survives the rewrite.
        assert_eq!(parse_query(r#"{note: 'it\'s "fine"'}"#).unwrap(), doc! { "note": "it's \"fine\"" });
        assert!(parse_query("{$where: function() { return 1 }}").is_err());
        assert!(parse_query("{a: 1, 'a': 2}").is_err());
        assert!(parse_query("{a: {b: 1}, b: 2}").is_ok());
        assert!(matches!(
            parse_query("{id: UUID('00010203-0405-0607-0809-0a0b0c0d0e0f')}").unwrap().get("id"),
            Some(Bson::Binary(binary)) if binary.bytes.len() == 16
        ));
        assert!(parse_query("{a: 'unclosed}").is_err());
        assert!(parse_query("[1, 2]").is_err());
    }
}
