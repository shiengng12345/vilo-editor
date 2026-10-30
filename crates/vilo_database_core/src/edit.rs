//! Single-document writes: the text a document is edited as, and the guarded update, delete and
//! insert built from it. Nothing here talks to a server.

use anyhow::{Result, anyhow, bail};
use mongodb::bson::{Bson, Document, doc};

use crate::shell::parse_query;

/// The document as Compass-style text that parses back to exactly the same document, or `None`
/// when some value would not survive the trip; such a document is not offered for editing.
pub fn to_edit_text(document: &Document) -> Option<String> {
    let mut text = String::new();
    write_document(document, 0, &mut text);
    (parse_query(&text).ok()? == *document).then_some(text)
}

fn write_key(key: &str, text: &mut String) {
    let plain = key
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    if plain {
        text.push_str(key);
    } else {
        text.push_str(&serde_json::Value::String(key.to_string()).to_string());
    }
}

fn write_document(document: &Document, depth: usize, text: &mut String) {
    if document.is_empty() {
        text.push_str("{}");
        return;
    }
    text.push_str("{\n");
    for (index, (key, value)) in document.iter().enumerate() {
        text.push_str(&"  ".repeat(depth + 1));
        write_key(key, text);
        text.push_str(": ");
        write_value(value, depth + 1, text);
        if index + 1 < document.len() {
            text.push(',');
        }
        text.push('\n');
    }
    text.push_str(&"  ".repeat(depth));
    text.push('}');
}

fn write_value(value: &Bson, depth: usize, text: &mut String) {
    match value {
        Bson::Document(document) => write_document(document, depth, text),
        Bson::Array(items) => {
            text.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    text.push_str(", ");
                }
                write_value(item, depth, text);
            }
            text.push(']');
        }
        Bson::String(string) => {
            text.push_str(&serde_json::Value::String(string.clone()).to_string())
        }
        Bson::Int32(number) => text.push_str(&number.to_string()),
        Bson::Int64(number) => text.push_str(&format!("NumberLong('{number}')")),
        // A whole double keeps its point so it reads back as a double, not an int.
        Bson::Double(number)
            if number.is_finite() && number.fract() == 0.0 && number.abs() < 1e15 =>
        {
            text.push_str(&format!("{number:.1}"))
        }
        Bson::Double(number) if number.is_finite() => text.push_str(&format!("{number:?}")),
        Bson::Boolean(flag) => text.push_str(if *flag { "true" } else { "false" }),
        Bson::Null => text.push_str("null"),
        Bson::ObjectId(id) => text.push_str(&format!("ObjectId('{}')", id.to_hex())),
        Bson::DateTime(date) => match date.try_to_rfc3339_string() {
            Ok(iso) => text.push_str(&format!("ISODate('{iso}')")),
            Err(_) => write_extended(value, text),
        },
        _ => write_extended(value, text),
    }
}

/// Rare types are written as canonical Extended JSON, which the query parser reads as data.
fn write_extended(value: &Bson, text: &mut String) {
    text.push_str(&value.clone().into_canonical_extjson().to_string());
}

/// The aggregation `$type` name, so a guard can tell Int32 from Int64 where `$eq` cannot.
fn type_name(value: Option<&Bson>) -> &'static str {
    match value {
        None => "missing",
        Some(Bson::Double(_)) => "double",
        Some(Bson::String(_)) => "string",
        Some(Bson::Document(_)) => "object",
        Some(Bson::Array(_)) => "array",
        Some(Bson::Binary(_)) => "binData",
        Some(Bson::Undefined) => "undefined",
        Some(Bson::ObjectId(_)) => "objectId",
        Some(Bson::Boolean(_)) => "bool",
        Some(Bson::DateTime(_)) => "date",
        Some(Bson::Null) => "null",
        Some(Bson::RegularExpression(_)) => "regex",
        Some(Bson::JavaScriptCode(_)) => "javascript",
        Some(Bson::JavaScriptCodeWithScope(_)) => "javascriptWithScope",
        Some(Bson::Int32(_)) => "int",
        Some(Bson::Timestamp(_)) => "timestamp",
        Some(Bson::Int64(_)) => "long",
        Some(Bson::Decimal128(_)) => "decimal",
        Some(Bson::MinKey) => "minKey",
        Some(Bson::MaxKey) => "maxKey",
        Some(Bson::Symbol(_)) => "symbol",
        Some(Bson::DbPointer(_)) => "dbPointer",
    }
}

/// A filter matching `_id` and, for each named top-level field, exactly the value it had in
/// `original` (or its absence). Values go through `$literal`, so a stored `{ $gt: 1 }` or regex is
/// compared as data, never run as a condition.
fn guard(original: &Document, fields: &[&str]) -> Result<Document> {
    let id = original
        .get("_id")
        .ok_or_else(|| anyhow!("The document has no _id"))?;
    let mut checks = Vec::new();
    for field in fields {
        let value = original.get(*field);
        let path = format!("${field}");
        checks.push(Bson::Document(
            doc! { "$eq": [{ "$type": path.clone() }, type_name(value)] },
        ));
        if let Some(value) = value {
            checks.push(Bson::Document(
                doc! { "$eq": [path, { "$literal": value.clone() }] },
            ));
        }
    }
    let mut filter = doc! { "_id": { "$eq": id.clone() } };
    if !checks.is_empty() {
        filter.insert("$expr", doc! { "$and": checks });
    }
    Ok(filter)
}

/// Top-level keys that could be read as a path or an operator are left alone in this release.
fn editable_key(key: &str) -> bool {
    !key.contains('.') && !key.starts_with('$')
}

/// The guarded `updateOne` that turns `original` into `edited`: changed and added fields are
/// `$set`, removed ones `$unset`, and the filter fails if any of them changed since loading.
pub fn build_update(original: &Document, edited: &Document) -> Result<(Document, Document)> {
    if original.get("_id") != edited.get("_id") {
        bail!(
            "_id cannot be changed or removed. Clone the document and delete the old one instead."
        );
    }
    let mut set = Document::new();
    let mut unset = Document::new();
    let mut touched = Vec::new();
    for (key, value) in edited {
        if original.get(key) != Some(value) {
            set.insert(key.clone(), value.clone());
            touched.push(key.as_str());
        }
    }
    for key in original.keys() {
        if !edited.contains_key(key) {
            unset.insert(key.clone(), "");
            touched.push(key.as_str());
        }
    }
    if touched.is_empty() {
        bail!("Nothing changed.");
    }
    if let Some(key) = touched.iter().find(|key| !editable_key(key)) {
        bail!("The field “{key}” has a . or $ in its name; Vilo does not edit such fields yet.");
    }
    let filter = guard(original, &touched)?;
    let mut update = Document::new();
    if !set.is_empty() {
        update.insert("$set", set);
    }
    if !unset.is_empty() {
        update.insert("$unset", unset);
    }
    Ok((filter, update))
}

/// The guarded `deleteOne` filter: the document goes only if no top-level field changed since it
/// was loaded.
pub fn build_delete(original: &Document) -> Result<Document> {
    let fields = original
        .keys()
        .filter(|key| *key != "_id")
        .map(String::as_str)
        .collect::<Vec<_>>();
    if let Some(key) = fields.iter().find(|key| !editable_key(key)) {
        bail!("The field “{key}” has a . or $ in its name; Vilo cannot check it before deleting.");
    }
    guard(original, &fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{DateTime, Decimal128, oid::ObjectId};

    fn sample() -> Document {
        doc! {
            "_id": ObjectId::parse_str("629823b3313df50047e05afd").unwrap(),
            "name": "a \"quoted\" name",
            "count": 1_i32,
            "big": 9_007_199_254_740_993_i64,
            "ratio": 2.0_f64,
            "half": 0.5_f64,
            "money": "1.10".parse::<Decimal128>().unwrap(),
            "when": DateTime::from_millis(1_654_473_600_123),
            "tags": ["x", 2_i32, { "deep": [1_i64] }],
            "nested": { "a": null, "weird key": true },
            "pattern": Bson::RegularExpression(mongodb::bson::Regex { pattern: "^a".into(), options: "i".into() }),
            "bytes": Bson::Binary(mongodb::bson::Binary { subtype: mongodb::bson::spec::BinarySubtype::Generic, bytes: vec![1, 2, 3] }),
        }
    }

    #[test]
    fn test_edit_text_round_trips_types() {
        let document = sample();
        let text = to_edit_text(&document).expect("every value should survive");
        assert_eq!(parse_query(&text).unwrap(), document);
        assert!(text.contains("count: 1,"));
        assert!(text.contains("NumberLong('9007199254740993')"));
        assert!(text.contains("ratio: 2.0"));
    }

    #[test]
    fn test_update_sets_unsets_and_guards() {
        let original = sample();
        let mut edited = original.clone();
        edited.insert("count", 2_i32);
        edited.remove("half");
        edited.insert("added", "new");
        let (filter, update) = build_update(&original, &edited).unwrap();
        assert_eq!(
            update.get_document("$set").unwrap(),
            &doc! { "count": 2_i32, "added": "new" }
        );
        assert_eq!(update.get_document("$unset").unwrap(), &doc! { "half": "" });
        let checks = filter
            .get_document("$expr")
            .unwrap()
            .get_array("$and")
            .unwrap();
        // count and half: type and value each; added: only that it is still missing.
        assert_eq!(checks.len(), 5);
        assert!(checks.contains(&Bson::Document(
            doc! { "$eq": [{ "$type": "$added" }, "missing"] }
        )));
        assert!(checks.contains(&Bson::Document(
            doc! { "$eq": [{ "$type": "$count" }, "int"] }
        )));
    }

    #[test]
    fn test_type_change_is_a_change() {
        let original = sample();
        let mut edited = original.clone();
        edited.insert("count", 1_i64);
        let (_, update) = build_update(&original, &edited).unwrap();
        assert_eq!(
            update.get_document("$set").unwrap(),
            &doc! { "count": 1_i64 }
        );
    }

    #[test]
    fn test_refusals() {
        let original = sample();
        assert!(build_update(&original, &original).is_err());
        let mut edited = original.clone();
        edited.insert("_id", 1);
        assert!(build_update(&original, &edited).is_err());
        let mut edited = original.clone();
        edited.remove("_id");
        assert!(build_update(&original, &edited).is_err());
        let mut edited = original.clone();
        edited.insert("a.b", 1);
        assert!(build_update(&original, &edited).is_err());
        assert!(build_delete(&doc! { "x": 1 }).is_err());
    }

    #[test]
    fn test_delete_guards_every_field() {
        let filter = build_delete(&doc! { "_id": 1, "a": 2, "b": "x" }).unwrap();
        assert_eq!(filter.get_document("_id").unwrap(), &doc! { "$eq": 1 });
        let checks = filter
            .get_document("$expr")
            .unwrap()
            .get_array("$and")
            .unwrap();
        assert_eq!(checks.len(), 4);
    }
}
