//! How values read in the document list, in the mongosh / Compass style: `ObjectId('…')`, `ISODate('…')`.

use mongodb::bson::Bson;

/// What a value is, so the view can colour it the way Compass does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    String,
    Number,
    ObjectId,
    Date,
    Boolean,
    Null,
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Shown {
    Scalar { text: String, kind: ValueKind },
    /// A nested document with this many fields.
    Object(usize),
    /// An array with this many items.
    Array(usize),
}

pub fn show(value: &Bson) -> Shown {
    let scalar = |text: String, kind: ValueKind| Shown::Scalar { text, kind };
    match value {
        Bson::Document(document) => Shown::Object(document.len()),
        Bson::Array(items) => Shown::Array(items.len()),
        Bson::String(text) => scalar(serde_json::Value::String(text.clone()).to_string(), ValueKind::String),
        Bson::Int32(number) => scalar(number.to_string(), ValueKind::Number),
        Bson::Int64(number) => scalar(format!("Long('{number}')"), ValueKind::Number),
        Bson::Double(number) => scalar(number.to_string(), ValueKind::Number),
        Bson::Decimal128(number) => scalar(format!("Decimal128('{number}')"), ValueKind::Number),
        Bson::ObjectId(id) => scalar(format!("ObjectId('{}')", id.to_hex()), ValueKind::ObjectId),
        Bson::DateTime(date) => scalar(
            format!("ISODate('{}')", date.try_to_rfc3339_string().unwrap_or_else(|_| date.timestamp_millis().to_string())),
            ValueKind::Date,
        ),
        Bson::Boolean(flag) => scalar(flag.to_string(), ValueKind::Boolean),
        Bson::Null => scalar("null".into(), ValueKind::Null),
        Bson::Undefined => scalar("undefined".into(), ValueKind::Null),
        Bson::RegularExpression(regex) => scalar(format!("/{}/{}", regex.pattern, regex.options), ValueKind::Other),
        Bson::Timestamp(timestamp) => scalar(format!("Timestamp({{ t: {}, i: {} }})", timestamp.time, timestamp.increment), ValueKind::Other),
        Bson::Binary(binary) => {
            use base64::Engine as _;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&binary.bytes);
            scalar(format!("Binary.createFromBase64('{encoded}', {})", u8::from(binary.subtype)), ValueKind::Other)
        }
        Bson::MinKey => scalar("MinKey()".into(), ValueKind::Other),
        Bson::MaxKey => scalar("MaxKey()".into(), ValueKind::Other),
        other => scalar(other.to_string(), ValueKind::Other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{DateTime, Decimal128, doc, oid::ObjectId};

    #[test]
    fn test_values_read_like_compass() {
        let id = ObjectId::parse_str("6ae41e934acd77bafc90923e").unwrap();
        assert_eq!(show(&Bson::ObjectId(id)), Shown::Scalar { text: "ObjectId('6ae41e934acd77bafc90923e')".into(), kind: ValueKind::ObjectId });
        assert_eq!(
            show(&Bson::DateTime(DateTime::from_millis(1_793_334_895_412))),
            Shown::Scalar { text: "ISODate('2026-10-30T04:34:55.412Z')".into(), kind: ValueKind::Date }
        );
        assert_eq!(show(&Bson::String("it's \"x\"".into())), Shown::Scalar { text: r#""it's \"x\"""#.into(), kind: ValueKind::String });
        assert_eq!(show(&Bson::Int64(9_007_199_254_740_993)), Shown::Scalar { text: "Long('9007199254740993')".into(), kind: ValueKind::Number });
        assert_eq!(
            show(&Bson::Decimal128("0.10".parse::<Decimal128>().unwrap())),
            Shown::Scalar { text: "Decimal128('0.10')".into(), kind: ValueKind::Number }
        );
        assert_eq!(show(&Bson::Document(doc! { "a": 1, "b": 2 })), Shown::Object(2));
        assert_eq!(show(&Bson::Array(vec![Bson::Null])), Shown::Array(1));
    }
}
