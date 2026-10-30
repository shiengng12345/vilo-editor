//! How values read in the document list, in the mongosh / Compass style: `ObjectId('…')`, `ISODate('…')`.

use mongodb::bson::{Bson, DateTime, Document, spec::BinarySubtype};

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
        Bson::DateTime(date) => match date.try_to_rfc3339_string() {
            Ok(text) => scalar(format!("ISODate('{text}')"), ValueKind::Date),
            // Outside what ISO 8601 can write; the raw milliseconds are the honest form.
            Err(_) => scalar(format!("{{\"$date\":{{\"$numberLong\":\"{}\"}}}}", date.timestamp_millis()), ValueKind::Date),
        },
        Bson::Boolean(flag) => scalar(flag.to_string(), ValueKind::Boolean),
        Bson::Null => scalar("null".into(), ValueKind::Null),
        Bson::Undefined => scalar("undefined".into(), ValueKind::Null),
        Bson::RegularExpression(regex) => scalar(format!("/{}/{}", regex.pattern, regex.options), ValueKind::Other),
        Bson::Timestamp(timestamp) => scalar(format!("Timestamp({{ t: {}, i: {} }})", timestamp.time, timestamp.increment), ValueKind::Other),
        Bson::Binary(binary) if binary.subtype == BinarySubtype::Uuid && binary.bytes.len() == 16 => {
            let hex = binary.bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            scalar(format!("UUID('{}-{}-{}-{}-{}')", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]), ValueKind::Other)
        }
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

/// A readable date next to the raw value, as Compass shows "October 30, 2026 at 4:34:55 AM UTC".
/// `local` uses this Mac's time zone instead of UTC.
pub fn human_date(date: &DateTime, local: bool) -> Option<String> {
    let format = time::macros::format_description!(
        "[month repr:long] [day padding:none], [year] at [hour repr:12 padding:none]:[minute]:[second] [period]"
    );
    let utc = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(date.timestamp_millis()) * 1_000_000).ok()?;
    let (moment, zone) = match time::UtcOffset::current_local_offset() {
        Ok(offset) if local => (utc.to_offset(offset), format_offset(offset)),
        _ => (utc, "UTC".to_string()),
    };
    Some(format!("{} {zone}", moment.format(format).ok()?))
}

fn format_offset(offset: time::UtcOffset) -> String {
    let (hours, minutes, _) = offset.as_hms();
    if minutes == 0 { format!("GMT{hours:+}") } else { format!("GMT{hours:+}:{:02}", minutes.abs()) }
}

/// Canonical Extended JSON keeps every type exact (Long, Decimal128, dates), so a copy pastes back unchanged.
pub fn to_canonical_json(document: &Document) -> String {
    serde_json::to_string_pretty(&Bson::Document(document.clone()).into_canonical_extjson()).unwrap_or_default()
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
        let uuid = mongodb::bson::Binary { subtype: BinarySubtype::Uuid, bytes: (0u8..16).collect() };
        assert_eq!(
            show(&Bson::Binary(uuid)),
            Shown::Scalar { text: "UUID('00010203-0405-0607-0809-0a0b0c0d0e0f')".into(), kind: ValueKind::Other }
        );
        let far = DateTime::from_millis(i64::MAX);
        assert!(matches!(show(&Bson::DateTime(far)), Shown::Scalar { text, .. } if text.contains("$numberLong")));
        assert_eq!(human_date(&DateTime::from_millis(1_793_334_895_412), false).unwrap(), "October 30, 2026 at 4:34:55 AM UTC");
        assert_eq!(human_date(&DateTime::from_millis(253_402_300_799_000), false).unwrap(), "December 31, 9999 at 11:59:59 PM UTC");
        let canonical = to_canonical_json(&doc! { "n": 9_007_199_254_740_993_i64 });
        assert!(canonical.contains("$numberLong"));
    }
}
