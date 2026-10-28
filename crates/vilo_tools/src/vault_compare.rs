use collections::{BTreeMap, BTreeSet};

use crate::vault::VersionState;

/// What one side of a comparison read back.
#[derive(Clone, Debug, PartialEq)]
pub enum SideRead {
    Found {
        values: BTreeMap<String, serde_json::Value>,
        version: Option<u64>,
    },
    /// The read succeeded and Vault confirmed nothing is stored at the path.
    MissingSecret,
    /// KV v2 keeps the current version, but its data was deleted or destroyed.
    Unavailable(VersionState),
    /// Could not read: the reason is shown; nothing is concluded from it.
    Unreadable(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Cell {
    /// The baseline side has this key.
    Present,
    Same,
    Different,
    /// Present but `null` or an empty string.
    Empty,
    MissingKey,
    MissingSecret,
    Deleted,
    Destroyed,
    Unreadable(String),
    /// This side has the key but the baseline could not be read, so there is
    /// nothing to compare against.
    NoBaseline,
}

impl Cell {
    pub fn is_difference(&self) -> bool {
        !matches!(self, Cell::Present | Cell::Same)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub key: String,
    pub cells: Vec<Cell>,
}

fn is_empty(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::String(text) => text.is_empty(),
        _ => false,
    }
}

/// Compares every side against the first one, the baseline. Equality is exact
/// on typed JSON: `"1"` and `1` differ, object key order does not matter.
/// Only two successful reads are ever called the same or different.
pub fn compare(sides: &[SideRead]) -> Vec<Row> {
    let keys = sides
        .iter()
        .filter_map(|side| match side {
            SideRead::Found { values, .. } => Some(values.keys().cloned()),
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let baseline = sides.first();
    keys.into_iter()
        .map(|key| {
            let baseline_value = match baseline {
                Some(SideRead::Found { values, .. }) => Some(values.get(&key)),
                _ => None,
            };
            let cells = sides
                .iter()
                .enumerate()
                .map(|(index, side)| match side {
                    SideRead::Unreadable(reason) => Cell::Unreadable(reason.clone()),
                    SideRead::MissingSecret => Cell::MissingSecret,
                    SideRead::Unavailable(VersionState::Destroyed) => Cell::Destroyed,
                    SideRead::Unavailable(_) => Cell::Deleted,
                    SideRead::Found { values, .. } => {
                        let Some(value) = values.get(&key) else {
                            return Cell::MissingKey;
                        };
                        if index == 0 {
                            return if is_empty(value) {
                                Cell::Empty
                            } else {
                                Cell::Present
                            };
                        }
                        match baseline_value {
                            Some(Some(baseline)) if baseline == value => Cell::Same,
                            Some(_) if is_empty(value) => Cell::Empty,
                            Some(_) => Cell::Different,
                            None => Cell::NoBaseline,
                        }
                    }
                })
                .collect();
            Row { key, cells }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn found(pairs: &[(&str, serde_json::Value)]) -> SideRead {
        SideRead::Found {
            values: pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone()))
                .collect(),
            version: Some(1),
        }
    }

    #[test]
    fn test_compare_against_baseline() {
        let rows = compare(&[
            found(&[("A", json!("x")), ("B", json!(1)), ("C", json!("c"))]),
            found(&[("A", json!("x")), ("B", json!("1")), ("D", json!(""))]),
            SideRead::Unreadable("403".into()),
            SideRead::MissingSecret,
        ]);
        let cells = |key: &str| rows.iter().find(|row| row.key == key).unwrap().cells.clone();
        assert_eq!(
            cells("A"),
            vec![Cell::Present, Cell::Same, Cell::Unreadable("403".into()), Cell::MissingSecret]
        );
        // Typed equality: the number 1 is not the string "1".
        assert_eq!(cells("B")[1], Cell::Different);
        assert_eq!(cells("C")[1], Cell::MissingKey);
        assert_eq!(cells("D")[0], Cell::MissingKey);
        assert_eq!(cells("D")[1], Cell::Empty);
    }

    #[test]
    fn test_unreadable_baseline_never_yields_same_or_different() {
        let rows = compare(&[
            SideRead::Unreadable("timeout".into()),
            found(&[("A", json!("x"))]),
        ]);
        assert_eq!(rows[0].cells[1], Cell::NoBaseline);
    }

    #[test]
    fn test_deleted_versions_are_not_missing() {
        let rows = compare(&[
            found(&[("A", json!({"b": 1, "a": 2}))]),
            SideRead::Unavailable(VersionState::Deleted),
            found(&[("A", json!({"a": 2, "b": 1}))]),
        ]);
        assert_eq!(rows[0].cells, vec![Cell::Present, Cell::Deleted, Cell::Same]);
    }
}
