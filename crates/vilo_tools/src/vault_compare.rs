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


/// One side of a comparison, as named in a report.
pub struct ReportSide {
    pub label: String,
    pub address: String,
    pub path: String,
}

/// One compared secret path; `path` is relative to each side's base path.
pub struct ReportPath<'a> {
    pub path: &'a str,
    pub reads: &'a [SideRead],
    pub versions: &'a [Option<u64>],
    pub rows: &'a [Row],
}

pub fn cell_label(cell: &Cell) -> &'static str {
    match cell {
        Cell::Present => "present",
        Cell::Same => "same",
        Cell::Different => "different",
        Cell::Empty => "empty",
        Cell::MissingKey => "missing key",
        Cell::MissingSecret => "missing secret",
        Cell::Deleted => "deleted version",
        Cell::Destroyed => "destroyed version",
        Cell::Unreadable(_) => "unreadable",
        Cell::NoBaseline => "baseline unreadable",
    }
}

/// A Markdown report of a comparison. It names keys and states, never values.
pub fn markdown_report(
    started_at: &str,
    finished_at: &str,
    sides: &[ReportSide],
    paths: &[ReportPath],
    coverage: &[String],
    is_not_applicable: impl Fn(&str, usize, &str) -> bool,
) -> String {
    let mut out = String::new();
    out.push_str("# Vault comparison\n\n");
    out.push_str(&format!(
        "Read between {started_at} and {finished_at}. These are reads collected over that \
         interval, not one snapshot.\n\n"
    ));
    out.push_str(
        "Differences are evidence, not requirements. Lanes are compared as stored. \
         Values are never included in this report.\n\n## Sides\n\n| | Connection | Address | Path |\n|---|---|---|---|\n",
    );
    for (index, side) in sides.iter().enumerate() {
        let role = if index == 0 { "Baseline".to_string() } else { format!("Side {index}") };
        out.push_str(&format!("| {role} | {} | {} | {} |\n", side.label, side.address, side.path));
    }
    out.push_str("\n## Coverage\n\n");
    if coverage.is_empty() {
        out.push_str("Every listed path was read.\n");
    } else {
        for note in coverage {
            out.push_str(&format!("- {note}\n"));
        }
    }
    out.push_str("\n## Differences\n");
    let mut any = false;
    for path in paths {
        let rows = path
            .rows
            .iter()
            .filter(|row| row.cells.iter().any(Cell::is_difference))
            .collect::<Vec<_>>();
        let unreadable = path
            .reads
            .iter()
            .enumerate()
            .filter_map(|(index, read)| match read {
                SideRead::Unreadable(reason) => Some(format!("side {index}: {reason}")),
                _ => None,
            })
            .collect::<Vec<_>>();
        if rows.is_empty() && unreadable.is_empty() {
            continue;
        }
        any = true;
        let title = if path.path.is_empty() { "(secret)" } else { path.path };
        let versions = path
            .versions
            .iter()
            .map(|version| version.map_or("-".to_string(), |version| version.to_string()))
            .collect::<Vec<_>>()
            .join(" / ");
        out.push_str(&format!("\n### {title}\n\nVersions: {versions}\n\n"));
        for note in &unreadable {
            out.push_str(&format!("- unreadable {note}\n"));
        }
        if rows.is_empty() {
            continue;
        }
        out.push_str("\n| Key |");
        for index in 0..sides.len() {
            out.push_str(if index == 0 { " Baseline |" } else { " Side |" });
        }
        out.push_str("\n|---|");
        out.push_str(&"---|".repeat(sides.len()));
        out.push('\n');
        for row in rows {
            out.push_str(&format!("| `{}` |", row.key));
            for (index, cell) in row.cells.iter().enumerate() {
                let label = cell_label(cell);
                if index > 0 && is_not_applicable(path.path, index, &row.key) {
                    out.push_str(&format!(" n/a ({label}) |"));
                } else {
                    out.push_str(&format!(" {label} |"));
                }
            }
            out.push('\n');
        }
    }
    if !any {
        out.push_str("\nNo differences.\n");
    }
    out
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

    #[test]
    fn test_report_never_contains_values() {
        let reads = vec![
            found(&[("PASSWORD", json!("hunter2")), ("PORT", json!(1))]),
            found(&[("PASSWORD", json!("swordfish"))]),
        ];
        let rows = compare(&reads);
        let report = markdown_report(
            "t0",
            "t1",
            &[
                ReportSide { label: "CP".into(), address: "http://a/".into(), path: "kv/auth/uat".into() },
                ReportSide { label: "NP".into(), address: "http://b/".into(), path: "kv/auth/uat".into() },
            ],
            &[ReportPath { path: "", reads: &reads, versions: &[Some(1), Some(2)], rows: &rows }],
            &[],
            |_, _, key| key == "PORT",
        );
        assert!(!report.contains("hunter2") && !report.contains("swordfish"));
        assert!(report.contains("| `PASSWORD` | present | different |"));
        assert!(report.contains("n/a (missing key)"));
    }
}
