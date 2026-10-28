use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

/// One connection in an import or export file. Exports never carry `token`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ConnectionEntry {
    pub name: String,
    #[serde(default)]
    pub site: String,
    #[serde(default)]
    pub environment: String,
    pub address: String,
    #[serde(default, skip_serializing)]
    pub token: Option<String>,
}

/// Reads a JSON array of entries, or lines of `name, site, environment, address[, token]`.
pub fn parse_entries(text: &str) -> Result<Vec<ConnectionEntry>> {
    let text = text.trim();
    if text.is_empty() {
        bail!("Paste a JSON list or one connection per line.");
    }
    if text.starts_with('[') {
        let entries: Vec<ConnectionEntry> =
            serde_json::from_str(text).context("The JSON must be a list of connections")?;
        return Ok(entries
            .into_iter()
            .map(|entry| ConnectionEntry {
                token: entry.token.filter(|token| !token.trim().is_empty()),
                ..entry
            })
            .collect());
    }
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .enumerate()
        .map(|(index, line)| {
            let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
            match fields.as_slice() {
                [name, site, environment, address] | [name, site, environment, address, ""] => {
                    Ok(ConnectionEntry {
                        name: name.to_string(),
                        site: site.to_string(),
                        environment: environment.to_string(),
                        address: address.to_string(),
                        token: None,
                    })
                }
                [name, site, environment, address, token] => Ok(ConnectionEntry {
                    name: name.to_string(),
                    site: site.to_string(),
                    environment: environment.to_string(),
                    address: address.to_string(),
                    token: Some(token.to_string()),
                }),
                _ => bail!(
                    "Line {} needs: name, site, environment, address[, token]",
                    index + 1
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_and_lines() {
        let entries = parse_entries(
            r#"[{"name":"CP UAT","site":"CP","environment":"UAT","address":"http://a:8200","token":"t"},
                {"name":"NP UAT","address":"http://b:8200","token":""}]"#,
        )
        .unwrap();
        assert_eq!(entries[0].token.as_deref(), Some("t"));
        assert_eq!(entries[1].site, "");
        assert_eq!(entries[1].token, None);

        let entries =
            parse_entries("# comment\nCP UAT, CP, UAT, http://a:8200\nBP UAT, BP, UAT, http://b:8200, tok\n")
                .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].token, None);
        assert_eq!(entries[1].token.as_deref(), Some("tok"));
        assert!(parse_entries("only, three, fields").is_err());
    }

    #[test]
    fn test_export_never_writes_tokens() {
        let entry = ConnectionEntry {
            name: "CP".into(),
            address: "http://a".into(),
            token: Some("secret".into()),
            ..Default::default()
        };
        assert!(!serde_json::to_string(&entry).unwrap().contains("secret"));
    }
}
