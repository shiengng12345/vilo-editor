//! Offline Redis command documentation for the Command Helper, built from the Redis 7.2 command
//! specs by `data/generate_redis_commands.py`. Looking a command up says nothing about whether
//! Vilo will run it; that is `redis::classify_words`.

use std::sync::LazyLock;

#[derive(Debug, serde::Deserialize)]
pub struct CommandDoc {
    pub name: String,
    pub summary: String,
    pub group: String,
    pub since: String,
    pub complexity: String,
    pub syntax: String,
    pub deprecated_since: Option<String>,
    pub replaced_by: Option<String>,
}

static DOCS: LazyLock<Result<Vec<CommandDoc>, String>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../data/redis_commands.json"))
        .map_err(|error| format!("The bundled command documentation is damaged: {error}"))
});

/// Every documented command, sorted by name.
pub fn command_docs() -> Result<&'static [CommandDoc], &'static str> {
    DOCS.as_deref().map_err(String::as_str)
}

/// The groups in the order the helper lists them.
pub fn command_groups(docs: &[CommandDoc]) -> Vec<&str> {
    let mut groups = docs
        .iter()
        .map(|doc| doc.group.as_str())
        .collect::<Vec<_>>();
    groups.sort_unstable();
    groups.dedup();
    groups
}

/// Commands matching `query` (case ignored) in `group`: names starting with it first, then names
/// containing it, then summaries containing it. An empty query lists the whole group.
pub fn search_commands<'a>(
    docs: &'a [CommandDoc],
    query: &str,
    group: Option<&str>,
) -> Vec<&'a CommandDoc> {
    let query = query.trim().to_lowercase();
    let mut ranked = docs
        .iter()
        .filter(|doc| group.is_none_or(|group| doc.group == group))
        .filter_map(|doc| {
            let name = doc.name.to_lowercase();
            let rank = if name.starts_with(&query) {
                0
            } else if name.contains(&query) {
                1
            } else if doc.summary.to_lowercase().contains(&query) {
                2
            } else {
                return None;
            };
            Some((rank, doc))
        })
        .collect::<Vec<_>>();
    // Stable, so each rank keeps the alphabetical order of the data.
    ranked.sort_by_key(|(rank, _)| *rank);
    ranked.into_iter().map(|(_, doc)| doc).collect()
}

/// The command a typed line starts with, preferring a subcommand (`CONFIG GET`) over its
/// container (`CONFIG`). Words are split as the CLI runs them, quotes included.
pub fn command_for_line(line: &str) -> Option<&'static CommandDoc> {
    let words = match crate::redis::split_command(line) {
        Ok(words) => words
            .iter()
            .map(|word| String::from_utf8_lossy(word).to_uppercase())
            .collect::<Vec<_>>(),
        // Half-typed, e.g. an open quote: the words so far still name the command.
        Err(_) => line.split_whitespace().map(str::to_uppercase).collect(),
    };
    let mut words = words.into_iter();
    let first = words.next()?;
    let docs = command_docs().ok()?;
    let find = |name: &str| docs.iter().find(|doc| doc.name == name);
    words
        .next()
        .and_then(|second| find(&format!("{first} {second}")))
        .or_else(|| find(&first))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundled_docs_load() {
        let docs = command_docs().unwrap();
        assert!(docs.len() > 300);
        let set = docs.iter().find(|doc| doc.name == "SET").unwrap();
        assert_eq!(set.group, "String");
        assert!(set.syntax.starts_with("SET key value [NX | XX]"));
        let georadius = docs.iter().find(|doc| doc.name == "GEORADIUS").unwrap();
        assert_eq!(georadius.deprecated_since.as_deref(), Some("6.2.0"));
        assert!(command_groups(command_docs().unwrap()).contains(&"Sorted Set"));
    }

    #[test]
    fn test_search_ranks_names_before_summaries() {
        let names = search_commands(command_docs().unwrap(), "hget", None)
            .iter()
            .map(|doc| doc.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["HGET", "HGETALL"]);
        let names = search_commands(command_docs().unwrap(), "get", None)
            .iter()
            .map(|doc| doc.name.as_str())
            .collect::<Vec<_>>();
        let position = |name| names.iter().position(|found| *found == name).unwrap();
        assert_eq!(names[0], "GET");
        assert!(
            position("GETRANGE") < position("HGET"),
            "starts-with before contains"
        );
        // STRLEN's name has no "length", only its summary does.
        assert!(
            search_commands(command_docs().unwrap(), "length", None)
                .iter()
                .any(|doc| doc.name == "STRLEN")
        );
        assert!(
            search_commands(command_docs().unwrap(), "", Some("Stream"))
                .iter()
                .all(|doc| doc.group == "Stream")
        );
        assert!(search_commands(command_docs().unwrap(), "no such thing", None).is_empty());
    }

    #[test]
    fn test_command_for_line() {
        assert_eq!(command_for_line("hgetall user:1").unwrap().name, "HGETALL");
        assert_eq!(
            command_for_line("config get maxmemory").unwrap().name,
            "CONFIG GET"
        );
        assert_eq!(command_for_line("config").unwrap().name, "CONFIG");
        assert_eq!(command_for_line("get config").unwrap().name, "GET");
        assert_eq!(command_for_line("\"GET\" key").unwrap().name, "GET");
        assert_eq!(
            command_for_line("XINFO \"STREAM\" key").unwrap().name,
            "XINFO STREAM"
        );
        assert_eq!(command_for_line("HGET \"user").unwrap().name, "HGET");
        assert!(command_for_line("   ").is_none());
        assert!(command_for_line("nonsense").is_none());
    }
}
