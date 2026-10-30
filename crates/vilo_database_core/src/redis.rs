use anyhow::{Context as _, Result};
use fred::{
    cmd,
    prelude::{Builder, Client, ClientLike as _, Config, Value},
};

/// Keys and values stay bytes end to end; text, hex and base64 are display choices made elsewhere.
pub type Bytes = Vec<u8>;

pub struct RedisSession {
    client: Client,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanPage {
    pub keys: Vec<Bytes>,
    /// The cursor for the next call; "0" means this node has been walked once. An empty page does not mean done.
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyInfo {
    /// "string", "hash", "list", "set", "zset", "stream", "ReJSON-RL", or "none" when the key is gone.
    pub kind: String,
    /// Milliseconds left; -1 means no expiry, -2 means the key does not exist.
    pub ttl_ms: i64,
}

fn bytes_arg(bytes: &[u8]) -> Value {
    Value::Bytes(bytes.to_vec().into())
}

impl RedisSession {
    pub async fn connect(url: &str) -> Result<Self> {
        let config = Config::from_url(url).context("The Redis URL is not valid")?;
        let mut builder = Builder::from_config(config);
        builder.with_connection_config(|connection| {
            // A command whose answer was lost is reported as unknown rather than sent again.
            connection.max_command_attempts = 1;
        });
        let client = builder.build()?;
        client.init().await.context("Could not reach Redis")?;
        Ok(Self { client })
    }

    /// One SCAN call. COUNT is a hint, so a page may hold more or fewer keys, or none, and keys may repeat across pages.
    // shortcut: walks the node the client is connected to; a Redis Cluster needs one cursor per primary, which comes with Cluster support.
    pub async fn scan(&self, cursor: &str, pattern: &str, count: u32, kind: Option<&str>) -> Result<ScanPage> {
        let mut args = vec![Value::from(cursor.to_string()), "MATCH".into(), bytes_arg(pattern.as_bytes()), "COUNT".into(), Value::from(count as i64)];
        if let Some(kind) = kind {
            args.push("TYPE".into());
            args.push(Value::from(kind.to_string()));
        }
        let reply: Value = self.client.custom(cmd!("SCAN"), args).await?;
        let mut parts = reply.into_array().into_iter();
        let cursor = parts.next().and_then(|cursor| cursor.as_string()).context("SCAN returned no cursor")?;
        let keys = parts
            .next()
            .map(|keys| keys.into_array().into_iter().filter_map(|key| key.as_bytes().map(<[u8]>::to_vec)).collect())
            .unwrap_or_default();
        Ok(ScanPage { keys, cursor })
    }

    pub async fn key_info(&self, key: &[u8]) -> Result<KeyInfo> {
        let kind: Value = self.client.custom(cmd!("TYPE"), vec![bytes_arg(key)]).await?;
        let ttl: Value = self.client.custom(cmd!("PTTL"), vec![bytes_arg(key)]).await?;
        Ok(KeyInfo {
            kind: kind.as_string().unwrap_or_else(|| "none".into()),
            ttl_ms: ttl.as_i64().context("PTTL returned no number")?,
        })
    }

    /// The raw value of a string key, or `None` when it does not exist.
    pub async fn get_string(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value: Value = self.client.custom(cmd!("GET"), vec![bytes_arg(key)]).await?;
        Ok(value.as_bytes().map(<[u8]>::to_vec))
    }

    pub async fn quit(&self) -> Result<()> {
        Ok(self.client.quit().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs against a real server: `VILO_TEST_REDIS=redis://127.0.0.1:57379 cargo test -p vilo_database_core`.
    #[tokio::test]
    async fn test_binary_keys_and_ttl_against_a_server() {
        let Ok(url) = std::env::var("VILO_TEST_REDIS") else {
            return;
        };
        let session = RedisSession::connect(&url).await.unwrap();
        let key = b"vilo_test:\x00\xff\xe4\xb8\xad".to_vec();
        let value = b"\x01\x02 not utf-8 \xff".to_vec();
        let _: Value = session
            .client
            .custom(cmd!("SET"), vec![bytes_arg(&key), bytes_arg(&value), "PX".into(), Value::from(60_000_i64)])
            .await
            .unwrap();

        let mut cursor = "0".to_string();
        let mut found = Vec::new();
        loop {
            let page = session.scan(&cursor, "vilo_test:*", 100, Some("string")).await.unwrap();
            found.extend(page.keys);
            cursor = page.cursor;
            if cursor == "0" {
                break;
            }
        }
        assert!(found.contains(&key), "SCAN must return the key with its exact bytes");

        let info = session.key_info(&key).await.unwrap();
        assert_eq!(info.kind, "string");
        assert!((1..=60_000).contains(&info.ttl_ms));
        assert_eq!(session.get_string(&key).await.unwrap(), Some(value));

        let missing = session.key_info(b"vilo_test:missing").await.unwrap();
        assert_eq!((missing.kind.as_str(), missing.ttl_ms), ("none", -2));
        assert_eq!(session.get_string(b"vilo_test:missing").await.unwrap(), None);
        session.quit().await.unwrap();
    }
}
