use std::time::Duration;

use anyhow::{Context as _, Result};
use futures::TryStreamExt as _;
use mongodb::{
    Client,
    bson::{Bson, Document, doc},
    options::ClientOptions,
};

const SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct MongoSession {
    client: Client,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseInfo {
    pub name: String,
    pub size_on_disk: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionInfo {
    pub name: String,
    /// "collection", "view" or "timeseries".
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FindRequest {
    pub database: String,
    pub collection: String,
    pub filter: Document,
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub skip: u64,
    pub page_size: u32,
    /// Execution budget the server enforces; it is set when the query is sent, not a way to cancel it later.
    pub max_time: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FindPage {
    /// Documents keep their BSON types; nothing is converted to JSON here.
    pub documents: Vec<Document>,
    /// Whether at least one more document matched after this page.
    pub has_more: bool,
}

impl MongoSession {
    pub async fn connect(uri: &str) -> Result<Self> {
        let mut options = ClientOptions::parse(uri).await.context("The connection string is not valid")?;
        options.app_name = Some("Vilo".into());
        // Writes are sent once; Vilo reports an unknown outcome instead of letting the driver resend.
        options.retry_writes = Some(false);
        options.server_selection_timeout = Some(SERVER_SELECTION_TIMEOUT);
        let client = Client::with_options(options)?;
        client.database("admin").run_command(doc! { "ping": 1 }).await.context("Could not reach MongoDB")?;
        Ok(Self { client })
    }

    pub async fn databases(&self) -> Result<Vec<DatabaseInfo>> {
        let mut databases = self
            .client
            .list_databases()
            .await?
            .into_iter()
            .map(|database| DatabaseInfo { name: database.name, size_on_disk: database.size_on_disk })
            .collect::<Vec<_>>();
        databases.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(databases)
    }

    pub async fn collections(&self, database: &str) -> Result<Vec<CollectionInfo>> {
        let mut collections = self
            .client
            .database(database)
            .list_collections()
            .await?
            .map_ok(|specification| CollectionInfo {
                name: specification.name,
                kind: format!("{:?}", specification.collection_type).to_lowercase(),
            })
            .try_collect::<Vec<_>>()
            .await?;
        collections.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(collections)
    }

    /// From collection metadata, so it is cheap and may lag behind the real count.
    pub async fn estimated_count(&self, database: &str, collection: &str) -> Result<u64> {
        Ok(self.client.database(database).collection::<Document>(collection).estimated_document_count().await?)
    }

    pub async fn find_page(&self, request: &FindRequest) -> Result<FindPage> {
        let collection = self.client.database(&request.database).collection::<Document>(&request.collection);
        let page_size = request.page_size.max(1);
        // One extra document tells whether another page exists without counting.
        let mut find = collection.find(request.filter.clone()).skip(request.skip).limit(i64::from(page_size) + 1);
        if let Some(projection) = &request.projection {
            find = find.projection(projection.clone());
        }
        if let Some(sort) = &request.sort {
            find = find.sort(sort.clone());
        }
        if let Some(max_time) = request.max_time {
            find = find.max_time(max_time);
        }
        let mut documents = find.await?.try_collect::<Vec<_>>().await?;
        let has_more = documents.len() > page_size as usize;
        documents.truncate(page_size as usize);
        Ok(FindPage { documents, has_more })
    }
}

/// Parses a filter, projection or sort typed in Extended JSON (e.g. `{"_id": {"$oid": "…"}}`). Empty means `{}`.
pub fn parse_document(text: &str) -> Result<Document> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(Document::new());
    }
    let value: serde_json::Value = serde_json::from_str(text).context("Not valid JSON")?;
    match Bson::try_from(value).context("Not valid Extended JSON")? {
        Bson::Document(document) => Ok(document),
        _ => anyhow::bail!("Expected an object like {{\"field\": \"value\"}}"),
    }
}

/// Relaxed Extended JSON for reading. It is a display form only: values are written back from the BSON document.
pub fn to_display_json(document: &Document) -> String {
    let value = Bson::Document(document.clone()).into_relaxed_extjson();
    serde_json::to_string_pretty(&value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{Decimal128, oid::ObjectId};

    #[test]
    fn test_parse_document_keeps_types() {
        let document = parse_document(
            r#"{"_id": {"$oid": "65f0c0ffee0000000000abcd"}, "amount": {"$numberDecimal": "0.10"}, "big": {"$numberLong": "9007199254740993"}}"#,
        )
        .unwrap();
        assert!(matches!(document.get("_id"), Some(Bson::ObjectId(_))));
        assert_eq!(document.get("amount"), Some(&Bson::Decimal128("0.10".parse::<Decimal128>().unwrap())));
        assert_eq!(document.get("big"), Some(&Bson::Int64(9_007_199_254_740_993)));
        assert_eq!(parse_document("  ").unwrap(), Document::new());
        assert!(parse_document("[1]").is_err());
        assert!(parse_document("{oops").is_err());
    }

    /// Runs against a real server: `VILO_TEST_MONGO=mongodb://127.0.0.1:57017/?directConnection=true cargo test -p vilo_database_core`.
    #[tokio::test]
    async fn test_find_pages_against_a_server() {
        let Ok(uri) = std::env::var("VILO_TEST_MONGO") else {
            return;
        };
        let session = MongoSession::connect(&uri).await.unwrap();
        let collection = session.client.database("vilo_test").collection::<Document>("paging");
        collection.drop().await.unwrap();
        let id = ObjectId::new();
        let documents = (0..5)
            .map(|index| doc! { "index": index, "amount": "1.10".parse::<Decimal128>().unwrap(), "owner": id })
            .collect::<Vec<_>>();
        collection.insert_many(documents).await.unwrap();

        let mut request = FindRequest {
            database: "vilo_test".into(),
            collection: "paging".into(),
            filter: parse_document(&format!(r#"{{"owner": {{"$oid": "{}"}}}}"#, id.to_hex())).unwrap(),
            projection: Some(doc! { "_id": 0 }),
            sort: Some(doc! { "index": 1 }),
            skip: 0,
            page_size: 2,
            max_time: Some(Duration::from_secs(5)),
        };
        let first = session.find_page(&request).await.unwrap();
        assert_eq!(first.documents.len(), 2);
        assert!(first.has_more);
        assert!(matches!(first.documents[0].get("amount"), Some(Bson::Decimal128(_))));
        request.skip = 4;
        let last = session.find_page(&request).await.unwrap();
        assert_eq!((last.documents.len(), last.has_more), (1, false));
        assert!(session.databases().await.unwrap().iter().any(|database| database.name == "vilo_test"));
        assert_eq!(session.collections("vilo_test").await.unwrap()[0].name, "paging");
        assert_eq!(session.estimated_count("vilo_test", "paging").await.unwrap(), 5);
    }
}
