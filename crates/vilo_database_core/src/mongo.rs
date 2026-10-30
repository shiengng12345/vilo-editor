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
        let mut options = ClientOptions::parse(uri)
            .await
            .context("The connection string is not valid")?;
        options.app_name = Some("Vilo".into());
        // Writes are sent once; Vilo reports an unknown outcome instead of letting the driver resend.
        options.retry_writes = Some(false);
        options.server_selection_timeout = Some(SERVER_SELECTION_TIMEOUT);
        let client = Client::with_options(options)?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .context("Could not reach MongoDB")?;
        Ok(Self { client })
    }

    pub async fn databases(&self) -> Result<Vec<DatabaseInfo>> {
        let mut databases = self
            .client
            .list_databases()
            .await?
            .into_iter()
            .map(|database| DatabaseInfo {
                name: database.name,
                size_on_disk: database.size_on_disk,
            })
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
        Ok(self
            .client
            .database(database)
            .collection::<Document>(collection)
            .estimated_document_count()
            .await?)
    }

    /// How many documents match, within `max_time`; slow counts give up rather than hold the page.
    pub async fn count(
        &self,
        database: &str,
        collection: &str,
        filter: Document,
        max_time: Duration,
    ) -> Result<u64> {
        Ok(self
            .client
            .database(database)
            .collection::<Document>(collection)
            .count_documents(filter)
            .max_time(max_time)
            .await?)
    }

    pub async fn find_page(&self, request: &FindRequest) -> Result<FindPage> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let page_size = request.page_size.max(1);
        // One extra document tells whether another page exists without counting.
        let mut find = collection
            .find(request.filter.clone())
            .skip(request.skip)
            .limit(i64::from(page_size) + 1);
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
        Ok(FindPage {
            documents,
            has_more,
        })
    }
}

/// What Explain says about one query, in the five numbers Compass leads with.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExplainSummary {
    pub returned: u64,
    pub documents_examined: u64,
    pub keys_examined: u64,
    /// Server execution time; network and Vilo time are not included.
    pub millis: u64,
    /// The winning plan's stages from the top down, e.g. FETCH → IXSCAN.
    pub stages: Vec<String>,
    pub indexes: Vec<String>,
}

fn number(document: &Document, key: &str) -> u64 {
    match document.get(key) {
        Some(Bson::Int32(value)) => u64::try_from(*value).unwrap_or(0),
        Some(Bson::Int64(value)) => u64::try_from(*value).unwrap_or(0),
        Some(Bson::Double(value)) if *value >= 0.0 => *value as u64,
        _ => 0,
    }
}

fn walk_plan(plan: &Document, stages: &mut Vec<String>, indexes: &mut Vec<String>) {
    if let Ok(stage) = plan.get_str("stage") {
        stages.push(stage.to_string());
    }
    if let Ok(index) = plan.get_str("indexName")
        && !indexes.iter().any(|known| known == index)
    {
        indexes.push(index.to_string());
    }
    // Classic plans nest through inputStage(s); slot-based plans wrap them in queryPlan.
    for key in ["queryPlan", "inputStage"] {
        if let Ok(child) = plan.get_document(key) {
            walk_plan(child, stages, indexes);
        }
    }
    if let Ok(children) = plan.get_array("inputStages") {
        for child in children.iter().filter_map(Bson::as_document) {
            walk_plan(child, stages, indexes);
        }
    }
}

pub fn summarize_explain(explain: &Document) -> ExplainSummary {
    let mut summary = ExplainSummary::default();
    if let Ok(stats) = explain.get_document("executionStats") {
        summary.returned = number(stats, "nReturned");
        summary.documents_examined = number(stats, "totalDocsExamined");
        summary.keys_examined = number(stats, "totalKeysExamined");
        summary.millis = number(stats, "executionTimeMillis");
    }
    if let Ok(plan) = explain
        .get_document("queryPlanner")
        .and_then(|planner| planner.get_document("winningPlan"))
    {
        walk_plan(plan, &mut summary.stages, &mut summary.indexes);
    }
    summary
}

/// A pipeline stage that writes, found at any depth (inside $lookup, $facet, $unionWith too).
pub fn write_stage(pipeline: &[Document]) -> Option<&'static str> {
    fn scan(value: &Bson) -> Option<&'static str> {
        match value {
            Bson::Document(document) => {
                document.iter().find_map(|(key, value)| match key.as_str() {
                    "$out" => Some("$out"),
                    "$merge" => Some("$merge"),
                    _ => scan(value),
                })
            }
            Bson::Array(items) => items.iter().find_map(scan),
            _ => None,
        }
    }
    pipeline
        .iter()
        .find_map(|stage| scan(&Bson::Document(stage.clone())))
}

impl MongoSession {
    /// Runs the query with `executionStats`, which really executes it, so it carries the same time limit.
    pub async fn explain(&self, request: &FindRequest) -> Result<Document> {
        let mut find = doc! { "find": &request.collection, "filter": request.filter.clone(), "skip": request.skip as i64, "limit": i64::from(request.page_size) };
        if let Some(projection) = &request.projection {
            find.insert("projection", projection.clone());
        }
        if let Some(sort) = &request.sort {
            find.insert("sort", sort.clone());
        }
        if let Some(max_time) = request.max_time {
            find.insert("maxTimeMS", max_time.as_millis() as i64);
        }
        Ok(self
            .client
            .database(&request.database)
            .run_command(doc! { "explain": find, "verbosity": "executionStats" })
            .await?)
    }

    /// The collection's options as stored (validator, validationLevel, validationAction, …), or `None` if it is gone.
    pub async fn collection_options(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<Document>> {
        let reply = self
            .client
            .database(database)
            .run_command(
                doc! { "listCollections": 1, "filter": { "name": collection }, "nameOnly": false },
            )
            .await?;
        Ok(reply
            .get_document("cursor")
            .and_then(|cursor| cursor.get_array("firstBatch"))
            .ok()
            .and_then(|batch| batch.first())
            .and_then(Bson::as_document)
            .map(|specification| {
                specification
                    .get_document("options")
                    .cloned()
                    .unwrap_or_default()
            }))
    }

    /// Index specifications as the server returns them, so nothing (partial filters, TTL, collation) is lost.
    pub async fn indexes(&self, database: &str, collection: &str) -> Result<Vec<Document>> {
        let reply = self
            .client
            .database(database)
            .run_command(doc! { "listIndexes": collection, "cursor": { "batchSize": 1000 } })
            .await?;
        Ok(reply
            .get_document("cursor")
            .and_then(|cursor| cursor.get_array("firstBatch"))
            .map(|batch| {
                batch
                    .iter()
                    .filter_map(|index| index.as_document().cloned())
                    .collect()
            })
            .unwrap_or_default())
    }

    /// One page of an aggregation. Pipelines that write are refused before anything is sent.
    pub async fn aggregate_page(
        &self,
        database: &str,
        collection: &str,
        pipeline: &[Document],
        skip: u64,
        page_size: u32,
        max_time: Duration,
    ) -> Result<FindPage> {
        if let Some(stage) = write_stage(pipeline) {
            anyhow::bail!(
                "This pipeline writes with {stage}; Vilo only runs read-only pipelines for now."
            );
        }
        let page_size = page_size.max(1);
        let mut stages = pipeline.to_vec();
        stages.push(doc! { "$skip": skip as i64 });
        stages.push(doc! { "$limit": i64::from(page_size) + 1 });
        let mut documents = self
            .client
            .database(database)
            .collection::<Document>(collection)
            .aggregate(stages)
            .max_time(max_time)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let has_more = documents.len() > page_size as usize;
        documents.truncate(page_size as usize);
        Ok(FindPage {
            documents,
            has_more,
        })
    }

    /// Every matching document, one at a time, for export. `cancel` stops after the current document.
    pub async fn for_each_match(
        &self,
        request: &FindRequest,
        pipeline: Option<&[Document]>,
        cancel: &std::sync::atomic::AtomicBool,
        mut each: impl FnMut(Document) -> Result<()>,
    ) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let mut cursor = match pipeline {
            Some(pipeline) => {
                if let Some(stage) = write_stage(pipeline) {
                    anyhow::bail!(
                        "This pipeline writes with {stage}; Vilo only runs read-only pipelines for now."
                    );
                }
                collection.aggregate(pipeline.to_vec()).await?
            }
            None => {
                let mut find = collection.find(request.filter.clone()).skip(request.skip);
                if let Some(projection) = &request.projection {
                    find = find.projection(projection.clone());
                }
                if let Some(sort) = &request.sort {
                    find = find.sort(sort.clone());
                }
                find.await?
            }
        };
        while let Some(document) = cursor.try_next().await? {
            if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                anyhow::bail!("Export cancelled");
            }
            each(document)?;
        }
        Ok(())
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
    fn test_explain_summary_and_write_stages() {
        let explain = doc! {
            "queryPlanner": { "winningPlan": { "stage": "FETCH", "inputStage": { "stage": "IXSCAN", "indexName": "status_1" } } },
            "executionStats": { "nReturned": 3, "totalDocsExamined": 3, "totalKeysExamined": 4, "executionTimeMillis": 2 }
        };
        let summary = summarize_explain(&explain);
        assert_eq!(summary.stages, vec!["FETCH", "IXSCAN"]);
        assert_eq!(summary.indexes, vec!["status_1"]);
        assert_eq!(
            (
                summary.returned,
                summary.documents_examined,
                summary.keys_examined,
                summary.millis
            ),
            (3, 3, 4, 2)
        );
        let slot_based =
            doc! { "queryPlanner": { "winningPlan": { "queryPlan": { "stage": "COLLSCAN" } } } };
        assert_eq!(summarize_explain(&slot_based).stages, vec!["COLLSCAN"]);

        assert_eq!(write_stage(&[doc! { "$match": {} }]), None);
        assert_eq!(
            write_stage(&[doc! { "$merge": { "into": "x" } }]),
            Some("$merge")
        );
        let nested = doc! { "$facet": { "a": [ { "$lookup": { "from": "y", "pipeline": [ { "$out": "z" } ], "as": "j" } } ] } };
        assert_eq!(write_stage(&[nested]), Some("$out"));
    }

    #[test]
    fn test_parse_document_keeps_types() {
        let document = parse_document(
            r#"{"_id": {"$oid": "65f0c0ffee0000000000abcd"}, "amount": {"$numberDecimal": "0.10"}, "big": {"$numberLong": "9007199254740993"}}"#,
        )
        .unwrap();
        assert!(matches!(document.get("_id"), Some(Bson::ObjectId(_))));
        assert_eq!(
            document.get("amount"),
            Some(&Bson::Decimal128("0.10".parse::<Decimal128>().unwrap()))
        );
        assert_eq!(
            document.get("big"),
            Some(&Bson::Int64(9_007_199_254_740_993))
        );
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
        let collection = session
            .client
            .database("vilo_test")
            .collection::<Document>("paging");
        collection.drop().await.unwrap();
        let id = ObjectId::new();
        let documents = (0..5)
            .map(|index| doc! { "index": index, "amount": "1.10".parse::<Decimal128>().unwrap(), "owner": id })
            .collect::<Vec<_>>();
        collection.insert_many(documents).await.unwrap();

        let mut request = FindRequest {
            database: "vilo_test".into(),
            collection: "paging".into(),
            filter: parse_document(&format!(r#"{{"owner": {{"$oid": "{}"}}}}"#, id.to_hex()))
                .unwrap(),
            projection: Some(doc! { "_id": 0 }),
            sort: Some(doc! { "index": 1 }),
            skip: 0,
            page_size: 2,
            max_time: Some(Duration::from_secs(5)),
        };
        let first = session.find_page(&request).await.unwrap();
        assert_eq!(first.documents.len(), 2);
        assert!(first.has_more);
        assert!(matches!(
            first.documents[0].get("amount"),
            Some(Bson::Decimal128(_))
        ));
        request.skip = 4;
        let last = session.find_page(&request).await.unwrap();
        assert_eq!((last.documents.len(), last.has_more), (1, false));
        assert!(
            session
                .databases()
                .await
                .unwrap()
                .iter()
                .any(|database| database.name == "vilo_test")
        );
        assert_eq!(
            session.collections("vilo_test").await.unwrap()[0].name,
            "paging"
        );
        assert_eq!(
            session
                .estimated_count("vilo_test", "paging")
                .await
                .unwrap(),
            5
        );
        assert_eq!(
            session
                .count(
                    "vilo_test",
                    "paging",
                    doc! { "index": { "$gte": 3 } },
                    Duration::from_secs(5)
                )
                .await
                .unwrap(),
            2
        );

        let explain = session.explain(&request).await.unwrap();
        let summary = summarize_explain(&explain);
        assert!(!summary.stages.is_empty());
        assert!(
            session
                .collection_options("vilo_test", "paging")
                .await
                .unwrap()
                .is_some()
        );
        let indexes = session.indexes("vilo_test", "paging").await.unwrap();
        assert_eq!(indexes[0].get_str("name").unwrap(), "_id_");
        let pipeline = vec![
            doc! { "$match": { "index": { "$gte": 1 } } },
            doc! { "$sort": { "index": 1 } },
        ];
        let page = session
            .aggregate_page(
                "vilo_test",
                "paging",
                &pipeline,
                1,
                2,
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(page.documents[0].get_i32("index").unwrap(), 2);
        assert!(page.has_more);
        let refused = session
            .aggregate_page(
                "vilo_test",
                "paging",
                &[doc! { "$out": "copy" }],
                0,
                2,
                Duration::from_secs(5),
            )
            .await;
        assert!(refused.is_err());
        let cancel = std::sync::atomic::AtomicBool::new(false);
        let mut seen = 0;
        session
            .for_each_match(&request, None, &cancel, |_| {
                seen += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(seen, 1, "export follows the query's own skip, not paging");
    }
}
