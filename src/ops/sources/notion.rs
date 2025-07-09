use crate::fields_value;
use async_stream::try_stream;
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::base::field_attrs;
use crate::base::value::FieldValues;
use crate::ops::sdk::*;

#[derive(Debug, Deserialize)]
pub struct Spec {
    token: String,
    source_type: String, // "database" or "page"
    database_ids: Option<Vec<String>>,
    page_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct NotionPage {
    id: String,
    created_time: String,
    last_edited_time: String,
    properties: HashMap<String, NotionProperty>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotionProperty {
    #[serde(rename = "type")]
    property_type: String,
    title: Option<Vec<NotionRichText>>,
    rich_text: Option<Vec<NotionRichText>>,
}

#[derive(Debug, Deserialize)]
struct NotionRichText {
    plain_text: String,
}

#[derive(Debug, Deserialize)]
struct NotionDatabaseQueryResponse {
    results: Vec<NotionPage>,
    has_more: bool,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotionPageResponse {
    id: String,
    created_time: String,
    last_edited_time: String,
    properties: HashMap<String, NotionProperty>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotionBlocksResponse {
    results: Vec<NotionBlock>,
    has_more: bool,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NotionBlock {
    id: String,
    #[serde(rename = "type")]
    block_type: String,
    paragraph: Option<NotionParagraph>,
    heading_1: Option<NotionHeading>,
    heading_2: Option<NotionHeading>,
    heading_3: Option<NotionHeading>,
    bulleted_list_item: Option<NotionListItem>,
    numbered_list_item: Option<NotionListItem>,
    code: Option<NotionCode>,
}

#[derive(Debug, Deserialize)]
struct NotionParagraph {
    rich_text: Vec<NotionRichText>,
}

#[derive(Debug, Deserialize)]
struct NotionHeading {
    rich_text: Vec<NotionRichText>,
}

#[derive(Debug, Deserialize)]
struct NotionListItem {
    rich_text: Vec<NotionRichText>,
}

#[derive(Debug, Deserialize)]
struct NotionCode {
    rich_text: Vec<NotionRichText>,
}

struct Executor {
    client: Client,
    token: String,
    source_type: String,
    database_ids: Vec<String>,
    page_ids: Vec<String>,
    // Cache to prevent concurrent processing of the same page
    processing_cache: Arc<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl Executor {
    fn new(spec: Spec) -> Self {
        let client = Client::new();
        let database_ids = spec.database_ids.unwrap_or_default();
        let page_ids = spec.page_ids.unwrap_or_default();

        Self {
            client,
            token: spec.token,
            source_type: spec.source_type,
            database_ids,
            page_ids,
            processing_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn fetch_database_pages(&self, database_id: &str) -> Result<Vec<NotionPage>> {
        let mut all_pages = Vec::new();
        let mut cursor = None;
        let mut seen_ids = std::collections::HashSet::new();

        loop {
            let url = format!("https://api.notion.com/v1/databases/{}/query", database_id);

            let mut body = serde_json::json!({
                "page_size": 100
            });

            if let Some(cursor) = cursor {
                body["start_cursor"] = serde_json::Value::String(cursor);
            }

            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("Notion-Version", "2022-06-28")
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!("Notion API error: {}", response.status()));
            }

            let query_response: NotionDatabaseQueryResponse = response.json().await?;

            // Filter out duplicates
            for page in query_response.results {
                if seen_ids.insert(page.id.clone()) {
                    all_pages.push(page);
                }
            }

            if !query_response.has_more {
                break;
            }
            cursor = query_response.next_cursor;
        }

        Ok(all_pages)
    }

    async fn fetch_page(&self, page_id: &str) -> Result<NotionPage> {
        let url = format!("https://api.notion.com/v1/pages/{}", page_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", "2022-06-28")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Notion API error: {}", response.status()));
        }

        let page: NotionPageResponse = response.json().await?;
        Ok(NotionPage {
            id: page.id,
            created_time: page.created_time,
            last_edited_time: page.last_edited_time,
            properties: page.properties,
            url: page.url,
        })
    }

    async fn fetch_page_content(&self, page_id: &str) -> Result<String> {
        let mut content = String::new();
        let mut cursor = None;

        loop {
            let mut url = format!("https://api.notion.com/v1/blocks/{}/children", page_id);
            if let Some(cursor) = &cursor {
                url.push_str(&format!("?start_cursor={}", cursor));
            }

            let response = self
                .client
                .get(&url)
                .header("Authorization", format!("Bearer {}", self.token))
                .header("Notion-Version", "2022-06-28")
                .send()
                .await?;

            if !response.status().is_success() {
                return Err(anyhow::anyhow!("Notion API error: {}", response.status()));
            }

            let blocks_response: NotionBlocksResponse = response.json().await?;

            for block in blocks_response.results {
                match block.block_type.as_str() {
                    "paragraph" => {
                        if let Some(paragraph) = block.paragraph {
                            let text = paragraph
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&text);
                            content.push('\n');
                        }
                    }
                    "heading_1" => {
                        if let Some(heading) = block.heading_1 {
                            let text = heading
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("# {}\n", text));
                        }
                    }
                    "heading_2" => {
                        if let Some(heading) = block.heading_2 {
                            let text = heading
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("## {}\n", text));
                        }
                    }
                    "heading_3" => {
                        if let Some(heading) = block.heading_3 {
                            let text = heading
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("### {}\n", text));
                        }
                    }
                    "bulleted_list_item" => {
                        if let Some(list_item) = block.bulleted_list_item {
                            let text = list_item
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("- {}\n", text));
                        }
                    }
                    "numbered_list_item" => {
                        if let Some(list_item) = block.numbered_list_item {
                            let text = list_item
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("1. {}\n", text));
                        }
                    }
                    "code" => {
                        if let Some(code) = block.code {
                            let text = code
                                .rich_text
                                .iter()
                                .map(|rt| rt.plain_text.as_str())
                                .collect::<Vec<_>>()
                                .join("");
                            content.push_str(&format!("```\n{}\n```\n", text));
                        }
                    }
                    _ => {}
                }
            }

            if !blocks_response.has_more {
                break;
            }
            cursor = blocks_response.next_cursor;
        }

        Ok(content)
    }

    fn extract_title_from_properties(
        &self,
        properties: &HashMap<String, NotionProperty>,
    ) -> String {
        for (_, property) in properties {
            if let Some(title) = &property.title {
                if !title.is_empty() {
                    return title
                        .iter()
                        .map(|rt| rt.plain_text.as_str())
                        .collect::<Vec<_>>()
                        .join("");
                }
            }
        }
        "Untitled".to_string()
    }

    fn parse_datetime(&self, datetime_str: &str) -> Result<Ordinal> {
        let dt = chrono::DateTime::parse_from_rfc3339(datetime_str)?;
        Ok(Ordinal(Some(dt.timestamp_micros())))
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        _options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<PartialSourceRowMetadata>>> {
        try_stream! {
            let mut batch = Vec::new();
            let mut seen_ids = HashSet::new();

            // Handle database pages
            if self.source_type == "database" {
                for database_id in &self.database_ids {
                    let pages = self.fetch_database_pages(database_id).await?;
                    for page in pages {
                        if seen_ids.insert(page.id.clone()) {
                            batch.push(PartialSourceRowMetadata {
                                key: KeyValue::Str(page.id.into()),
                                ordinal: Some(self.parse_datetime(&page.last_edited_time)?),
                            });
                        }
                    }
                }
            }

            // Handle individual pages
            if self.source_type == "page" {
                for page_id in &self.page_ids {
                    let page = self.fetch_page(page_id).await?;
                    if seen_ids.insert(page.id.clone()) {
                        batch.push(PartialSourceRowMetadata {
                            key: KeyValue::Str(page.id.into()),
                            ordinal: Some(self.parse_datetime(&page.last_edited_time)?),
                        });
                    }
                }
            }

            if !batch.is_empty() {
                yield batch;
            }
        }
        .boxed()
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData> {
        let page_id = key.str_value()?;

        // Get or create a mutex for this specific page to prevent concurrent processing
        let page_mutex = {
            let mut cache = self.processing_cache.lock().unwrap();
            cache
                .entry(page_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        // Lock the mutex for this page to ensure only one thread processes it at a time
        let _lock = page_mutex.lock().await;

        let page = match self.fetch_page(page_id.as_ref()).await {
            Ok(page) => page,
            Err(_) => {
                return Ok(PartialSourceRowData {
                    value: Some(SourceValue::NonExistence),
                    ordinal: Some(Ordinal::unavailable()),
                });
            }
        };

        let ordinal = if options.include_ordinal {
            Some(self.parse_datetime(&page.last_edited_time)?)
        } else {
            None
        };

        let value = if options.include_value {
            let title = self.extract_title_from_properties(&page.properties);
            let content = self.fetch_page_content(page_id.as_ref()).await?;

            let fields = vec![
                page.id.into(),
                title.into(),
                content.into(),
                page.url.unwrap_or_default().into(),
            ];

            Some(SourceValue::Existence(FieldValues { fields }))
        } else {
            None
        };

        Ok(PartialSourceRowData { value, ordinal })
    }

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
        Ok(None)
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "Notion"
    }

    async fn get_output_schema(
        &self,
        _spec: &Spec,
        _context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType> {
        let mut struct_schema = StructSchema::default();
        let mut schema_builder = StructSchemaBuilder::new(&mut struct_schema);

        schema_builder.add_field(FieldSchema::new(
            "id",
            make_output_type(BasicValueType::Str),
        ));

        let title_field = schema_builder.add_field(FieldSchema::new(
            "title",
            make_output_type(BasicValueType::Str),
        ));

        schema_builder.add_field(FieldSchema::new(
            "content",
            make_output_type(BasicValueType::Str).with_attr(
                field_attrs::CONTENT_FILENAME,
                serde_json::to_value(title_field.to_field_ref())?,
            ),
        ));

        schema_builder.add_field(FieldSchema::new(
            "url",
            make_output_type(BasicValueType::Str),
        ));

        Ok(make_output_type(TableSchema::new(
            TableKind::KTable,
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(Executor::new(spec)))
    }
}
