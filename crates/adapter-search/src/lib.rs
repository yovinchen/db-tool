use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{BoundedList, IndexInfo, Value},
    port::{
        capability::{
            SearchDeleteIndexOutcome, SearchDocument, SearchEngine, SearchHits, SearchOptions,
            SearchWriteOutcome,
        },
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::ListLimiter,
};
use futures::future::BoxFuture;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::ServerName;
use serde_json::{json, Map, Value as JsonValue};
use std::{fs::File, io::BufReader, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
use url::Url;

const MAX_HTTP_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
// CAT indices has no reliable cursor/limit contract across OpenSearch and
// Elasticsearch versions. Keep its unavoidable whole response materially
// smaller than ordinary search responses before retaining only N+1 entries.
const MAX_INDEX_CATALOG_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const MAX_HTTP_RESPONSE_HEADER_BYTES: usize = 64 * 1024;

pub struct SearchAdapter {
    client: SearchHttpClient,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = SearchHttpClient::from_dsn(&dsn)?;
        Ok(Box::new(SearchAdapter {
            client,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for SearchAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            search: true,
            ..Default::default()
        }
    }

    fn operations(&self) -> Vec<CapabilityOperation> {
        search_operations(self.capabilities())
    }

    async fn ping(&self) -> Result<()> {
        self.client.request_json("GET", "/", None).await.map(|_| ())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_search(&self) -> Option<&dyn SearchEngine> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl SearchEngine for SearchAdapter {
    async fn list_indices(&self) -> Result<Vec<IndexInfo>> {
        let response = self
            .client
            .request_json("GET", "/_cat/indices?format=json&h=index", None)
            .await?;
        indices_from_response(&response)
    }

    async fn list_indices_bounded(&self, max_items: usize) -> Result<BoundedList<IndexInfo>> {
        let limiter = ListLimiter::new(max_items);
        let probe_items = limiter.probe_items()?;
        let response = self
            .client
            .request_json_with_body_limit(
                "GET",
                "/_cat/indices?format=json&h=index&s=index",
                None,
                MAX_INDEX_CATALOG_RESPONSE_BODY_BYTES,
            )
            .await?;
        indices_from_response_bounded(&response, limiter, probe_items)
    }

    async fn search(
        &self,
        index: &str,
        query: Value,
        options: SearchOptions,
    ) -> Result<SearchHits> {
        validate_resource(index, "index")?;
        let body = search_body(query, &options)?;
        let response = self
            .client
            .request_json(
                "POST",
                &format!("/{}/_search", percent_encode_path_segment(index)),
                Some(&body),
            )
            .await?;
        search_hits_from_response(&response)
    }

    async fn index_doc(&self, index: &str, doc: Value) -> Result<SearchWriteOutcome> {
        validate_resource(index, "index")?;
        let body = core_value_to_json(doc)?;
        let response = self
            .client
            .request_json(
                "POST",
                &format!("/{}/_doc", percent_encode_path_segment(index)),
                Some(&body),
            )
            .await?;
        parse_write_response(response, "index document")
    }

    async fn put_doc(&self, index: &str, id: &str, doc: Value) -> Result<SearchWriteOutcome> {
        validate_resource(index, "index")?;
        validate_resource(id, "document id")?;
        let body = core_value_to_json(doc)?;
        let response = self
            .client
            .request_json("PUT", &document_path(index, id, "_doc"), Some(&body))
            .await?;
        parse_write_response(response, "put document")
    }

    async fn get_doc(&self, index: &str, id: &str) -> Result<Option<SearchDocument>> {
        validate_resource(index, "index")?;
        validate_resource(id, "document id")?;
        self.client
            .request_json_optional("GET", &document_path(index, id, "_doc"), None)
            .await?
            .map(parse_document_response)
            .transpose()
    }

    async fn update_doc(&self, index: &str, id: &str, patch: Value) -> Result<SearchWriteOutcome> {
        validate_resource(index, "index")?;
        validate_resource(id, "document id")?;
        let body = update_body(core_value_to_json(patch)?)?;
        let response = self
            .client
            .request_json("POST", &document_path(index, id, "_update"), Some(&body))
            .await?;
        parse_write_response(response, "update document")
    }

    async fn delete_doc(&self, index: &str, id: &str) -> Result<SearchWriteOutcome> {
        validate_resource(index, "index")?;
        validate_resource(id, "document id")?;
        let response = self
            .client
            .request_json("DELETE", &document_path(index, id, "_doc"), None)
            .await?;
        parse_write_response(response, "delete document")
    }

    async fn delete_index(&self, index: &str) -> Result<SearchDeleteIndexOutcome> {
        validate_resource(index, "index")?;
        let response = self
            .client
            .request_json(
                "DELETE",
                &format!("/{}", percent_encode_path_segment(index)),
                None,
            )
            .await?;
        parse_delete_index_response(response)
    }
}

#[derive(Debug, Clone)]
struct SearchHttpClient {
    host: String,
    port: u16,
    base_path: String,
    authorization: Option<String>,
    transport: SearchTransport,
    tls_ca: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchTransport {
    Plain,
    Tls,
}

impl SearchHttpClient {
    fn from_dsn(dsn: &Dsn) -> Result<Self> {
        let url = Url::parse(&dsn.raw).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;
        let transport = match url.scheme() {
            "opensearch" | "elasticsearch" => SearchTransport::Plain,
            "opensearch+https" | "elasticsearch+https" => SearchTransport::Tls,
            scheme => {
                return Err(Error::Dsn(format!(
                    "search DSN must use opensearch://, elasticsearch://, opensearch+https://, or elasticsearch+https://, got {scheme}"
                )))
            }
        };

        let host = url
            .host_str()
            .ok_or_else(|| Error::Dsn("search DSN requires a host".into()))?
            .to_owned();
        if transport == SearchTransport::Tls {
            validate_tls_server_name(&host)?;
        }
        let port = url.port().unwrap_or(9200);
        let tls_ca = search_tls_ca(&url);
        let username = percent_decode(url.username())?;
        let password = url
            .password()
            .map(percent_decode)
            .transpose()?
            .unwrap_or_default();
        let authorization = if username.is_empty() {
            None
        } else {
            Some(base64_encode(format!("{username}:{password}").as_bytes()))
        };
        let base_path = normalize_base_path(url.path());

        Ok(Self {
            host,
            port,
            base_path,
            authorization,
            transport,
            tls_ca,
        })
    }

    async fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<JsonValue> {
        let response = self.request_json_response(method, path, body).await?;
        response.into_success()
    }

    async fn request_json_with_body_limit(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
        max_body_bytes: usize,
    ) -> Result<JsonValue> {
        let response = self
            .request_json_response_with_body_limit(method, path, body, max_body_bytes)
            .await?;
        response.into_success()
    }

    async fn request_json_optional(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<Option<JsonValue>> {
        let response = self.request_json_response(method, path, body).await?;
        response.into_optional()
    }

    async fn request_json_response(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<SearchHttpResponse> {
        self.request_json_response_with_body_limit(method, path, body, MAX_HTTP_RESPONSE_BODY_BYTES)
            .await
    }

    async fn request_json_response_with_body_limit(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
        max_body_bytes: usize,
    ) -> Result<SearchHttpResponse> {
        let (request, body) = self.build_request(method, path, body)?;

        let stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        if self.transport == SearchTransport::Tls {
            let connector =
                TlsConnector::from(Arc::new(tls_client_config(self.tls_ca.as_deref())?));
            let server_name = ServerName::try_from(self.host.clone())
                .map_err(|e| Error::Dsn(format!("invalid TLS server name: {e}")))?;
            let stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            return send_http_request(stream, &request, &body, max_body_bytes).await;
        }

        send_http_request(stream, &request, &body, max_body_bytes).await
    }

    fn build_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<(String, Vec<u8>)> {
        let body = body
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let body = body.unwrap_or_default();
        let path = self.full_path(path);
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}:{}\r\nAccept: application/json\r\nConnection: close\r\n",
            self.host, self.port
        );
        if let Some(authorization) = &self.authorization {
            request.push_str(&format!("Authorization: Basic {authorization}\r\n"));
        }
        if !body.is_empty() {
            request.push_str("Content-Type: application/json\r\n");
        }
        request.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        Ok((request, body))
    }

    fn full_path(&self, path: &str) -> String {
        if self.base_path.is_empty() {
            path.to_owned()
        } else if path == "/" {
            self.base_path.clone()
        } else {
            format!("{}{}", self.base_path, path)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SearchHttpResponse {
    status: u16,
    body: JsonValue,
}

impl SearchHttpResponse {
    fn into_success(self) -> Result<JsonValue> {
        if (200..300).contains(&self.status) {
            return Ok(self.body);
        }

        Err(Error::Query(
            json!({
                "backend": "search",
                "http_status": self.status,
                "summary": format!("HTTP {}", self.status),
                "response": self.body,
            })
            .to_string(),
        ))
    }

    fn into_optional(self) -> Result<Option<JsonValue>> {
        if self.status == 404 {
            return Ok(None);
        }
        self.into_success().map(Some)
    }
}

async fn send_http_request<S>(
    mut stream: S,
    request: &str,
    body: &[u8],
    max_body_bytes: usize,
) -> Result<SearchHttpResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;
    if !body.is_empty() {
        stream
            .write_all(body)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
    }

    let response = read_bounded_http_response(&mut stream, max_body_bytes, true).await?;
    parse_http_json_with_limit(&response, max_body_bytes)
}

async fn read_bounded_http_response<S>(
    stream: &mut S,
    max_body_bytes: usize,
    tolerate_tls_close_notify_eof: bool,
) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut response = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut body_start = None;

    loop {
        let read = match stream.read(&mut buffer).await {
            Ok(read) => read,
            Err(error)
                if tolerate_tls_close_notify_eof
                    && !response.is_empty()
                    && is_tls_close_notify_eof(&error) =>
            {
                break;
            }
            Err(error) => return Err(Error::Connection(error.to_string())),
        };
        if read == 0 {
            break;
        }

        let next_len = response
            .len()
            .checked_add(read)
            .ok_or_else(|| Error::Connection("search HTTP response size overflow".into()))?;
        if let Some(body_start) = body_start {
            let body_len = next_len.checked_sub(body_start).ok_or_else(|| {
                Error::Connection("search HTTP response body size underflow".into())
            })?;
            ensure_http_body_within_limit(body_len, max_body_bytes, "body")?;
        }
        response.extend_from_slice(&buffer[..read]);

        if body_start.is_none() {
            if let Some(header_end) = find_http_header_end(&response) {
                ensure_http_headers_within_limit(header_end)?;
                let start = header_end.checked_add(4).ok_or_else(|| {
                    Error::Connection("search HTTP response header size overflow".into())
                })?;
                let header_text = std::str::from_utf8(&response[..header_end])
                    .map_err(|e| Error::Connection(format!("invalid HTTP headers: {e}")))?;
                ensure_content_length_within_limit(header_text, max_body_bytes)?;
                let body_len = response.len().checked_sub(start).ok_or_else(|| {
                    Error::Connection("search HTTP response body size underflow".into())
                })?;
                ensure_http_body_within_limit(body_len, max_body_bytes, "body")?;
                body_start = Some(start);
            } else if response.len() > MAX_HTTP_RESPONSE_HEADER_BYTES + 3 {
                return Err(Error::Connection(format!(
                    "search HTTP response headers exceed {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
                )));
            }
        }
    }

    Ok(response)
}

fn is_tls_close_notify_eof(error: &std::io::Error) -> bool {
    error
        .to_string()
        .contains("peer closed connection without sending TLS close_notify")
}

fn validate_tls_server_name(host: &str) -> Result<()> {
    ServerName::try_from(host.to_owned())
        .map(|_| ())
        .map_err(|e| Error::Dsn(format!("invalid TLS server name: {e}")))
}

fn tls_client_config(tls_ca: Option<&str>) -> Result<ClientConfig> {
    let cert_result = rustls_native_certs::load_native_certs();
    let load_errors = cert_result
        .errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    let mut roots = RootCertStore::empty();
    let (valid, invalid) = roots.add_parsable_certificates(cert_result.certs);
    let custom_valid = if let Some(path) = tls_ca {
        add_custom_ca_file(&mut roots, path)?
    } else {
        0
    };

    if valid + custom_valid == 0 {
        let mut reason = format!("no usable native root certificates found; ignored {invalid}");
        if !load_errors.is_empty() {
            reason.push_str(&format!("; load errors: {load_errors}"));
        }
        return Err(Error::Connection(reason));
    }

    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn search_tls_ca(url: &Url) -> Option<String> {
    url.query_pairs().find_map(|(key, value)| {
        matches!(key.as_ref(), "tls-ca" | "ssl-ca").then(|| value.into_owned())
    })
}

fn add_custom_ca_file(roots: &mut RootCertStore, path: &str) -> Result<usize> {
    let file = File::open(path)
        .map_err(|e| Error::Config(format!("failed to open TLS CA file {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Config(format!("failed to read TLS CA file {path}: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Config(format!(
            "TLS CA file {path} does not contain PEM certificates"
        )));
    }

    let (valid, invalid) = roots.add_parsable_certificates(certs);
    if valid == 0 {
        return Err(Error::Config(format!(
            "TLS CA file {path} did not contain usable certificates; ignored {invalid}"
        )));
    }

    Ok(valid)
}

fn normalize_base_path(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() || path == "/" {
        String::new()
    } else {
        path.to_owned()
    }
}

fn indices_from_response(response: &JsonValue) -> Result<Vec<IndexInfo>> {
    let indices = response
        .as_array()
        .ok_or_else(|| Error::Serialization("search index list response is not an array".into()))?;
    let mut indices = indices
        .iter()
        .map(|entry| {
            let name = entry
                .get("index")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    Error::Serialization("search index entry is missing index name".into())
                })?
                .to_owned();
            Ok(IndexInfo {
                name,
                columns: vec![],
                unique: false,
                primary: false,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    indices.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(indices)
}

fn indices_from_response_bounded(
    response: &JsonValue,
    limiter: ListLimiter,
    probe_items: usize,
) -> Result<BoundedList<IndexInfo>> {
    let indices = response
        .as_array()
        .ok_or_else(|| Error::Serialization("search index list response is not an array".into()))?;
    let mut bounded = Vec::with_capacity(probe_items.min(256));
    for entry in indices.iter().take(probe_items) {
        let name = entry
            .get("index")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| Error::Serialization("search index entry is missing index name".into()))?
            .to_owned();
        bounded.push(IndexInfo {
            name,
            columns: vec![],
            unique: false,
            primary: false,
        });
    }
    bounded.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(limiter.finish(bounded))
}

fn search_body(query: Value, options: &SearchOptions) -> Result<JsonValue> {
    let query = core_value_to_json(query)?;
    let mut body = if looks_like_search_body(&query) {
        query
    } else {
        json!({ "query": query })
    };

    let object = body
        .as_object_mut()
        .ok_or_else(|| Error::Serialization("search body must be a JSON object".into()))?;
    if let Some(limit) = options.size {
        let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
        let size = match object.get("size") {
            Some(value) => value
                .as_u64()
                .ok_or_else(|| {
                    Error::Serialization("search body size must be a non-negative integer".into())
                })?
                .min(limit_u64)
                .try_into()
                .unwrap_or(limit),
            None => limit,
        };
        object.insert("size".to_owned(), JsonValue::Number(size.into()));
    }
    if let Some(from) = options.from {
        object.insert("from".to_owned(), JsonValue::Number(from.into()));
    }
    if options.source {
        object.insert("_source".to_owned(), JsonValue::Bool(true));
    }

    Ok(body)
}

fn looks_like_search_body(value: &JsonValue) -> bool {
    value.as_object().is_some_and(|object| {
        object.keys().any(|key| {
            matches!(
                key.as_str(),
                "query" | "aggs" | "aggregations" | "sort" | "size" | "from" | "_source"
            )
        })
    })
}

fn search_hits_from_response(response: &JsonValue) -> Result<SearchHits> {
    let mut top_level = response
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Serialization("search response is not an object".into()))?;
    let mut hits_container = top_level
        .remove("hits")
        .ok_or_else(|| Error::Serialization("search response is missing hits".into()))?;
    let hits_object = hits_container
        .as_object_mut()
        .ok_or_else(|| Error::Serialization("search response hits is not an object".into()))?;
    let total_value = hits_object
        .remove("total")
        .ok_or_else(|| Error::Serialization("search response is missing hits.total".into()))?;
    let (total, total_relation) = match total_value {
        JsonValue::Number(number) => (
            number.as_u64().ok_or_else(|| {
                Error::Serialization("search response hits.total is not an unsigned integer".into())
            })?,
            "eq".to_owned(),
        ),
        JsonValue::Object(mut object) => {
            let total = object
                .remove("value")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| {
                    Error::Serialization(
                        "search response hits.total.value is not an unsigned integer".into(),
                    )
                })?;
            let relation = object
                .remove("relation")
                .map(|value| {
                    value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                        Error::Serialization(
                            "search response hits.total.relation is not a string".into(),
                        )
                    })
                })
                .transpose()?
                .unwrap_or_else(|| "eq".to_owned());
            if !object.is_empty() {
                hits_object.insert("total_metadata".to_owned(), JsonValue::Object(object));
            }
            (total, relation)
        }
        _ => {
            return Err(Error::Serialization(
                "search response hits.total has an unsupported shape".into(),
            ))
        }
    };
    let hits = hits_object
        .remove("hits")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| Error::Serialization("search response hits.hits is not an array".into()))?;
    let took_ms = optional_u64(&mut top_level, "took")?.unwrap_or_default();
    let timed_out = optional_bool(&mut top_level, "timed_out")?.unwrap_or(false);
    let aggregations = top_level.remove("aggregations");

    Ok(SearchHits {
        total,
        total_relation,
        hits,
        took_ms,
        timed_out,
        aggregations,
        hits_metadata: std::mem::take(hits_object),
        extra: top_level,
    })
}

fn core_value_to_json(value: Value) -> Result<JsonValue> {
    value.to_plain_json()
}

fn parse_write_response(response: JsonValue, operation: &str) -> Result<SearchWriteOutcome> {
    serde_json::from_value(response).map_err(|e| {
        Error::Serialization(format!(
            "invalid {operation} response from search backend: {e}"
        ))
    })
}

fn parse_document_response(response: JsonValue) -> Result<SearchDocument> {
    serde_json::from_value(response).map_err(|e| {
        Error::Serialization(format!(
            "invalid get document response from search backend: {e}"
        ))
    })
}

fn parse_delete_index_response(response: JsonValue) -> Result<SearchDeleteIndexOutcome> {
    serde_json::from_value(response).map_err(|e| {
        Error::Serialization(format!(
            "invalid delete index response from search backend: {e}"
        ))
    })
}

fn update_body(patch: JsonValue) -> Result<JsonValue> {
    patch
        .as_object()
        .ok_or_else(|| Error::Config("search update patch must be a JSON object".into()))?;
    Ok(json!({ "doc": patch }))
}

fn validate_resource(resource: &str, label: &str) -> Result<()> {
    if resource.is_empty() {
        return Err(Error::Config(format!("search {label} must not be empty")));
    }
    Ok(())
}

fn document_path(index: &str, id: &str, operation: &str) -> String {
    format!(
        "/{}/{}/{}",
        percent_encode_path_segment(index),
        operation,
        percent_encode_path_segment(id)
    )
}

fn optional_u64(object: &mut Map<String, JsonValue>, field: &str) -> Result<Option<u64>> {
    object
        .remove(field)
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                Error::Serialization(format!(
                    "search response {field} is not an unsigned integer"
                ))
            })
        })
        .transpose()
}

fn optional_bool(object: &mut Map<String, JsonValue>, field: &str) -> Result<Option<bool>> {
    object
        .remove(field)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                Error::Serialization(format!("search response {field} is not a boolean"))
            })
        })
        .transpose()
}

fn parse_http_json_with_limit(
    response: &[u8],
    max_body_bytes: usize,
) -> Result<SearchHttpResponse> {
    let header_end = find_http_header_end(response)
        .ok_or_else(|| Error::Connection("invalid HTTP response from search backend".into()))?;
    ensure_http_headers_within_limit(header_end)?;
    let body_start = header_end
        .checked_add(4)
        .ok_or_else(|| Error::Connection("search HTTP response header size overflow".into()))?;
    let headers = &response[..header_end];
    let body = response
        .get(body_start..)
        .ok_or_else(|| Error::Connection("invalid HTTP response from search backend".into()))?;
    let header_text = std::str::from_utf8(headers)
        .map_err(|e| Error::Connection(format!("invalid HTTP headers: {e}")))?;
    ensure_content_length_within_limit(header_text, max_body_bytes)?;
    ensure_http_body_within_limit(body.len(), max_body_bytes, "body")?;
    let status = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| Error::Connection("missing HTTP status".into()))?;
    let body = if has_chunked_transfer_encoding(header_text) {
        decode_chunked_body_with_limit(body, max_body_bytes)?
    } else {
        body.to_vec()
    };
    let body_text = std::str::from_utf8(&body).map_err(|e| Error::Serialization(e.to_string()))?;

    if body_text.trim().is_empty() {
        return Ok(SearchHttpResponse {
            status,
            body: JsonValue::Object(Map::new()),
        });
    }

    let body = match serde_json::from_str(body_text) {
        Ok(body) => body,
        Err(_) if !(200..300).contains(&status) => json!({ "raw": body_text }),
        Err(error) => return Err(Error::Serialization(error.to_string())),
    };
    Ok(SearchHttpResponse { status, body })
}

fn search_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.push(CapabilityOperation::SearchListIndicesBounded);
    operations
}

fn find_http_header_end(response: &[u8]) -> Option<usize> {
    response.windows(4).position(|window| window == b"\r\n\r\n")
}

fn ensure_http_headers_within_limit(header_bytes: usize) -> Result<()> {
    if header_bytes > MAX_HTTP_RESPONSE_HEADER_BYTES {
        return Err(Error::Connection(format!(
            "search HTTP response headers exceed {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn ensure_content_length_within_limit(header_text: &str, max_body_bytes: usize) -> Result<()> {
    let mut content_length = None;
    for line in header_text.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if !name.trim().eq_ignore_ascii_case("content-length") {
            continue;
        }
        let parsed = value
            .trim()
            .parse::<usize>()
            .map_err(|e| Error::Connection(format!("invalid search HTTP Content-Length: {e}")))?;
        if let Some(existing) = content_length {
            if existing != parsed {
                return Err(Error::Connection(
                    "conflicting search HTTP Content-Length headers".into(),
                ));
            }
        }
        content_length = Some(parsed);
    }

    if let Some(content_length) = content_length {
        if content_length > max_body_bytes {
            return Err(Error::Connection(format!(
                "search HTTP Content-Length {content_length} exceeds limit of {max_body_bytes} bytes"
            )));
        }
    }
    Ok(())
}

fn ensure_http_body_within_limit(
    body_bytes: usize,
    max_body_bytes: usize,
    body_kind: &str,
) -> Result<()> {
    if body_bytes > max_body_bytes {
        return Err(Error::Connection(format!(
            "search HTTP response {body_kind} exceeds limit of {max_body_bytes} bytes"
        )));
    }
    Ok(())
}

fn has_chunked_transfer_encoding(header_text: &str) -> bool {
    header_text.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("transfer-encoding")
                && value
                    .split(',')
                    .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        })
    })
}

fn decode_chunked_body_with_limit(body: &[u8], max_body_bytes: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut position = 0;

    loop {
        let remaining = body
            .get(position..)
            .ok_or_else(|| Error::Connection("invalid chunked response".into()))?;
        let line_end = find_crlf(remaining)
            .and_then(|offset| position.checked_add(offset))
            .ok_or_else(|| Error::Connection("invalid chunked response".into()))?;
        let size_line = std::str::from_utf8(&body[position..line_end])
            .map_err(|e| Error::Connection(format!("invalid chunk header: {e}")))?;
        let size = usize::from_str_radix(size_line.split(';').next().unwrap_or_default(), 16)
            .map_err(|e| Error::Connection(format!("invalid chunk size: {e}")))?;
        position = line_end
            .checked_add(2)
            .ok_or_else(|| Error::Connection("search chunk position overflow".into()))?;

        if size == 0 {
            break;
        }

        let chunk_end = position
            .checked_add(size)
            .ok_or_else(|| Error::Connection("search chunk size overflow".into()))?;
        let framed_end = chunk_end
            .checked_add(2)
            .ok_or_else(|| Error::Connection("search chunk framing overflow".into()))?;
        if framed_end > body.len() {
            return Err(Error::Connection("truncated chunked response".into()));
        }
        if body.get(chunk_end..framed_end) != Some(b"\r\n") {
            return Err(Error::Connection("invalid chunk terminator".into()));
        }
        let decoded_len = decoded
            .len()
            .checked_add(size)
            .ok_or_else(|| Error::Connection("search decoded body size overflow".into()))?;
        ensure_http_body_within_limit(decoded_len, max_body_bytes, "decoded body")?;
        decoded.extend_from_slice(&body[position..chunk_end]);
        position = framed_end;
    }

    Ok(decoded)
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|window| window == b"\r\n")
}

fn percent_encode_path_segment(input: &str) -> String {
    let mut encoded = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn percent_decode(input: &str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(Error::Dsn("invalid percent escape in DSN".into()));
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|e| Error::Dsn(format!("invalid percent escape: {e}")))?;
            decoded.push(
                u8::from_str_radix(hex, 16)
                    .map_err(|e| Error::Dsn(format!("invalid percent escape: {e}")))?,
            );
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|e| Error::Dsn(format!("invalid UTF-8 in DSN: {e}")))
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    let mut chunks = input.chunks(3).peekable();

    while let Some(chunk) = chunks.next() {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);

        output.push(TABLE[(b0 >> 2) as usize] as char);
        output.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            output.push('=');
        }

        if chunks.peek().is_none() {
            break;
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_plain_query_and_applies_limit() {
        let body = search_body(
            Value::Json(json!({ "match_all": {} })),
            &SearchOptions {
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(body["query"]["match_all"], json!({}));
        assert_eq!(body["size"], 5);
    }

    #[test]
    fn clamps_body_size_to_the_option_limit() {
        let body = search_body(
            Value::Json(json!({ "query": { "match_all": {} }, "size": 500 })),
            &SearchOptions {
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(body["size"], 5);
    }

    #[test]
    fn preserves_body_size_below_the_option_limit() {
        let body = search_body(
            Value::Json(json!({ "query": { "match_all": {} }, "size": 3 })),
            &SearchOptions {
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(body["size"], 3);
    }

    #[test]
    fn explicit_from_option_overrides_body_offset() {
        let body = search_body(
            Value::Json(json!({ "query": { "match_all": {} }, "from": 20 })),
            &SearchOptions {
                from: Some(7),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(body["from"], 7);
    }

    #[test]
    fn explicit_source_option_overrides_body_false() {
        let body = search_body(
            Value::Json(json!({
                "query": { "match_all": {} },
                "_source": false
            })),
            &SearchOptions {
                source: true,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(body["_source"], true);
    }

    #[test]
    fn rejects_non_integer_body_size_when_a_limit_is_applied() {
        let error = search_body(
            Value::Json(json!({ "query": { "match_all": {} }, "size": -1 })),
            &SearchOptions {
                size: Some(5),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("non-negative")
        ));
    }

    #[test]
    fn parses_search_hits_total_shapes() {
        let hits = search_hits_from_response(&json!({
            "took": 7,
            "timed_out": false,
            "aggregations": { "roles": { "buckets": [{"key": "reader", "doc_count": 2}] } },
            "_shards": { "total": 1, "successful": 1 },
            "hits": {
                "total": { "value": 2, "relation": "gte" },
                "max_score": 1.0,
                "hits": [
                    { "_id": "1", "_source": { "name": "alice" } },
                    { "_id": "2", "_source": { "name": "bob" } }
                ]
            }
        }))
        .unwrap();

        assert_eq!(hits.total, 2);
        assert_eq!(hits.total_relation, "gte");
        assert_eq!(hits.hits.len(), 2);
        assert_eq!(hits.took_ms, 7);
        assert!(!hits.timed_out);
        assert_eq!(
            hits.aggregations.as_ref().unwrap()["roles"]["buckets"][0]["key"],
            "reader"
        );
        assert_eq!(hits.hits_metadata["max_score"], 1.0);
        assert_eq!(hits.extra["_shards"]["successful"], 1);

        let legacy = search_hits_from_response(&json!({
            "hits": { "total": 4, "hits": [] }
        }))
        .unwrap();
        assert_eq!(legacy.total, 4);
        assert_eq!(legacy.total_relation, "eq");
    }

    #[test]
    fn parses_write_and_get_responses_without_dropping_backend_metadata() {
        let outcome = parse_write_response(
            json!({
                "_index": "users",
                "_id": "user-1",
                "_version": 3,
                "_seq_no": 8,
                "_primary_term": 1,
                "result": "updated",
                "forced_refresh": true,
                "_shards": { "successful": 1 }
            }),
            "update document",
        )
        .unwrap();
        assert_eq!(outcome.index, "users");
        assert_eq!(outcome.id, "user-1");
        assert_eq!(outcome.version, Some(3));
        assert_eq!(outcome.result, "updated");
        assert_eq!(outcome.extra["forced_refresh"], true);
        assert_eq!(outcome.extra["_shards"]["successful"], 1);

        let document = parse_document_response(json!({
            "_index": "users",
            "_id": "user-1",
            "_version": 3,
            "found": true,
            "_source": { "name": "alice" },
            "fields": { "role": ["reader"] }
        }))
        .unwrap();
        assert_eq!(document.id, "user-1");
        assert_eq!(document.source.unwrap()["name"], "alice");
        assert_eq!(document.extra["fields"]["role"][0], "reader");
    }

    #[test]
    fn update_body_always_wraps_the_caller_patch_without_reinterpreting_fields() {
        assert_eq!(
            update_body(json!({"role": "writer"})).unwrap(),
            json!({"doc": {"role": "writer"}})
        );
        assert_eq!(
            update_body(json!({"doc": "literal document field"})).unwrap(),
            json!({"doc": {"doc": "literal document field"}})
        );
        assert!(matches!(
            update_body(json!(["not", "an", "object"])),
            Err(Error::Config(message)) if message.contains("JSON object")
        ));
    }

    #[test]
    fn document_paths_percent_encode_each_caller_controlled_segment() {
        assert_eq!(
            document_path("people/current", "alice smith/1", "_doc"),
            "/people%2Fcurrent/_doc/alice%20smith%2F1"
        );
    }

    #[test]
    fn parses_index_list() {
        let indices = indices_from_response(&json!([
            { "index": "users" },
            { "index": "orders" }
        ]))
        .unwrap();

        assert_eq!(indices[0].name, "orders");
        assert_eq!(indices[1].name, "users");
    }

    #[test]
    fn bounded_index_list_uses_an_n_plus_one_probe() {
        let limiter = ListLimiter::new(2);
        let bounded = indices_from_response_bounded(
            &json!([
                { "index": "users" },
                { "index": "orders" },
                { "index": "audit" },
                { "index": "ignored" }
            ]),
            limiter,
            3,
        )
        .unwrap();

        assert_eq!(bounded.items.len(), 2);
        assert_eq!(bounded.items[0].name, "audit");
        assert_eq!(bounded.items[1].name, "orders");
        assert!(bounded.truncated);
    }

    #[test]
    fn search_declares_only_its_verified_bounded_catalog_extension() {
        let operations = search_operations(Capabilities {
            search: true,
            ..Default::default()
        });

        assert!(operations.contains(&CapabilityOperation::SearchListIndices));
        assert!(operations.contains(&CapabilityOperation::SearchListIndicesBounded));
        assert!(!operations.contains(&CapabilityOperation::DocumentListCollectionsBounded));
    }

    #[test]
    fn decodes_chunked_json_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\n{\"ok\":1\r\n1\r\n}\r\n0\r\n\r\n";
        let value = parse_http_json_with_limit(response, MAX_HTTP_RESPONSE_BODY_BYTES).unwrap();

        assert_eq!(value.status, 200);
        assert_eq!(value.body["ok"], 1);
    }

    #[test]
    fn rejects_content_length_above_http_body_budget() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n";

        let error = parse_http_json_with_limit(response, 8).unwrap_err();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("Content-Length") && message.contains("8 bytes")
        ));
    }

    #[test]
    fn rejects_raw_http_body_above_budget() {
        let response = b"HTTP/1.1 200 OK\r\n\r\n123456789";

        let error = parse_http_json_with_limit(response, 8).unwrap_err();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("body") && message.contains("8 bytes")
        ));
    }

    #[test]
    fn rejects_decoded_chunked_body_above_budget() {
        let error = decode_chunked_body_with_limit(b"9\r\n123456789\r\n0\r\n\r\n", 8).unwrap_err();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("decoded body") && message.contains("8 bytes")
        ));
    }

    #[test]
    fn rejects_chunk_size_arithmetic_overflow() {
        let body = format!("{:X}\r\n", usize::MAX);

        let error = decode_chunked_body_with_limit(body.as_bytes(), usize::MAX).unwrap_err();

        assert!(matches!(
            error,
            Error::Connection(message) if message.contains("overflow")
        ));
    }

    #[tokio::test]
    async fn bounded_reader_rejects_streamed_body_above_budget() {
        let (mut reader, mut writer) = tokio::io::duplex(128);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(b"HTTP/1.1 200 OK\r\n\r\n123456789")
                .await
                .unwrap();
        });

        let error = read_bounded_http_response(&mut reader, 8, false)
            .await
            .unwrap_err();
        writer_task.await.unwrap();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("body") && message.contains("8 bytes")
        ));
    }

    #[test]
    fn non_success_http_error_retains_status_and_json_body() {
        let response = b"HTTP/1.1 409 Conflict\r\nContent-Length: 55\r\n\r\n{\"error\":{\"type\":\"version_conflict_engine_exception\"}}";
        let error = parse_http_json_with_limit(response, MAX_HTTP_RESPONSE_BODY_BYTES)
            .unwrap()
            .into_success()
            .unwrap_err();
        let Error::Query(message) = error else {
            panic!("expected query error");
        };
        let detail: JsonValue = serde_json::from_str(&message).unwrap();
        assert_eq!(detail["backend"], "search");
        assert_eq!(detail["http_status"], 409);
        assert_eq!(
            detail["response"]["error"]["type"],
            "version_conflict_engine_exception"
        );
    }

    #[test]
    fn get_response_maps_http_404_to_none() {
        let response = parse_http_json_with_limit(
            b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\r\n{\"_index\":\"users\",\"_id\":\"missing\",\"found\":false}",
            MAX_HTTP_RESPONSE_BODY_BYTES,
        )
        .unwrap()
        .into_optional()
        .unwrap();
        assert_eq!(response, None);
    }

    #[test]
    fn builds_index_list_request() {
        let dsn = Dsn::parse("opensearch://search.local:9200").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        let (request, body) = client
            .build_request("GET", "/_cat/indices?format=json&h=index", None)
            .unwrap();

        assert!(request.starts_with("GET /_cat/indices?format=json&h=index HTTP/1.1"));
        assert!(request.contains("Host: search.local:9200"));
        assert!(body.is_empty());
    }

    #[test]
    fn accepts_https_transport_schemes() {
        let dsn = Dsn::parse("opensearch+https://search.local:9200/root").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        let (request, body) = client
            .build_request("GET", "/_cat/indices?format=json&h=index", None)
            .unwrap();

        assert_eq!(client.transport, SearchTransport::Tls);
        assert!(request.starts_with("GET /root/_cat/indices?format=json&h=index HTTP/1.1"));
        assert!(request.contains("Host: search.local:9200"));
        assert!(body.is_empty());
    }

    #[test]
    fn accepts_elasticsearch_https_transport_scheme() {
        let dsn = Dsn::parse("elasticsearch+https://search.local").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();

        assert_eq!(client.transport, SearchTransport::Tls);
        assert_eq!(client.port, 9200);
    }

    #[test]
    fn accepts_custom_tls_ca_query_params() {
        let dsn = Dsn::parse("opensearch+https://search.local?tls-ca=/tmp/search-ca.pem").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        assert_eq!(client.tls_ca.as_deref(), Some("/tmp/search-ca.pem"));

        let dsn =
            Dsn::parse("elasticsearch+https://search.local?ssl-ca=/tmp/search-ca.pem").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        assert_eq!(client.tls_ca.as_deref(), Some("/tmp/search-ca.pem"));
    }

    #[test]
    fn builds_search_request_with_path_prefix() {
        let dsn = Dsn::parse("elasticsearch://search.local:9200/root").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        let body = search_body(
            Value::Json(json!({"match_all": {}})),
            &SearchOptions {
                size: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        let (request, body) = client
            .build_request("POST", "/users/_search", Some(&body))
            .unwrap();
        let body = String::from_utf8(body).unwrap();

        assert!(request.starts_with("POST /root/users/_search HTTP/1.1"));
        assert!(request.contains("Content-Type: application/json"));
        assert!(body.contains(r#""match_all":{}"#));
        assert!(body.contains(r#""size":2"#));
    }

    #[test]
    fn builds_index_doc_request_with_basic_auth() {
        let dsn = Dsn::parse("opensearch://alice:secret@search.local:9201").unwrap();
        let client = SearchHttpClient::from_dsn(&dsn).unwrap();
        let body = core_value_to_json(Value::Json(json!({"name": "alice"}))).unwrap();
        let (request, body) = client
            .build_request("POST", "/users/_doc", Some(&body))
            .unwrap();
        let body = String::from_utf8(body).unwrap();

        assert!(request.starts_with("POST /users/_doc HTTP/1.1"));
        assert!(request.contains("Host: search.local:9201"));
        assert!(request.contains("Authorization: Basic YWxpY2U6c2VjcmV0"));
        assert!(body.contains(r#""name":"alice""#));
    }
}
