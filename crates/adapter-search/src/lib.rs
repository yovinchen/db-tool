use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{IndexInfo, Value},
    port::{
        capability::{SearchEngine, SearchHits, SearchOptions},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::ServerName;
use serde_json::{json, Map, Value as JsonValue};
use std::sync::Arc;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;
use url::Url;

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

    async fn search(
        &self,
        index: &str,
        query: Value,
        options: SearchOptions,
    ) -> Result<SearchHits> {
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

    async fn index_doc(&self, index: &str, doc: Value) -> Result<()> {
        let body = core_value_to_json(doc)?;
        self.client
            .request_json(
                "POST",
                &format!("/{}/_doc", percent_encode_path_segment(index)),
                Some(&body),
            )
            .await
            .map(|_| ())
    }
}

#[derive(Debug, Clone)]
struct SearchHttpClient {
    host: String,
    port: u16,
    base_path: String,
    authorization: Option<String>,
    transport: SearchTransport,
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
        })
    }

    async fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<JsonValue> {
        let (request, body) = self.build_request(method, path, body)?;

        let stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        if self.transport == SearchTransport::Tls {
            let connector = TlsConnector::from(Arc::new(tls_client_config()?));
            let server_name = ServerName::try_from(self.host.clone())
                .map_err(|e| Error::Dsn(format!("invalid TLS server name: {e}")))?;
            let stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            return send_http_request(stream, &request, &body).await;
        }

        send_http_request(stream, &request, &body).await
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

async fn send_http_request<S>(mut stream: S, request: &str, body: &[u8]) -> Result<JsonValue>
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

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;
    parse_http_json(&response)
}

fn validate_tls_server_name(host: &str) -> Result<()> {
    ServerName::try_from(host.to_owned())
        .map(|_| ())
        .map_err(|e| Error::Dsn(format!("invalid TLS server name: {e}")))
}

fn tls_client_config() -> Result<ClientConfig> {
    let cert_result = rustls_native_certs::load_native_certs();
    let load_errors = cert_result
        .errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ");
    let mut roots = RootCertStore::empty();
    let (valid, invalid) = roots.add_parsable_certificates(cert_result.certs);

    if valid == 0 {
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
            })
        })
        .collect::<Result<Vec<_>>>()?;
    indices.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(indices)
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
    if let Some(size) = options.size {
        object
            .entry("size".to_owned())
            .or_insert_with(|| JsonValue::Number(size.into()));
    }
    if let Some(from) = options.from {
        object
            .entry("from".to_owned())
            .or_insert_with(|| JsonValue::Number(from.into()));
    }
    if options.source {
        object
            .entry("_source".to_owned())
            .or_insert_with(|| JsonValue::Bool(true));
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
    let hits = response
        .get("hits")
        .ok_or_else(|| Error::Serialization("search response is missing hits".into()))?;
    let total = match hits.get("total") {
        Some(JsonValue::Number(number)) => number.as_u64().unwrap_or_default(),
        Some(JsonValue::Object(object)) => object
            .get("value")
            .and_then(JsonValue::as_u64)
            .unwrap_or_default(),
        _ => 0,
    };
    let hits = hits
        .get("hits")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| Error::Serialization("search response hits.hits is not an array".into()))?
        .clone();

    Ok(SearchHits { total, hits })
}

fn core_value_to_json(value: Value) -> Result<JsonValue> {
    serde_json::to_value(value).map_err(|e| Error::Serialization(e.to_string()))
}

fn parse_http_json(response: &[u8]) -> Result<JsonValue> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| Error::Connection("invalid HTTP response from search backend".into()))?;
    let (headers, body) = response.split_at(header_end);
    let body = &body[4..];
    let header_text = std::str::from_utf8(headers)
        .map_err(|e| Error::Connection(format!("invalid HTTP headers: {e}")))?;
    let status = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| Error::Connection("missing HTTP status".into()))?;
    let body = if header_text
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        decode_chunked_body(body)?
    } else {
        body.to_vec()
    };
    let body_text = std::str::from_utf8(&body).map_err(|e| Error::Serialization(e.to_string()))?;

    if !(200..300).contains(&status) {
        return Err(Error::Query(format!(
            "search backend returned HTTP {status}: {body_text}"
        )));
    }

    if body_text.trim().is_empty() {
        return Ok(JsonValue::Object(Map::new()));
    }

    serde_json::from_str(body_text).map_err(|e| Error::Serialization(e.to_string()))
}

fn decode_chunked_body(body: &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut position = 0;

    loop {
        let line_end = find_crlf(&body[position..])
            .map(|offset| position + offset)
            .ok_or_else(|| Error::Connection("invalid chunked response".into()))?;
        let size_line = std::str::from_utf8(&body[position..line_end])
            .map_err(|e| Error::Connection(format!("invalid chunk header: {e}")))?;
        let size = usize::from_str_radix(size_line.split(';').next().unwrap_or_default(), 16)
            .map_err(|e| Error::Connection(format!("invalid chunk size: {e}")))?;
        position = line_end + 2;

        if size == 0 {
            break;
        }

        let chunk_end = position + size;
        if chunk_end + 2 > body.len() {
            return Err(Error::Connection("truncated chunked response".into()));
        }
        decoded.extend_from_slice(&body[position..chunk_end]);
        position = chunk_end + 2;
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
    fn parses_search_hits_total_shapes() {
        let hits = search_hits_from_response(&json!({
            "hits": {
                "total": { "value": 2 },
                "hits": [
                    { "_id": "1", "_source": { "name": "alice" } },
                    { "_id": "2", "_source": { "name": "bob" } }
                ]
            }
        }))
        .unwrap();

        assert_eq!(hits.total, 2);
        assert_eq!(hits.hits.len(), 2);
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
    fn decodes_chunked_json_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n7\r\n{\"ok\":1\r\n1\r\n}\r\n0\r\n\r\n";
        let value = parse_http_json(response).unwrap();

        assert_eq!(value["ok"], 1);
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
