use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{series::Series, Point, SeriesSet, TimeRange},
    port::{
        capability::TimeSeriesStore,
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use serde_json::{Map, Value as JsonValue};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::{form_urlencoded, Url};

pub struct PrometheusAdapter {
    client: PrometheusHttpClient,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = PrometheusHttpClient::from_dsn(&dsn)?;
        Ok(Box::new(PrometheusAdapter {
            client,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for PrometheusAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            time_series: true,
            ..Default::default()
        }
    }

    async fn ping(&self) -> Result<()> {
        self.client
            .request_json("GET", "/api/v1/status/buildinfo", None)
            .await
            .and_then(|response| ensure_success(&response))
            .map(|_| ())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_timeseries(&self) -> Option<&dyn TimeSeriesStore> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl TimeSeriesStore for PrometheusAdapter {
    async fn list_measurements(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .request_json("GET", "/api/v1/label/__name__/values", None)
            .await?;
        measurement_names_from_response(&response)
    }

    async fn write_points(&self, _points: Vec<Point>) -> Result<()> {
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "TimeSeriesStore::write_points",
        })
    }

    async fn query_range(&self, query: &str, range: TimeRange) -> Result<SeriesSet> {
        let path = self.client.query_range_path(query, range);
        let response = self.client.request_json("GET", &path, None).await?;
        series_set_from_response(&response)
    }
}

#[derive(Debug, Clone)]
struct PrometheusHttpClient {
    host: String,
    port: u16,
    base_path: String,
    authorization: Option<String>,
    step: String,
}

impl PrometheusHttpClient {
    fn from_dsn(dsn: &Dsn) -> Result<Self> {
        let url = Url::parse(&dsn.raw).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;
        match url.scheme() {
            "prometheus" | "prometheus+http" => {}
            scheme => {
                return Err(Error::Dsn(format!(
                    "time-series DSN must use prometheus:// or prometheus+http://, got {scheme}"
                )))
            }
        }

        let host = url
            .host_str()
            .ok_or_else(|| Error::Dsn("prometheus DSN requires a host".into()))?
            .to_owned();
        let port = url.port().unwrap_or(9090);
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
        let step = url
            .query_pairs()
            .find(|(key, _)| key == "step")
            .map(|(_, value)| value.into_owned())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "15s".to_owned());

        Ok(Self {
            host,
            port,
            base_path: normalize_base_path(url.path()),
            authorization,
            step,
        })
    }

    async fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<JsonValue> {
        let (request, body) = self.build_request(method, path, body)?;
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        if !body.is_empty() {
            stream
                .write_all(&body)
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

    fn build_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&JsonValue>,
    ) -> Result<(String, Vec<u8>)> {
        let body = body
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|e| Error::Serialization(e.to_string()))?
            .unwrap_or_default();
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

    fn query_range_path(&self, query: &str, range: TimeRange) -> String {
        let end_ms = range.end.unwrap_or_else(now_millis);
        let start_ms = range.start.unwrap_or(end_ms - 60 * 60 * 1000);
        let mut query_string = form_urlencoded::Serializer::new(String::new());
        query_string.append_pair("query", query);
        query_string.append_pair("start", &millis_to_seconds(start_ms));
        query_string.append_pair("end", &millis_to_seconds(end_ms));
        query_string.append_pair("step", &self.step);
        format!("/api/v1/query_range?{}", query_string.finish())
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

fn normalize_base_path(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() || path == "/" {
        String::new()
    } else {
        path.to_owned()
    }
}

fn measurement_names_from_response(response: &JsonValue) -> Result<Vec<String>> {
    ensure_success(response)?;
    let mut names = response
        .get("data")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            Error::Serialization("prometheus label response data is not an array".into())
        })?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                Error::Serialization("prometheus metric name is not a string".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

fn series_set_from_response(response: &JsonValue) -> Result<SeriesSet> {
    ensure_success(response)?;
    let results = response
        .get("data")
        .and_then(|data| data.get("result"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            Error::Serialization("prometheus query response result is not an array".into())
        })?;
    let mut series = Vec::with_capacity(results.len());

    for item in results {
        let metric = item
            .get("metric")
            .and_then(JsonValue::as_object)
            .cloned()
            .unwrap_or_default();
        let labels = sorted_labels(&metric);
        let name = series_name(&metric, &labels);
        let mut columns = vec!["timestamp".to_owned(), "value".to_owned()];
        columns.extend(labels.iter().map(|(key, _)| key.clone()));
        let values = if let Some(samples) = item.get("values").and_then(JsonValue::as_array) {
            samples
                .iter()
                .map(|sample| sample_row(sample, &labels))
                .collect::<Result<Vec<_>>>()?
        } else if let Some(sample) = item.get("value") {
            vec![sample_row(sample, &labels)?]
        } else {
            Vec::new()
        };

        series.push(Series {
            name,
            columns,
            values,
        });
    }

    Ok(SeriesSet {
        series,
        truncated: false,
    })
}

fn ensure_success(response: &JsonValue) -> Result<()> {
    if response.get("status").and_then(JsonValue::as_str) == Some("success") {
        return Ok(());
    }

    let error = response
        .get("error")
        .and_then(JsonValue::as_str)
        .unwrap_or("prometheus API returned non-success status");
    Err(Error::Query(error.to_owned()))
}

fn sorted_labels(metric: &Map<String, JsonValue>) -> Vec<(String, String)> {
    let mut labels = metric
        .iter()
        .filter(|(key, _)| key.as_str() != "__name__")
        .map(|(key, value)| {
            (
                key.clone(),
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            )
        })
        .collect::<Vec<_>>();
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    labels
}

fn series_name(metric: &Map<String, JsonValue>, labels: &[(String, String)]) -> String {
    if let Some(name) = metric.get("__name__").and_then(JsonValue::as_str) {
        return name.to_owned();
    }
    if labels.is_empty() {
        "series".to_owned()
    } else {
        labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn sample_row(sample: &JsonValue, labels: &[(String, String)]) -> Result<Vec<JsonValue>> {
    let sample = sample
        .as_array()
        .ok_or_else(|| Error::Serialization("prometheus sample is not an array".into()))?;
    if sample.len() < 2 {
        return Err(Error::Serialization(
            "prometheus sample must contain timestamp and value".into(),
        ));
    }
    let timestamp = timestamp_to_millis(&sample[0])?;
    let value = sample_value(&sample[1]);
    let mut row = vec![JsonValue::Number(timestamp.into()), value];
    row.extend(
        labels
            .iter()
            .map(|(_, value)| JsonValue::String(value.clone())),
    );
    Ok(row)
}

fn timestamp_to_millis(value: &JsonValue) -> Result<i64> {
    if let Some(seconds) = value.as_f64() {
        return Ok((seconds * 1000.0).round() as i64);
    }
    if let Some(seconds) = value.as_str().and_then(|raw| raw.parse::<f64>().ok()) {
        return Ok((seconds * 1000.0).round() as i64);
    }
    Err(Error::Serialization(
        "prometheus sample timestamp is not numeric".into(),
    ))
}

fn sample_value(value: &JsonValue) -> JsonValue {
    value
        .as_str()
        .and_then(|raw| raw.parse::<f64>().ok())
        .and_then(serde_json::Number::from_f64)
        .map(JsonValue::Number)
        .unwrap_or_else(|| value.clone())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn millis_to_seconds(millis: i64) -> String {
    if millis % 1000 == 0 {
        (millis / 1000).to_string()
    } else {
        format!("{:.3}", millis as f64 / 1000.0)
    }
}

fn parse_http_json(response: &[u8]) -> Result<JsonValue> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| Error::Connection("invalid HTTP response from prometheus backend".into()))?;
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
            "prometheus backend returned HTTP {status}: {body_text}"
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

    for chunk in input.chunks(3) {
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
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_measurement_names() {
        let names = measurement_names_from_response(&json!({
            "status": "success",
            "data": ["process_cpu_seconds_total", "up"]
        }))
        .unwrap();

        assert_eq!(names, vec!["process_cpu_seconds_total", "up"]);
    }

    #[test]
    fn parses_matrix_query_response() {
        let result = series_set_from_response(&json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "instance": "localhost:9090",
                            "job": "prometheus"
                        },
                        "values": [
                            [1710000000.5, "1"],
                            [1710000015, "0"]
                        ]
                    }
                ]
            }
        }))
        .unwrap();

        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].name, "up");
        assert_eq!(
            result.series[0].columns,
            vec!["timestamp", "value", "instance", "job"]
        );
        assert_eq!(result.series[0].values[0][0], json!(1710000000500_i64));
        assert_eq!(result.series[0].values[0][1], json!(1.0));
        assert_eq!(result.series[0].values[0][2], json!("localhost:9090"));
    }

    #[test]
    fn builds_query_range_request_with_prefix_and_step() {
        let dsn = Dsn::parse("prometheus://prom.local:9090/base?step=30s").unwrap();
        let client = PrometheusHttpClient::from_dsn(&dsn).unwrap();
        let path = client.query_range_path(
            r#"rate(http_requests_total{job="api"}[5m])"#,
            TimeRange {
                start: Some(1710000000000),
                end: Some(1710000060000),
            },
        );
        let (request, body) = client.build_request("GET", &path, None).unwrap();

        assert!(request.starts_with("GET /base/api/v1/query_range?"));
        assert!(
            request.contains("query=rate%28http_requests_total%7Bjob%3D%22api%22%7D%5B5m%5D%29")
        );
        assert!(request.contains("start=1710000000"));
        assert!(request.contains("end=1710000060"));
        assert!(request.contains("step=30s"));
        assert!(body.is_empty());
    }

    #[test]
    fn builds_basic_auth_header() {
        let dsn = Dsn::parse("prometheus://alice:secret@prom.local:9091").unwrap();
        let client = PrometheusHttpClient::from_dsn(&dsn).unwrap();
        let (request, _) = client
            .build_request("GET", "/api/v1/status/buildinfo", None)
            .unwrap();

        assert!(request.contains("Host: prom.local:9091"));
        assert!(request.contains("Authorization: Basic YWxpY2U6c2VjcmV0"));
    }

    #[test]
    fn decodes_chunked_json_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n13\r\n{\"status\":\"success\"\r\n1\r\n}\r\n0\r\n\r\n";
        let value = parse_http_json(response).unwrap();

        assert_eq!(value["status"], "success");
    }
}
