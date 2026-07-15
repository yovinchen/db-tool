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
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::{form_urlencoded, Url};

const MAX_HTTP_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_HTTP_RESPONSE_HEADER_BYTES: usize = 64 * 1024;

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

    async fn write_points(&self, points: Vec<Point>) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        let protobuf = remote_write_protobuf(&points)?;
        let payload = snappy_literal_block(&protobuf)?;
        self.client.request_remote_write(&payload).await
    }

    async fn query_range(&self, query: &str, range: TimeRange) -> Result<SeriesSet> {
        let path = self.client.query_range_path(query, &range)?;
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

        let response =
            read_bounded_http_response(&mut stream, MAX_HTTP_RESPONSE_BODY_BYTES).await?;
        parse_http_json(&response)
    }

    async fn request_remote_write(&self, body: &[u8]) -> Result<()> {
        let request = self.build_remote_write_request(body.len());
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        stream
            .write_all(body)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let response =
            read_bounded_http_response(&mut stream, MAX_HTTP_RESPONSE_BODY_BYTES).await?;
        parse_http_success(&response, "prometheus remote write")
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

    fn build_remote_write_request(&self, body_len: usize) -> String {
        let path = self.full_path("/api/v1/write");
        let mut request = format!(
            "POST {path} HTTP/1.1\r\nHost: {}:{}\r\nAccept: application/json\r\nConnection: close\r\n",
            self.host, self.port
        );
        if let Some(authorization) = &self.authorization {
            request.push_str(&format!("Authorization: Basic {authorization}\r\n"));
        }
        request.push_str("Content-Type: application/x-protobuf\r\n");
        request.push_str("Content-Encoding: snappy\r\n");
        request.push_str("X-Prometheus-Remote-Write-Version: 0.1.0\r\n");
        request.push_str(&format!("Content-Length: {body_len}\r\n\r\n"));
        request
    }

    fn query_range_path(&self, query: &str, range: &TimeRange) -> Result<String> {
        if query.trim().is_empty() {
            return Err(Error::Config(
                "Prometheus range query must not be empty".into(),
            ));
        }
        let (start_ms, end_ms) = range.require_closed()?;
        let mut query_string = form_urlencoded::Serializer::new(String::new());
        query_string.append_pair("query", query);
        query_string.append_pair("start", &millis_to_seconds(start_ms));
        query_string.append_pair("end", &millis_to_seconds(end_ms));
        query_string.append_pair("step", &self.step);
        Ok(format!("/api/v1/query_range?{}", query_string.finish()))
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

async fn read_bounded_http_response<S>(stream: &mut S, max_body_bytes: usize) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut response = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut body_start = None;

    loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        if read == 0 {
            break;
        }

        let next_len = response
            .len()
            .checked_add(read)
            .ok_or_else(|| Error::Connection("prometheus HTTP response size overflow".into()))?;
        if let Some(body_start) = body_start {
            let body_len = next_len.checked_sub(body_start).ok_or_else(|| {
                Error::Connection("prometheus HTTP response body size underflow".into())
            })?;
            ensure_http_body_within_limit(body_len, max_body_bytes, "body")?;
        }
        response.extend_from_slice(&buffer[..read]);

        if body_start.is_none() {
            if let Some(header_end) = find_http_header_end(&response) {
                ensure_http_headers_within_limit(header_end)?;
                let start = header_end.checked_add(4).ok_or_else(|| {
                    Error::Connection("prometheus HTTP response header size overflow".into())
                })?;
                let header_text = std::str::from_utf8(&response[..header_end])
                    .map_err(|e| Error::Connection(format!("invalid HTTP headers: {e}")))?;
                ensure_content_length_within_limit(header_text, max_body_bytes)?;
                let body_len = response.len().checked_sub(start).ok_or_else(|| {
                    Error::Connection("prometheus HTTP response body size underflow".into())
                })?;
                ensure_http_body_within_limit(body_len, max_body_bytes, "body")?;
                body_start = Some(start);
            } else if response.len() > MAX_HTTP_RESPONSE_HEADER_BYTES + 3 {
                return Err(Error::Connection(format!(
                    "prometheus HTTP response headers exceed {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
                )));
            }
        }
    }

    Ok(response)
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
    let result_type = response
        .get("data")
        .and_then(|data| data.get("resultType"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            Error::Serialization("prometheus query response is missing resultType".into())
        })?;
    if result_type != "matrix" {
        return Err(Error::Serialization(format!(
            "prometheus range query returned unsupported resultType '{result_type}', expected matrix"
        )));
    }
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

fn remote_write_protobuf(points: &[Point]) -> Result<Vec<u8>> {
    let mut request = Vec::new();

    for point in points {
        validate_metric_name(&point.measurement)?;
        let mut fields = point.fields.iter().collect::<Vec<_>>();
        fields.sort_by(|a, b| a.0.cmp(b.0));
        for (field, value) in fields {
            let metric = metric_name_for_field(point, field)?;
            let mut timeseries = Vec::new();

            encode_label(&mut timeseries, "__name__", &metric);
            let mut tags = point.tags.iter().collect::<Vec<_>>();
            tags.sort_by(|a, b| a.0.cmp(b.0));
            for (name, value) in tags {
                validate_label_name(name)?;
                encode_label(&mut timeseries, name, value);
            }
            encode_sample(&mut timeseries, *value, point.timestamp);
            encode_len_delimited(1, &timeseries, &mut request);
        }
    }

    Ok(request)
}

fn metric_name_for_field(point: &Point, field: &str) -> Result<String> {
    validate_label_name(field)?;
    let metric = if point.fields.len() == 1 && field == "value" {
        point.measurement.clone()
    } else {
        format!("{}_{}", point.measurement, field)
    };
    validate_metric_name(&metric)?;
    Ok(metric)
}

fn encode_label(output: &mut Vec<u8>, name: &str, value: &str) {
    let mut label = Vec::new();
    encode_string(1, name, &mut label);
    encode_string(2, value, &mut label);
    encode_len_delimited(1, &label, output);
}

fn encode_sample(output: &mut Vec<u8>, value: f64, timestamp: i64) {
    let mut sample = Vec::new();
    encode_key(1, 1, &mut sample);
    sample.extend_from_slice(&value.to_le_bytes());
    encode_key(2, 0, &mut sample);
    encode_varint(timestamp as u64, &mut sample);
    encode_len_delimited(2, &sample, output);
}

fn encode_string(field_number: u32, value: &str, output: &mut Vec<u8>) {
    encode_len_delimited(field_number, value.as_bytes(), output);
}

fn encode_len_delimited(field_number: u32, value: &[u8], output: &mut Vec<u8>) {
    encode_key(field_number, 2, output);
    encode_varint(value.len() as u64, output);
    output.extend_from_slice(value);
}

fn encode_key(field_number: u32, wire_type: u8, output: &mut Vec<u8>) {
    encode_varint(((field_number as u64) << 3) | u64::from(wire_type), output);
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn snappy_literal_block(input: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    encode_varint(input.len() as u64, &mut output);

    if input.is_empty() {
        return Ok(output);
    }

    let literal_len = input.len() - 1;
    if literal_len < 60 {
        output.push((literal_len as u8) << 2);
    } else {
        let mut length_bytes = Vec::new();
        let mut value = literal_len;
        while value > 0 {
            length_bytes.push((value & 0xff) as u8);
            value >>= 8;
        }
        if length_bytes.len() > 4 {
            return Err(Error::Serialization(
                "prometheus remote write payload is too large".into(),
            ));
        }
        output.push(((59 + length_bytes.len()) as u8) << 2);
        output.extend_from_slice(&length_bytes);
    }
    output.extend_from_slice(input);
    Ok(output)
}

fn validate_metric_name(name: &str) -> Result<()> {
    validate_name(name, true, "prometheus metric name")
}

fn validate_label_name(name: &str) -> Result<()> {
    validate_name(name, false, "prometheus label name")
}

fn validate_name(name: &str, allow_colon: bool, label: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(Error::Config(format!("{label} must not be empty")));
    };
    let valid_first = first == '_' || first.is_ascii_alphabetic() || (allow_colon && first == ':');
    if !valid_first {
        return Err(Error::Config(format!("invalid {label}: {name}")));
    }
    let valid_rest =
        chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric() || (allow_colon && ch == ':'));
    if valid_rest {
        Ok(())
    } else {
        Err(Error::Config(format!("invalid {label}: {name}")))
    }
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

fn millis_to_seconds(millis: i64) -> String {
    if millis % 1000 == 0 {
        (millis / 1000).to_string()
    } else {
        format!("{:.3}", millis as f64 / 1000.0)
    }
}

fn parse_http_json(response: &[u8]) -> Result<JsonValue> {
    let (status, body) = parse_http_response(response)?;
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

fn parse_http_success(response: &[u8], operation: &str) -> Result<()> {
    let (status, body) = parse_http_response(response)?;
    if (200..300).contains(&status) {
        return Ok(());
    }
    let body_text = String::from_utf8_lossy(&body);
    Err(Error::Query(format!(
        "{operation} returned HTTP {status}: {body_text}"
    )))
}

fn parse_http_response(response: &[u8]) -> Result<(u16, Vec<u8>)> {
    parse_http_response_with_limit(response, MAX_HTTP_RESPONSE_BODY_BYTES)
}

fn parse_http_response_with_limit(
    response: &[u8],
    max_body_bytes: usize,
) -> Result<(u16, Vec<u8>)> {
    let header_end = find_http_header_end(response)
        .ok_or_else(|| Error::Connection("invalid HTTP response from prometheus backend".into()))?;
    ensure_http_headers_within_limit(header_end)?;
    let body_start = header_end
        .checked_add(4)
        .ok_or_else(|| Error::Connection("prometheus HTTP response header size overflow".into()))?;
    let headers = &response[..header_end];
    let body = response
        .get(body_start..)
        .ok_or_else(|| Error::Connection("invalid HTTP response from prometheus backend".into()))?;
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
    Ok((status, body))
}

fn find_http_header_end(response: &[u8]) -> Option<usize> {
    response.windows(4).position(|window| window == b"\r\n\r\n")
}

fn ensure_http_headers_within_limit(header_bytes: usize) -> Result<()> {
    if header_bytes > MAX_HTTP_RESPONSE_HEADER_BYTES {
        return Err(Error::Connection(format!(
            "prometheus HTTP response headers exceed {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
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
        let parsed = value.trim().parse::<usize>().map_err(|e| {
            Error::Connection(format!("invalid prometheus HTTP Content-Length: {e}"))
        })?;
        if let Some(existing) = content_length {
            if existing != parsed {
                return Err(Error::Connection(
                    "conflicting prometheus HTTP Content-Length headers".into(),
                ));
            }
        }
        content_length = Some(parsed);
    }

    if let Some(content_length) = content_length {
        if content_length > max_body_bytes {
            return Err(Error::Connection(format!(
                "prometheus HTTP Content-Length {content_length} exceeds limit of {max_body_bytes} bytes"
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
            "prometheus HTTP response {body_kind} exceeds limit of {max_body_bytes} bytes"
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
            .ok_or_else(|| Error::Connection("prometheus chunk position overflow".into()))?;

        if size == 0 {
            break;
        }

        let chunk_end = position
            .checked_add(size)
            .ok_or_else(|| Error::Connection("prometheus chunk size overflow".into()))?;
        let framed_end = chunk_end
            .checked_add(2)
            .ok_or_else(|| Error::Connection("prometheus chunk framing overflow".into()))?;
        if framed_end > body.len() {
            return Err(Error::Connection("truncated chunked response".into()));
        }
        if body.get(chunk_end..framed_end) != Some(b"\r\n") {
            return Err(Error::Connection("invalid chunk terminator".into()));
        }
        let decoded_len = decoded
            .len()
            .checked_add(size)
            .ok_or_else(|| Error::Connection("prometheus decoded body size overflow".into()))?;
        ensure_http_body_within_limit(decoded_len, max_body_bytes, "decoded body")?;
        decoded.extend_from_slice(&body[position..chunk_end]);
        position = framed_end;
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
    use std::collections::HashMap;

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
        let path = client
            .query_range_path(
                r#"rate(http_requests_total{job="api"}[5m])"#,
                &TimeRange {
                    start: Some(1710000000000),
                    end: Some(1710000060000),
                },
            )
            .unwrap();
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
    fn range_request_rejects_open_reversed_and_empty_inputs() {
        let client =
            PrometheusHttpClient::from_dsn(&Dsn::parse("prometheus://prom.local:9090").unwrap())
                .unwrap();
        assert!(client
            .query_range_path(
                "up",
                &TimeRange {
                    start: Some(1),
                    end: None,
                },
            )
            .is_err());
        assert!(client
            .query_range_path(
                "up",
                &TimeRange {
                    start: Some(2),
                    end: Some(1),
                },
            )
            .is_err());
        assert!(client
            .query_range_path(
                "  ",
                &TimeRange {
                    start: Some(1),
                    end: Some(2),
                },
            )
            .is_err());
    }

    #[test]
    fn range_response_rejects_non_matrix_result_types() {
        let response = json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []}
        });
        assert!(matches!(
            series_set_from_response(&response),
            Err(Error::Serialization(message)) if message.contains("expected matrix")
        ));
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
    fn builds_remote_write_request_headers() {
        let dsn = Dsn::parse("prometheus://alice:secret@prom.local:9091/base").unwrap();
        let client = PrometheusHttpClient::from_dsn(&dsn).unwrap();
        let request = client.build_remote_write_request(123);

        assert!(request.starts_with("POST /base/api/v1/write HTTP/1.1"));
        assert!(request.contains("Authorization: Basic YWxpY2U6c2VjcmV0"));
        assert!(request.contains("Content-Type: application/x-protobuf"));
        assert!(request.contains("Content-Encoding: snappy"));
        assert!(request.contains("X-Prometheus-Remote-Write-Version: 0.1.0"));
        assert!(request.contains("Content-Length: 123"));
    }

    #[test]
    fn remote_write_protobuf_encodes_labels_and_samples() {
        let point = Point {
            measurement: "http_requests_total".to_owned(),
            tags: HashMap::from([
                ("method".to_owned(), "GET".to_owned()),
                ("service".to_owned(), "api".to_owned()),
            ]),
            fields: HashMap::from([("value".to_owned(), 42.5)]),
            timestamp: 1_710_000_000_123,
        };

        let encoded = remote_write_protobuf(&[point]).unwrap();

        assert!(contains_bytes(&encoded, b"__name__"));
        assert!(contains_bytes(&encoded, b"http_requests_total"));
        assert!(contains_bytes(&encoded, b"method"));
        assert!(contains_bytes(&encoded, b"GET"));
        assert!(contains_bytes(&encoded, b"service"));
        assert!(contains_bytes(&encoded, b"api"));
        assert!(contains_bytes(&encoded, &42.5_f64.to_le_bytes()));
    }

    #[test]
    fn remote_write_rejects_invalid_prometheus_names() {
        let point = Point {
            measurement: "bad metric".to_owned(),
            tags: HashMap::new(),
            fields: HashMap::from([("value".to_owned(), 1.0)]),
            timestamp: 1,
        };

        assert!(matches!(
            remote_write_protobuf(&[point]),
            Err(Error::Config(message)) if message.contains("metric name")
        ));
    }

    #[test]
    fn snappy_literal_block_round_trips_without_external_dependency() {
        let payload = (0..300).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let block = snappy_literal_block(&payload).unwrap();
        let decoded = decode_test_snappy_literal(&block);

        assert_eq!(decoded, payload);
    }

    #[test]
    fn decodes_chunked_json_response() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n13\r\n{\"status\":\"success\"\r\n1\r\n}\r\n0\r\n\r\n";
        let value = parse_http_json(response).unwrap();

        assert_eq!(value["status"], "success");
    }

    #[test]
    fn rejects_content_length_above_http_body_budget() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n";

        let error = parse_http_response_with_limit(response, 8).unwrap_err();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("Content-Length") && message.contains("8 bytes")
        ));
    }

    #[test]
    fn rejects_raw_http_body_above_budget() {
        let response = b"HTTP/1.1 200 OK\r\n\r\n123456789";

        let error = parse_http_response_with_limit(response, 8).unwrap_err();

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

        let error = read_bounded_http_response(&mut reader, 8)
            .await
            .unwrap_err();
        writer_task.await.unwrap();

        assert!(matches!(
            error,
            Error::Connection(message)
                if message.contains("body") && message.contains("8 bytes")
        ));
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn decode_test_snappy_literal(block: &[u8]) -> Vec<u8> {
        let mut pos = 0;
        let mut len = 0usize;
        let mut shift = 0;
        loop {
            let byte = block[pos];
            pos += 1;
            len |= ((byte & 0x7f) as usize) << shift;
            if byte < 0x80 {
                break;
            }
            shift += 7;
        }

        let tag = block[pos];
        pos += 1;
        assert_eq!(tag & 0b11, 0);
        let literal_len = match tag >> 2 {
            n @ 0..=59 => n as usize + 1,
            n => {
                let length_bytes = (n - 59) as usize;
                let mut value = 0usize;
                for index in 0..length_bytes {
                    value |= (block[pos + index] as usize) << (index * 8);
                }
                pos += length_bytes;
                value + 1
            }
        };
        assert_eq!(literal_len, len);
        block[pos..pos + literal_len].to_vec()
    }
}
