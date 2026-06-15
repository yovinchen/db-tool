use crate::validate_queue;
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{LagInfo, PartitionWatermark, TopicDetail, TopicInfo},
    port::{
        capability::AdminInspect,
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use serde_json::Value;
use std::collections::HashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::Url;

pub struct RabbitManagementAdapter {
    client: RabbitManagementClient,
    kind: ConnectorKind,
}

pub fn management_factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = RabbitManagementClient::from_dsn(&dsn)?;
        Ok(Box::new(RabbitManagementAdapter {
            client,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for RabbitManagementAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            admin: true,
            ..Default::default()
        }
    }

    async fn ping(&self) -> Result<()> {
        self.client.get_json("/api/overview").await.map(|_| ())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_admin(&self) -> Option<&dyn AdminInspect> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl AdminInspect for RabbitManagementAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let queues = self.client.get_json(&self.client.queues_path()).await?;
        let queues = queues.as_array().ok_or_else(|| {
            Error::Serialization("RabbitMQ queues response is not an array".into())
        })?;
        let mut topics = queues
            .iter()
            .map(queue_topic_info)
            .collect::<Result<Vec<_>>>()?;
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        validate_queue(name)?;
        let queue = self.client.get_json(&self.client.queue_path(name)).await?;
        queue_detail(&queue)
    }

    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>> {
        validate_queue(group)?;
        let queue = self.client.get_json(&self.client.queue_path(group)).await?;
        let ready = json_i64(&queue, "messages_ready");
        let unacked = json_i64(&queue, "messages_unacknowledged");
        let total = ready + unacked;

        Ok(vec![LagInfo {
            topic: json_string(&queue, "name").unwrap_or_else(|| group.to_owned()),
            partition: 0,
            group: group.to_owned(),
            committed: unacked,
            latest: total,
            lag: ready,
        }])
    }
}

struct RabbitManagementClient {
    host: String,
    port: u16,
    vhost: String,
    authorization: String,
}

impl RabbitManagementClient {
    fn from_dsn(dsn: &Dsn) -> Result<Self> {
        let url = Url::parse(&dsn.raw).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;
        if url.scheme() != "rabbitmq+http" {
            return Err(Error::Dsn(format!(
                "RabbitMQ management DSN must use rabbitmq+http, got {}",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| Error::Dsn("RabbitMQ management DSN requires a host".into()))?
            .to_owned();
        let port = url.port().unwrap_or(15672);
        let username = percent_decode(url.username())?;
        if username.is_empty() {
            return Err(Error::Dsn(
                "RabbitMQ management DSN requires a username".into(),
            ));
        }
        let password = url
            .password()
            .map(percent_decode)
            .transpose()?
            .unwrap_or_default();
        let vhost = match url.path().trim_start_matches('/') {
            "" => "/".to_owned(),
            path => percent_decode(path)?,
        };
        let authorization = basic_auth(&username, &password);

        Ok(Self {
            host,
            port,
            vhost,
            authorization,
        })
    }

    fn queues_path(&self) -> String {
        format!("/api/queues/{}", percent_encode(&self.vhost))
    }

    fn queue_path(&self, queue: &str) -> String {
        format!("{}/{}", self.queues_path(), percent_encode(queue))
    }

    async fn get_json(&self, path: &str) -> Result<Value> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {}:{}\r\nAuthorization: Basic {}\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
            self.host, self.port, self.authorization
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        parse_http_json(&response)
    }
}

fn queue_topic_info(queue: &Value) -> Result<TopicInfo> {
    let name = json_string(queue, "name")
        .ok_or_else(|| Error::Serialization("RabbitMQ queue is missing name".into()))?;
    Ok(TopicInfo {
        name,
        partitions: 1,
        replicas: 1,
    })
}

fn queue_detail(queue: &Value) -> Result<TopicDetail> {
    let info = queue_topic_info(queue)?;
    let mut config = HashMap::new();
    for key in [
        "vhost",
        "state",
        "type",
        "durable",
        "auto_delete",
        "exclusive",
        "messages",
        "messages_ready",
        "messages_unacknowledged",
        "consumers",
    ] {
        if let Some(value) = queue.get(key) {
            config.insert(key.to_owned(), json_config_value(value));
        }
    }
    config.insert(
        "message_count".to_owned(),
        json_i64(queue, "messages").to_string(),
    );
    config.insert(
        "consumer_count".to_owned(),
        json_i64(queue, "consumers").to_string(),
    );
    let total = json_i64(queue, "messages");

    Ok(TopicDetail {
        info,
        config,
        watermarks: vec![PartitionWatermark {
            partition: 0,
            low: 0,
            high: total,
        }],
    })
}

fn json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn json_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or_default()
}

fn json_config_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_owned(),
        other => other.to_string(),
    }
}

fn parse_http_json(response: &[u8]) -> Result<Value> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            Error::Connection("invalid HTTP response from RabbitMQ management".into())
        })?;
    let (headers, body) = response.split_at(header_end);
    let body = &body[4..];
    let header_text = std::str::from_utf8(headers)
        .map_err(|e| Error::Connection(format!("invalid HTTP headers: {e}")))?;
    let status = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| Error::Connection("missing HTTP status from RabbitMQ management".into()))?;
    let body = if header_text
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        decode_chunked_body(body)?
    } else {
        body.to_vec()
    };
    let body_text = std::str::from_utf8(&body)
        .map_err(|e| Error::Connection(format!("invalid HTTP response body: {e}")))?;

    if !(200..300).contains(&status) {
        if status == 401 || status == 403 {
            return Err(Error::Auth(body_text.to_owned()));
        }
        return Err(Error::Query(format!(
            "RabbitMQ management returned HTTP {status}: {body_text}"
        )));
    }

    serde_json::from_str(body_text).map_err(|e| Error::Serialization(e.to_string()))
}

fn decode_chunked_body(mut body: &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = body
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::Connection("invalid chunked RabbitMQ response".into()))?;
        let size_text = std::str::from_utf8(&body[..line_end])
            .map_err(|e| Error::Connection(format!("invalid chunk size: {e}")))?;
        let size_text = size_text.split(';').next().unwrap_or(size_text);
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|e| Error::Connection(format!("invalid chunk size: {e}")))?;
        body = &body[line_end + 2..];
        if size == 0 {
            return Ok(decoded);
        }
        if body.len() < size + 2 {
            return Err(Error::Connection(
                "truncated chunked RabbitMQ response".into(),
            ));
        }
        decoded.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
}

fn basic_auth(username: &str, password: &str) -> String {
    base64_encode(format!("{username}:{password}").as_bytes())
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn percent_encode(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    out
}

fn percent_decode(input: &str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(Error::Dsn("invalid percent escape in DSN".into()));
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .map_err(|e| Error::Dsn(format!("invalid percent escape: {e}")))?;
            decoded.push(
                u8::from_str_radix(hex, 16)
                    .map_err(|e| Error::Dsn(format!("invalid percent escape: {e}")))?,
            );
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(decoded).map_err(|e| Error::Dsn(format!("invalid UTF-8 in DSN: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_dsn_extracts_vhost_and_auth() {
        let dsn = Dsn::parse("rabbitmq+http://dbtool:secret@127.0.0.1:15672/%2F").unwrap();
        let client = RabbitManagementClient::from_dsn(&dsn).unwrap();

        assert_eq!(client.host, "127.0.0.1");
        assert_eq!(client.port, 15672);
        assert_eq!(client.vhost, "/");
        assert_eq!(client.authorization, "ZGJ0b29sOnNlY3JldA==");
        assert_eq!(client.queues_path(), "/api/queues/%2F");
    }

    #[test]
    fn queue_path_escapes_vhost_and_queue() {
        let dsn = Dsn::parse("rabbitmq+http://dbtool:secret@localhost/vhost/a").unwrap();
        let client = RabbitManagementClient::from_dsn(&dsn).unwrap();

        assert_eq!(
            client.queue_path("jobs/email"),
            "/api/queues/vhost%2Fa/jobs%2Femail"
        );
    }

    #[test]
    fn queue_detail_maps_management_fields() {
        let value = serde_json::json!({
            "name": "jobs",
            "vhost": "dbtool_it",
            "messages": 3,
            "messages_ready": 2,
            "messages_unacknowledged": 1,
            "consumers": 4,
            "state": "running"
        });

        let detail = queue_detail(&value).unwrap();

        assert_eq!(detail.info.name, "jobs");
        assert_eq!(detail.config["message_count"], "3");
        assert_eq!(detail.config["consumer_count"], "4");
        assert_eq!(detail.watermarks[0].high, 3);
    }
}
