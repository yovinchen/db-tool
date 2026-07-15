use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// Unified value type — all adapters map their native types to/from this.
///
/// JSON-native scalar variants keep their natural representation. Variants
/// whose identity cannot be represented by a JSON primitive use the reserved
/// `{"$dbtool": {"codec": "dbtool-value-v2", ...}}` extension object. The
/// previous untagged representation could not distinguish integers from
/// timestamps, byte arrays from arrays, or JSON objects from typed maps.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    /// Serialized as canonical RFC 4648 base64 by the v2 wire codec.
    Bytes(Vec<u8>),
    /// Milliseconds since Unix epoch, UTC.
    Timestamp(i64),
    Json(serde_json::Value),
    Array(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

const VALUE_WIRE_CODEC: &str = "dbtool-value-v2";

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelopeRef<'a> {
    #[serde(rename = "$dbtool")]
    extension: WireValueRef<'a>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireValueRef<'a> {
    Bytes {
        codec: &'static str,
        value: String,
    },
    Timestamp {
        codec: &'static str,
        value: i64,
    },
    Json {
        codec: &'static str,
        value: &'a serde_json::Value,
    },
    Array {
        codec: &'static str,
        value: &'a [Value],
    },
    Map {
        codec: &'static str,
        value: &'a BTreeMap<String, Value>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelope {
    #[serde(rename = "$dbtool")]
    extension: WireValue,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WireValue {
    Bytes {
        codec: String,
        value: String,
    },
    Timestamp {
        codec: String,
        value: i64,
    },
    Json {
        codec: String,
        value: serde_json::Value,
    },
    Array {
        codec: String,
        value: Vec<Value>,
    },
    Map {
        codec: String,
        value: BTreeMap<String, Value>,
    },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireInput {
    Null(()),
    Bool(bool),
    Int(i64),
    Unsigned(u64),
    Float(f64),
    Text(String),
    Extended(WireEnvelope),
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Float(value) => {
                if !value.is_finite() {
                    return Err(serde::ser::Error::custom(
                        "dbtool-value-v2 cannot encode a non-finite float",
                    ));
                }
                serializer.serialize_f64(*value)
            }
            Self::Text(value) => serializer.serialize_str(value),
            Self::Bytes(value) => WireEnvelopeRef {
                extension: WireValueRef::Bytes {
                    codec: VALUE_WIRE_CODEC,
                    value: encode_base64(value),
                },
            }
            .serialize(serializer),
            Self::Timestamp(value) => WireEnvelopeRef {
                extension: WireValueRef::Timestamp {
                    codec: VALUE_WIRE_CODEC,
                    value: *value,
                },
            }
            .serialize(serializer),
            Self::Json(value) => WireEnvelopeRef {
                extension: WireValueRef::Json {
                    codec: VALUE_WIRE_CODEC,
                    value,
                },
            }
            .serialize(serializer),
            Self::Array(value) => WireEnvelopeRef {
                extension: WireValueRef::Array {
                    codec: VALUE_WIRE_CODEC,
                    value,
                },
            }
            .serialize(serializer),
            Self::Map(value) => WireEnvelopeRef {
                extension: WireValueRef::Map {
                    codec: VALUE_WIRE_CODEC,
                    value,
                },
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match WireInput::deserialize(deserializer)? {
            WireInput::Null(()) => Self::Null,
            WireInput::Bool(value) => Self::Bool(value),
            WireInput::Int(value) => Self::Int(value),
            WireInput::Unsigned(value) => Self::Int(i64::try_from(value).map_err(|_| {
                serde::de::Error::custom(format!(
                    "unsigned integer {value} exceeds the dbtool Value::Int range"
                ))
            })?),
            WireInput::Float(value) => {
                if !value.is_finite() {
                    return Err(serde::de::Error::custom(
                        "dbtool-value-v2 cannot decode a non-finite float",
                    ));
                }
                Self::Float(value)
            }
            WireInput::Text(value) => Self::Text(value),
            WireInput::Extended(envelope) => match envelope.extension {
                WireValue::Bytes { codec, value } => {
                    validate_wire_codec::<D::Error>(&codec)?;
                    Self::Bytes(decode_canonical_base64(&value).map_err(serde::de::Error::custom)?)
                }
                WireValue::Timestamp { codec, value } => {
                    validate_wire_codec::<D::Error>(&codec)?;
                    Self::Timestamp(value)
                }
                WireValue::Json { codec, value } => {
                    validate_wire_codec::<D::Error>(&codec)?;
                    Self::Json(value)
                }
                WireValue::Array { codec, value } => {
                    validate_wire_codec::<D::Error>(&codec)?;
                    Self::Array(value)
                }
                WireValue::Map { codec, value } => {
                    validate_wire_codec::<D::Error>(&codec)?;
                    Self::Map(value)
                }
            },
        })
    }
}

fn validate_wire_codec<E>(codec: &str) -> std::result::Result<(), E>
where
    E: serde::de::Error,
{
    if codec == VALUE_WIRE_CODEC {
        Ok(())
    } else {
        Err(E::custom(format!(
            "unsupported dbtool Value wire codec {codec:?}; expected {VALUE_WIRE_CODEC:?}"
        )))
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or_default();
        let third = chunk.get(2).copied().unwrap_or_default();
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

/// Decode canonical RFC 4648 base64, including mandatory padding and zero
/// trailing bits. This is shared by the typed value codec and CLI inputs so a
/// byte sequence has exactly one accepted textual representation.
pub fn decode_canonical_base64(encoded: &str) -> std::result::Result<Vec<u8>, String> {
    let bytes = encoded.as_bytes();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if !bytes.len().is_multiple_of(4) {
        return Err("base64 length must be a multiple of four".to_owned());
    }

    let mut decoded = Vec::with_capacity((bytes.len() / 4) * 3);
    let chunk_count = bytes.len() / 4;
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let is_last = index + 1 == chunk_count;
        let first = decode_base64_digit(chunk[0])?;
        let second = decode_base64_digit(chunk[1])?;
        decoded.push((first << 2) | (second >> 4));

        match (chunk[2], chunk[3]) {
            (b'=', b'=') if is_last && second & 0x0f == 0 => {}
            (b'=', b'=') => {
                return Err("base64 has non-canonical or misplaced padding".to_owned());
            }
            (b'=', _) => return Err("base64 padding is only valid at the end".to_owned()),
            (third, b'=') => {
                let third = decode_base64_digit(third)?;
                if !is_last || third & 0x03 != 0 {
                    return Err("base64 has non-canonical or misplaced padding".to_owned());
                }
                decoded.push((second << 4) | (third >> 2));
            }
            (third, fourth) => {
                let third = decode_base64_digit(third)?;
                let fourth = decode_base64_digit(fourth)?;
                decoded.push((second << 4) | (third >> 2));
                decoded.push((third << 6) | fourth);
            }
        }
    }
    Ok(decoded)
}

fn decode_base64_digit(byte: u8) -> std::result::Result<u8, String> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(format!("invalid base64 character: {byte:?}")),
    }
}

impl Value {
    /// Stable identifier embedded in every extended (non-JSON-scalar) value.
    pub const WIRE_CODEC: &'static str = VALUE_WIRE_CODEC;

    /// Convert to the historical plain JSON shape for backend payloads and
    /// human-facing compatibility. This deliberately does not preserve
    /// `Value` variant identity and must not be used as the persistence codec.
    pub fn to_plain_json(&self) -> crate::Result<serde_json::Value> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(*value),
            Self::Int(value) | Self::Timestamp(value) => (*value).into(),
            Self::Float(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    crate::Error::Serialization(
                        "non-finite floats cannot be converted to plain JSON".to_owned(),
                    )
                })?,
            Self::Text(value) => serde_json::Value::String(value.clone()),
            Self::Bytes(value) => serde_json::Value::Array(
                value
                    .iter()
                    .map(|byte| serde_json::Value::from(*byte))
                    .collect(),
            ),
            Self::Json(value) => value.clone(),
            Self::Array(values) => serde_json::Value::Array(
                values
                    .iter()
                    .map(Self::to_plain_json)
                    .collect::<crate::Result<Vec<_>>>()?,
            ),
            Self::Map(values) => serde_json::Value::Object(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), value.to_plain_json()?)))
                    .collect::<crate::Result<serde_json::Map<String, serde_json::Value>>>()?,
            ),
        })
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_owned())
    }
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}
impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}
impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}
impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        Value::Json(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(value: Value) {
        let encoded = serde_json::to_value(&value).unwrap();
        let decoded: Value = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn every_variant_and_nested_value_round_trips_exactly() {
        let nested_map = BTreeMap::from([
            ("bytes".to_owned(), Value::Bytes(vec![0, 1, 254, 255])),
            (
                "nested".to_owned(),
                Value::Array(vec![
                    Value::Timestamp(1_700_000_000_123),
                    Value::Json(json!({"array": [1, true, null]})),
                ]),
            ),
        ]);
        for value in [
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(3.5),
            Value::Text("中文 text".to_owned()),
            Value::Bytes(vec![0, 1, 254, 255]),
            Value::Timestamp(1_700_000_000_123),
            Value::Json(json!({"object": {"nested": [1, 2, 3]}})),
            Value::Array(vec![Value::Int(7), Value::Text("seven".to_owned())]),
            Value::Map(nested_map),
        ] {
            round_trip(value);
        }
    }

    #[test]
    fn json_native_scalars_keep_the_public_json_contract() {
        for (value, encoded) in [
            (Value::Null, json!(null)),
            (Value::Bool(true), json!(true)),
            (Value::Int(-42), json!(-42)),
            (Value::Int(i64::MAX), json!(i64::MAX)),
            (Value::Float(3.5), json!(3.5)),
            (Value::Float(1.0), json!(1.0)),
            (Value::Text("text".to_owned()), json!("text")),
        ] {
            assert_eq!(serde_json::to_value(&value).unwrap(), encoded);
            assert_eq!(serde_json::from_value::<Value>(encoded).unwrap(), value);
        }

        assert!(serde_json::from_value::<Value>(json!(i64::MAX as u64 + 1)).is_err());
    }

    #[test]
    fn bytes_use_canonical_base64_and_timestamp_keeps_its_tag() {
        assert_eq!(
            serde_json::to_value(Value::Bytes(vec![0, 1, 254, 255])).unwrap(),
            json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "bytes",
                    "value": "AAH+/w=="
                }
            })
        );
        assert_eq!(
            serde_json::to_value(Value::Timestamp(42)).unwrap(),
            json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "timestamp",
                    "value": 42
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<Value>(json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "timestamp",
                    "value": 42
                }
            }))
            .unwrap(),
            Value::Timestamp(42)
        );
    }

    #[test]
    fn array_map_and_json_tags_do_not_overlap() {
        let array = Value::Array(vec![Value::Int(1)]);
        let map = Value::Map(BTreeMap::from([("one".to_owned(), Value::Int(1))]));
        let json = Value::Json(json!({"one": 1}));

        assert_eq!(
            serde_json::from_value::<Value>(serde_json::to_value(&array).unwrap()).unwrap(),
            array
        );
        assert_eq!(
            serde_json::from_value::<Value>(serde_json::to_value(&map).unwrap()).unwrap(),
            map
        );
        assert_eq!(
            serde_json::from_value::<Value>(serde_json::to_value(&json).unwrap()).unwrap(),
            json
        );
    }

    #[test]
    fn plain_json_conversion_is_explicit_and_recursive() {
        let value = Value::Map(BTreeMap::from([
            ("bytes".to_owned(), Value::Bytes(vec![0, 255])),
            (
                "values".to_owned(),
                Value::Array(vec![
                    Value::Timestamp(42),
                    Value::Json(json!({"raw": true})),
                ]),
            ),
        ]));

        assert_eq!(
            value.to_plain_json().unwrap(),
            json!({"bytes": [0, 255], "values": [42, {"raw": true}]})
        );
        assert_ne!(
            serde_json::to_value(value).unwrap(),
            json!({"values": [42]})
        );
    }

    #[test]
    fn unmarked_collections_and_unknown_or_forged_extensions_fail_closed() {
        for legacy in [json!([1, 2]), json!({"name": "legacy"})] {
            assert!(serde_json::from_value::<Value>(legacy).is_err());
        }

        let error = serde_json::from_value::<Value>(json!({
            "$dbtool": {
                "codec": "dbtool-value-v1",
                "type": "timestamp",
                "value": 42
            }
        }))
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported dbtool Value wire codec"));

        for forged in [
            json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "timestamp",
                    "value": 42,
                    "unexpected": true
                }
            }),
            json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "unknown",
                    "value": 42
                }
            }),
            json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "timestamp",
                    "value": 42
                },
                "unexpected": true
            }),
        ] {
            assert!(serde_json::from_value::<Value>(forged).is_err());
        }
    }

    #[test]
    fn malformed_or_non_canonical_base64_is_rejected() {
        for encoded in ["A", "AA=A", "AB==", "AA A", "AA==="] {
            let error = serde_json::from_value::<Value>(json!({
                "$dbtool": {
                    "codec": "dbtool-value-v2",
                    "type": "bytes",
                    "value": encoded
                }
            }))
            .unwrap_err();
            assert!(error.to_string().contains("base64"));
        }
    }

    #[test]
    fn non_finite_floats_are_not_silently_changed() {
        assert!(serde_json::to_value(Value::Float(f64::NAN)).is_err());
        assert!(serde_json::to_value(Value::Float(f64::INFINITY)).is_err());
    }
}
