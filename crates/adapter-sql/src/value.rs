use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dbtool_core::{model::Value, Error, Result};
use sqlx::types::Json;
use sqlx::{
    mysql::{types::MySqlTime, MySqlRow, MySqlTypeInfo, MySqlValueRef},
    postgres::{PgRow, PgValueFormat},
    sqlite::SqliteRow,
    Column, Decode, MySql, Postgres, Row, Sqlite, Type, TypeInfo, ValueRef,
};

pub(crate) fn column_type_name<C>(column: &C) -> String
where
    C: Column,
{
    column.type_info().name().to_owned()
}

pub(crate) fn mysql_value(row: &MySqlRow, index: usize) -> Result<Value> {
    let ty = type_name(row, index);
    if is_null(row, index)? {
        return Ok(Value::Null);
    }

    if is_bool_type(&ty) {
        if let Some(value) = mysql_get::<bool>(row, index) {
            return Ok(Value::Bool(value));
        }
        if let Some(value) = mysql_get::<i64>(row, index) {
            return Ok(Value::Bool(value != 0));
        }
        if let Some(value) = mysql_get::<String>(row, index).and_then(text_to_bool) {
            return Ok(Value::Bool(value));
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = mysql_get::<i64>(row, index) {
            return Ok(Value::Int(value));
        }
        if let Some(value) = mysql_get::<i32>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = mysql_get::<i16>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = mysql_get::<u64>(row, index) {
            return Ok(unsigned_value(value));
        }
        if let Some(value) = mysql_get::<u32>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = mysql_get::<u16>(row, index) {
            return Ok(Value::Int(value.into()));
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = mysql_get::<f64>(row, index) {
            return Ok(Value::Float(value));
        }
        if let Some(value) = mysql_get::<f32>(row, index) {
            return Ok(Value::Float(value.into()));
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = mysql_get::<Vec<u8>>(row, index) {
            return Ok(Value::Bytes(value));
        }
    }

    if is_timestamp_type(&ty) {
        if let Some(value) = mysql_get::<NaiveDateTime>(row, index) {
            return Ok(Value::Timestamp(value.and_utc().timestamp_millis()));
        }
    }

    if ty == "DATE" {
        if let Some(value) = mysql_get::<NaiveDate>(row, index) {
            return Ok(Value::Text(value.format("%Y-%m-%d").to_string()));
        }
    }

    if ty == "TIME" {
        if let Some(value) = mysql_get::<MySqlTime>(row, index) {
            return Ok(Value::Text(value.to_string()));
        }
    }

    if is_json_type(&ty) {
        if let Some(Json(value)) = mysql_get::<Json<serde_json::Value>>(row, index) {
            return Ok(Value::Json(value));
        }
    }

    mysql_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| mysql_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| mysql_fallback(row, index))
        .or_else(|| mysql_get::<MySqlLosslessText>(row, index).map(|value| Value::Text(value.0)))
        .ok_or_else(|| non_null_decode_error("MySQL", index, &ty))
}

pub(crate) fn postgres_value(row: &PgRow, index: usize) -> Result<Value> {
    let ty = type_name(row, index);
    if is_null(row, index)? {
        return Ok(Value::Null);
    }

    if is_bool_type(&ty) {
        if let Some(value) = postgres_get::<bool>(row, index) {
            return Ok(Value::Bool(value));
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = postgres_get::<i64>(row, index) {
            return Ok(Value::Int(value));
        }
        if let Some(value) = postgres_get::<i32>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = postgres_get::<i16>(row, index) {
            return Ok(Value::Int(value.into()));
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = postgres_get::<f64>(row, index) {
            return Ok(Value::Float(value));
        }
        if let Some(value) = postgres_get::<f32>(row, index) {
            return Ok(Value::Float(value.into()));
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = postgres_get::<Vec<u8>>(row, index) {
            return Ok(Value::Bytes(value));
        }
    }

    if is_timestamp_type(&ty) {
        if let Some(value) = postgres_get::<DateTime<Utc>>(row, index) {
            return Ok(Value::Timestamp(value.timestamp_millis()));
        }
        if let Some(value) = postgres_get::<NaiveDateTime>(row, index) {
            return Ok(Value::Timestamp(value.and_utc().timestamp_millis()));
        }
    }

    if ty == "DATE" {
        if let Some(value) = postgres_get::<NaiveDate>(row, index) {
            return Ok(Value::Text(value.format("%Y-%m-%d").to_string()));
        }
    }

    if ty == "TIME" {
        if let Some(value) = postgres_get::<NaiveTime>(row, index) {
            return Ok(Value::Text(value.format("%H:%M:%S%.f").to_string()));
        }
    }

    if ty == "UUID" {
        return postgres_uuid(row, index).map(Value::Text);
    }

    if ty == "NUMERIC" || ty == "DECIMAL" {
        return postgres_numeric(row, index).map(Value::Text);
    }

    if ty.ends_with("[]") {
        return postgres_array(row, index, &ty)
            .ok_or_else(|| non_null_decode_error("PostgreSQL", index, &ty));
    }

    if is_json_type(&ty) {
        if let Some(Json(value)) = postgres_get::<Json<serde_json::Value>>(row, index) {
            return Ok(Value::Json(value));
        }
    }

    postgres_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| postgres_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| postgres_fallback(row, index))
        .ok_or_else(|| non_null_decode_error("PostgreSQL", index, &ty))
}

pub(crate) fn sqlite_value(row: &SqliteRow, index: usize) -> Result<Value> {
    let ty = type_name(row, index);
    if is_null(row, index)? {
        return Ok(Value::Null);
    }

    if is_bool_type(&ty) {
        if let Some(value) = sqlite_get::<bool>(row, index) {
            return Ok(Value::Bool(value));
        }
        if let Some(value) = sqlite_get::<i64>(row, index) {
            return Ok(Value::Bool(value != 0));
        }
        if let Some(value) = sqlite_get::<String>(row, index).and_then(text_to_bool) {
            return Ok(Value::Bool(value));
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = sqlite_get::<i64>(row, index) {
            return Ok(Value::Int(value));
        }
        if let Some(value) = sqlite_get::<i32>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = sqlite_get::<i16>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = sqlite_get::<u64>(row, index) {
            return Ok(unsigned_value(value));
        }
        if let Some(value) = sqlite_get::<u32>(row, index) {
            return Ok(Value::Int(value.into()));
        }
        if let Some(value) = sqlite_get::<u16>(row, index) {
            return Ok(Value::Int(value.into()));
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = sqlite_get::<f64>(row, index) {
            return Ok(Value::Float(value));
        }
        if let Some(value) = sqlite_get::<f32>(row, index) {
            return Ok(Value::Float(value.into()));
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = sqlite_get::<Vec<u8>>(row, index) {
            return Ok(Value::Bytes(value));
        }
    }

    if is_timestamp_type(&ty) {
        if let Some(value) = sqlite_get::<DateTime<Utc>>(row, index) {
            return Ok(Value::Timestamp(value.timestamp_millis()));
        }
        if let Some(value) = sqlite_get::<NaiveDateTime>(row, index) {
            return Ok(Value::Timestamp(value.and_utc().timestamp_millis()));
        }
    }

    if is_json_type(&ty) {
        if let Some(Json(value)) = sqlite_get::<Json<serde_json::Value>>(row, index) {
            return Ok(Value::Json(value));
        }
    }

    sqlite_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| sqlite_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| sqlite_fallback(row, index))
        .ok_or_else(|| non_null_decode_error("SQLite", index, &ty))
}

fn type_name<R>(row: &R, index: usize) -> String
where
    R: Row,
{
    row.columns()[index].type_info().name().to_ascii_uppercase()
}

fn is_null<R>(row: &R, index: usize) -> Result<bool>
where
    R: Row,
    usize: sqlx::ColumnIndex<R>,
{
    row.try_get_raw(index)
        .map(|value| value.is_null())
        .map_err(|error| Error::Query(format!("cannot inspect column {}: {error}", index + 1)))
}

#[derive(Debug)]
struct MySqlLosslessText(String);

impl Type<MySql> for MySqlLosslessText {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }

    fn compatible(_ty: &MySqlTypeInfo) -> bool {
        true
    }
}

impl<'r> Decode<'r, MySql> for MySqlLosslessText {
    fn decode(value: MySqlValueRef<'r>) -> std::result::Result<Self, sqlx::error::BoxDynError> {
        <&str as Decode<MySql>>::decode(value).map(|value| Self(value.to_owned()))
    }
}

fn mysql_get<T>(row: &MySqlRow, index: usize) -> Option<T>
where
    for<'r> T: Decode<'r, MySql> + Type<MySql>,
{
    row.try_get::<T, _>(index).ok()
}

fn postgres_get<T>(row: &PgRow, index: usize) -> Option<T>
where
    for<'r> T: Decode<'r, Postgres> + Type<Postgres>,
{
    row.try_get::<T, _>(index).ok()
}

fn sqlite_get<T>(row: &SqliteRow, index: usize) -> Option<T>
where
    for<'r> T: Decode<'r, Sqlite> + Type<Sqlite>,
{
    row.try_get::<T, _>(index).ok()
}

fn mysql_fallback(row: &MySqlRow, index: usize) -> Option<Value> {
    mysql_get::<i64>(row, index)
        .map(Value::Int)
        .or_else(|| mysql_get::<i32>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| mysql_get::<i16>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| mysql_get::<u64>(row, index).map(unsigned_value))
        .or_else(|| mysql_get::<u32>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| mysql_get::<u16>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| mysql_get::<f64>(row, index).map(Value::Float))
        .or_else(|| mysql_get::<f32>(row, index).map(|value| Value::Float(value.into())))
        .or_else(|| mysql_get::<bool>(row, index).map(Value::Bool))
        .or_else(|| mysql_get::<String>(row, index).map(Value::Text))
        .or_else(|| mysql_get::<Vec<u8>>(row, index).map(Value::Bytes))
}

fn postgres_fallback(row: &PgRow, index: usize) -> Option<Value> {
    postgres_get::<i64>(row, index)
        .map(Value::Int)
        .or_else(|| postgres_get::<i32>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| postgres_get::<i16>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| postgres_get::<f64>(row, index).map(Value::Float))
        .or_else(|| postgres_get::<f32>(row, index).map(|value| Value::Float(value.into())))
        .or_else(|| postgres_get::<bool>(row, index).map(Value::Bool))
        .or_else(|| postgres_get::<String>(row, index).map(Value::Text))
        .or_else(|| postgres_get::<Vec<u8>>(row, index).map(Value::Bytes))
}

fn sqlite_fallback(row: &SqliteRow, index: usize) -> Option<Value> {
    sqlite_get::<i64>(row, index)
        .map(Value::Int)
        .or_else(|| sqlite_get::<i32>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| sqlite_get::<i16>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| sqlite_get::<u64>(row, index).map(unsigned_value))
        .or_else(|| sqlite_get::<u32>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| sqlite_get::<u16>(row, index).map(|value| Value::Int(value.into())))
        .or_else(|| sqlite_get::<f64>(row, index).map(Value::Float))
        .or_else(|| sqlite_get::<f32>(row, index).map(|value| Value::Float(value.into())))
        .or_else(|| sqlite_get::<bool>(row, index).map(Value::Bool))
        .or_else(|| sqlite_get::<String>(row, index).map(Value::Text))
        .or_else(|| sqlite_get::<Vec<u8>>(row, index).map(Value::Bytes))
}

fn text_value(ty: &str, text: String) -> Value {
    if is_json_type(ty) {
        serde_json::from_str(&text)
            .map(Value::Json)
            .unwrap_or(Value::Text(text))
    } else {
        Value::Text(text)
    }
}

fn unsigned_value(value: u64) -> Value {
    i64::try_from(value)
        .map(Value::Int)
        .unwrap_or_else(|_| Value::Text(value.to_string()))
}

fn non_null_decode_error(database: &str, index: usize, ty: &str) -> Error {
    Error::Query(format!(
        "cannot losslessly decode non-NULL {database} column {} with type {ty}",
        index + 1
    ))
}

fn postgres_uuid(row: &PgRow, index: usize) -> Result<String> {
    let value = row
        .try_get_raw(index)
        .map_err(|error| Error::Query(format!("cannot decode UUID column: {error}")))?;
    match value.format() {
        PgValueFormat::Text => value
            .as_str()
            .map(str::to_owned)
            .map_err(|error| Error::Query(format!("cannot decode UUID text: {error}"))),
        PgValueFormat::Binary => {
            let bytes = value
                .as_bytes()
                .map_err(|error| Error::Query(format!("cannot decode UUID bytes: {error}")))?;
            if bytes.len() != 16 {
                return Err(Error::Query(format!(
                    "invalid PostgreSQL UUID length: expected 16, got {}",
                    bytes.len()
                )));
            }
            Ok(format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3],
                bytes[4],
                bytes[5],
                bytes[6],
                bytes[7],
                bytes[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12],
                bytes[13],
                bytes[14],
                bytes[15]
            ))
        }
    }
}

fn postgres_numeric(row: &PgRow, index: usize) -> Result<String> {
    let value = row
        .try_get_raw(index)
        .map_err(|error| Error::Query(format!("cannot decode NUMERIC column: {error}")))?;
    match value.format() {
        PgValueFormat::Text => value
            .as_str()
            .map(str::to_owned)
            .map_err(|error| Error::Query(format!("cannot decode NUMERIC text: {error}"))),
        PgValueFormat::Binary => value
            .as_bytes()
            .map_err(|error| Error::Query(format!("cannot decode NUMERIC bytes: {error}")))
            .and_then(decode_postgres_numeric),
    }
}

fn decode_postgres_numeric(bytes: &[u8]) -> Result<String> {
    const POSITIVE: u16 = 0x0000;
    const NEGATIVE: u16 = 0x4000;
    const NAN: u16 = 0xC000;
    const POSITIVE_INFINITY: u16 = 0xD000;
    const NEGATIVE_INFINITY: u16 = 0xF000;

    if bytes.len() < 8 || !bytes.len().is_multiple_of(2) {
        return Err(Error::Query("invalid PostgreSQL NUMERIC payload".into()));
    }
    let read_i16 = |offset: usize| i16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
    let read_u16 = |offset: usize| u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
    let digit_count = usize::try_from(read_i16(0))
        .map_err(|_| Error::Query("negative PostgreSQL NUMERIC digit count".into()))?;
    let weight = read_i16(2);
    let sign = read_u16(4);
    let decimal_scale = usize::from(read_u16(6));
    let expected_length = 8usize
        .checked_add(
            digit_count
                .checked_mul(2)
                .ok_or_else(|| Error::Query("PostgreSQL NUMERIC size overflow".into()))?,
        )
        .ok_or_else(|| Error::Query("PostgreSQL NUMERIC size overflow".into()))?;
    if bytes.len() != expected_length {
        return Err(Error::Query(format!(
            "invalid PostgreSQL NUMERIC length: expected {expected_length}, got {}",
            bytes.len()
        )));
    }

    match sign {
        NAN => return Ok("NaN".into()),
        POSITIVE_INFINITY => return Ok("Infinity".into()),
        NEGATIVE_INFINITY => return Ok("-Infinity".into()),
        POSITIVE | NEGATIVE => {}
        other => {
            return Err(Error::Query(format!(
                "unknown PostgreSQL NUMERIC sign 0x{other:04x}"
            )))
        }
    }

    let mut digits = Vec::with_capacity(digit_count);
    for index in 0..digit_count {
        let offset = 8 + index * 2;
        let digit = read_u16(offset);
        if digit > 9_999 {
            return Err(Error::Query(format!(
                "invalid PostgreSQL NUMERIC base-10000 digit {digit}"
            )));
        }
        digits.push(digit);
    }
    let digit_at_weight = |target_weight: i32| -> u16 {
        let source_index = i32::from(weight) - target_weight;
        usize::try_from(source_index)
            .ok()
            .and_then(|index| digits.get(index).copied())
            .unwrap_or(0)
    };

    let integer_groups = (i32::from(weight) + 1).max(0);
    let mut output = String::new();
    if integer_groups == 0 {
        output.push('0');
    } else {
        for group_index in 0..integer_groups {
            let group_weight = integer_groups - group_index - 1;
            let digit = digit_at_weight(group_weight);
            if group_index == 0 {
                output.push_str(&digit.to_string());
            } else {
                output.push_str(&format!("{digit:04}"));
            }
        }
    }

    if decimal_scale > 0 {
        output.push('.');
        let fractional_groups = decimal_scale.div_ceil(4);
        let fraction_start = output.len();
        for index in 0..fractional_groups {
            let group_weight = -i32::try_from(index)
                .map_err(|_| Error::Query("PostgreSQL NUMERIC fractional scale overflow".into()))?
                - 1;
            output.push_str(&format!("{:04}", digit_at_weight(group_weight)));
        }
        output.truncate(fraction_start + decimal_scale);
    }

    let is_zero = digits.iter().all(|digit| *digit == 0);
    if sign == NEGATIVE && !is_zero {
        output.insert(0, '-');
    }
    Ok(output)
}

fn postgres_array(row: &PgRow, index: usize, ty: &str) -> Option<Value> {
    macro_rules! scalar_array {
        ($rust:ty, $map:expr) => {
            postgres_get::<Vec<$rust>>(row, index)
                .map(|values| Value::Array(values.into_iter().map($map).collect()))
        };
    }

    match ty {
        "BOOL[]" => scalar_array!(bool, Value::Bool),
        "INT2[]" => scalar_array!(i16, |value| Value::Int(value.into())),
        "INT4[]" => scalar_array!(i32, |value| Value::Int(value.into())),
        "INT8[]" => scalar_array!(i64, Value::Int),
        "FLOAT4[]" => scalar_array!(f32, |value| Value::Float(value.into())),
        "FLOAT8[]" => scalar_array!(f64, Value::Float),
        "TEXT[]" | "VARCHAR[]" | "BPCHAR[]" | "NAME[]" => {
            scalar_array!(String, Value::Text)
        }
        "TIMESTAMP[]" => scalar_array!(NaiveDateTime, |value| {
            Value::Timestamp(value.and_utc().timestamp_millis())
        }),
        "TIMESTAMPTZ[]" => scalar_array!(DateTime<Utc>, |value| {
            Value::Timestamp(value.timestamp_millis())
        }),
        "DATE[]" => scalar_array!(NaiveDate, |value| {
            Value::Text(value.format("%Y-%m-%d").to_string())
        }),
        "TIME[]" => scalar_array!(NaiveTime, |value| {
            Value::Text(value.format("%H:%M:%S%.f").to_string())
        }),
        "JSON[]" | "JSONB[]" => {
            postgres_get::<Vec<Json<serde_json::Value>>>(row, index).map(|values| {
                Value::Array(
                    values
                        .into_iter()
                        .map(|Json(value)| Value::Json(value))
                        .collect(),
                )
            })
        }
        _ => None,
    }
}

fn text_to_bool(value: String) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

fn is_bool_type(ty: &str) -> bool {
    matches!(ty, "BOOL" | "BOOLEAN")
}

fn is_integer_type(ty: &str) -> bool {
    matches!(
        ty,
        "INT"
            | "INT2"
            | "INT4"
            | "INT8"
            | "INTEGER"
            | "BIGINT"
            | "SMALLINT"
            | "MEDIUMINT"
            | "TINYINT"
            | "SERIAL"
            | "BIGSERIAL"
    )
}

fn is_float_type(ty: &str) -> bool {
    matches!(
        ty,
        "FLOAT"
            | "FLOAT4"
            | "FLOAT8"
            | "REAL"
            | "DOUBLE"
            | "DOUBLE PRECISION"
            | "NUMERIC"
            | "DECIMAL"
    )
}

fn is_binary_type(ty: &str) -> bool {
    ty.contains("BLOB") || ty.contains("BINARY") || ty == "BYTEA"
}

fn is_json_type(ty: &str) -> bool {
    ty == "JSON" || ty == "JSONB"
}

fn is_timestamp_type(ty: &str) -> bool {
    matches!(
        ty,
        "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" | "DATETIME"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_payload(weight: i16, sign: u16, scale: u16, digits: &[u16]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(8 + digits.len() * 2);
        payload.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        payload.extend_from_slice(&weight.to_be_bytes());
        payload.extend_from_slice(&sign.to_be_bytes());
        payload.extend_from_slice(&scale.to_be_bytes());
        for digit in digits {
            payload.extend_from_slice(&digit.to_be_bytes());
        }
        payload
    }

    #[test]
    fn postgres_numeric_binary_is_rendered_without_float_loss() {
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(1, 0, 4, &[1, 2345, 6789])).unwrap(),
            "12345.6789"
        );
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(-1, 0, 4, &[12])).unwrap(),
            "0.0012"
        );
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(0, 0x4000, 2, &[42])).unwrap(),
            "-42.00"
        );
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(0, 0x4000, 3, &[0])).unwrap(),
            "0.000"
        );
    }

    #[test]
    fn postgres_numeric_special_and_invalid_payloads_are_explicit() {
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(0, 0xC000, 0, &[])).unwrap(),
            "NaN"
        );
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(0, 0xD000, 0, &[])).unwrap(),
            "Infinity"
        );
        assert_eq!(
            decode_postgres_numeric(&numeric_payload(0, 0xF000, 0, &[])).unwrap(),
            "-Infinity"
        );
        assert!(decode_postgres_numeric(&[0; 7]).is_err());
        assert!(decode_postgres_numeric(&numeric_payload(0, 0, 0, &[10_000])).is_err());
    }

    #[test]
    fn unsigned_values_never_wrap_or_become_null() {
        assert_eq!(unsigned_value(i64::MAX as u64), Value::Int(i64::MAX));
        assert_eq!(
            unsigned_value(u64::MAX),
            Value::Text("18446744073709551615".into())
        );
    }
}
