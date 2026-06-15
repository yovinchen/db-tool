use dbtool_core::model::Value;
use sqlx::{
    mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, Column, Decode, MySql, Postgres, Row,
    Sqlite, Type, TypeInfo, ValueRef,
};

pub(crate) fn column_type_name<C>(column: &C) -> String
where
    C: Column,
{
    column.type_info().name().to_owned()
}

pub(crate) fn mysql_value(row: &MySqlRow, index: usize) -> Value {
    let ty = type_name(row, index);
    if is_null(row, index) {
        return Value::Null;
    }

    if is_bool_type(&ty) {
        if let Some(value) = mysql_get::<bool>(row, index) {
            return Value::Bool(value);
        }
        if let Some(value) = mysql_get::<i64>(row, index) {
            return Value::Bool(value != 0);
        }
        if let Some(value) = mysql_get::<String>(row, index).and_then(text_to_bool) {
            return Value::Bool(value);
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = mysql_get::<i64>(row, index) {
            return Value::Int(value);
        }
        if let Some(value) = mysql_get::<i32>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = mysql_get::<i16>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = mysql_get::<u64>(row, index).and_then(unsigned_to_i64) {
            return Value::Int(value);
        }
        if let Some(value) = mysql_get::<u32>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = mysql_get::<u16>(row, index) {
            return Value::Int(value.into());
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = mysql_get::<f64>(row, index) {
            return Value::Float(value);
        }
        if let Some(value) = mysql_get::<f32>(row, index) {
            return Value::Float(value.into());
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = mysql_get::<Vec<u8>>(row, index) {
            return Value::Bytes(value);
        }
    }

    mysql_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| mysql_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| mysql_fallback(row, index))
        .unwrap_or(Value::Null)
}

pub(crate) fn postgres_value(row: &PgRow, index: usize) -> Value {
    let ty = type_name(row, index);
    if is_null(row, index) {
        return Value::Null;
    }

    if is_bool_type(&ty) {
        if let Some(value) = postgres_get::<bool>(row, index) {
            return Value::Bool(value);
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = postgres_get::<i64>(row, index) {
            return Value::Int(value);
        }
        if let Some(value) = postgres_get::<i32>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = postgres_get::<i16>(row, index) {
            return Value::Int(value.into());
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = postgres_get::<f64>(row, index) {
            return Value::Float(value);
        }
        if let Some(value) = postgres_get::<f32>(row, index) {
            return Value::Float(value.into());
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = postgres_get::<Vec<u8>>(row, index) {
            return Value::Bytes(value);
        }
    }

    postgres_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| postgres_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| postgres_fallback(row, index))
        .unwrap_or(Value::Null)
}

pub(crate) fn sqlite_value(row: &SqliteRow, index: usize) -> Value {
    let ty = type_name(row, index);
    if is_null(row, index) {
        return Value::Null;
    }

    if is_bool_type(&ty) {
        if let Some(value) = sqlite_get::<bool>(row, index) {
            return Value::Bool(value);
        }
        if let Some(value) = sqlite_get::<i64>(row, index) {
            return Value::Bool(value != 0);
        }
        if let Some(value) = sqlite_get::<String>(row, index).and_then(text_to_bool) {
            return Value::Bool(value);
        }
    }

    if is_integer_type(&ty) {
        if let Some(value) = sqlite_get::<i64>(row, index) {
            return Value::Int(value);
        }
        if let Some(value) = sqlite_get::<i32>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = sqlite_get::<i16>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = sqlite_get::<u64>(row, index).and_then(unsigned_to_i64) {
            return Value::Int(value);
        }
        if let Some(value) = sqlite_get::<u32>(row, index) {
            return Value::Int(value.into());
        }
        if let Some(value) = sqlite_get::<u16>(row, index) {
            return Value::Int(value.into());
        }
    }

    if is_float_type(&ty) {
        if let Some(value) = sqlite_get::<f64>(row, index) {
            return Value::Float(value);
        }
        if let Some(value) = sqlite_get::<f32>(row, index) {
            return Value::Float(value.into());
        }
    }

    if is_binary_type(&ty) {
        if let Some(value) = sqlite_get::<Vec<u8>>(row, index) {
            return Value::Bytes(value);
        }
    }

    sqlite_get::<String>(row, index)
        .map(|text| text_value(&ty, text))
        .or_else(|| sqlite_get::<Vec<u8>>(row, index).map(Value::Bytes))
        .or_else(|| sqlite_fallback(row, index))
        .unwrap_or(Value::Null)
}

fn type_name<R>(row: &R, index: usize) -> String
where
    R: Row,
{
    row.columns()[index].type_info().name().to_ascii_uppercase()
}

fn is_null<R>(row: &R, index: usize) -> bool
where
    R: Row,
    usize: sqlx::ColumnIndex<R>,
{
    row.try_get_raw(index)
        .map(|value| value.is_null())
        .unwrap_or(false)
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
        .or_else(|| {
            mysql_get::<u64>(row, index)
                .and_then(unsigned_to_i64)
                .map(Value::Int)
        })
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
        .or_else(|| {
            sqlite_get::<u64>(row, index)
                .and_then(unsigned_to_i64)
                .map(Value::Int)
        })
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

fn unsigned_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
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
