// SQLite-to-Zero value conversion, mirrored from the reference core's
// `toZeroValue` (src/sync-server/sync-server.ts) and soot's `toZeroValue`.
// rowsPatch values must match the zero schema's column types exactly, and
// floats must round-trip with full fidelity: SQLite's json_object formats
// REAL at 15 significant digits (0.1+0.2 -> "0.3"), so patch values are always
// read from LIVE rows here, never from logged images, and formatted with the
// shortest round-trip representation JavaScript's JSON.stringify produces.

use serde_json::{Number, Value};

use crate::db::SqlValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroColumnType {
    String,
    Number,
    Boolean,
    Json,
    Null,
}

impl ZeroColumnType {
    // parse a zero schema column type string (createSchema() result)
    pub fn from_type_str(ty: &str) -> ZeroColumnType {
        match ty {
            "boolean" => ZeroColumnType::Boolean,
            "number" => ZeroColumnType::Number,
            "json" => ZeroColumnType::Json,
            "null" => ZeroColumnType::Null,
            // "string" and any unknown default to string (the reference core
            // stores the raw type string; unsupported types never reach here)
            _ => ZeroColumnType::String,
        }
    }
}

// format an f64 the way JavaScript's JSON.stringify does: an integral value
// prints with no decimal point ("2", not serde's default "2.0"), everything
// else prints shortest-round-trip via serde_json/ryu ("1.5", "0.3...04").
// this is what keeps rank:2 and rank:0.1+0.2 both byte-identical to the TS
// reference core on the wire.
pub fn f64_to_json(f: f64) -> Value {
    if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        Value::Number(Number::from(f as i64))
    } else {
        Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

// SQLite/pg timestamp columns can reach the engine as SQL text even though
// Zero declares them as `number` (epoch milliseconds). This mirrors the
// reference host's `new Date(raw).getTime()` conversion for the stable SQL/ISO
// forms the data tier emits, interpreting a missing offset as UTC (the worker
// runtime's timezone).
fn timestamp_text_to_epoch_ms(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 19
        || !matches!(bytes[10], b' ' | b'T')
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let digits = |start: usize, len: usize| -> Option<i32> {
        let mut out = 0i32;
        for byte in bytes.get(start..start + len)? {
            if !byte.is_ascii_digit() {
                return None;
            }
            out = out.checked_mul(10)?.checked_add(i32::from(*byte - b'0'))?;
        }
        Some(out)
    };
    let year = digits(0, 4)?;
    let month = digits(5, 2)?;
    let day = digits(8, 2)?;
    let hour = digits(11, 2)?;
    let minute = digits(14, 2)?;
    let second = digits(17, 2)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if day < 1 || day > days_in_month || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let mut cursor = 19;
    let mut millis = 0i64;
    if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
            if cursor < fraction_start + 3 {
                millis = millis * 10 + i64::from(bytes[cursor] - b'0');
            }
            cursor += 1;
        }
        if cursor == fraction_start {
            return None;
        }
        for _ in cursor.saturating_sub(fraction_start)..3 {
            millis *= 10;
        }
    }

    let offset_seconds = match bytes.get(cursor..) {
        Some([]) | Some([b'Z']) => 0i64,
        // Zero declares timestamps as `number`, but the DO data tier stores
        // them as postgres timestamp TEXT and emits the offset in any of pg's
        // shapes: `+00` (hour only, the common case), `+0000`, or `+00:00`.
        // Accept all three, plus a bare `Z`/none, so every value the feed ships
        // decodes instead of falling through to a schema-number error.
        Some([sign @ (b'+' | b'-'), rest @ ..]) => {
            let two_digit = |pair: &[u8]| -> Option<i64> {
                match pair {
                    [a, b] if a.is_ascii_digit() && b.is_ascii_digit() => {
                        Some(i64::from((a - b'0') * 10 + (b - b'0')))
                    }
                    _ => None,
                }
            };
            let (hours, minutes) = match rest {
                [h1, h2] => (two_digit(&[*h1, *h2])?, 0),
                [h1, h2, m1, m2] => (two_digit(&[*h1, *h2])?, two_digit(&[*m1, *m2])?),
                [h1, h2, b':', m1, m2] => (two_digit(&[*h1, *h2])?, two_digit(&[*m1, *m2])?),
                _ => return None,
            };
            if hours > 23 || minutes > 59 {
                return None;
            }
            let seconds = hours * 3600 + minutes * 60;
            if *sign == b'-' { -seconds } else { seconds }
        }
        _ => return None,
    };

    // Howard Hinnant's civil-date conversion, yielding days since 1970-01-01.
    let adjusted_year = year - i32::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = i64::from(era * 146_097 + day_of_era - 719_468);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour * 3600 + minute * 60 + second))?
        .checked_sub(offset_seconds)?;
    seconds.checked_mul(1000)?.checked_add(millis)
}

// raw SQLite value -> serde_json::Value with no type coercion (the shape the
// reference core's node:sqlite driver hands `toZeroValue`). used as the base
// before applying the zero column type.
fn sql_to_json(v: &SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Number(Number::from(*i)),
        SqlValue::Real(f) => f64_to_json(*f),
        SqlValue::Text(s) => Value::String(s.clone()),
        // no zero column type is a blob; a blob under a text/json column is
        // decoded lossily so it still serializes (never happens on the tested
        // surface — the schema types are string/number/boolean/json/null)
        SqlValue::Blob(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
    }
}

// convert a live SQLite column value to its Zero-typed JSON, following the
// reference core's `toZeroValue` branch structure exactly.
pub fn to_zero_value(ty: ZeroColumnType, raw: &SqlValue) -> Value {
    if matches!(raw, SqlValue::Null) {
        return Value::Null;
    }
    let base = sql_to_json(raw);
    to_zero_value_json(ty, base)
}

// the same conversion applied to an already-JSON value — used for `del` ids,
// whose primary-key columns arrive parsed from the change log's json_object.
pub fn to_zero_value_json(ty: ZeroColumnType, raw: Value) -> Value {
    if raw.is_null() {
        return Value::Null;
    }
    match ty {
        ZeroColumnType::Boolean => Value::Bool(is_truthy(&raw)),
        ZeroColumnType::Number => match raw {
            Value::String(ref s) => {
                let n: f64 = s.parse().unwrap_or(f64::NAN);
                if n.is_finite() {
                    f64_to_json(n)
                } else if let Some(epoch_ms) = timestamp_text_to_epoch_ms(s) {
                    Value::Number(Number::from(epoch_ms))
                } else {
                    raw
                }
            }
            other => other,
        },
        ZeroColumnType::Json => match raw {
            Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
            other => other,
        },
        ZeroColumnType::String | ZeroColumnType::Null => raw,
    }
}

// the reference core's boolean coercion: raw === 1 || '1' || 'true' || 't'
fn is_truthy(raw: &Value) -> bool {
    match raw {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_i64() == Some(1),
        Value::String(s) => matches!(s.as_str(), "1" | "true" | "t"),
        _ => false,
    }
}

// build a zero-typed rowsPatch `value`: read logical query aliases and emit
// physical downstream-wire column names
pub fn zero_row(
    tables: &crate::schema::Tables,
    table: &str,
    spec: &crate::schema::TableSpec,
    row: &crate::db::Row,
) -> Result<Value, crate::error::EngineError> {
    let mut value = serde_json::Map::new();
    for (col, ty) in &spec.columns {
        let raw = row.get(col).cloned().unwrap_or(SqlValue::Null);
        let converted = to_zero_value(*ty, &raw);
        if *ty == ZeroColumnType::Number && matches!(converted, Value::String(_)) {
            return Err(crate::error::EngineError::internal(format!(
                "cannot convert schema-number column {col} value to number: {raw:?}"
            )));
        }
        let physical = tables.physical_column(table, col).ok_or_else(|| {
            crate::error::EngineError::internal(format!(
                "missing physical column mapping for {table}.{col}"
            ))
        })?;
        value.insert(physical.to_string(), converted);
    }
    Ok(Value::Object(value))
}

// build a zero-typed rowsPatch `id`: read logical journal keys and emit physical
// downstream-wire column names
pub fn zero_pk_id(
    tables: &crate::schema::Tables,
    table: &str,
    spec: &crate::schema::TableSpec,
    pk: &Value,
) -> Result<Value, crate::error::EngineError> {
    let mut id = serde_json::Map::new();
    for col in &spec.primary_key {
        let ty = spec.column_type(col).unwrap_or(ZeroColumnType::String);
        let raw = pk.get(col).cloned().unwrap_or(Value::Null);
        let converted = to_zero_value_json(ty, raw);
        if ty == ZeroColumnType::Number && matches!(converted, Value::String(_)) {
            return Err(crate::error::EngineError::internal(format!(
                "cannot convert schema-number primary key {col} to number"
            )));
        }
        let physical = tables.physical_column(table, col).ok_or_else(|| {
            crate::error::EngineError::internal(format!(
                "missing physical column mapping for {table}.{col}"
            ))
        })?;
        id.insert(physical.to_string(), converted);
    }
    Ok(Value::Object(id))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::Value;

    use super::{ZeroColumnType, to_zero_value_json, zero_row};
    use crate::db::{Row, SqlValue};
    use crate::schema::{TableSpec, Tables};

    #[test]
    fn zero_number_storage_text_matches_the_shared_host_fixture() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../harness/fixtures/zero-number-storage-values.json"
        ))
        .unwrap();
        for case in fixture["accepted"].as_array().unwrap() {
            let input = case["input"].as_str().unwrap();
            assert_eq!(
                to_zero_value_json(ZeroColumnType::Number, Value::String(input.into())),
                case["expected"],
                "accepted storage value {input}"
            );
        }
        for input in fixture["rejected"].as_array().unwrap() {
            let input = input.as_str().unwrap();
            assert_eq!(
                to_zero_value_json(ZeroColumnType::Number, Value::String(input.into())),
                Value::String(input.into()),
                "rejected storage value {input}"
            );
        }
    }

    #[test]
    fn invalid_schema_number_text_is_an_error_not_a_string_on_the_wire() {
        let spec = TableSpec {
            columns: vec![("createdAt".into(), ZeroColumnType::Number)],
            primary_key: vec![],
            encrypted_columns: Default::default(),
            encrypted_physical_columns: Default::default(),
        };
        let tables = Tables::new().with("record", spec.clone());
        let row = Row {
            columns: Arc::from(["createdAt".to_string()]),
            values: vec![SqlValue::Text("not-a-number-or-date".into())],
        };
        let error = zero_row(&tables, "record", &spec, &row).unwrap_err();
        assert_eq!(error.status, 500);
        assert!(
            error
                .message
                .contains("cannot convert schema-number column createdAt")
        );
    }
}
