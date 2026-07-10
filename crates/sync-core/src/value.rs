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
    pub fn from_str(ty: &str) -> ZeroColumnType {
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
        Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
    }
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
                if n.is_finite() { f64_to_json(n) } else { raw }
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
