// wire counter representation. watermarks/cookies/last-mutation-ids are i64
// end to end; this module is the single place the JSON boundary is crossed, so
// the number-vs-string decision lives in exactly one flip point.
//
// coordinated with sol-m0 (M0 platform proof): inbound counters accept a
// non-negative safe-integer JSON number (the vendored http-pull transport, which
// round-trips the request cookie through Number()) OR a canonical base-10 string
// in 0..=i64::MAX (sol-m0's precision-safe boundary format). outbound counters
// are emitted as JSON numbers, which is lossless for baseline watermarks/lmids
// (always well below 2^53), byte-compatible with the vendored transport, and
// matches the v51 poke's numeric lastMutationIDChanges.

use serde_json::{Number, Value};

// JavaScript's Number.MAX_SAFE_INTEGER — the reference core's isNonNegativeInteger
// ceiling for a JSON-number cookie.
pub const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

// emit a counter to its JSON wire form. THE single flip point: if the HTTP wire
// ever moves to base-10 strings end to end, change this to Value::String and
// update the vendored transport + poke path to match (see the crate docs).
pub fn counter_to_json(value: i64) -> Value {
    Value::Number(Number::from(value))
}

// the reference core's isNonNegativeInteger: a JSON number that is a
// non-negative integer within the JS safe range. an integral float is accepted
// (JS makes no int/float distinction); fractional or out-of-range is rejected.
pub fn non_negative_safe_int(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                (0..=MAX_SAFE_INTEGER).contains(&i).then_some(i)
            } else if let Some(f) = n.as_f64() {
                (f.fract() == 0.0 && f >= 0.0 && f <= MAX_SAFE_INTEGER as f64).then_some(f as i64)
            } else {
                None
            }
        }
        _ => None,
    }
}

// parse an inbound cookie field. accepts JSON null (fresh client), a
// non-negative safe-integer JSON number, or a canonical unsigned base-10 string
// in 0..=i64::MAX. anything else is a malformed request. returns Ok(None) for a
// null/absent cookie.
pub fn parse_cookie(value: Option<&Value>) -> Result<Option<i64>, ()> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(n @ Value::Number(_)) => non_negative_safe_int(n).map(Some).ok_or(()),
        Some(Value::String(s)) => parse_canonical_u63(s).map(Some).ok_or(()),
        _ => Err(()),
    }
}

// canonical unsigned base-10 in 0..=i64::MAX: digits only, no sign, no leading
// zeros (except "0" itself), no whitespace. this is the exact grammar sol-m0's
// boundary emits, so we don't silently accept sloppy variants.
fn parse_canonical_u63(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if s.len() > 1 && s.starts_with('0') {
        return None;
    }
    s.parse::<i64>().ok().filter(|v| *v >= 0)
}
