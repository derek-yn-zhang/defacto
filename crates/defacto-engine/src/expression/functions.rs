//! Built-in function implementations for the expression evaluator.
//!
//! 38 functions organized by domain: string manipulation, math, date/time
//! (intervals, extraction, parsing), type casting, and null handling.
//!
//! Each function validates its argument count and types, returning
//! descriptive errors on mismatch. The dispatch function `call_builtin`
//! is called from the evaluator whenever a FunctionCall expression is
//! evaluated.

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use regex::Regex;

use crate::error::DefactoError;
use crate::expression::evaluator::type_name;
use crate::types::Value;

// ---------------------------------------------------------------------------
// Main dispatch
// ---------------------------------------------------------------------------

/// Evaluate a built-in function call with pre-evaluated arguments.
///
/// Dispatches to the appropriate function group based on the function name.
/// Returns `DefactoError::Build` for unknown functions, wrong arity, or
/// wrong argument types — with messages that include the function name
/// and expected signature.
pub fn call_builtin(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    match name {
        // String functions (12)
        "str::to_lowercase" | "str::to_uppercase" | "str::trim" | "str::substring"
        | "starts_with" | "ends_with" | "replace" | "regex_match" | "regex_extract"
        | "to_string" => call_string_fn(name, args),

        // These work on both String and Array
        "contains" | "len" => call_string_fn(name, args),

        // Math functions (6)
        "math::abs" | "floor" | "ceil" | "round" | "min" | "max" => call_math_fn(name, args),

        // Date/time functions (13)
        "now" | "days_between" | "hours_between" | "minutes_between" | "days_since"
        | "year_of" | "month_of" | "day_of" | "hour_of" | "minute_of" | "day_of_week"
        | "date_trunc" | "parse_datetime" => call_datetime_fn(name, args),

        // Array functions (2)
        "split" | "join" => call_array_fn(name, args),

        // Type casting (2)
        "to_int" | "to_float" => call_type_fn(name, args),

        // Null handling (1, variadic)
        "coalesce" => call_coalesce(args),

        _ => Err(DefactoError::Build(format!(
            "Unknown function '{}' — see the expression language reference for available functions",
            name
        ))),
    }
}

// ---------------------------------------------------------------------------
// Arity and type helpers
// ---------------------------------------------------------------------------

/// Validate exact argument count.
fn expect_arity(name: &str, args: &[Value], expected: usize) -> Result<(), DefactoError> {
    if args.len() != expected {
        Err(DefactoError::Build(format!(
            "'{}' expects {} argument{}, got {}",
            name,
            expected,
            if expected == 1 { "" } else { "s" },
            args.len()
        )))
    } else {
        Ok(())
    }
}

/// Validate minimum argument count (for variadic functions).
fn expect_min_arity(name: &str, args: &[Value], min: usize) -> Result<(), DefactoError> {
    if args.len() < min {
        Err(DefactoError::Build(format!(
            "'{}' expects at least {} argument{}, got {}",
            name,
            min,
            if min == 1 { "" } else { "s" },
            args.len()
        )))
    } else {
        Ok(())
    }
}

/// Build a type error for a function argument.
fn type_error(fn_name: &str, expected: &str, got: &Value) -> DefactoError {
    DefactoError::Build(format!(
        "'{}' expects {}, got {}",
        fn_name,
        expected,
        type_name(got)
    ))
}

// ---------------------------------------------------------------------------
// String functions (10)
// ---------------------------------------------------------------------------

fn call_string_fn(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    match name {
        "str::to_lowercase" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_lowercase())),
                other => Err(type_error(name, "String", other)),
            }
        }

        "str::to_uppercase" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_uppercase())),
                other => Err(type_error(name, "String", other)),
            }
        }

        "str::trim" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.trim().to_string())),
                other => Err(type_error(name, "String", other)),
            }
        }

        "str::substring" => {
            expect_arity(name, &args, 3)?;
            match (&args[0], &args[1], &args[2]) {
                (Value::String(s), Value::Int(start), Value::Int(len)) => {
                    let start = *start as usize;
                    let len = *len as usize;
                    let result: String = s.chars().skip(start).take(len).collect();
                    Ok(Value::String(result))
                }
                _ => Err(DefactoError::Build(format!(
                    "'str::substring' expects (String, Int, Int), got ({}, {}, {})",
                    type_name(&args[0]),
                    type_name(&args[1]),
                    type_name(&args[2])
                ))),
            }
        }

        "starts_with" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(prefix)) => {
                    Ok(Value::Bool(s.starts_with(prefix.as_str())))
                }
                _ => Err(DefactoError::Build(format!(
                    "'starts_with' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        "ends_with" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(suffix)) => {
                    Ok(Value::Bool(s.ends_with(suffix.as_str())))
                }
                _ => Err(DefactoError::Build(format!(
                    "'ends_with' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        // contains() is dual-mode: String contains substring, or Array contains value
        "contains" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(substr)) => {
                    Ok(Value::Bool(s.contains(substr.as_str())))
                }
                (Value::Array(items), needle) => {
                    // Value derives PartialEq, so direct comparison works across types
                    Ok(Value::Bool(items.iter().any(|item| item == needle)))
                }
                _ => Err(DefactoError::Build(format!(
                    "'contains' expects (String, String) or (Array, value), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        "replace" => {
            expect_arity(name, &args, 3)?;
            match (&args[0], &args[1], &args[2]) {
                (Value::String(s), Value::String(from), Value::String(to)) => {
                    Ok(Value::String(s.replace(from.as_str(), to.as_str())))
                }
                _ => Err(DefactoError::Build(format!(
                    "'replace' expects (String, String, String), got ({}, {}, {})",
                    type_name(&args[0]),
                    type_name(&args[1]),
                    type_name(&args[2])
                ))),
            }
        }

        "regex_match" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(pattern)) => {
                    let re = Regex::new(pattern).map_err(|e| {
                        DefactoError::Build(format!("Invalid regex pattern '{}': {}", pattern, e))
                    })?;
                    Ok(Value::Bool(re.is_match(s)))
                }
                _ => Err(DefactoError::Build(format!(
                    "'regex_match' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        // regex_extract(str, pattern) — return first capture group, or null
        // if no match. Example: regex_extract(event.order_id, "ORD-(\d+)")
        "regex_extract" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(pattern)) => {
                    let re = Regex::new(pattern).map_err(|e| {
                        DefactoError::Build(format!("Invalid regex pattern '{}': {}", pattern, e))
                    })?;
                    match re.captures(s) {
                        Some(caps) => {
                            // Return first capture group, or full match if no groups
                            let matched = caps.get(1)
                                .or_else(|| caps.get(0))
                                .map(|m| m.as_str().to_string())
                                .unwrap_or_default();
                            Ok(Value::String(matched))
                        }
                        None => Ok(Value::Null),
                    }
                }
                _ => Err(DefactoError::Build(format!(
                    "'regex_extract' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        // to_string(value) — convert any value to its string representation
        "to_string" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.clone())),
                Value::Int(n) => Ok(Value::String(n.to_string())),
                Value::Float(f) => Ok(Value::String(f.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                Value::Null => Ok(Value::String("".to_string())),
                _ => Ok(Value::String(format!("{:?}", args[0]))),
            }
        }

        // len() is dual-mode: String length or Array length
        "len" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::Int(s.len() as i64)),
                Value::Array(items) => Ok(Value::Int(items.len() as i64)),
                other => Err(type_error(name, "String or Array", other)),
            }
        }

        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Math functions (6)
// ---------------------------------------------------------------------------

fn call_math_fn(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    match name {
        "math::abs" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::Int(n) => Ok(Value::Int(n.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                other => Err(type_error(name, "Int or Float", other)),
            }
        }

        "floor" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::Float(f) => Ok(Value::Int(f.floor() as i64)),
                Value::Int(n) => Ok(Value::Int(*n)), // floor of int is itself
                other => Err(type_error(name, "Float or Int", other)),
            }
        }

        "ceil" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::Float(f) => Ok(Value::Int(f.ceil() as i64)),
                Value::Int(n) => Ok(Value::Int(*n)),
                other => Err(type_error(name, "Float or Int", other)),
            }
        }

        "round" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::Float(f) => Ok(Value::Int(f.round() as i64)),
                Value::Int(n) => Ok(Value::Int(*n)),
                other => Err(type_error(name, "Float or Int", other)),
            }
        }

        "min" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.min(b))),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.min(*b))),
                // Int/Float coercion
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float((*a as f64).min(*b))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a.min(*b as f64))),
                _ => Err(DefactoError::Build(format!(
                    "'min' expects two numbers, got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        "max" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(*a.max(b))),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.max(*b))),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float((*a as f64).max(*b))),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a.max(*b as f64))),
                _ => Err(DefactoError::Build(format!(
                    "'max' expects two numbers, got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Date/time functions (13)
// ---------------------------------------------------------------------------

fn call_datetime_fn(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    match name {
        "now" => {
            expect_arity(name, &args, 0)?;
            Ok(Value::DateTime(Utc::now()))
        }

        "days_between" => {
            expect_arity(name, &args, 2)?;
            let (a, b) = expect_two_datetimes(name, &args)?;
            let diff = (b - a).num_days();
            Ok(Value::Int(diff.abs()))
        }

        "hours_between" => {
            expect_arity(name, &args, 2)?;
            let (a, b) = expect_two_datetimes(name, &args)?;
            let diff = (b - a).num_hours();
            Ok(Value::Int(diff.abs()))
        }

        "minutes_between" => {
            expect_arity(name, &args, 2)?;
            let (a, b) = expect_two_datetimes(name, &args)?;
            let diff = (b - a).num_minutes();
            Ok(Value::Int(diff.abs()))
        }

        "days_since" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => {
                    let diff = (Utc::now() - *dt).num_days();
                    Ok(Value::Int(diff.abs()))
                }
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "year_of" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => Ok(Value::Int(dt.year() as i64)),
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "month_of" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => Ok(Value::Int(dt.month() as i64)),
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "day_of" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => Ok(Value::Int(dt.day() as i64)),
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "hour_of" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => Ok(Value::Int(dt.hour() as i64)),
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "minute_of" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                Value::DateTime(dt) => Ok(Value::Int(dt.minute() as i64)),
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "day_of_week" => {
            expect_arity(name, &args, 1)?;
            match &args[0] {
                // chrono: Monday=0 through Sunday=6 (num_days_from_monday)
                Value::DateTime(dt) => {
                    Ok(Value::Int(dt.weekday().num_days_from_monday() as i64))
                }
                other => Err(type_error(name, "DateTime", other)),
            }
        }

        "date_trunc" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::DateTime(dt), Value::String(unit)) => {
                    let truncated = match unit.as_str() {
                        "year" => dt
                            .with_month(1)
                            .and_then(|d| d.with_day(1))
                            .and_then(|d| d.with_hour(0))
                            .and_then(|d| d.with_minute(0))
                            .and_then(|d| d.with_second(0))
                            .and_then(|d| d.with_nanosecond(0)),
                        "month" => dt
                            .with_day(1)
                            .and_then(|d| d.with_hour(0))
                            .and_then(|d| d.with_minute(0))
                            .and_then(|d| d.with_second(0))
                            .and_then(|d| d.with_nanosecond(0)),
                        "day" => dt
                            .with_hour(0)
                            .and_then(|d| d.with_minute(0))
                            .and_then(|d| d.with_second(0))
                            .and_then(|d| d.with_nanosecond(0)),
                        "hour" => dt
                            .with_minute(0)
                            .and_then(|d| d.with_second(0))
                            .and_then(|d| d.with_nanosecond(0)),
                        "minute" => dt
                            .with_second(0)
                            .and_then(|d| d.with_nanosecond(0)),
                        other => {
                            return Err(DefactoError::Build(format!(
                                "'date_trunc' unit must be 'year', 'month', 'day', 'hour', or 'minute', got '{}'",
                                other
                            )));
                        }
                    };
                    truncated
                        .map(Value::DateTime)
                        .ok_or_else(|| DefactoError::Build("Failed to truncate datetime".into()))
                }
                _ => Err(DefactoError::Build(format!(
                    "'date_trunc' expects (DateTime, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        "parse_datetime" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(format)) => {
                    // Parse with chrono's strftime syntax, assume UTC
                    let naive = NaiveDateTime::parse_from_str(s, format).map_err(|e| {
                        DefactoError::Build(format!(
                            "'parse_datetime' failed to parse '{}' with format '{}': {}",
                            s, format, e
                        ))
                    })?;
                    Ok(Value::DateTime(DateTime::from_naive_utc_and_offset(
                        naive, Utc,
                    )))
                }
                _ => Err(DefactoError::Build(format!(
                    "'parse_datetime' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        _ => unreachable!(),
    }
}

/// Extract two DateTime values from args, or return a type error.
fn expect_two_datetimes(
    name: &str,
    args: &[Value],
) -> Result<(DateTime<Utc>, DateTime<Utc>), DefactoError> {
    match (&args[0], &args[1]) {
        (Value::DateTime(a), Value::DateTime(b)) => Ok((*a, *b)),
        _ => Err(DefactoError::Build(format!(
            "'{}' expects (DateTime, DateTime), got ({}, {})",
            name,
            type_name(&args[0]),
            type_name(&args[1])
        ))),
    }
}

// ---------------------------------------------------------------------------
// Type casting functions (2)
// ---------------------------------------------------------------------------

fn call_type_fn(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    expect_arity(name, &args, 1)?;

    match name {
        "to_int" => match &args[0] {
            Value::Int(n) => Ok(Value::Int(*n)),
            Value::Float(f) => Ok(Value::Int(*f as i64)),
            Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
            Value::String(s) => s.parse::<i64>().map(Value::Int).map_err(|_| {
                DefactoError::Build(format!(
                    "'to_int' cannot parse '{}' as an integer",
                    s
                ))
            }),
            other => Err(type_error(name, "Int, Float, Bool, or String", other)),
        },

        "to_float" => match &args[0] {
            Value::Float(f) => Ok(Value::Float(*f)),
            Value::Int(n) => Ok(Value::Float(*n as f64)),
            Value::String(s) => s.parse::<f64>().map(Value::Float).map_err(|_| {
                DefactoError::Build(format!(
                    "'to_float' cannot parse '{}' as a float",
                    s
                ))
            }),
            other => Err(type_error(name, "Float, Int, or String", other)),
        },

        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Null handling (1)
// ---------------------------------------------------------------------------

/// Returns the first non-Null argument. Variadic — at least 1 argument required.
fn call_coalesce(args: Vec<Value>) -> Result<Value, DefactoError> {
    expect_min_arity("coalesce", &args, 1)?;

    for arg in args {
        if !matches!(arg, Value::Null) {
            return Ok(arg);
        }
    }

    // All arguments were Null — return Null
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// Array functions (2): split, join
// ---------------------------------------------------------------------------

fn call_array_fn(name: &str, args: Vec<Value>) -> Result<Value, DefactoError> {
    match name {
        "split" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::String(s), Value::String(delim)) => {
                    let parts: Vec<Value> = s
                        .split(delim.as_str())
                        .map(|p| Value::String(p.to_string()))
                        .collect();
                    Ok(Value::Array(parts))
                }
                _ => Err(DefactoError::Build(format!(
                    "'split' expects (String, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        "join" => {
            expect_arity(name, &args, 2)?;
            match (&args[0], &args[1]) {
                (Value::Array(items), Value::String(delim)) => {
                    let strings: Vec<String> = items
                        .iter()
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            Value::Int(n) => n.to_string(),
                            Value::Float(f) => f.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => "null".to_string(),
                            _ => format!("{:?}", v),
                        })
                        .collect();
                    Ok(Value::String(strings.join(delim.as_str())))
                }
                _ => Err(DefactoError::Build(format!(
                    "'join' expects (Array, String), got ({}, {})",
                    type_name(&args[0]),
                    type_name(&args[1])
                ))),
            }
        }

        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── String functions ──

    #[test]
    fn fn_to_lowercase() {
        let result = call_builtin("str::to_lowercase", vec![Value::String("HELLO".into())]);
        assert_eq!(result.unwrap(), Value::String("hello".into()));
    }

    #[test]
    fn fn_to_uppercase() {
        let result = call_builtin("str::to_uppercase", vec![Value::String("hello".into())]);
        assert_eq!(result.unwrap(), Value::String("HELLO".into()));
    }

    #[test]
    fn fn_trim() {
        let result = call_builtin("str::trim", vec![Value::String("  hello  ".into())]);
        assert_eq!(result.unwrap(), Value::String("hello".into()));
    }

    #[test]
    fn fn_substring() {
        let result = call_builtin(
            "str::substring",
            vec![
                Value::String("hello world".into()),
                Value::Int(6),
                Value::Int(5),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("world".into()));
    }

    #[test]
    fn fn_starts_with() {
        let result = call_builtin(
            "starts_with",
            vec![
                Value::String("https://example.com".into()),
                Value::String("https".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn fn_ends_with() {
        let result = call_builtin(
            "ends_with",
            vec![
                Value::String("alice@test.edu".into()),
                Value::String(".edu".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn fn_contains_string() {
        let result = call_builtin(
            "contains",
            vec![
                Value::String("hello world".into()),
                Value::String("world".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn fn_contains_array() {
        let result = call_builtin(
            "contains",
            vec![
                Value::Array(vec![
                    Value::String("admin".into()),
                    Value::String("user".into()),
                ]),
                Value::String("admin".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn fn_replace() {
        let result = call_builtin(
            "replace",
            vec![
                Value::String("555-1234".into()),
                Value::String("-".into()),
                Value::String("".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("5551234".into()));
    }

    #[test]
    fn fn_regex_match() {
        let result = call_builtin(
            "regex_match",
            vec![
                Value::String("alice@company.com".into()),
                Value::String(r".*@company\.com".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Bool(true));
    }

    #[test]
    fn fn_len_string() {
        let result = call_builtin("len", vec![Value::String("hello".into())]);
        assert_eq!(result.unwrap(), Value::Int(5));
    }

    #[test]
    fn fn_len_array() {
        let result = call_builtin(
            "len",
            vec![Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])],
        );
        assert_eq!(result.unwrap(), Value::Int(3));
    }

    // ── Math functions ──

    #[test]
    fn fn_abs_int() {
        assert_eq!(
            call_builtin("math::abs", vec![Value::Int(-42)]).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn fn_abs_float() {
        assert_eq!(
            call_builtin("math::abs", vec![Value::Float(-3.14)]).unwrap(),
            Value::Float(3.14)
        );
    }

    #[test]
    fn fn_floor() {
        assert_eq!(
            call_builtin("floor", vec![Value::Float(3.7)]).unwrap(),
            Value::Int(3)
        );
    }

    #[test]
    fn fn_ceil() {
        assert_eq!(
            call_builtin("ceil", vec![Value::Float(3.2)]).unwrap(),
            Value::Int(4)
        );
    }

    #[test]
    fn fn_round() {
        assert_eq!(
            call_builtin("round", vec![Value::Float(3.5)]).unwrap(),
            Value::Int(4)
        );
    }

    #[test]
    fn fn_min() {
        assert_eq!(
            call_builtin("min", vec![Value::Int(10), Value::Int(20)]).unwrap(),
            Value::Int(10)
        );
    }

    #[test]
    fn fn_max() {
        assert_eq!(
            call_builtin("max", vec![Value::Float(1.5), Value::Float(2.5)]).unwrap(),
            Value::Float(2.5)
        );
    }

    #[test]
    fn fn_min_mixed_types() {
        // Int and Float — coerces to Float
        assert_eq!(
            call_builtin("min", vec![Value::Int(10), Value::Float(5.5)]).unwrap(),
            Value::Float(5.5)
        );
    }

    // ── Date/time functions ──

    #[test]
    fn fn_now() {
        let result = call_builtin("now", vec![]).unwrap();
        assert!(matches!(result, Value::DateTime(_)));
    }

    #[test]
    fn fn_days_between() {
        let a = Utc::now();
        let b = a + chrono::Duration::days(10);
        let result = call_builtin(
            "days_between",
            vec![Value::DateTime(a), Value::DateTime(b)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(10));
    }

    #[test]
    fn fn_hours_between() {
        let a = Utc::now();
        let b = a + chrono::Duration::hours(48);
        let result = call_builtin(
            "hours_between",
            vec![Value::DateTime(a), Value::DateTime(b)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(48));
    }

    #[test]
    fn fn_minutes_between() {
        let a = Utc::now();
        let b = a + chrono::Duration::minutes(90);
        let result = call_builtin(
            "minutes_between",
            vec![Value::DateTime(a), Value::DateTime(b)],
        )
        .unwrap();
        assert_eq!(result, Value::Int(90));
    }

    #[test]
    fn fn_days_since() {
        let past = Utc::now() - chrono::Duration::days(5);
        let result = call_builtin("days_since", vec![Value::DateTime(past)]).unwrap();
        // Should be approximately 5 (might be 4 or 5 depending on time of day)
        match result {
            Value::Int(n) => assert!(n >= 4 && n <= 6),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn fn_year_of() {
        let dt = Utc::now();
        let result = call_builtin("year_of", vec![Value::DateTime(dt)]).unwrap();
        assert_eq!(result, Value::Int(dt.year() as i64));
    }

    #[test]
    fn fn_month_of() {
        let dt = Utc::now();
        let result = call_builtin("month_of", vec![Value::DateTime(dt)]).unwrap();
        assert_eq!(result, Value::Int(dt.month() as i64));
    }

    #[test]
    fn fn_day_of() {
        let dt = Utc::now();
        let result = call_builtin("day_of", vec![Value::DateTime(dt)]).unwrap();
        assert_eq!(result, Value::Int(dt.day() as i64));
    }

    #[test]
    fn fn_hour_of() {
        let dt = Utc::now();
        let result = call_builtin("hour_of", vec![Value::DateTime(dt)]).unwrap();
        assert_eq!(result, Value::Int(dt.hour() as i64));
    }

    #[test]
    fn fn_minute_of() {
        let dt = Utc::now();
        let result = call_builtin("minute_of", vec![Value::DateTime(dt)]).unwrap();
        assert_eq!(result, Value::Int(dt.minute() as i64));
    }

    #[test]
    fn fn_day_of_week() {
        let dt = Utc::now();
        let result = call_builtin("day_of_week", vec![Value::DateTime(dt)]).unwrap();
        match result {
            Value::Int(n) => assert!((0..=6).contains(&n)),
            other => panic!("expected Int 0-6, got {:?}", other),
        }
    }

    #[test]
    fn fn_date_trunc_day() {
        let dt = Utc::now();
        let result = call_builtin(
            "date_trunc",
            vec![Value::DateTime(dt), Value::String("day".into())],
        )
        .unwrap();
        match result {
            Value::DateTime(truncated) => {
                assert_eq!(truncated.hour(), 0);
                assert_eq!(truncated.minute(), 0);
                assert_eq!(truncated.second(), 0);
            }
            other => panic!("expected DateTime, got {:?}", other),
        }
    }

    #[test]
    fn fn_date_trunc_month() {
        let dt = Utc::now();
        let result = call_builtin(
            "date_trunc",
            vec![Value::DateTime(dt), Value::String("month".into())],
        )
        .unwrap();
        match result {
            Value::DateTime(truncated) => {
                assert_eq!(truncated.day(), 1);
                assert_eq!(truncated.hour(), 0);
            }
            other => panic!("expected DateTime, got {:?}", other),
        }
    }

    #[test]
    fn fn_parse_datetime() {
        let result = call_builtin(
            "parse_datetime",
            vec![
                Value::String("2026-04-13 10:30:00".into()),
                Value::String("%Y-%m-%d %H:%M:%S".into()),
            ],
        )
        .unwrap();
        match result {
            Value::DateTime(dt) => {
                assert_eq!(dt.year(), 2026);
                assert_eq!(dt.month(), 4);
                assert_eq!(dt.day(), 13);
                assert_eq!(dt.hour(), 10);
                assert_eq!(dt.minute(), 30);
            }
            other => panic!("expected DateTime, got {:?}", other),
        }
    }

    // ── Type casting ──

    #[test]
    fn fn_to_int_from_float() {
        assert_eq!(
            call_builtin("to_int", vec![Value::Float(3.7)]).unwrap(),
            Value::Int(3)
        );
    }

    #[test]
    fn fn_to_int_from_string() {
        assert_eq!(
            call_builtin("to_int", vec![Value::String("42".into())]).unwrap(),
            Value::Int(42)
        );
    }

    #[test]
    fn fn_to_int_from_bool() {
        assert_eq!(
            call_builtin("to_int", vec![Value::Bool(true)]).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn fn_to_float_from_int() {
        assert_eq!(
            call_builtin("to_float", vec![Value::Int(42)]).unwrap(),
            Value::Float(42.0)
        );
    }

    #[test]
    fn fn_to_float_from_string() {
        assert_eq!(
            call_builtin("to_float", vec![Value::String("3.14".into())]).unwrap(),
            Value::Float(3.14)
        );
    }

    // ── Null handling ──

    #[test]
    fn fn_coalesce_first_non_null() {
        let result = call_builtin(
            "coalesce",
            vec![Value::Null, Value::String("fallback".into())],
        )
        .unwrap();
        assert_eq!(result, Value::String("fallback".into()));
    }

    #[test]
    fn fn_coalesce_first_is_non_null() {
        let result = call_builtin(
            "coalesce",
            vec![Value::String("first".into()), Value::String("second".into())],
        )
        .unwrap();
        assert_eq!(result, Value::String("first".into()));
    }

    #[test]
    fn fn_coalesce_all_null() {
        let result = call_builtin("coalesce", vec![Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── Error cases ──

    #[test]
    fn fn_unknown_error() {
        assert!(call_builtin("nonexistent", vec![]).is_err());
    }

    #[test]
    fn fn_wrong_arity_error() {
        assert!(call_builtin("str::to_lowercase", vec![]).is_err());
    }

    #[test]
    fn fn_wrong_type_error() {
        assert!(call_builtin("str::to_lowercase", vec![Value::Int(42)]).is_err());
    }

    #[test]
    fn fn_invalid_regex_error() {
        let result = call_builtin(
            "regex_match",
            vec![
                Value::String("test".into()),
                Value::String("[invalid".into()),
            ],
        );
        assert!(result.is_err());
    }

    #[test]
    fn fn_regex_extract_capture_group() {
        let result = call_builtin(
            "regex_extract",
            vec![
                Value::String("ORD-12345-US".into()),
                Value::String(r"ORD-(\d+)".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("12345".into()));
    }

    #[test]
    fn fn_regex_extract_no_match() {
        let result = call_builtin(
            "regex_extract",
            vec![
                Value::String("no match here".into()),
                Value::String(r"ORD-(\d+)".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn fn_regex_extract_full_match() {
        // No capture group — returns full match
        let result = call_builtin(
            "regex_extract",
            vec![
                Value::String("abc123def".into()),
                Value::String(r"\d+".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("123".into()));
    }

    #[test]
    fn fn_to_string_number() {
        assert_eq!(
            call_builtin("to_string", vec![Value::Int(42)]).unwrap(),
            Value::String("42".into()),
        );
    }

    #[test]
    fn fn_to_string_float() {
        assert_eq!(
            call_builtin("to_string", vec![Value::Float(3.14)]).unwrap(),
            Value::String("3.14".into()),
        );
    }

    #[test]
    fn fn_to_string_bool() {
        assert_eq!(
            call_builtin("to_string", vec![Value::Bool(true)]).unwrap(),
            Value::String("true".into()),
        );
    }

    #[test]
    fn fn_to_string_null() {
        assert_eq!(
            call_builtin("to_string", vec![Value::Null]).unwrap(),
            Value::String("".into()),
        );
    }

    #[test]
    fn fn_to_int_invalid_string() {
        let result = call_builtin("to_int", vec![Value::String("not_a_number".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn fn_date_trunc_invalid_unit() {
        let result = call_builtin(
            "date_trunc",
            vec![Value::DateTime(Utc::now()), Value::String("week".into())],
        );
        assert!(result.is_err());
    }

    #[test]
    fn fn_coalesce_empty_error() {
        assert!(call_builtin("coalesce", vec![]).is_err());
    }

    // ── Array functions ──

    #[test]
    fn fn_split_basic() {
        let result = call_builtin(
            "split",
            vec![Value::String("a.b.c".into()), Value::String(".".into())],
        );
        assert_eq!(
            result.unwrap(),
            Value::Array(vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ])
        );
    }

    #[test]
    fn fn_split_no_match() {
        let result = call_builtin(
            "split",
            vec![Value::String("hello".into()), Value::String("x".into())],
        );
        assert_eq!(
            result.unwrap(),
            Value::Array(vec![Value::String("hello".into())])
        );
    }

    #[test]
    fn fn_split_empty() {
        let result = call_builtin(
            "split",
            vec![Value::String("".into()), Value::String(".".into())],
        );
        assert_eq!(
            result.unwrap(),
            Value::Array(vec![Value::String("".into())])
        );
    }

    #[test]
    fn fn_split_email() {
        let result = call_builtin(
            "split",
            vec![
                Value::String("alice@test.com".into()),
                Value::String("@".into()),
            ],
        );
        assert_eq!(
            result.unwrap(),
            Value::Array(vec![
                Value::String("alice".into()),
                Value::String("test.com".into()),
            ])
        );
    }

    #[test]
    fn fn_join_basic() {
        let result = call_builtin(
            "join",
            vec![
                Value::Array(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                    Value::String("c".into()),
                ]),
                Value::String(".".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("a.b.c".into()));
    }

    #[test]
    fn fn_join_single() {
        let result = call_builtin(
            "join",
            vec![
                Value::Array(vec![Value::String("a".into())]),
                Value::String(",".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("a".into()));
    }

    #[test]
    fn fn_join_empty() {
        let result = call_builtin(
            "join",
            vec![Value::Array(vec![]), Value::String(",".into())],
        );
        assert_eq!(result.unwrap(), Value::String("".into()));
    }

    #[test]
    fn fn_join_coerces_types() {
        let result = call_builtin(
            "join",
            vec![
                Value::Array(vec![
                    Value::String("count".into()),
                    Value::Int(42),
                ]),
                Value::String(": ".into()),
            ],
        );
        assert_eq!(result.unwrap(), Value::String("count: 42".into()));
    }
}
