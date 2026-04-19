//! Expression evaluator — executes parsed ASTs against a context.
//!
//! Called during interpretation for every guard, condition, computed
//! property, and normalization expression. Performance-critical — this
//! runs on every event for every matching handler.
//!
//! The context provides the variables available during evaluation:
//! - Guards/conditions: `event.*` and `entity.*`
//! - Computed properties: `entity.*` only
//! - Source normalizations: `event.*` only (raw event fields)
//! - Identity normalizations: `value` only (bare variable via `variables` map)
//!
//! Type dispatch:
//! - Arithmetic operators are type-aware: Int+Int→Int, Int+Float→Float (coerce),
//!   DateTime+Duration→DateTime, String+String→String (concatenation).
//! - Missing fields return Null (not an error) so coalesce() and null checks work.
//! - The evaluator never panics — every error path returns DefactoError::Build
//!   with a message describing what went wrong and what types were involved.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::error::DefactoError;
use crate::expression::functions;
use crate::expression::Expression;
use crate::types::{Op, UnaryOp, Value};

// ---------------------------------------------------------------------------
// Evaluation context
// ---------------------------------------------------------------------------

/// Variables available during expression evaluation.
///
/// The context determines what field access expressions resolve to.
/// A guard expression like `event.amount > entity.threshold` needs both
/// event and entity contexts populated. An identity normalize expression
/// like `str::to_lowercase(value)` uses the `variables` map.
///
/// Field access resolution:
/// - Path starts with "event" → look up remaining path in `event`
/// - Path starts with "entity" → look up remaining path in `entity`
/// - Path length 1 (bare variable like "value") → look up in `variables`
///
/// Event and entity data are borrowed, not cloned. This is critical for
/// performance — the interpreter evaluates guards, conditions, and compute
/// expressions on every event for every handler. Cloning the event/entity
/// maps on each evaluation would waste microseconds per expression on data
/// that doesn't change between evaluations (event data is immutable, entity
/// data only changes when effects are applied).
#[derive(Debug)]
pub struct EvalContext<'a> {
    /// Event fields — available in guards, conditions, source mappings.
    pub event: &'a HashMap<String, Value>,
    /// Entity fields — available in guards, conditions, computed properties.
    pub entity: &'a HashMap<String, Value>,
    /// Bare variables — used for identity normalize (`value`).
    /// Owned because it's constructed per-call for the rare identity normalize case.
    pub variables: HashMap<String, Value>,
}

/// Empty map used as default context for constructors that don't need
/// event or entity data (identity normalize, testing).
static EMPTY_MAP: std::sync::LazyLock<HashMap<String, Value>> =
    std::sync::LazyLock::new(HashMap::new);

impl<'a> EvalContext<'a> {
    /// Create an empty context. Useful for testing.
    pub fn empty() -> Self {
        Self {
            event: &EMPTY_MAP,
            entity: &EMPTY_MAP,
            variables: HashMap::new(),
        }
    }

    /// Create a context with only event fields (source mapping compute).
    pub fn event_only(event: &'a HashMap<String, Value>) -> Self {
        Self {
            event,
            entity: &EMPTY_MAP,
            variables: HashMap::new(),
        }
    }

    /// Create a context with only a bare variable (identity normalize).
    pub fn value_only(value: Value) -> Self {
        Self {
            event: &EMPTY_MAP,
            entity: &EMPTY_MAP,
            variables: HashMap::from([("value".into(), value)]),
        }
    }

    /// Create a full context with event and entity fields (guards, conditions).
    pub fn full(
        event: &'a HashMap<String, Value>,
        entity: &'a HashMap<String, Value>,
    ) -> Self {
        Self {
            event,
            entity,
            variables: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Main evaluation entry point
// ---------------------------------------------------------------------------

/// Evaluate a parsed expression against a context, producing a Value.
///
/// Recursively walks the AST. Each variant has its own dispatch function
/// for readability. The evaluator never panics — all error paths return
/// `DefactoError::Build` with descriptive messages.
pub fn evaluate(expr: &Expression, ctx: &EvalContext<'_>) -> Result<Value, DefactoError> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::FieldAccess(path) => resolve_field(path, ctx),
        Expression::BinaryOp(lhs, op, rhs) => eval_binary(lhs, op, rhs, ctx),
        Expression::UnaryOp(op, operand) => eval_unary(op, operand, ctx),
        Expression::FunctionCall(name, args) => eval_function(name, args, ctx),
        Expression::If(cond, then_branch, else_branch) => {
            eval_if(cond, then_branch, else_branch, ctx)
        }
        Expression::Index(base_expr, index_expr) => {
            let base = evaluate(base_expr, ctx)?;
            let idx = evaluate(index_expr, ctx)?;
            eval_index(&base, &idx)
        }
    }
}

// ---------------------------------------------------------------------------
// Field access resolution
// ---------------------------------------------------------------------------

/// Resolve a dot-separated field path against the context.
///
/// - `["event", "email"]` → looks up "email" in ctx.event
/// - `["entity", "mrr"]` → looks up "mrr" in ctx.entity
/// - `["value"]` → looks up "value" in ctx.variables
/// - `["event", "user", "address"]` → nested lookup with array index support
///
/// Missing fields return Value::Null (not an error). This is intentional:
/// `coalesce(event.nickname, event.name, "unknown")` relies on missing
/// fields being Null, and `event.email != null` is a common guard pattern.
fn resolve_field(path: &[String], ctx: &EvalContext<'_>) -> Result<Value, DefactoError> {
    let first = path
        .first()
        .ok_or_else(|| DefactoError::Build("Empty field path".into()))?;

    match first.as_str() {
        "event" if path.len() > 1 => resolve_nested(&path[1..], ctx.event),
        "entity" if path.len() > 1 => resolve_nested(&path[1..], ctx.entity),
        _ if path.len() == 1 => {
            // Bare variable (e.g., "value" in identity normalize)
            Ok(ctx
                .variables
                .get(first.as_str())
                .cloned()
                .unwrap_or(Value::Null))
        }
        _ => Err(DefactoError::Build(format!(
            "Unknown context '{}' in '{}' — expected 'event' or 'entity'",
            first,
            path.join(".")
        ))),
    }
}

/// Walk a HashMap by path segments, supporting nested Value::Array indexing.
///
/// Numeric segments (e.g., "0" in "items.0.product_id") index into arrays.
/// Missing keys at any level return Value::Null.
fn resolve_nested(path: &[String], map: &HashMap<String, Value>) -> Result<Value, DefactoError> {
    if path.is_empty() {
        return Err(DefactoError::Build("Empty nested path".into()));
    }

    let first = &path[0];
    let value = map.get(first.as_str()).cloned().unwrap_or(Value::Null);

    if path.len() == 1 {
        return Ok(value);
    }

    // Walk deeper into nested structures
    walk_value(&value, &path[1..])
}

/// Continue resolving a path into a Value (for nested objects and arrays).
fn walk_value(value: &Value, remaining: &[String]) -> Result<Value, DefactoError> {
    if remaining.is_empty() {
        return Ok(value.clone());
    }

    let segment = &remaining[0];

    match value {
        // Numeric segment indexes into an array
        Value::Array(items) => {
            if let Ok(index) = segment.parse::<usize>() {
                let item = items.get(index).cloned().unwrap_or(Value::Null);
                walk_value(&item, &remaining[1..])
            } else {
                // Non-numeric segment on an array — can't resolve
                Ok(Value::Null)
            }
        }
        // Any other type with remaining path segments — can't go deeper
        _ => Ok(Value::Null),
    }
}

// ---------------------------------------------------------------------------
// Binary operator evaluation
// ---------------------------------------------------------------------------

/// Evaluate a binary operation: evaluate both sides, then dispatch on types.
fn eval_binary(
    lhs: &Expression,
    op: &Op,
    rhs: &Expression,
    ctx: &EvalContext<'_>,
) -> Result<Value, DefactoError> {
    let left = evaluate(lhs, ctx)?;
    let right = evaluate(rhs, ctx)?;

    match op {
        // Logical operators — short description: both must be Bool
        Op::And => eval_logical_and(&left, &right),
        Op::Or => eval_logical_or(&left, &right),

        // Equality — works on all types including Null
        Op::Eq => Ok(Value::Bool(values_equal(&left, &right))),
        Op::Neq => Ok(Value::Bool(!values_equal(&left, &right))),

        // Comparison — same-type only (or Int/Float coercion)
        Op::Gt | Op::Lt | Op::Gte | Op::Lte => eval_comparison(&left, op, &right),

        // Arithmetic and concatenation — type-dependent behavior
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Pow => {
            eval_arithmetic(&left, op, &right)
        }
    }
}

/// Logical AND — both sides must be Bool.
fn eval_logical_and(left: &Value, right: &Value) -> Result<Value, DefactoError> {
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
        _ => Err(DefactoError::Build(format!(
            "'and' requires Bool on both sides, got {} and {}",
            type_name(left),
            type_name(right)
        ))),
    }
}

/// Logical OR — both sides must be Bool.
fn eval_logical_or(left: &Value, right: &Value) -> Result<Value, DefactoError> {
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
        _ => Err(DefactoError::Build(format!(
            "'or' requires Bool on both sides, got {} and {}",
            type_name(left),
            type_name(right)
        ))),
    }
}

/// Equality comparison — works across all types.
/// Null == Null is true. Different types are never equal.
fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        // Int/Float cross-comparison: coerce Int to Float
        (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
        (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::DateTime(a), Value::DateTime(b)) => a == b,
        (Value::Duration(a), Value::Duration(b)) => a == b,
        _ => false,
    }
}

/// Ordered comparison — requires same type (or Int/Float coercion).
fn eval_comparison(left: &Value, op: &Op, right: &Value) -> Result<Value, DefactoError> {
    let ordering = compare_values(left, right)?;
    let result = match op {
        Op::Gt => ordering == Ordering::Greater,
        Op::Lt => ordering == Ordering::Less,
        Op::Gte => ordering != Ordering::Less,
        Op::Lte => ordering != Ordering::Greater,
        _ => unreachable!(),
    };
    Ok(Value::Bool(result))
}

/// Compare two values, returning their ordering.
/// Supports Int, Float, String, DateTime, Duration. Int/Float are cross-comparable.
fn compare_values(left: &Value, right: &Value) -> Result<Ordering, DefactoError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| DefactoError::Build("Cannot compare NaN values".into())),
        // Int/Float coercion
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| DefactoError::Build("Cannot compare NaN values".into())),
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| DefactoError::Build("Cannot compare NaN values".into())),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        (Value::DateTime(a), Value::DateTime(b)) => Ok(a.cmp(b)),
        (Value::Duration(a), Value::Duration(b)) => Ok(a.cmp(b)),
        _ => Err(DefactoError::Build(format!(
            "Cannot compare {} with {} — both sides must be the same type",
            type_name(left),
            type_name(right)
        ))),
    }
}

/// Arithmetic operations — type-aware dispatch.
///
/// Supports:
/// - Int ± Int → Int, Float ± Float → Float, Int ± Float → Float (coerce)
/// - String + String → String (concatenation)
/// - DateTime ± Duration → DateTime
/// - DateTime - DateTime → Duration
/// - Duration ± Duration → Duration
/// - Duration * Int → Duration, Int * Duration → Duration
fn eval_arithmetic(left: &Value, op: &Op, right: &Value) -> Result<Value, DefactoError> {
    match (left, op, right) {
        // -- Integer arithmetic --
        (Value::Int(a), Op::Add, Value::Int(b)) => Ok(Value::Int(a + b)),
        (Value::Int(a), Op::Sub, Value::Int(b)) => Ok(Value::Int(a - b)),
        (Value::Int(a), Op::Mul, Value::Int(b)) => Ok(Value::Int(a * b)),
        (Value::Int(a), Op::Div, Value::Int(b)) => {
            if *b == 0 {
                return Err(DefactoError::Build("Division by zero".into()));
            }
            Ok(Value::Int(a / b))
        }
        (Value::Int(a), Op::Pow, Value::Int(b)) => {
            // Negative exponents don't make sense for integer power
            if *b < 0 {
                return Err(DefactoError::Build(format!(
                    "Cannot raise integer to negative power {} — use floats for fractional results",
                    b
                )));
            }
            Ok(Value::Int(a.pow(*b as u32)))
        }

        // -- Float arithmetic --
        (Value::Float(a), Op::Add, Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Float(a), Op::Sub, Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Float(a), Op::Mul, Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Float(a), Op::Div, Value::Float(b)) => {
            if *b == 0.0 {
                return Err(DefactoError::Build("Division by zero".into()));
            }
            Ok(Value::Float(a / b))
        }
        (Value::Float(a), Op::Pow, Value::Float(b)) => Ok(Value::Float(a.powf(*b))),

        // -- Int/Float coercion: promote Int to Float --
        (Value::Int(a), op, Value::Float(b)) => {
            eval_arithmetic(&Value::Float(*a as f64), op, &Value::Float(*b))
        }
        (Value::Float(a), op, Value::Int(b)) => {
            eval_arithmetic(&Value::Float(*a), op, &Value::Float(*b as f64))
        }

        // -- String concatenation via + --
        (Value::String(a), Op::Add, Value::String(b)) => {
            Ok(Value::String(format!("{}{}", a, b)))
        }

        // -- DateTime ± Duration --
        (Value::DateTime(dt), Op::Add, Value::Duration(dur)) => {
            let chrono_dur = chrono::Duration::from_std(*dur)
                .map_err(|_| DefactoError::Build("Duration too large for datetime arithmetic".into()))?;
            Ok(Value::DateTime(*dt + chrono_dur))
        }
        (Value::DateTime(dt), Op::Sub, Value::Duration(dur)) => {
            let chrono_dur = chrono::Duration::from_std(*dur)
                .map_err(|_| DefactoError::Build("Duration too large for datetime arithmetic".into()))?;
            Ok(Value::DateTime(*dt - chrono_dur))
        }

        // -- DateTime - DateTime → Duration --
        (Value::DateTime(a), Op::Sub, Value::DateTime(b)) => {
            let diff = *a - *b;
            diff.to_std()
                .map(Value::Duration)
                .map_err(|_| DefactoError::Build(format!(
                    "Negative duration: the first datetime ({}) is before the second ({})",
                    a, b
                )))
        }

        // -- Duration ± Duration --
        (Value::Duration(a), Op::Add, Value::Duration(b)) => Ok(Value::Duration(*a + *b)),
        (Value::Duration(a), Op::Sub, Value::Duration(b)) => {
            a.checked_sub(*b)
                .map(Value::Duration)
                .ok_or_else(|| DefactoError::Build(
                    "Duration subtraction would produce a negative result".into()
                ))
        }

        // -- Duration * Int (scaling) --
        (Value::Duration(d), Op::Mul, Value::Int(n)) => Ok(Value::Duration(*d * (*n as u32))),
        (Value::Int(n), Op::Mul, Value::Duration(d)) => Ok(Value::Duration(*d * (*n as u32))),

        // -- Catch-all: type mismatch --
        _ => Err(DefactoError::Build(format!(
            "Cannot apply '{}' to {} and {}",
            op_symbol(op),
            type_name(left),
            type_name(right)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Unary operator evaluation
// ---------------------------------------------------------------------------

/// Evaluate a unary operation.
fn eval_unary(
    op: &UnaryOp,
    operand: &Expression,
    ctx: &EvalContext<'_>,
) -> Result<Value, DefactoError> {
    let val = evaluate(operand, ctx)?;
    match (op, &val) {
        (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOp::Negate, Value::Int(n)) => Ok(Value::Int(-n)),
        (UnaryOp::Negate, Value::Float(f)) => Ok(Value::Float(-f)),
        (UnaryOp::Not, _) => Err(DefactoError::Build(format!(
            "'not' requires Bool, got {}",
            type_name(&val)
        ))),
        (UnaryOp::Negate, _) => Err(DefactoError::Build(format!(
            "'-' (negate) requires Int or Float, got {}",
            type_name(&val)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Function call evaluation
// ---------------------------------------------------------------------------

/// Evaluate a function call: evaluate all arguments, then dispatch to built-in.
fn eval_function(
    name: &str,
    arg_exprs: &[Expression],
    ctx: &EvalContext<'_>,
) -> Result<Value, DefactoError> {
    // Evaluate all arguments eagerly (left to right)
    let args: Vec<Value> = arg_exprs
        .iter()
        .map(|arg| evaluate(arg, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    functions::call_builtin(name, args)
}

// ---------------------------------------------------------------------------
// Conditional evaluation
// ---------------------------------------------------------------------------

/// Evaluate if-then-else: condition must be Bool, only the matching branch
/// is evaluated (lazy — the other branch is never touched).
fn eval_if(
    cond: &Expression,
    then_branch: &Expression,
    else_branch: &Expression,
    ctx: &EvalContext<'_>,
) -> Result<Value, DefactoError> {
    match evaluate(cond, ctx)? {
        Value::Bool(true) => evaluate(then_branch, ctx),
        Value::Bool(false) => evaluate(else_branch, ctx),
        other => Err(DefactoError::Build(format!(
            "'if' condition must be Bool, got {} — wrap in a comparison like '> 0' or '!= null'",
            type_name(&other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Index evaluation
// ---------------------------------------------------------------------------

/// Index into a value by integer position.
///
/// Positive indices count from the start, negative from the end.
/// Out-of-bounds returns Null (no panic, no error).
/// Works on Array and String (single character extraction).
fn eval_index(base: &Value, idx: &Value) -> Result<Value, DefactoError> {
    let i = match idx {
        Value::Int(n) => *n,
        other => {
            return Err(DefactoError::Build(format!(
                "Index must be Int, got {}",
                type_name(other)
            )));
        }
    };

    match base {
        Value::Array(items) => {
            let len = items.len() as i64;
            let resolved = if i < 0 { len + i } else { i };
            if resolved < 0 || resolved >= len {
                Ok(Value::Null)
            } else {
                Ok(items[resolved as usize].clone())
            }
        }
        Value::String(s) => {
            let chars: Vec<char> = s.chars().collect();
            let len = chars.len() as i64;
            let resolved = if i < 0 { len + i } else { i };
            if resolved < 0 || resolved >= len {
                Ok(Value::Null)
            } else {
                Ok(Value::String(chars[resolved as usize].to_string()))
            }
        }
        other => Err(DefactoError::Build(format!(
            "Cannot index into {}, expected Array or String",
            type_name(other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Human-readable type name for error messages.
pub fn type_name(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "String",
        Value::Float(_) => "Float",
        Value::Int(_) => "Int",
        Value::Bool(_) => "Bool",
        Value::DateTime(_) => "DateTime",
        Value::Duration(_) => "Duration",
        Value::Array(_) => "Array",
        Value::Null => "Null",
    }
}

/// Human-readable operator symbol for error messages.
fn op_symbol(op: &Op) -> &'static str {
    match op {
        Op::Add => "+",
        Op::Sub => "-",
        Op::Mul => "*",
        Op::Div => "/",
        Op::Pow => "**",
        Op::Eq => "==",
        Op::Neq => "!=",
        Op::Gt => ">",
        Op::Lt => "<",
        Op::Gte => ">=",
        Op::Lte => "<=",
        Op::And => "and",
        Op::Or => "or",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use chrono::Utc;

    /// Helper: build event and entity data maps for testing.
    /// Returns owned maps — callers create `EvalContext::full(&event, &entity)`.
    ///
    /// Usage pattern in tests:
    /// ```
    /// let (event, entity) = test_data();
    /// let ctx = EvalContext::full(&event, &entity);
    /// ```
    fn test_data() -> (HashMap<String, Value>, HashMap<String, Value>) {
        let event = HashMap::from([
            ("amount".into(), Value::Int(150)),
            ("plan".into(), Value::String("pro".into())),
            ("email".into(), Value::String("ALICE@TEST.COM".into())),
            ("score".into(), Value::Float(85.5)),
            ("active".into(), Value::Bool(true)),
            (
                "items".into(),
                Value::Array(vec![
                    Value::String("a".into()),
                    Value::String("b".into()),
                ]),
            ),
        ]);
        let entity = HashMap::from([
            ("mrr".into(), Value::Float(99.0)),
            ("plan".into(), Value::String("free".into())),
            ("active".into(), Value::Bool(false)),
            ("high_score".into(), Value::Float(90.0)),
        ]);
        (event, entity)
    }

    // ── Literal evaluation ──

    #[test]
    fn eval_literal_int() {
        let expr = Expression::Literal(Value::Int(42));
        assert_eq!(evaluate(&expr, &EvalContext::empty()).unwrap(), Value::Int(42));
    }

    #[test]
    fn eval_literal_string() {
        let expr = Expression::Literal(Value::String("hello".into()));
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::String("hello".into())
        );
    }

    #[test]
    fn eval_literal_null() {
        let expr = Expression::Literal(Value::Null);
        assert_eq!(evaluate(&expr, &EvalContext::empty()).unwrap(), Value::Null);
    }

    // ── Field access ──

    #[test]
    fn eval_event_field() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::FieldAccess(vec!["event".into(), "amount".into()]);
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::Int(150));
    }

    #[test]
    fn eval_entity_field() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::FieldAccess(vec!["entity".into(), "mrr".into()]);
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::Float(99.0));
    }

    #[test]
    fn eval_bare_variable() {
        let ctx = EvalContext::value_only(Value::String("alice@test.com".into()));
        let expr = Expression::FieldAccess(vec!["value".into()]);
        assert_eq!(
            evaluate(&expr, &ctx).unwrap(),
            Value::String("alice@test.com".into())
        );
    }

    #[test]
    fn eval_missing_field_returns_null() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::FieldAccess(vec!["event".into(), "nonexistent".into()]);
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::Null);
    }

    #[test]
    fn eval_array_index() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::FieldAccess(vec!["event".into(), "items".into(), "0".into()]);
        assert_eq!(
            evaluate(&expr, &ctx).unwrap(),
            Value::String("a".into())
        );
    }

    #[test]
    fn eval_array_index_out_of_bounds() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::FieldAccess(vec!["event".into(), "items".into(), "99".into()]);
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::Null);
    }

    // ── Index expression ──

    #[test]
    fn eval_index_array_positive() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::Index(
            Box::new(Expression::FieldAccess(vec!["event".into(), "items".into()])),
            Box::new(Expression::Literal(Value::Int(0))),
        );
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::String("a".into()));
    }

    #[test]
    fn eval_index_array_negative() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::Index(
            Box::new(Expression::FieldAccess(vec!["event".into(), "items".into()])),
            Box::new(Expression::Literal(Value::Int(-1))),
        );
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::String("b".into()));
    }

    #[test]
    fn eval_index_out_of_bounds_returns_null() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        let expr = Expression::Index(
            Box::new(Expression::FieldAccess(vec!["event".into(), "items".into()])),
            Box::new(Expression::Literal(Value::Int(99))),
        );
        assert_eq!(evaluate(&expr, &ctx).unwrap(), Value::Null);
    }

    #[test]
    fn eval_index_string() {
        let expr = Expression::Index(
            Box::new(Expression::Literal(Value::String("hello".into()))),
            Box::new(Expression::Literal(Value::Int(1))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::String("e".into())
        );
    }

    #[test]
    fn eval_split_then_index() {
        let (event, entity) = test_data();
        let ctx = EvalContext::full(&event, &entity);
        // split(event.email, "@")[1] → "TEST.COM" (email is "ALICE@TEST.COM")
        let expr = Expression::Index(
            Box::new(Expression::FunctionCall(
                "split".into(),
                vec![
                    Expression::FieldAccess(vec!["event".into(), "email".into()]),
                    Expression::Literal(Value::String("@".into())),
                ],
            )),
            Box::new(Expression::Literal(Value::Int(1))),
        );
        assert_eq!(
            evaluate(&expr, &ctx).unwrap(),
            Value::String("TEST.COM".into())
        );
    }

    // ── Integer arithmetic ──

    #[test]
    fn eval_int_add() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Int(10))),
            Op::Add,
            Box::new(Expression::Literal(Value::Int(20))),
        );
        assert_eq!(evaluate(&expr, &EvalContext::empty()).unwrap(), Value::Int(30));
    }

    #[test]
    fn eval_int_div() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Int(10))),
            Op::Div,
            Box::new(Expression::Literal(Value::Int(3))),
        );
        assert_eq!(evaluate(&expr, &EvalContext::empty()).unwrap(), Value::Int(3));
    }

    #[test]
    fn eval_division_by_zero() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Int(10))),
            Op::Div,
            Box::new(Expression::Literal(Value::Int(0))),
        );
        assert!(evaluate(&expr, &EvalContext::empty()).is_err());
    }

    // ── Float arithmetic with coercion ──

    #[test]
    fn eval_float_mul() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Float(3.0))),
            Op::Mul,
            Box::new(Expression::Literal(Value::Float(4.5))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Float(13.5)
        );
    }

    #[test]
    fn eval_int_float_coercion() {
        // Int + Float → Float
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Int(10))),
            Op::Add,
            Box::new(Expression::Literal(Value::Float(0.5))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Float(10.5)
        );
    }

    // ── String concatenation ──

    #[test]
    fn eval_string_concat() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::String("hello".into()))),
            Op::Add,
            Box::new(Expression::Literal(Value::String(" world".into()))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::String("hello world".into())
        );
    }

    // ── DateTime arithmetic ──

    #[test]
    fn eval_datetime_plus_duration() {
        let dt = Utc::now();
        let dur = Duration::from_secs(86400); // 1 day
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::DateTime(dt))),
            Op::Add,
            Box::new(Expression::Literal(Value::Duration(dur))),
        );
        let result = evaluate(&expr, &EvalContext::empty()).unwrap();
        match result {
            Value::DateTime(result_dt) => {
                let diff = result_dt - dt;
                assert_eq!(diff.num_seconds(), 86400);
            }
            other => panic!("expected DateTime, got {:?}", other),
        }
    }

    #[test]
    fn eval_datetime_minus_datetime() {
        let dt1 = Utc::now();
        let dt2 = dt1 - chrono::Duration::hours(2);
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::DateTime(dt1))),
            Op::Sub,
            Box::new(Expression::Literal(Value::DateTime(dt2))),
        );
        let result = evaluate(&expr, &EvalContext::empty()).unwrap();
        match result {
            Value::Duration(d) => assert_eq!(d.as_secs(), 7200),
            other => panic!("expected Duration, got {:?}", other),
        }
    }

    // ── Comparison ──

    #[test]
    fn eval_int_comparison() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Int(10))),
            Op::Gt,
            Box::new(Expression::Literal(Value::Int(5))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn eval_datetime_comparison() {
        let now = Utc::now();
        let earlier = now - chrono::Duration::hours(1);
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::DateTime(now))),
            Op::Gt,
            Box::new(Expression::Literal(Value::DateTime(earlier))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn eval_null_equality() {
        // null == null → true
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Null)),
            Op::Eq,
            Box::new(Expression::Literal(Value::Null)),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(true)
        );

        // null != "something" → true
        let expr2 = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Null)),
            Op::Neq,
            Box::new(Expression::Literal(Value::String("x".into()))),
        );
        assert_eq!(
            evaluate(&expr2, &EvalContext::empty()).unwrap(),
            Value::Bool(true)
        );
    }

    // ── Logical operators ──

    #[test]
    fn eval_logical_and() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Bool(true))),
            Op::And,
            Box::new(Expression::Literal(Value::Bool(false))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn eval_logical_or() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::Bool(false))),
            Op::Or,
            Box::new(Expression::Literal(Value::Bool(true))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(true)
        );
    }

    // ── Unary operators ──

    #[test]
    fn eval_not() {
        let expr = Expression::UnaryOp(
            UnaryOp::Not,
            Box::new(Expression::Literal(Value::Bool(true))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn eval_negate_int() {
        let expr = Expression::UnaryOp(
            UnaryOp::Negate,
            Box::new(Expression::Literal(Value::Int(42))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Int(-42)
        );
    }

    #[test]
    fn eval_negate_float() {
        let expr = Expression::UnaryOp(
            UnaryOp::Negate,
            Box::new(Expression::Literal(Value::Float(3.14))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::Float(-3.14)
        );
    }

    // ── If-then-else ──

    #[test]
    fn eval_if_true() {
        let expr = Expression::If(
            Box::new(Expression::Literal(Value::Bool(true))),
            Box::new(Expression::Literal(Value::String("yes".into()))),
            Box::new(Expression::Literal(Value::String("no".into()))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::String("yes".into())
        );
    }

    #[test]
    fn eval_if_false() {
        let expr = Expression::If(
            Box::new(Expression::Literal(Value::Bool(false))),
            Box::new(Expression::Literal(Value::String("yes".into()))),
            Box::new(Expression::Literal(Value::String("no".into()))),
        );
        assert_eq!(
            evaluate(&expr, &EvalContext::empty()).unwrap(),
            Value::String("no".into())
        );
    }

    #[test]
    fn eval_if_non_bool_error() {
        let expr = Expression::If(
            Box::new(Expression::Literal(Value::Int(1))),
            Box::new(Expression::Literal(Value::String("yes".into()))),
            Box::new(Expression::Literal(Value::String("no".into()))),
        );
        assert!(evaluate(&expr, &EvalContext::empty()).is_err());
    }

    // ── Type mismatch errors ──

    #[test]
    fn eval_add_string_int_error() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::String("x".into()))),
            Op::Add,
            Box::new(Expression::Literal(Value::Int(1))),
        );
        assert!(evaluate(&expr, &EvalContext::empty()).is_err());
    }

    #[test]
    fn eval_compare_different_types_error() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Value::String("x".into()))),
            Op::Gt,
            Box::new(Expression::Literal(Value::Int(1))),
        );
        assert!(evaluate(&expr, &EvalContext::empty()).is_err());
    }

    #[test]
    fn eval_not_on_int_error() {
        let expr = Expression::UnaryOp(
            UnaryOp::Not,
            Box::new(Expression::Literal(Value::Int(1))),
        );
        assert!(evaluate(&expr, &EvalContext::empty()).is_err());
    }
}
