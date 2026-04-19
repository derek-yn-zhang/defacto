//! Expression language — guards, conditions, computed properties, normalizations.
//!
//! All dynamic logic in Defacto definitions uses the same expression language:
//! guards on handlers, conditions on effects, computed property values, and
//! source field normalization. Expressions are parsed once at definition
//! compilation time and stored as ASTs for fast evaluation during interpretation.
//!
//! The expression language supports field access (`event.email`, `entity.mrr`),
//! arithmetic (`entity.mrr * 12`), comparison (`event.amount > 0`),
//! logical operators (`and`, `or`, `not`), null checks, and built-in
//! functions (`str::to_lowercase`, `math::abs`, etc.).

pub mod evaluator;
pub mod functions;
pub mod parser;

use serde::{Deserialize, Serialize};

use crate::types::{Op, UnaryOp, Value};

/// A parsed expression ready for evaluation.
///
/// Expressions are compiled from strings at definition registration time
/// using chumsky. The same Expression enum is used for guards, conditions,
/// computes, and normalizations — the difference is the evaluation context
/// (what variables are available: `event.*`, `entity.*`, or `value`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    /// A literal value: `42`, `"hello"`, `true`, `null`, `30d`.
    Literal(Value),

    /// Access a field via dot-separated path.
    /// `event.email` → `["event", "email"]`
    /// `event.user.address.city` → `["event", "user", "address", "city"]`
    /// `event.items.0.product_id` → `["event", "items", "0", "product_id"]` (numeric = array index)
    /// `value` → `["value"]` (bare variable for identity normalize)
    FieldAccess(Vec<String>),

    /// Binary operation: `left op right`.
    /// Arithmetic: `entity.mrr * 12`, `event.timestamp + 30d`
    /// Comparison: `event.amount > 0`
    /// Logical: `event.a > 0 and event.b < 100`
    /// String concatenation: `event.first_name + " " + event.last_name`
    BinaryOp(Box<Expression>, Op, Box<Expression>),

    /// Unary operation: `op operand`.
    /// `not entity.active`, `-event.delta`
    UnaryOp(UnaryOp, Box<Expression>),

    /// Built-in function call: `function_name(args...)`.
    /// `str::to_lowercase(event.email)`, `days_since(entity.signup_date)`,
    /// `coalesce(event.nickname, event.name, "unknown")`
    FunctionCall(String, Vec<Expression>),

    /// Conditional expression: `if condition then value_if_true else value_if_false`.
    /// Both branches are required. Returns the value of the matching branch.
    If(Box<Expression>, Box<Expression>, Box<Expression>),

    /// Index into an expression result: `split(event.email, "@")[1]`, `event.items[0]`.
    /// Evaluates the base expression, then indexes by the second expression.
    /// Supports negative indices: `[-1]` for last element.
    /// Out of bounds returns Null.
    Index(Box<Expression>, Box<Expression>),
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── AST construction tests (verify types and derives work) ──

    #[test]
    fn expression_serde_roundtrip() {
        let expr = Expression::BinaryOp(
            Box::new(Expression::FieldAccess(vec!["event".into(), "amount".into()])),
            Op::Gt,
            Box::new(Expression::Literal(Value::Int(0))),
        );
        let json = serde_json::to_string(&expr).unwrap();
        let back: Expression = serde_json::from_str(&json).unwrap();
        assert_eq!(expr, back);
    }

    // ── Integration tests: parse + evaluate real expressions end to end ──
    //
    // These test expressions from the docs/definitions.md examples,
    // verifying that the parser and evaluator work together correctly.

    use std::collections::HashMap;

    /// Parse an expression and evaluate it against a context. Panics on failure.
    fn eval(input: &str, ctx: &evaluator::EvalContext<'_>) -> Value {
        let expr = parser::parse(input).unwrap_or_else(|e| panic!("parse failed for '{}': {}", input, e));
        evaluator::evaluate(&expr, ctx).unwrap_or_else(|e| panic!("eval failed for '{}': {}", input, e))
    }

    /// Standard test data: event with common fields and entity with state.
    fn test_data() -> (HashMap<String, Value>, HashMap<String, Value>) {
        let event = HashMap::from([
            ("amount".into(), Value::Int(150)),
            ("plan".into(), Value::String("pro".into())),
            ("email".into(), Value::String("ALICE@TEST.COM".into())),
            ("first_name".into(), Value::String("Alice".into())),
            ("last_name".into(), Value::String("Smith".into())),
            ("nickname".into(), Value::Null),
        ]);
        let entity = HashMap::from([
            ("mrr".into(), Value::Float(99.0)),
            ("plan".into(), Value::String("free".into())),
            ("active".into(), Value::Bool(true)),
        ]);
        (event, entity)
    }

    #[test]
    fn integration_simple_guard() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(eval("event.amount > 0", &ctx), Value::Bool(true));
    }

    #[test]
    fn integration_plan_comparison() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(eval("event.plan != entity.plan", &ctx), Value::Bool(true));
    }

    #[test]
    fn integration_computed_property() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(eval("entity.mrr * 12", &ctx), Value::Float(1188.0));
    }

    #[test]
    fn integration_string_normalization() {
        let ctx = evaluator::EvalContext::value_only(Value::String("  ALICE@TEST.COM  ".into()));
        assert_eq!(
            eval("str::to_lowercase(str::trim(value))", &ctx),
            Value::String("alice@test.com".into())
        );
    }

    #[test]
    fn integration_coalesce_with_null() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("coalesce(event.nickname, event.first_name, 'unknown')", &ctx),
            Value::String("Alice".into())
        );
    }

    #[test]
    fn integration_conditional_compute() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("if event.amount > 100 then 'high' else 'low'", &ctx),
            Value::String("high".into())
        );
    }

    #[test]
    fn integration_string_concat() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("event.first_name + ' ' + event.last_name", &ctx),
            Value::String("Alice Smith".into())
        );
    }

    #[test]
    fn integration_null_check_guard() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(eval("event.email != null", &ctx), Value::Bool(true));
        assert_eq!(eval("event.nickname == null", &ctx), Value::Bool(true));
    }

    #[test]
    fn integration_multi_condition_guard() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("event.amount > 0 and entity.active", &ctx),
            Value::Bool(true)
        );
    }

    #[test]
    fn integration_nested_function_with_coalesce() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("str::to_lowercase(coalesce(event.nickname, event.first_name, 'unknown'))", &ctx),
            Value::String("alice".into())
        );
    }

    #[test]
    fn integration_complex_arithmetic() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("(entity.mrr * 12 + 100) / 2", &ctx),
            Value::Float(644.0)
        );
    }

    #[test]
    fn integration_missing_field_coalesce() {
        let (event, entity) = test_data();
        let ctx = evaluator::EvalContext::full(&event, &entity);
        assert_eq!(
            eval("coalesce(event.nonexistent, 'default')", &ctx),
            Value::String("default".into())
        );
    }
}
