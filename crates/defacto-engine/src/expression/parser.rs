//! Expression parser — converts expression strings into ASTs using chumsky.
//!
//! Called once per expression at definition compilation time. The parsed
//! Expression is stored and reused for every evaluation — parsing cost
//! is paid once, not per event.
//!
//! Uses chumsky 0.10 with pratt parsing for operator precedence. Direct
//! char parsing (no separate lexer) since expressions are short one-liners
//! from YAML definitions.

use std::time::Duration;

use chumsky::prelude::*;
use chumsky::pratt::*;

use crate::error::DefactoError;
use crate::expression::Expression;
use crate::types::{Op, UnaryOp, Value};

/// Parse an expression string into an AST.
///
/// Examples:
///   `"event.amount > 0"`              → `BinaryOp(FieldAccess, Gt, Literal)`
///   `"str::to_lowercase(event.email)"` → `FunctionCall("str::to_lowercase", [FieldAccess])`
///   `"if event.amount > 100 then 'high' else 'low'"` → `If(...)`
///
/// Returns `DefactoError::Definition` if the expression is syntactically invalid,
/// with a message indicating the position and what was expected.
pub fn parse(input: &str) -> Result<Expression, DefactoError> {
    expression_parser()
        .parse(input)
        .into_result()
        .map_err(|errors| format_errors(&errors, input))
}

// ---------------------------------------------------------------------------
// Keyword helpers
// ---------------------------------------------------------------------------

/// Word boundary — succeeds if the next char is NOT alphanumeric/underscore,
/// or we're at end of input. Used by `kw()` to prevent matching "android" as "and".
///
/// **Why we don't use `text::ascii::keyword()`:**
///
/// chumsky 0.10's `keyword()` captures internal state that conflicts with the
/// `recursive()` combinator's lifetime bounds. Specifically, `recursive()` requires
/// the parser returned from its closure to satisfy `'b: 'static`, but `keyword()`
/// produces a type that borrows the `'src` lifetime, making the two incompatible.
/// This is a known limitation in chumsky 0.10 — the examples on the main branch
/// (0.13+) work because the API was updated. Our `kw()` function replicates the
/// same word boundary behavior: `just(word)` + "next char is not identifier-like".
/// If we upgrade to a chumsky version where `keyword()` works with `recursive()`,
/// this workaround can be replaced with `text::ascii::keyword()` directly.
fn word_boundary<'src>(
) -> impl Parser<'src, &'src str, (), extra::Err<Rich<'src, char>>> + Clone {
    any()
        .filter(|c: &char| c.is_alphanumeric() || *c == '_')
        .not()
        .rewind()
        .or(end())
}

/// Match a keyword: the exact string followed by a word boundary.
/// `kw("and")` matches "and" but not "android".
fn kw<'src>(
    word: &'static str,
) -> impl Parser<'src, &'src str, &'src str, extra::Err<Rich<'src, char>>> + Clone {
    just(word).then_ignore(word_boundary()).to_slice()
}

// ---------------------------------------------------------------------------
// Parser construction
// ---------------------------------------------------------------------------

/// Build the expression parser.
///
/// Separated from `parse()` because chumsky's `recursive()` requires the parser
/// to be constructed independently of any input reference.
fn expression_parser<'src>(
) -> impl Parser<'src, &'src str, Expression, extra::Err<Rich<'src, char>>> {
    recursive(|expr| {
        // ── Escape sequences for string literals ──
        let escape = just('\\').ignore_then(choice((
            just('\\'),
            just('"'),
            just('\''),
            just('n').to('\n'),
            just('r').to('\r'),
            just('t').to('\t'),
        )));

        // ── String literals — double and single quoted ──
        let double_string = none_of("\\\"")
            .or(escape.clone())
            .repeated()
            .collect::<String>()
            .delimited_by(just('"'), just('"'));

        let single_string = none_of("\\'")
            .or(escape)
            .repeated()
            .collect::<String>()
            .delimited_by(just('\''), just('\''));

        let string = double_string
            .or(single_string)
            .map(|s| Expression::Literal(Value::String(s)))
            .labelled("string");

        // ── Numeric literals ──
        let digits = text::digits::<_, extra::Err<Rich<char>>>(10).to_slice();

        // Duration: 30d, 24h, 90m, 45s — tried before plain numbers so "30d"
        // parses as Duration, not Int(30) followed by error on "d".
        let duration = digits
            .clone()
            .then(one_of("dhms"))
            .map(|(n_str, unit): (&str, char)| {
                // Safe to unwrap: the digits parser guarantees only valid numeric chars
                let n: u64 = n_str.parse().unwrap();
                let secs = match unit {
                    'd' => n * 86400,
                    'h' => n * 3600,
                    'm' => n * 60,
                    's' => n,
                    _ => unreachable!(),
                };
                Expression::Literal(Value::Duration(Duration::from_secs(secs)))
            })
            .labelled("duration");

        // Float: digits.digits — tried before int so "3.14" doesn't parse as Int(3).
        let float = digits
            .clone()
            .then_ignore(just('.'))
            .then(digits.clone())
            .to_slice()
            .from_str::<f64>()
            .unwrapped()
            .map(|f| Expression::Literal(Value::Float(f)))
            .labelled("float");

        // Integer
        let int = digits
            .from_str::<i64>()
            .unwrapped()
            .map(|n| Expression::Literal(Value::Int(n)))
            .labelled("integer");

        // ── Boolean and null keywords ──
        let bool_null = choice((
            kw("true").to(Expression::Literal(Value::Bool(true))),
            kw("false").to(Expression::Literal(Value::Bool(false))),
            kw("null").to(Expression::Literal(Value::Null)),
        ));

        // ── Identifiers — reject keywords so "and" doesn't parse as a field name ──
        let raw_ident = text::ascii::ident().to_slice();

        let ident = raw_ident.try_map(|s: &str, span| match s {
            "and" | "or" | "not" | "if" | "then" | "else" | "true" | "false" | "null" => Err(
                Rich::custom(span, format!("'{}' is a reserved keyword", s)),
            ),
            _ => Ok(s),
        });

        // ── Function call: name(args) or namespace::name(args) ──
        // Tried before field_access because both start with an identifier,
        // but function calls are followed by "(" (possibly after "::name").
        let func_name = ident
            .clone()
            .then(just("::").ignore_then(ident.clone()).or_not())
            .map(|(name, sub): (&str, Option<&str>)| match sub {
                Some(s) => format!("{}::{}", name, s),
                None => name.to_string(),
            });

        let args = expr
            .clone()
            .separated_by(just(',').padded())
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(just('(').padded(), just(')').padded());

        let func_call = func_name
            .then(args)
            .map(|(name, args)| Expression::FunctionCall(name, args))
            .labelled("function call");

        // ── Field access: ident(.ident)* ──
        // "value" → ["value"], "event.email" → ["event", "email"],
        // "event.user.address" → ["event", "user", "address"]
        // Numeric segments (e.g., "items.0") are valid for array indexing.
        let field_access = ident
            .clone()
            .then(
                just('.')
                    .ignore_then(raw_ident)
                    .repeated()
                    .collect::<Vec<_>>(),
            )
            .map(|(first, rest): (&str, Vec<&str>)| {
                let mut parts = vec![first.to_string()];
                parts.extend(rest.into_iter().map(String::from));
                Expression::FieldAccess(parts)
            })
            .labelled("field access");

        // ── If-then-else ──
        let if_expr = kw("if")
            .padded()
            .ignore_then(expr.clone())
            .then_ignore(kw("then").padded())
            .then(expr.clone())
            .then_ignore(kw("else").padded())
            .then(expr.clone())
            .map(|((cond, then_branch), else_branch)| {
                Expression::If(
                    Box::new(cond),
                    Box::new(then_branch),
                    Box::new(else_branch),
                )
            })
            .labelled("if-then-else");

        // ── Parenthesized expression ──
        let parens = expr
            .clone()
            .delimited_by(just('(').padded(), just(')').padded());

        // ── Atom — primary expression before operator application ──
        // Order: more specific parsers first to avoid ambiguity.
        // - if_expr before ident (both could start with a letter)
        // - bool_null before ident (true/false/null are keywords)
        // - duration before float/int (30d vs 30)
        // - float before int (3.14 vs 3)
        // - func_call before field_access (both start with ident, but call has parens)
        let base_atom = choice((
            if_expr,
            bool_null,
            duration,
            float,
            int,
            string,
            func_call,
            field_access,
            parens,
        ))
        .padded()
        .labelled("expression");

        // ── Bracket indexing: expr[n] ──
        // Parsed as a postfix on any atom. Supports chaining: expr[0][1].
        // Binds tighter than all binary operators.
        let atom = base_atom
            .then(
                expr.clone()
                    .delimited_by(just('[').padded(), just(']').padded())
                    .repeated()
                    .collect::<Vec<_>>(),
            )
            .map(|(base, indices)| {
                indices.into_iter().fold(base, |acc, idx| {
                    Expression::Index(Box::new(acc), Box::new(idx))
                })
            })
            .boxed(); // boxed to simplify the type for pratt + recursive

        // ── Operators via pratt parsing ──
        //
        // All precedence and associativity in one place. Higher binding
        // power = tighter binding. The pratt combinator handles left/right
        // associativity and prefix operators naturally.
        //
        // Level 7 (tightest): unary not/! -
        // Level 6: ** (right-associative)
        // Level 5: * /
        // Level 4: + - (also string concat and datetime arithmetic)
        // Level 3: == != > < >= <=
        // Level 2: and &&
        // Level 1 (loosest): or ||

        atom.pratt((
            // Unary prefix (highest precedence)
            prefix(7, kw("not").padded(), |_, rhs, _: &mut _| {
                Expression::UnaryOp(UnaryOp::Not, Box::new(rhs))
            }),
            prefix(7, just('!').padded(), |_, rhs, _: &mut _| {
                Expression::UnaryOp(UnaryOp::Not, Box::new(rhs))
            }),
            prefix(7, just('-').padded(), |_, rhs, _: &mut _| {
                Expression::UnaryOp(UnaryOp::Negate, Box::new(rhs))
            }),
            // Power (right-associative)
            infix(right(6), just("**").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Pow, Box::new(r))
            }),
            // Multiplicative
            infix(left(5), just('*').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Mul, Box::new(r))
            }),
            infix(left(5), just('/').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Div, Box::new(r))
            }),
            // Additive (also string concatenation and datetime arithmetic)
            infix(left(4), just('+').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Add, Box::new(r))
            }),
            infix(left(4), just('-').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Sub, Box::new(r))
            }),
            // Comparison (multi-char operators first to avoid ">=" matching as ">")
            infix(left(3), just(">=").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Gte, Box::new(r))
            }),
            infix(left(3), just("<=").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Lte, Box::new(r))
            }),
            infix(left(3), just("!=").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Neq, Box::new(r))
            }),
            infix(left(3), just("==").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Eq, Box::new(r))
            }),
            infix(left(3), just('>').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Gt, Box::new(r))
            }),
            infix(left(3), just('<').padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Lt, Box::new(r))
            }),
            // Logical AND
            infix(left(2), kw("and").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::And, Box::new(r))
            }),
            infix(left(2), just("&&").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::And, Box::new(r))
            }),
            // Logical OR (lowest precedence)
            infix(left(1), kw("or").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Or, Box::new(r))
            }),
            infix(left(1), just("||").padded(), |l, _, r, _: &mut _| {
                Expression::BinaryOp(Box::new(l), Op::Or, Box::new(r))
            }),
        ))
        .boxed() // boxed so recursive's 'b lifetime is satisfied via type erasure
    })
    .then_ignore(end())
}

// ---------------------------------------------------------------------------
// Error formatting
// ---------------------------------------------------------------------------

/// Format chumsky Rich errors into a human-readable DefactoError.
///
/// Each error includes the position in the input, what was found vs expected,
/// and the original expression for context.
fn format_errors(errors: &[Rich<'_, char>], input: &str) -> DefactoError {
    let messages: Vec<String> = errors
        .iter()
        .map(|e| {
            let span = e.span();
            let found = e
                .found()
                .map(|c| format!("'{}'", c))
                .unwrap_or_else(|| "end of expression".to_string());

            let expected: Vec<String> = e.expected().map(|exp| format!("{}", exp)).collect();

            let expected_str = if expected.is_empty() {
                "something else".to_string()
            } else {
                expected.join(", ")
            };

            format!(
                "at position {}: found {}, expected {}",
                span.start, found, expected_str
            )
        })
        .collect();

    DefactoError::Definition(format!(
        "Parse error in '{}': {}",
        input,
        messages.join("; ")
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Literal parsing ──

    #[test]
    fn parse_int() {
        assert_eq!(parse("42").unwrap(), Expression::Literal(Value::Int(42)));
    }

    #[test]
    fn parse_int_zero() {
        assert_eq!(parse("0").unwrap(), Expression::Literal(Value::Int(0)));
    }

    #[test]
    fn parse_float() {
        assert_eq!(
            parse("3.14").unwrap(),
            Expression::Literal(Value::Float(3.14))
        );
    }

    #[test]
    fn parse_string_double() {
        assert_eq!(
            parse("\"hello\"").unwrap(),
            Expression::Literal(Value::String("hello".into()))
        );
    }

    #[test]
    fn parse_string_single() {
        assert_eq!(
            parse("'world'").unwrap(),
            Expression::Literal(Value::String("world".into()))
        );
    }

    #[test]
    fn parse_string_with_escape() {
        assert_eq!(
            parse(r#""he\"llo""#).unwrap(),
            Expression::Literal(Value::String("he\"llo".into()))
        );
    }

    #[test]
    fn parse_true() {
        assert_eq!(
            parse("true").unwrap(),
            Expression::Literal(Value::Bool(true))
        );
    }

    #[test]
    fn parse_false() {
        assert_eq!(
            parse("false").unwrap(),
            Expression::Literal(Value::Bool(false))
        );
    }

    #[test]
    fn parse_null() {
        assert_eq!(parse("null").unwrap(), Expression::Literal(Value::Null));
    }

    #[test]
    fn parse_duration_days() {
        assert_eq!(
            parse("30d").unwrap(),
            Expression::Literal(Value::Duration(Duration::from_secs(30 * 86400)))
        );
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(
            parse("24h").unwrap(),
            Expression::Literal(Value::Duration(Duration::from_secs(24 * 3600)))
        );
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(
            parse("90m").unwrap(),
            Expression::Literal(Value::Duration(Duration::from_secs(90 * 60)))
        );
    }

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(
            parse("45s").unwrap(),
            Expression::Literal(Value::Duration(Duration::from_secs(45)))
        );
    }

    // ── Field access ──

    #[test]
    fn parse_bare_variable() {
        assert_eq!(
            parse("value").unwrap(),
            Expression::FieldAccess(vec!["value".into()])
        );
    }

    #[test]
    fn parse_event_field() {
        assert_eq!(
            parse("event.email").unwrap(),
            Expression::FieldAccess(vec!["event".into(), "email".into()])
        );
    }

    #[test]
    fn parse_nested_field() {
        assert_eq!(
            parse("event.user.address.city").unwrap(),
            Expression::FieldAccess(vec![
                "event".into(),
                "user".into(),
                "address".into(),
                "city".into(),
            ])
        );
    }

    #[test]
    fn parse_entity_field() {
        assert_eq!(
            parse("entity.mrr").unwrap(),
            Expression::FieldAccess(vec!["entity".into(), "mrr".into()])
        );
    }

    // ── Bracket indexing ──

    #[test]
    fn parse_field_index() {
        let result = parse("event.items[0]").unwrap();
        assert!(matches!(result, Expression::Index(_, _)));
        if let Expression::Index(base, idx) = result {
            assert!(matches!(*base, Expression::FieldAccess(_)));
            assert_eq!(*idx, Expression::Literal(Value::Int(0)));
        }
    }

    #[test]
    fn parse_function_index() {
        let result = parse("split(event.email, \"@\")[1]").unwrap();
        assert!(matches!(result, Expression::Index(_, _)));
        if let Expression::Index(base, idx) = result {
            assert!(matches!(*base, Expression::FunctionCall(_, _)));
            assert_eq!(*idx, Expression::Literal(Value::Int(1)));
        }
    }

    #[test]
    fn parse_negative_index() {
        let result = parse("event.items[-1]").unwrap();
        if let Expression::Index(_, idx) = result {
            assert_eq!(*idx, Expression::UnaryOp(UnaryOp::Negate, Box::new(Expression::Literal(Value::Int(1)))));
        }
    }

    #[test]
    fn parse_chained_index() {
        let result = parse("event.data[0][1]").unwrap();
        // Should be Index(Index(FieldAccess, 0), 1)
        assert!(matches!(result, Expression::Index(_, _)));
        if let Expression::Index(inner, _) = result {
            assert!(matches!(*inner, Expression::Index(_, _)));
        }
    }

    // ── Operators and precedence ──

    #[test]
    fn parse_comparison_gt() {
        let result = parse("event.amount > 0").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::Gt, _)));
    }

    #[test]
    fn parse_equality() {
        let result = parse("event.plan != entity.plan").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::Neq, _)));
    }

    #[test]
    fn parse_arithmetic() {
        let result = parse("entity.mrr * 12").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::Mul, _)));
    }

    #[test]
    fn parse_precedence_mul_over_add() {
        // a + b * c → Add(a, Mul(b, c))
        let result = parse("entity.a + entity.b * entity.c").unwrap();
        match result {
            Expression::BinaryOp(_, Op::Add, rhs) => {
                assert!(matches!(*rhs, Expression::BinaryOp(_, Op::Mul, _)));
            }
            other => panic!("expected Add at top level, got {:?}", other),
        }
    }

    #[test]
    fn parse_precedence_and_over_or() {
        // a or b and c → Or(a, And(b, c))
        let result = parse("entity.a or entity.b and entity.c").unwrap();
        match result {
            Expression::BinaryOp(_, Op::Or, rhs) => {
                assert!(matches!(*rhs, Expression::BinaryOp(_, Op::And, _)));
            }
            other => panic!("expected Or at top level, got {:?}", other),
        }
    }

    #[test]
    fn parse_power_right_associative() {
        // 2 ** 3 ** 2 → Pow(2, Pow(3, 2))
        let result = parse("2 ** 3 ** 2").unwrap();
        match result {
            Expression::BinaryOp(_, Op::Pow, rhs) => {
                assert!(matches!(*rhs, Expression::BinaryOp(_, Op::Pow, _)));
            }
            other => panic!("expected Pow at top level, got {:?}", other),
        }
    }

    #[test]
    fn parse_unary_not() {
        let result = parse("not entity.active").unwrap();
        assert!(matches!(result, Expression::UnaryOp(UnaryOp::Not, _)));
    }

    #[test]
    fn parse_unary_negate() {
        let result = parse("-event.delta").unwrap();
        assert!(matches!(result, Expression::UnaryOp(UnaryOp::Negate, _)));
    }

    #[test]
    fn parse_parentheses() {
        // (a + b) * c → Mul(Add(a, b), c)
        let result = parse("(entity.a + entity.b) * entity.c").unwrap();
        match result {
            Expression::BinaryOp(lhs, Op::Mul, _) => {
                assert!(matches!(*lhs, Expression::BinaryOp(_, Op::Add, _)));
            }
            other => panic!("expected Mul at top level, got {:?}", other),
        }
    }

    #[test]
    fn parse_null_check() {
        let result = parse("event.email != null").unwrap();
        match result {
            Expression::BinaryOp(_, Op::Neq, rhs) => {
                assert_eq!(*rhs, Expression::Literal(Value::Null));
            }
            other => panic!("expected Neq with Null, got {:?}", other),
        }
    }

    #[test]
    fn parse_string_concat() {
        let result = parse("event.first + \" \" + event.last").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::Add, _)));
    }

    #[test]
    fn parse_logical_and_keyword() {
        let result = parse("event.a > 0 and event.b < 10").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::And, _)));
    }

    #[test]
    fn parse_logical_or_symbol() {
        let result = parse("event.a > 0 || event.b > 0").unwrap();
        assert!(matches!(result, Expression::BinaryOp(_, Op::Or, _)));
    }

    // ── Function calls ──

    #[test]
    fn parse_simple_function() {
        match parse("len(event.name)").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "len");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_namespaced_function() {
        match parse("str::to_lowercase(event.email)").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "str::to_lowercase");
                assert_eq!(args.len(), 1);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_zero_arg_function() {
        match parse("now()").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "now");
                assert_eq!(args.len(), 0);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_multi_arg_function() {
        match parse("str::substring(event.phone, 0, 3)").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "str::substring");
                assert_eq!(args.len(), 3);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_nested_function() {
        match parse("str::to_lowercase(str::trim(value))").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "str::to_lowercase");
                assert!(matches!(&args[0], Expression::FunctionCall(..)));
            }
            other => panic!("expected nested FunctionCall, got {:?}", other),
        }
    }

    #[test]
    fn parse_variadic_function() {
        match parse("coalesce(event.a, event.b, 'default')").unwrap() {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "coalesce");
                assert_eq!(args.len(), 3);
            }
            other => panic!("expected FunctionCall, got {:?}", other),
        }
    }

    // ── If-then-else ──

    #[test]
    fn parse_if_simple() {
        assert!(matches!(
            parse("if true then 1 else 2").unwrap(),
            Expression::If(..)
        ));
    }

    #[test]
    fn parse_if_complex() {
        match parse("if event.amount > 100 then 'high' else 'low'").unwrap() {
            Expression::If(cond, then_branch, else_branch) => {
                assert!(matches!(*cond, Expression::BinaryOp(_, Op::Gt, _)));
                assert_eq!(
                    *then_branch,
                    Expression::Literal(Value::String("high".into()))
                );
                assert_eq!(
                    *else_branch,
                    Expression::Literal(Value::String("low".into()))
                );
            }
            other => panic!("expected If, got {:?}", other),
        }
    }

    // ── Error cases ──

    #[test]
    fn parse_error_empty_input() {
        assert!(parse("").is_err());
    }

    #[test]
    fn parse_error_trailing_operator() {
        assert!(parse("event.amount >").is_err());
    }

    #[test]
    fn parse_error_unclosed_string() {
        assert!(parse("\"hello").is_err());
    }

    #[test]
    fn parse_error_unclosed_paren() {
        assert!(parse("(event.amount + 1").is_err());
    }

    #[test]
    fn parse_error_if_missing_else() {
        assert!(parse("if true then 1").is_err());
    }
}
