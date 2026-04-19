//! Criterion benchmarks for the Defacto computation core.
//!
//! Measures the computational ceiling — how fast the Rust engine can go
//! with zero I/O overhead. Compare against Python pytest-benchmark numbers
//! (benchmarks/test_components.py) to isolate PyO3 boundary cost.
//!
//! Substantiates claims:
//!   - S3.1: Worker scaling (normalize + interpret throughput at varying workers)
//!   - S3.2: PyO3 boundary cost (compare these numbers vs Python benchmarks)
//!   - F3.5: Rayon partitions by entity_id (interpret scales with entities)
//!
//! Usage:
//!     cargo bench -p defacto-engine

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use defacto_engine::definitions::CompiledDefinitions;
use defacto_engine::interpreter;
use defacto_engine::normalizer;
use defacto_engine::store::Store;
use defacto_engine::tick;
use defacto_engine::types::{EntityState, NormalizedEvent, Value};

use chrono::{Duration, Utc};
use serde_json::json;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Test definitions — simple customer entity with lead → active state machine
// ---------------------------------------------------------------------------

fn bench_definitions_json() -> serde_json::Value {
    json!({
        "entities": {
            "customer": {
                "starts": "lead",
                "properties": {
                    "email": {"type": "string"},
                    "plan": {"type": "string", "default": "free"},
                    "mrr": {"type": "number", "default": 0}
                },
                "identity": {
                    "email": {"match": "exact"}
                },
                "states": {
                    "lead": {
                        "when": {
                            "signup": {
                                "effects": [
                                    "create",
                                    {"set": {"property": "email", "from": "event.email"}},
                                    {"set": {"property": "mrr", "from": "event.mrr"}}
                                ]
                            },
                            "upgrade": {
                                "effects": [
                                    {"transition": {"to": "active"}},
                                    {"set": {"property": "plan", "from": "event.plan"}}
                                ]
                            }
                        }
                    },
                    "active": {
                        "when": {
                            "upgrade": {
                                "effects": [
                                    {"set": {"property": "plan", "from": "event.plan"}},
                                    {"set": {"property": "mrr", "from": "event.mrr"}}
                                ]
                            }
                        },
                        "time_rules": [{
                            "type": "inactivity",
                            "threshold": "30d",
                            "effects": [{"transition": {"to": "inactive"}}]
                        }]
                    },
                    "inactive": {
                        "when": {
                            "signup": {
                                "effects": [
                                    {"transition": {"to": "active"}},
                                    {"set": {"property": "mrr", "from": "event.mrr"}}
                                ]
                            }
                        }
                    }
                }
            }
        },
        "sources": {
            "bench": {
                "event_type": "type",
                "timestamp": "ts",
                "events": {
                    "signup": {
                        "raw_type": "signup",
                        "mappings": {
                            "email": {"from": "email"},
                            "mrr": {"from": "mrr"}
                        },
                        "hints": {"customer": ["email"]}
                    },
                    "upgrade": {
                        "raw_type": "upgrade",
                        "mappings": {
                            "email": {"from": "email"},
                            "plan": {"from": "plan"},
                            "mrr": {"from": "mrr"}
                        },
                        "hints": {"customer": ["email"]}
                    }
                }
            }
        },
        "schemas": {}
    })
}

fn compile_definitions() -> CompiledDefinitions {
    CompiledDefinitions::compile(&bench_definitions_json()).unwrap()
}

// ---------------------------------------------------------------------------
// Data generators
// ---------------------------------------------------------------------------

fn generate_raw_events(n: usize) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            if i % 3 == 0 {
                json!({
                    "type": "signup",
                    "ts": format!("2024-01-{:02}T{:02}:00:00Z", (i % 28) + 1, i % 24),
                    "email": format!("user{}@test.com", i % (n / 5).max(1)),
                    "mrr": (i % 100) as f64,
                })
            } else {
                json!({
                    "type": "upgrade",
                    "ts": format!("2024-02-{:02}T{:02}:00:00Z", (i % 28) + 1, i % 24),
                    "email": format!("user{}@test.com", i % (n / 5).max(1)),
                    "plan": format!("tier_{}", i % 5),
                    "mrr": ((i % 100) + 50) as f64,
                })
            }
        })
        .collect()
}

fn generate_normalized_events(n: usize) -> Vec<NormalizedEvent> {
    let base = Utc::now() - Duration::days(90);
    (0..n)
        .map(|i| {
            let ts = base + Duration::hours(i as i64);
            let (event_type, data) = if i % 3 == 0 {
                (
                    "signup".to_string(),
                    HashMap::from([
                        ("email".into(), Value::String(format!("user{}@test.com", i % (n / 5).max(1)))),
                        ("mrr".into(), Value::Float((i % 100) as f64)),
                    ]),
                )
            } else {
                (
                    "upgrade".to_string(),
                    HashMap::from([
                        ("email".into(), Value::String(format!("user{}@test.com", i % (n / 5).max(1)))),
                        ("plan".into(), Value::String(format!("tier_{}", i % 5))),
                        ("mrr".into(), Value::Float(((i % 100) + 50) as f64)),
                    ]),
                )
            };

            NormalizedEvent {
                event_id: format!("e_{}", i),
                timestamp: ts,
                source: "bench".into(),
                event_type,
                data,
                raw: "{}".into(),
                resolution_hints: HashMap::new(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn normalize_throughput(c: &mut Criterion) {
    let defs = compile_definitions();
    let source = defs.sources.get("bench").unwrap();

    let mut group = c.benchmark_group("normalize");
    for n in [100, 1000, 5000, 10000] {
        let events = generate_raw_events(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &events, |b, events| {
            b.iter(|| {
                normalizer::normalize(source, &defs.schemas, &defs.entities, events)
                    .unwrap()
            });
        });
    }
    group.finish();
}

fn interpret_throughput(c: &mut Criterion) {
    let defs = compile_definitions();

    let mut group = c.benchmark_group("interpret");
    for n in [100, 500, 1000, 5000] {
        let n_entities = n / 5;
        let events = generate_normalized_events(n);
        let entity_mapping: Vec<(usize, String, String)> = events
            .iter()
            .enumerate()
            .map(|(i, _)| {
                (i, format!("ent_{}", i % n_entities.max(1)), "customer".into())
            })
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let store = Store::new();
                interpreter::interpret(&events, &entity_mapping, &store, &defs)
            });
        });
    }
    group.finish();
}

fn tick_throughput(c: &mut Criterion) {
    let defs = compile_definitions();
    let now = Utc::now();

    let mut group = c.benchmark_group("tick");
    for n in [100, 1000, 5000] {
        // Pre-populate store with entities in 'active' state (has time rules)
        let store = Store::new();
        let old_time = now - Duration::days(60);
        for i in 0..n {
            let mut props = HashMap::new();
            props.insert("email".into(), Value::String(format!("user{}@test.com", i)));
            props.insert("plan".into(), Value::String("pro".into()));
            props.insert("mrr".into(), Value::Float(99.0));

            store.update_entity(EntityState {
                entity_id: format!("ent_{}", i),
                entity_type: "customer".into(),
                state: "active".into(),
                properties: props,
                last_event_time: Some(old_time),
                state_entered_time: Some(old_time),
                created_time: Some(old_time),
            });
        }

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| tick::tick(now, &store, &defs).unwrap());
        });
    }
    group.finish();
}

fn expression_evaluation(c: &mut Criterion) {
    use defacto_engine::expression::parser::parse;
    use defacto_engine::expression::evaluator::{evaluate, EvalContext};

    let mut group = c.benchmark_group("expression");

    // Simple: field access
    let simple_expr = parse("event.email").unwrap();
    let event_data: HashMap<String, Value> = HashMap::from([
        ("email".into(), Value::String("alice@test.com".into())),
        ("mrr".into(), Value::Float(99.0)),
    ]);
    let entity_props: HashMap<String, Value> = HashMap::from([
        ("plan".into(), Value::String("free".into())),
    ]);

    group.bench_function("field_access", |b| {
        let ctx = EvalContext::full(&event_data, &entity_props);
        b.iter(|| evaluate(&simple_expr, &ctx).unwrap());
    });

    // Medium: function call
    let medium_expr = parse("str::to_lowercase(event.email)").unwrap();
    group.bench_function("function_call", |b| {
        let ctx = EvalContext::full(&event_data, &entity_props);
        b.iter(|| evaluate(&medium_expr, &ctx).unwrap());
    });

    // Complex: comparison + arithmetic
    let complex_expr = parse("event.mrr + 10 > 100").unwrap();
    group.bench_function("comparison_arithmetic", |b| {
        let ctx = EvalContext::full(&event_data, &entity_props);
        b.iter(|| evaluate(&complex_expr, &ctx).unwrap());
    });

    group.finish();
}

criterion_group!(
    benches,
    normalize_throughput,
    interpret_throughput,
    tick_throughput,
    expression_evaluation,
);
criterion_main!(benches);
