//! Defacto computation engine — the Rust core.
//!
//! This crate contains all computation logic: normalization, expression
//! evaluation, interpretation (state machine evaluation), entity state
//! management, and time rule evaluation. It has no I/O dependencies —
//! all database and Kafka operations happen in Python.
//!
//! The crate is used by `defacto-python` (the PyO3 bindings) which exposes
//! `DefactoCore` as a Python class. It can also be tested standalone via
//! `cargo test -p defacto-engine --lib`.

pub mod definitions;
pub mod error;
pub mod expression;
pub mod interpreter;
pub mod normalizer;
pub mod store;
pub mod tick;
pub mod types;
