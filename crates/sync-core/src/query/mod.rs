// the query-aware layer (M4b): Zero v51 AST validation, SQLite compilation, and
// (later slices) durable membership, refcounts, desired-query lifecycle, and
// transformation-version invalidation. host-agnostic and wasm-compilable like
// the rest of sync-core. the query-aware durable schema is created only when a
// host enables the feature (init_query_schema), so the baseline M1 surface is
// untouched.

pub mod ast;
pub mod compile;

pub use ast::{
    Ast, Condition, CorrelatedSubquery, OrderPart, RightVal, Scalar, SimpleOp, ValueRef, parse_ast,
};
pub use compile::{CompiledQuery, compile};
