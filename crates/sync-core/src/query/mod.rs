// the query-aware layer (M4b): Zero v51 AST validation, SQLite compilation, and
// (later slices) durable membership, refcounts, desired-query lifecycle, and
// transformation-version invalidation. host-agnostic and wasm-compilable like
// the rest of sync-core. the query-aware durable schema is created only when a
// host enables the feature (init_query_schema), so the baseline M1 surface is
// untouched.

pub mod ast;
pub mod compile;
pub mod membership;
pub mod opacity;
pub mod qpull;
pub mod transaction;

pub use ast::{
    Ast, Condition, CorrelatedSubquery, OrderPart, RightVal, Scalar, SimpleOp, ValueRef,
    collect_dependency_tables, parse_ast,
};
pub use compile::{CompiledQuery, compile};
pub use membership::{
    clear_desires, init_query_schema, recompute_group, register_query, remove_desire, set_desire,
};
pub use opacity::validate_encrypted_column_usage;
pub use qpull::handle_query_pull;
pub use transaction::{
    CompiledQueryNode, CompiledQueryPlan, CompiledRelationship, QueryBinding, QueryColumn,
    QueryFormat, QuerySchema, compile_transaction_query, parse_query_format, parse_query_schema,
};
