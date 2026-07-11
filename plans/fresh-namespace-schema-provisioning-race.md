# Fresh namespace schema-provisioning race

## Symptom

Fresh production namespaces intermittently return HTTP 500 while startup code
runs `SELECT 1 FROM file ...` before the application schema has created or
migrated the `file` table. The 2026-07-11 production wave observed this twice;
both instances self-cleared after provisioning completed.

## Required fix

- Make schema provisioning and the first application query one ordered startup
  barrier for each namespace/DO instance.
- Do not treat `no such table` as an empty query result or hide arbitrary SQL
  errors. Queries should wait for the migration barrier, and migration failure
  should remain a visible startup error.
- Add a cold-namespace concurrency test that starts migration and a `file`
  existence query together, then proves the query cannot execute before the
  table is available.
- Validate both `/__soot_pg` and `/__soot_query`, including DO eviction and a
  newly allocated namespace.

## Scope note

This race is separate from the Rust poke row-type encoder bug. It belongs in
the namespace provisioning/query startup path and should land independently.
