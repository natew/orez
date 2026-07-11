# Differential regression seeds

Proptest writes minimized failing seeds to `differential.txt` in this directory.
Commit that generated file when a failure becomes a permanent regression case;
it is replayed automatically before newly generated cases.

Use `PROPTEST_RNG_SEED=<seed> PROPTEST_CASES=1 cargo test -p sync-core --test
differential rust_matches_the_ts_reference_core_on_generated_traces` to replay
an original generated stream from a seed printed by proptest.
