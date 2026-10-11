# env registry generation

Owner direction, 2026-10-11, relayed by coordinator: "Why not move to the orez repo ... That was the plan". The generator belongs in `packages/scripts`; repair it upstream without changing credentials or deployed secrets.

Regenerating GitHub workflow markers must preserve each existing non-version entry's explicit value, including a renamed source key bound to an unchanged deployed secret name. Each marker block owns its bindings. New keys use `secrets.<key>`, removed keys disappear, and dependency versions remain generated literals.

The package's CLI regression runs the generator against a temporary workflow with separate production and staging bindings, an unchanged key, a new key, a removed key, and a stale version secret. It compares the entire generated workflow and verifies a second run is unchanged. The package test participates in the root test command.

Binding lookup scans each generated block once and uses a map for row generation. This adds no network calls, credential reads, or writes beyond existing generated files.
