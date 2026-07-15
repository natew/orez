# Two ZeroCache Durable Objects in one isolate

This is acceptance coverage for routing every embedded zero-cache generation to
the Durable Object instance that owns it. It runs two named `ZeroCacheDO`
instances concurrently under local workerd. A synchronized runner then checks
four live boundaries for both `alpha` and `bravo`:

- DO SQLite storage
- the PostgreSQL wire proxy and its upstream `SourceDO`
- Fastify request dispatch
- replication-handler health state and the replica's `_zero.replicationState` table

Run it from this directory:

```sh
bun install --frozen-lockfile
bun run proof
```

At Orez `0150c9bab428136023a05c62aed9caf95d112e2c`, the command must fail with:

```text
zero-cache CF embed: another generation is active or still tearing down
```

Removing that guard alone is insufficient. The same proof then reaches both
instances and fails when workerd catches one instance using the other object's
SQLite I/O context. The instance-routing fix is accepted only when the command
returns two independent, exact namespace reports and exits successfully.

The fixture replaces only zero-cache's top-level worker orchestration with the
synchronized probe runner. The Orez embed lifecycle, DO SQLite shim,
`BrowserProxy`/`DoBackend` PostgreSQL protocol path, Fastify shim, replication
state module, and workerd Durable Objects are the production implementations.
