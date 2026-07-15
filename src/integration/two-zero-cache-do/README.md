# Two ZeroCache Durable Objects in one isolate

This is acceptance coverage for routing every embedded zero-cache generation to
the Durable Object instance that owns it. It runs two named `ZeroCacheDO`
instances concurrently under local workerd. The proof checks these live
boundaries for both `alpha` and `bravo`:

- the real zero-cache 1.7 worker boots in each logical DO
- each upstream `SourceDO` reaches the matching embedded PostgreSQL proxy
- initial and live source rows replicate into the matching DO SQLite replica
- the real Zero WebSocket query stream returns only that instance's replicated row

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

The bundled worker is the patched zero-cache 1.7 worker used by production.
The Orez embed lifecycle, DO SQLite shim, `BrowserProxy`/`DoBackend` PostgreSQL
protocol path, Fastify and WebSocket shims, replication handler, and workerd
Durable Objects are all production implementations.
