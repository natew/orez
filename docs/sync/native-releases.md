# Native npm releases

`orez-sync-native` ships the generic standalone `sync-native` host through npm
without a `postinstall` script. Applications keep authentication, named-query
policy, migrations, and pushes in their JavaScript server. The Rust process owns
SQLite replicas, pull, change tracking, and wake delivery.

The npm launcher has one optional dependency for each supported operating
system, CPU, and Linux libc combination. npm installs only the matching binary
package.

The native package uses its Cargo version, independently from the `orez` npm
version. Ordinary `orez` releases reuse the published native version and never
wait for Rust builds.

## Standalone contract

The supervisor starts the binary with file-backed schema and migration inputs:

```sh
sync-native serve \
  --schema zero-schema.json \
  --init-sql init-sql.json \
  --data-dir .orez/sync-native \
  --host 127.0.0.1 \
  --port 5048 \
  --admin-token-env SYNC_NATIVE_ADMIN_TOKEN \
  --auth-url http://127.0.0.1:3000/api/zero/auth \
  --wake-authorize-url http://127.0.0.1:3000/api/zero/wake-authorize \
  --query-transform-url http://127.0.0.1:3000/api/zero/pull
```

Supervisors serving browser clients also pass `--allow-origin <origin>`, once
per public application origin. The value is an exact URL origin such as
`https://example.com` or `http://localhost:3000`, with no path or trailing
slash. Requests that contain an `Origin` header are denied unless it matches a
configured value. Native requests without `Origin` remain allowed, while admin
routes reject every request that contains `Origin`.

`zero-schema.json` is the ordinary Zero schema object. `init-sql.json` is an
ordered JSON array of idempotent SQL strings. Orez hashes that array and stores
the hash in each namespace. It applies the array when the hash changes, inside
the same transaction and before installing or updating its internal schema and
triggers. A namespace already on the current hash performs one metadata lookup
and skips the application initializer. This ordering lets an existing namespace
reach the application shape referenced by the current Zero schema before Orez
uses that shape. Application-owned data migrations can also run through the
serialized `/<namespace>/admin/sql` transaction before the supervisor reports
ready. Supervisors discover those namespaces through
`GET /admin/namespaces` with the process `x-admin-key`; its response is
`{ "namespaces": [...] }` in lexical order. The on-disk filename layout is not
an application contract.

The standalone process creates and resets `--data-dir` to owner-only mode
(`0700`) on Unix. On Windows, choose a user-private application directory whose
ACL is inherited by the namespace databases.

All callback URLs must be explicit-port `http://` URLs on `localhost`,
`127.0.0.1`, or `[::1]`; redirects are disabled. Orez strips an inbound
`x-admin-key`, attaches the process-owned key, and uses these contracts:

- Auth: POST `{ "namespace": "..." }` while forwarding the original request
  headers. The application must authorize both the session and that exact
  namespace before returning claims; HTTP 401 or 403 rejects the request before
  Orez creates or opens its replica. HTTP 200 returns a NormalizedClaims object
  with a non-empty string `userID`; other claims are preserved.
- Query transform: POST the standard Zero body
  `["transform", [{ "id", "name", "args" }]]`, preserving request order and
  forwarding the original auth headers. Orez also attaches trusted
  `x-orez-namespace` and `x-orez-user-id` headers. The response is
  `{ "queryTransformVersion", "queries": [...] }`; each query result contains
  the matching `id` and either `ast` or `error`. The returned version is stored
  with the AST and participates in invalidation. Orez asks for the current
  version on every pull, including pulls with no new named queries, so a policy
  version change revokes stored ASTs without waiting for a client query patch.
- Wake: POST `{ "namespace", "token" }`, where `token` is the WebSocket's
  `wakeToken` query value. Only HTTP 204 permits the upgrade.

Direct pushes to the native host are disabled. Clients push through the
application endpoint, and the application calls
`/<namespace>/admin/settle-push` with the same process token after its SQLite
transaction commits.

`--host` defaults to `127.0.0.1`; containers may explicitly use `0.0.0.0` so
clients can reach pull and wake. Admin routes still require the actual TCP peer
address to be loopback and ignore forwarded-address headers. A missing peer is
untrusted. Rust embedders that intentionally have an in-process admin trust
boundary must opt in visibly with `into_router_trusted()`.

## Supported targets

The authoritative target list is `scripts/sync-native-platforms.ts`:

- macOS ARM64 and x64
- Linux ARM64 and x64, each for glibc and musl
- Windows ARM64 and x64

GNU Linux packages target glibc 2.17 through `cargo-zigbuild`. Musl packages
remain statically linked to musl.

Every platform npm package contains `LICENSE` for Orez and `LICENSES.txt` for
the exact Rust dependency graph used to build the executable. Embedded hosts
must copy both files beside their distributed binary. Regenerate the latter
with cargo-about 0.9.1 after changing Rust dependencies:

```sh
cargo about generate --locked --manifest-path crates/sync-native/Cargo.toml \
  --fail --output-file LICENSES.txt scripts/sync-native-licenses.hbs
bun scripts/normalize-sync-native-licenses.ts LICENSES.txt
```

## Release flow

Prepare a new native version and commit it with the Rust changes:

```sh
bun run release:native:prepare 0.1.1
```

After that commit reaches `main` and CI is green for that exact commit, run the
`Release sync-native` workflow manually. The workflow verifies both conditions
before publishing.

1. The small `orez-sync-native@0.1.1` launcher publishes first. Its optional
   dependencies point at the exact platform package versions that are about to
   be built. npm permits missing optional dependencies, so `orez` can already
   reference this version without breaking installs.
2. Every native target starts after the launcher. The macOS ARM64 target has
   its own job and publishes as soon as its build and smoke test pass. At that
   point it is immediately usable, while every other target keeps building:

   ```sh
   npm install orez-sync-native@0.1.1
   npx sync-native --version
   ```

3. The other target jobs continue independently and publish whenever they
   finish. Each target installs the launcher from npm and runs its own binary
   before its job completes.

Every published package records its source commit. A retry may reuse a package
only when it came from the same commit, so one native version cannot combine
binaries from different source revisions. If a source change is needed after a
partial release, prepare a new native version before dispatching again.

An `orez` install performed before its native packages exist succeeds because
the dependency is optional. Reinstall after the relevant platform package has
published to add the binary. There is no `postinstall` download and no release
script blocks waiting for every Rust target.

## First release bootstrap

Each new npm package must exist before npm trusted publishing can be configured.
After the upgrade regression and hosted multi-platform CI pass, the release
owner runs one command from a clean, current `main` checkout:

```sh
bun run release:native:bootstrap
```

The script verifies that its exact commit is current `origin/main` with green
hosted CI, installs its own Node.js 24.16.0 and npm 12.0.1 runtime into
`.release`, verifies npm is authenticated as `nwienert`, and asks for an
explicit irreversible-action confirmation. It then
publishes every missing unscoped name together at `0.0.0-bootstrap.0` under the
`bootstrap` tag and configures their `natew/orez` / `release-sync-native.yml`
trusted publishers. When the npm browser approval opens, approve with Touch ID
or the registered security key and select the five-minute 2FA skip; the script
paces all nine trust requests inside that window. A rerun skips names and exact
trust configurations already completed, and stops if a name has another owner
or trusted publisher.

The bootstrap packages contain no executable and never consume the real native
version. Subsequent workflow publishes use GitHub OIDC and carry npm provenance
without stored npm tokens. Never publish the checked-in platform directories
directly because they intentionally contain no binaries.

Publishing or bootstrapping packages remains a release action and requires
explicit approval.
