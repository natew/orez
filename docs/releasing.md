# Releasing Orez

Every ordinary push to `main` publishes a canary after the full `CI` workflow
passes. `.github/workflows/release.yml` publishes every public workspace package
with npm trusted publishing. Canary versions use the current stable version plus
`-canary.<timestamp>` and the npm `canary` dist-tag.

Canary publishing does not edit manifests, create a release commit or tag, or
push back to `main`. Stable release commits, whose subject matches the checked-in
`v<version>`, are skipped instead of producing a canary of the same version.

## npm trusted publishers

Configure the same GitHub Actions trusted publisher for each public package:

- `orez`
- `bedrock-sqlite`
- `orez-sync-cf-host`
- `drizzle-zero-sqlite`

Use these settings in each package's npm **Settings → Trusted publishing** page:

- Organization or user: `natew`
- Repository: `orez`
- Workflow filename: `release.yml`
- Environment name: leave blank
- Allowed actions: `npm publish`

The release workflow deliberately has no npm token. GitHub issues a short-lived
OIDC identity for the publish job, and npm exchanges it for package-specific
publish access. The workflow uses npm 12.0.1, Node 24, and a GitHub-hosted runner.

Configure all packages before `release.yml` reaches `main`. A package
without the trusted publisher will reject its publish after earlier packages may
already have been published.

New shared package names are bootstrapped from a clean, current `main` checkout
after hosted CI passes:

```sh
bun run release:shared:bootstrap
```

The command claims any missing `@o/helpers`, `@o/env`, and `@o/cli` names at
`0.0.0-bootstrap.0` under the `bootstrap` tag, then configures the
`natew/orez` / `release.yml` trusted publisher. Reruns verify and skip completed
names. Bootstrapping is a release action and requires explicit approval.

## Stable releases

Stable releases remain explicit. From a clean, current `main` checkout, dispatch
one with:

```sh
bun release --patch --ci
```

The local command verifies the checkout, dispatches `release-sync-native.yml`,
prints the run URL, and exits. GitHub then owns the full release under OIDC. The
native workflow publishes a new native package version when the durable
contract changed, then dispatches `release.yml` itself. That second top-level
workflow publishes the public workspace family and finally creates and pushes
the stable version commit and tag. Keeping both as top-level workflows matters:
npm validates the workflow filename registered for each package's trusted
publisher. No local npm login or second release command is involved.

The workflow also accepts `minor` and `major` dispatch inputs. If npm stops
after publishing only part of the package family, rerun the same dispatch. The
release script checks npm before every publish and skips versions that already
reached the registry.

Stable publishing still requires an explicit dispatch; ordinary main pushes
only publish canaries.

## Local consumer validation

To exercise the current Orez source in a local consumer, use a consumer whose
installed public Orez packages match this checkout, then run:

```sh
cd ~/orez
bun release --into ~/soot
```

The command builds and installs the current source without publishing to npm.
It replaces ordinary packages only when their installed versions match the
source checkout. Native packages are the exception because their checked-in
version is a template and CI may have allocated a newer published version. If
the consumer has native `0.1.7` while this source template says `0.1.6`, the
local flow builds the current native binary as `0.1.8` and updates the staged
Orez dependency to that local version. Do not copy `dist` files or install a
different transport artifact to work around this mismatch. An ordinary Orez
package version mismatch means the consumer and source checkout are not the
same release line; use a matching Orez checkout before rerunning `--into`.
