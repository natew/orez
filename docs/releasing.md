# Releasing Orez

Every ordinary push to `main` publishes a canary after the full `CI` workflow
passes. `.github/workflows/release.yml` publishes every public workspace package
with npm trusted publishing. Canary versions use the current stable version plus
`-canary.<timestamp>` and the npm `canary` dist-tag.

Canary publishing does not edit manifests, create a release commit or tag, or
push back to `main`.

## npm trusted publishers

Configure the same GitHub Actions trusted publisher for each public package:

- `orez`
- `bedrock-sqlite`
- `pg-to-sqlite`
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

Configure all five packages before `release.yml` reaches `main`. A package
without the trusted publisher will reject its publish after earlier packages may
already have been published.

## Manual releases

Stable `bun release --patch`, `bun release --minor`, and `bun release --major`
remain explicit. They use the interactive npm credential on the local machine,
then commit and tag the stable version.
