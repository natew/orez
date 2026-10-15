# Native FTS5 tokenizers

The shared SQLite build statically links `better_trigram` and `html` alongside
`vec0`. SQLite's existing `SQLITE_EXTRA_INIT` owner registers native auto-extensions;
every connection receives the tokenizers. The callback adapts SQLite's three-argument
extension ABI to the upstream one-argument core initializers and returns their errors.
There is one SQLite engine and no SQL rewriting or tokenizer substitution.

## Reproducible inputs

The native identity comes from [Notesnook commit
5748795b9920a4ba71f5456e126f5227bd5b8a51](https://github.com/streetwriters/notesnook/tree/5748795b9920a4ba71f5456e126f5227bd5b8a51),
`apps/mobile/patches/react-native-quick-sqlite+8.2.7.patch`, SHA256
`3cc27458802c4897114d3162b9d18869eec586dff08635c31185938ee490868c`.
Its installed CommonJS entry is unchanged quick-sqlite 8.2.7, SHA256
`8e49360e259987d8722d047e6d1b3664bdef025ee8ca1c35d24f4a1f69d6cc39`.
The package version alone does not identify its native sources.

The Makefile checks the patch's digest before extracting its new C/header files
from `better-trigram`, `sqlite3-fts5-html`, and `lexbor`. All 446 files match the
retained installed app inputs byte for byte. No upstream source is modified.
`SQLITE_CORE` selects their existing native initializers; `SQLITE_PRIVATE=` gives
those initializers external linkage in separate translation units. Lexbor builds
as a static archive with Windows ports excluded, matching the app's podspec.
LTO discards unused code. The app's SQLite amalgamation and bridge patches are
excluded: Orez keeps its existing Bedrock SQLite engine and VFS.

The tokenizers are Abdullah Atta's public-domain
[better-trigram](https://github.com/streetwriters/sqlite-better-trigram) and
[sqlite3-fts5-html](https://github.com/streetwriters/sqlite3-fts5-html).
Their notices ship in `LICENSE.fts5-tokenizers`. Lexbor is copyright Alexander
Borisov and contributors, licensed under Apache-2.0 (`LICENSE.lexbor`). Its pinned
app copy differs from release v2.4.0; that release is not a substitute build input.

Build all entry points with the existing pinned toolchain:

```sh
bun install --frozen-lockfile
cd sqlite-wasm
make docker
```

## Scope and limits

Assigned native scope, 2026-10-15: "Compile the real better_trigram and html
tokenizers with their Lexbor dependency, preserving exact upstream C semantics."
Browser app integration and Notesnook offline home acceptance belong to the parent
Contrast task, t-mv9iupcn-13ri0. A native build is not UI or production acceptance.

Focused regression coverage lives in `test/database.test.ts`: exact vocabulary,
short words, BMP CJK, HTML and numeric/named entities, ignored script/style text,
diacritic folding, two connections, persisted close/reopen, invalid constructors,
transaction rollback and coexistence with nearest-neighbor vec queries.

Known upstream limits are preserved. Custom `better_trigram` does not provide
FTS5's built-in trigram LIKE/GLOB index optimization. The pinned tokenizer defines
`u32` as `uint16_t`, so supplementary-plane Unicode is not claimed here. The HTML
extension can return incorrect original-byte highlight offsets for entities;
search results and highlight offsets are separate claims. Allocation-exhaustion
behavior inside the unmodified HTML parser is not certified by ordinary query
coverage. This build adds no Durable Object queries, tables or writes.

## Native validation and cost

TESTED on studio-64 with Emscripten 5.0.0, container digest
`sha256:67df07bc59f86685818fa801d340b54e576fe4276cfd3bcfc4da61d564217737`:
SQLite suite 42/42, existing WASM compatibility suite 22/22, Node and Bun
synchronous/asynchronous entry points, and Chromium 149.0.7827.55 browser queries.
The original browser pair rejects both tokenizer names. A control rebuilt with
the original initializer and the same toolchain reproduces that pair byte for byte.
A separate native probe injects `SQLITE_ERROR` and `SQLITE_NOMEM` from the real
HTML initializer's `xCreateTokenizer` call. `sqlite3_open_v2` returns exactly 1
and 7; the JavaScript constructor throws, and open/search succeeds after restoring
the ordinary auto-extension list. Probe exports are excluded from shipped binaries.

The browser JavaScript is unchanged, SHA256
`d764c46f93618142ee20c58d0bc3a6e5d16b05281c36d2ab63e82ddf272e3d71`.
The original WASM is SHA256
`fa82879ab4085e02f2a47e215ba201eeff64f48d7246178a1ac6a68799533b97`;
the new browser and Node WASM are both SHA256
`5a4e809301dfdcd10eea4f27aed6e45e56300ea3020a9c4e659d6017449ab3fc`.
WASM grows from 1,654,808 to 1,910,243 bytes, adding 255,435 bytes (15.4%).
Gzip grows from 660,479 to 763,693 bytes, adding 103,214 bytes. The pinned build,
including control and failure-probe links, took 43 seconds after image acquisition.

The first browser probe observed factory/load/open at 22 ms for the original pair
and 16.7 ms for the new pair; this single sequential sample does not establish a
startup improvement. Both had open/close p95 near the browser's 0.1 ms timer step.
Source manifests, paired artifacts, build logs and executable probes are retained
under `orez/scripts/tmp/peach-sqlite-fts-native` on studio-64. Further optimization
could reduce archive compilation to Lexbor's required dependency closure; this
change keeps the app's original source selection and lets LTO remove unused code.
