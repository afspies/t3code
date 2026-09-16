# Companion build: `alex/rm-async-delivery` on upstream T3 Code

This branch is upstream [pingdotgg/t3code](https://github.com/pingdotgg/t3code)
at tag `v0.0.43-nightly.20260916.1825` plus one feature commit, built into npm
tarballs for a self-hosted server. Nothing here is published to npm or sent
upstream.

## What the fork changes

One commit, `feat(server): support conditional durable turn delivery`
(originally `03d61c0` on `alex/rm-async-delivery`, rebased here):

- **Capability** `orchestrationConditionalTurnStart: true` in the environment
  descriptor (`/.well-known/t3/environment`), so clients can detect the feature.
- **Conditional turn starts.** `thread.turn.start` accepts `onlyIfIdle: true`.
  The decider rejects it with a side-effect-free `409`/`OrchestrationConflict`
  (`turn_start_not_idle` with an `idleBlocker`) when the thread is busy, instead
  of queueing the message. Targeted `thread.turn.interrupt` likewise rejects a
  stale turn id (`turn_interrupt_stale`) and is passed through to adapters that
  key their active turn by id.
- **Durable delivery records.** Each accepted conditional start gets a row in
  `orchestration_turn_deliveries` (`accepted → attempted → started | failed |
unresolved | abandoned`). The provider reactor claims the attempt before the
  provider call and resolves it after, under a per-thread admission gate, so a
  restart can tell an unattempted start from an attempted-but-unknown one and
  never re-sends the latter. Delivery evidence is appended as thread activities
  (`provider.turn.start.attempted`, `provider.turn.started`,
  `provider.turn.start.unresolved`).
- **HTTP/WS plumbing.** `GET /api/orchestration/turn-deliveries/:commandId` and
  `POST /api/orchestration/turn-deliveries/:commandId/resolve` (operator reconciliation of an
  `unresolved` record to `abandoned`), the conflict error on the WS dispatch
  error, and the corresponding contracts in `packages/contracts`.
- Only the `codex` and `claudeAgent` drivers support conditional delivery (their
  `sendTurn` returns at provider admission with a turn id); other drivers get a
  proven pre-attempt failure.

Files: `apps/server/src/orchestration/{decider,http,Errors}.ts`,
`apps/server/src/orchestration/Layers/{OrchestrationEngine,ProviderCommandReactor}.ts`,
`apps/server/src/persistence/{Layers,Services}/OrchestrationTurnDeliveries.ts`,
`apps/server/src/provider/Layers/{ClaudeAdapter,CodexSessionRuntime}.ts`,
`apps/server/src/{ws,auth/http,environment/ServerEnvironment}.ts`,
`packages/contracts/src/{orchestration,environment,environmentHttp}.ts`, plus
tests.

## Why the table lives outside the migration ledger

Upstream migrations are numbered entries in
`apps/server/src/persistence/Migrations.ts`, run by Effect's `Migrator`. The
Migrator records the highest applied id in `effect_sql_migrations` and, on
each boot, runs only entries whose id is **greater than the latest applied
id**. A fork-only numbered migration therefore always misbehaves:

- numbered below upstream's newest id, it is silently skipped on any database
  upstream has already migrated past that id;
- numbered above it, it takes an id upstream will assign to its own next
  migration. That happened here: the fork's `050_OrchestrationTurnDeliveries`
  collided with upstream's `050_ProjectionThreadPullRequests`, and whichever
  ran first shadowed the other.

So the fork no longer registers a migration. `OrchestrationTurnDeliveryRepositoryLive`
(`apps/server/src/persistence/Layers/OrchestrationTurnDeliveries.ts`) creates
the table and its index at layer construction with `CREATE TABLE IF NOT EXISTS`
/ `CREATE INDEX IF NOT EXISTS`, using the exact schema of the removed migration.
Databases created by the old fork build already have the table and a ledger
row `50 OrchestrationTurnDeliveries`; both are left alone. Ledger row 50's
upstream meaning (`ProjectionThreadPullRequests`) is handled separately from
this branch.

## Rebase

```sh
git clone https://github.com/afspies/t3code && cd t3code
git remote add upstream https://github.com/pingdotgg/t3code
git fetch --tags upstream
git checkout -b alex/rm-async-delivery-0.0.43 v0.0.43-nightly.20260916.1825
git cherry-pick 03d61c0          # resolve conflicts, keep upstream behaviour
git rm apps/server/src/persistence/Migrations/050_OrchestrationTurnDeliveries.ts
# drop its entry from apps/server/src/persistence/Migrations.ts; see above
```

Conflict resolutions worth knowing when rebasing again:

- `Schema.TaggedErrorClass` was renamed `Schema.TaggedError` upstream.
- `ProviderCommandReactor.ts`: upstream now queues ordinary turn starts behind
  context compaction and replays them under a fresh server command id.
  Conditional starts are excluded from that queue (a replay would orphan the
  durable record) and treat a compacting thread as busy. Upstream also added
  its own `thread.session-set` case (title refinement); the fork's re-drive of
  deferred conditional starts is merged into that same case, otherwise the
  first `case` label wins and the re-drive never runs.

Verify:

```sh
pnpm install --filter=t3... --filter=@t3tools/web... --filter=@t3tools/scripts...
pnpm exec vp run --filter t3 typecheck
pnpm exec vp run --filter @t3tools/contracts typecheck
cd apps/server && pnpm exec vp test run \
  src/orchestration/Layers/OrchestrationEngine.conditionalTurnStart.test.ts \
  src/orchestration/Layers/ProviderCommandReactor.test.ts \
  src/provider/Layers/ClaudeAdapter.test.ts \
  src/server.test.ts
```

## Build

`scripts/companion-build.sh` runs the same steps as
`.github/workflows/release.yml` (jobs "Build JS bundle", the per-platform CLI
archive, and "Build npm packages from CLI archives") for one platform,
non-interactively:

1. `node scripts/update-release-package-versions.ts <version>` (restored on exit).
2. `vp run --filter t3 build` → web client + server bundle in `apps/server/dist`.
3. `node apps/server/scripts/cli.ts build-exe --target linux-x64` → Node
   single-executable in `apps/server/dist-exe` (the embedded Node is pinned in
   `apps/server/vite.config.ts`; the host needs Node ≥ 25.7 for `--build-sea`).
4. `node scripts/build-cli-archive.ts --platform linux --arch x64 ...` → the
   release archive: executable, `client/`, `resource-monitor/`, and a hoisted
   production install of the runtime-external native packages for the target.
5. `node scripts/build-npm-platform-packages.ts --allow-missing` → the `t3`
   launcher and `@t3code/t3-linux-x64` npm tarballs.
6. A self-contained launcher tarball with the platform package bundled inside
   (`bundleDependencies`), because the platform package is not on npm.

Two native pieces cannot be cross-built from macOS, so by default the script
takes them from upstream's GitHub release archive of the same base version
(downloaded once, verified against its `SHA256SUMS`):

- the Rust resource monitor (`native/resource-monitor` is unchanged at that
  tag); `T3_COMPANION_RESOURCE_MONITOR=none` ships without it and the server
  degrades resource telemetry;
- node-pty's compiled `pty.node`. node-pty publishes no Linux prebuild, so
  upstream compiles it with node-gyp on the Linux runner; a macOS host's
  cross-install leaves it without a binary. The script copies upstream's
  binary (same node-pty version, checked) into `build/Release/` and mirrors it
  into `prebuilds/linux-x64/`. That directory is all node-pty's install script
  checks for, so `npm install` on the target does not need python3/make/g++.
  Upstream's own npm package lacks the prebuilds directory and does compile
  on install; `T3_COMPANION_NODE_PTY=none` reproduces that.

```sh
T3_COMPANION_OUT=/tmp/t3-companion/artifacts scripts/companion-build.sh
```

Version string: `<upstream version>-rm-async-delivery.<short sha of HEAD>`,
for example `0.0.43-nightly.20260916.1825-rm-async-delivery.abc1234`
(`T3_COMPANION_VERSION` overrides it). Because the suffix follows the nightly
date, the server's release-channel detection treats this version as `stable`
for self-update checks; do not enable self-update against upstream releases.

Relay / T3 Connect constants (`T3CODE_RELAY_URL`, Clerk keys) come from CI
secrets upstream; a local build leaves them empty unless set in `.env`.

Cross-building from macOS works for the executable and the npm tree
(pnpm `supportedArchitectures` installs the Linux prebuilds), but the archive
smoke test only runs when host and target match. On a Linux x64 box with
Node ≥ 25.7 the same script also runs
`scripts/smoke-cli-archive.ts`.

## Install

Target: Linux x64, Node 22 or newer for npm and the launcher (the server itself
runs on the embedded Node). No compiler is needed: npm runs node-pty's install
script even for bundled packages, and it exits early because the prebuilds
directory is present.

```sh
npm install -g --prefix <prefix> ./t3-<version>.tgz
<prefix>/bin/t3 serve --base-dir <data dir> --host 127.0.0.1 --port 3000 --no-browser
```

Layout after install:

```
<prefix>/lib/node_modules/t3/bin/t3.js                       # npm bin target
<prefix>/lib/node_modules/t3/dist/bin.mjs                    # legacy launcher path
<prefix>/lib/node_modules/t3/node_modules/@t3code/t3-linux-x64/t3
```

Alternative two-tarball install (same layout is not guaranteed; the launcher
resolves the sibling global package):

```sh
npm install -g --prefix <prefix> ./t3code-t3-linux-x64-<version>.tgz ./t3-launcher-only-<version>.tgz
```

Check the running server:

```sh
curl -s http://127.0.0.1:3000/.well-known/t3/environment | jq '{serverVersion, cap: .capabilities.orchestrationConditionalTurnStart}'
```
