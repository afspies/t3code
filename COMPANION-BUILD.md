# Companion build: `alex/rm-async-delivery` on upstream T3 Code

This branch is upstream [pingdotgg/t3code](https://github.com/pingdotgg/t3code)
at tag `v0.0.45` plus one feature commit, built into npm
tarballs for a self-hosted server. Nothing here is published to npm or sent
upstream.

## What the fork changes

One commit, `feat(server): support conditional durable turn delivery`
(originally `03d61c0` on `alex/rm-async-delivery`, last rebased from
`alex/rm-async-delivery-0.0.43`, which sat on `v0.0.43-nightly.20260916.1825`):

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
git checkout -b alex/rm-async-delivery-<new base> v<new base>
# the two commits at the tip of the previous companion branch: the feature, then this doc + script
git cherry-pick origin/alex/rm-async-delivery-0.0.45~1 origin/alex/rm-async-delivery-0.0.45
# resolve conflicts, keep upstream behaviour; update the tag references in this file
```

The feature commit no longer carries a numbered migration (the original
`03d61c0` did, as `050_OrchestrationTurnDeliveries`). Do not reintroduce one,
and check that `apps/server/src/persistence/Migrations.ts` is identical to
upstream's after the rebase; see above.

Conflict resolutions worth knowing when rebasing again:

- `Schema.TaggedErrorClass` was renamed `Schema.TaggedError` upstream.
- `ProviderCommandReactor.ts`: upstream now queues ordinary turn starts behind
  context compaction and replays them under a fresh server command id.
  Conditional starts are excluded from that queue (a replay would orphan the
  durable record) and treat a compacting thread as busy. Upstream also added
  its own `thread.session-set` case (title refinement); the fork's re-drive of
  deferred conditional starts is merged into that same case, otherwise the
  first `case` label wins and the re-drive never runs.

Added by the rebase onto `v0.0.45`:

- `ProviderCommandReactor.ts`, `ensureThreadWorktree`: upstream passes
  `{ submodules }` to `createWorktree` and switched to `Effect.catchCauseIf`.
  Keep both; the fork's `failOnError` flag goes into the predicate
  (`!Cause.hasInterruptsOnly(cause) && !failOnError`).
- `ProviderCommandReactor.ts`, `processTurnStartRequested`: upstream added a
  first-turn `titleSeed` to the `buildSendTurnRequestForThread` call; keep it
  ahead of the fork's conditional path.
- Workspace lease (no textual conflict). Upstream now wraps the
  `thread.turn-start-requested` handler in `withWorkspaceLease` so storage
  cleanup cannot remove a worktree during session setup. The conditional path
  forks its setup, which would run after the handler has released the lease, so
  the fork wraps the forked effect in the lease again. The lease goes outside
  the per-thread admission gate: an ordinary start waits for the gate while
  holding the lease, so the opposite order can deadlock.
- `ClaudeAdapter.ts` and `CodexSessionRuntime.ts`, `interruptTurn`: upstream
  now settles the interrupted turn (Claude) and parked approvals and user-input
  prompts (Codex) at the top of the function. The fork's stale-turn check goes
  before those, otherwise a stale targeted interrupt has side effects.
- `packages/contracts/src/orchestration.ts`: upstream widened
  `bootstrapThreadDisposition` to `Schema.Literals(["deleted", "not-created"])`;
  keep that and add the fork's `conflict` field after it.
- Tests: `ProviderCommandReactor.test.ts` and
  `CodexCollabRuntime.integration.test.ts` conflict because both sides append
  tests at the same place; keep both sets. The reactor test harness needs
  upstream's `closeIdleTerminals` mock and the fork's typed `createWorktree`
  mock (it has the `createWorktreeEffect` hook). `OrchestrationEngineHarness.integration.ts`
  needs upstream's `TerminalManager` mock layer and the fork's
  `OrchestrationTurnDeliveryRepositoryLive`.
- The reactor test "marks a lost sendTurn response unresolved…" read the
  session status straight after the unresolved activity appeared. The forked
  delivery fiber writes the activity first and the session error a few
  milliseconds later, and on this base the read won the race, so the test now
  waits for the status to leave `starting` before asserting `error`.

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

One native piece cannot be cross-built from macOS, so by default the script
takes it from upstream's GitHub release archive of the same base version
(downloaded once, verified against its `SHA256SUMS`):

- the Rust resource monitor (`native/resource-monitor` is unchanged at that
  tag); `T3_COMPANION_RESOURCE_MONITOR=none` ships without it and the server
  degrades resource telemetry.

node-pty needs nothing special since upstream moved to node-pty 1.2 (in
`v0.0.45`): the package ships prebuilds for every platform,
`scripts/build-cli-archive.ts` keeps `prebuilds/linux-x64/pty.node` for the
target, and node-pty's install script exits early when that directory exists,
so `npm install` on the target does not need python3/make/g++. The script
checks for that file and leaves the archive alone (on `v0.0.45` the resulting
`node_modules/node-pty` tree is byte-identical to upstream's archive). The
old repair path is kept as a fallback for a base with node-pty < 1.2, which
had no Linux prebuild: it copies upstream's compiled `pty.node` (same node-pty
version, checked) into `build/Release/` and mirrors it into
`prebuilds/linux-x64/`; `T3_COMPANION_NODE_PTY=none` disables that fallback.

```sh
T3_COMPANION_OUT=/tmp/t3-companion/artifacts scripts/companion-build.sh
```

Version string: `<upstream version>-rm-async-delivery.<short sha of HEAD>`,
for example `0.0.45-rm-async-delivery.abc1234`
(`T3_COMPANION_VERSION` overrides it). The server's release-channel detection
treats this version as `stable` for self-update checks (it only recognises a
trailing `-nightly.<date>.<n>` or `-preview.<date>.<n>`), and semver orders it
below upstream's `0.0.45`; do not enable self-update against upstream releases.

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
