# Event-log retention (fork change)

The server's SQLite state grows without bound: on the companion devbox it reached
2.5 GB on 10-02 and 2.9 GB on 10-06, almost all of it in `orchestration_events`
(1.1 GB), `projection_thread_activities` (685 MB) and
`orchestration_command_receipts` (104 MB). This change prunes those tables in
bounded batches while the server runs and returns the space to the filesystem.
Paths below are under `apps/server/src/`; line numbers are on this branch.

## Guarantees

1. **Projections never rebuild from a pruned log.** Events are pruned only below
   the lowest projector cursor. If a projector's cursor is ever below the pruned
   floor (for example a projector added by a later upstream release, which starts
   at 0), startup fails with `ProjectionPipeline.bootstrap:retentionFloor`
   instead of building partial state. `T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY=true`
   overrides this.
2. **A WS resume never skips pruned events, however slow the subscriber.** The
   highest pruned sequence is committed as a floor before anything at or below
   it is deleted, and the floor never falls. A shell or thread subscription whose
   `afterSequence` is below the floor gets a full `snapshot`, the same fallback
   it already gets for an oversized gap. The bounded replay is read first and
   the floor is checked again afterwards, so a prune that lands between the
   first check and the read also ends in a snapshot.
3. **Conditional delivery keeps working.** Pending delivery records and the
   `thread.turn-start-requested` events they re-read are never pruned.
4. **No long write lock.** Every delete covers at most `batchSize` rows, in a
   transaction that also holds the select it was decided from. On a 550 MB copy of real data the longest event-loop stall
   during a full first pass was 35 ms.

## What each consumer needs (evidence)

**(a) Projections are authoritative; boot replays only from each cursor.** The
engine runs `projectionPipeline.bootstrap` and then loads its command model from
the projection tables, not from events (`orchestration/Layers/OrchestrationEngine.ts:461-462`).
`bootstrapProjector` replays from each projector's `last_applied_sequence`
(`orchestration/Layers/ProjectionPipeline.ts:2069`). No migration resets
`projection_state`; only `005_Projections` touches it. So a full replay happens
only for a projector with no state row. Guarantee 1 covers that case
(`ProjectionPipeline.ts:2140`; `persistence/RetentionStorage.ts`, `assertProjectorsCoverPrunedEvents`).

The bootstrap attachment cleanup has its own cursor,
`projection.attachment-cleanup`, which advances only at boot. It rescans for
`thread.reverted` and `thread.deleted` events from that cursor
(`ProjectionPipeline.ts:2166`). Those two event types are kept above that cursor,
so a long-running server can still prune everything else.

**Point lookups on old events.** `hasEventAfter` (`OrchestrationEngine.ts:180,196`,
`ProjectionPipeline.ts` attachment side effects) asks whether a stream has any
event, or a `thread.created`, after a sequence. The thread-detail watermark asks
for the stream's newest event of the thread-detail types
(`orchestration/Layers/ProjectionSnapshotQuery.ts:1748`). `append` derives
`stream_version` from the stream's newest row
(`persistence/Layers/OrchestrationEventStore.ts:151`). **Keeping the newest event of
each (stream, event type)** answers all of these exactly. That is about one row
per stream and type, roughly 2,000 rows for 70 threads.

**(b) Thread restart and continuation do not read events.** Provider session
resume uses `provider_session_runtime` and `projection_thread_sessions`.
Checkpoints and diffs use `projection_turns` and git refs. Proposed plans,
pending approvals and user input are projection tables. The one exception is a
conditional start: the provider reactor re-reads the requested event of an
`accepted` delivery, both when re-driving it and in boot recovery, and **dies at
boot if the event is missing** (`orchestration/Layers/ProviderCommandReactor.ts:2158,2408`).
Events referenced by `requested_sequence` of `accepted`, `attempted` or
`unresolved` deliveries are kept.

**(c) WS replay.** Shell resume replays when the gap is at most 1,000 events and
8 MB (`ws.ts:379,588`). Thread resume counts only the thread's own rows, so a
quiet thread can resume from an arbitrarily old cursor (`ws.ts:383`). Without a
floor, either path would silently skip pruned events. Both replay-stat queries
now report `rangePruned` (`ProjectionSnapshotQuery.ts:1100`,
`OrchestrationEventStore.getAggregateReplayStats`), and `ws.ts` takes the
snapshot path when it is set (`ws.ts:613,2572`).

A check before the read is not enough on its own: a subscriber can pass it,
stall (for example on a network ack), and read after a prune. Both paths
therefore read their bounded range into memory and then check the floor again
(`orchestration/ReplayGuard.ts`, used at `ws.ts:2421,2584`). Retention commits
the raised floor before it deletes anything at or below it, and the floor never
falls. So if the second check still finds the cursor at or above the floor, no
delete touching the range had started when the read finished, and the replay is
complete. Otherwise the subscriber gets a snapshot. The 5 s `floorGraceMs` only
reduces how often that fallback happens; correctness does not depend on it.

**(d) Receipts** are read only by `getByCommandId` at dispatch
(`OrchestrationEngine.ts:148`) to deduplicate a retried command id. Clients retry
within seconds, and the mailbox retries only across its own restarts, so a window
of days is ample. For conditional starts, the delivery row is a second guard. Its
`command_id` primary key makes a stale retry fail inside the dispatch transaction
(`OrchestrationEngine.ts:345`), so delivery rows are kept at least as long as
receipts.

**(e) Turn deliveries.** Reconciliation and the idle check read only pending
statuses (`findPendingForThread`, `listByStatus`). Terminal rows
(`started`/`failed`/`abandoned`) serve `GET /api/orchestration/turn-deliveries/:id`
and are pruned after 30 days.

**Activities: what the UI and server read.** The thread view loads the newest 500
activities, or a turn window with the same limit
(`ProjectionSnapshotQuery.ts:98`). It also pins unresolved approval and
user-input requests whatever their age (`ProjectionSnapshotQuery.ts:1868`).
Server logic reads activities of these kinds:

- the pending user-input count and approval restore read the request
  lifecycle kinds (`ProjectionPipeline.ts:162,590,1871`; `ProviderRuntimeIngestion.ts:2354`);
- answer routing reads `user-input.*` kinds (`ProjectionSnapshotQuery.ts:1511`);
- attachment retention reads `user-input.answer-submitted` (`ProjectionPipeline.ts:2019`);
- startup worktree reconciliation reads `worktree-setup` (`serverRuntimeStartup.ts:764`);
- compaction token counts read the latest `context-window.updated` activities
  (`ProviderRuntimeIngestion.ts:2588`);
- task-title recovery reads the newest titled `task.started`/`task.progress` row
  **for a task id** (`ProviderRuntimeIngestion.ts:2537`).

The client's subagent fold (`packages/client-runtime/src/state/subagentRuntime.ts:463`)
replays each task's `task.*` and `tool.progress` rows in order. A task joins the
agent roster only through its first row's `agentKind`; terminal rows usually
lack it (`subagentRuntime.ts:119`). Task lifecycles are therefore kept **per
task id**, not per kind. Keeping the newest row of each kind would pair one
task's start with another's completion: for B-start, A-start, A-complete,
B-complete it would keep A-start and B-complete, and the fold would show A
running. For each task id the pass keeps three things: the first row, which
fixes roster membership; the newest row of each task kind, which carries the
terminal evidence and the last state, in their original order; and the newest
titled row, which title recovery reads. Other activities keep their newest row
per kind.

Provider updates rewrite some rows in place under a stable id (task progress,
`ProviderRuntimeIngestion.ts:733`; the upsert rewrites `created_at`,
`persistence/Layers/ProjectionThreadActivities.ts:84`). Each activity batch
therefore selects and deletes inside one transaction, so a row is judged on the
state it has when it is deleted. A row refreshed during a pass is young again
and leaves the walk.

Old tool output (`tool.completed` and `tool.updated` carry most of the bytes) is
read by nothing except the work log of older turns.

## Policy and defaults

| Table                            | Pruned                                                                                                                 | Kept                                                                                                                                                                               |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `orchestration_events`           | `sequence` ≤ min(every projector cursor, head − 5,000, last event older than 3 days)                                   | newest per (stream, type); requested events of pending deliveries; reverts and deletions after the cleanup cursor                                                                  |
| `orchestration_command_receipts` | `accepted_at` older than 7 days                                                                                        | everything newer                                                                                                                                                                   |
| `projection_thread_activities`   | older than 3 days **and** outside the newest 200 of a live thread (newest 0 of an archived, deleted or missing thread) | request-lifecycle kinds, `user-input.answer-submitted`, `worktree-setup`; per task id the first, newest-per-kind and newest titled rows; for other kinds the newest row per thread |
| `orchestration_turn_deliveries`  | terminal and resolved more than max(30, receipt days) days ago                                                         | `accepted`, `attempted`, `unresolved`                                                                                                                                              |

The age boundary is conservative. `occurred_at` comes from command timestamps and
is only roughly monotonic, so the pass stops at the first event, in sequence
order, that is not old. The same applies to receipts in `result_sequence` order.

All settings are environment variables, read at startup:

| Variable                                                                                                    | Default               |
| ----------------------------------------------------------------------------------------------------------- | --------------------- |
| `T3CODE_RETENTION_ENABLED` (off switch)                                                                     | `true`                |
| `T3CODE_RETENTION_INTERVAL_MINUTES`                                                                         | `60`                  |
| `T3CODE_RETENTION_EVENTS_DAYS` / `_EVENTS_KEEP_RECENT`                                                      | `3` / `5000`          |
| `T3CODE_RETENTION_RECEIPTS_DAYS`                                                                            | `7`                   |
| `T3CODE_RETENTION_ACTIVITIES_DAYS` / `_ACTIVITIES_KEEP_PER_THREAD` / `_ARCHIVED_ACTIVITIES_KEEP_PER_THREAD` | `3` / `200` / `0`     |
| `T3CODE_RETENTION_DELIVERIES_DAYS`                                                                          | `30`                  |
| `T3CODE_RETENTION_BATCH_SIZE` / `_BATCH_PAUSE_MS` / `_FLOOR_GRACE_MS`                                       | `500` / `10` / `5000` |
| `T3CODE_RETENTION_VACUUM_PAGES_PER_BATCH`                                                                   | `512`                 |
| `T3CODE_RETENTION_STARTUP_VACUUM` (`auto` / `off` / `force`)                                                | `auto`                |
| `T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY`                                                                     | `false`               |

Disabling retention stops pruning and vacuuming. The floor checks stay active,
because an earlier pass may already have pruned.

## Schedule and space reclamation

`orchestration/Retention.ts` (`layer`, provided in `server.ts`) waits for the
engine, whose construction replays the projections. It then runs a pass at
server activation and every hour after that. A pass prunes events, receipts,
activities and deliveries in turn, then runs `reclaimFreePages`. On an
incremental auto-vacuum database, that function issues
`PRAGMA incremental_vacuum(1)` 512 times per transaction until the freelist is
empty. node:sqlite steps a column-less pragma only once, so
`incremental_vacuum(N)` would free one page; repeating `(1)` is correct under
either stepping behaviour.

**The one-time VACUUM.** An existing database has `auto_vacuum = NONE`. Switching
it to incremental requires a full `VACUUM`, which rewrites the file while holding
the only connection. It therefore runs only in server startup
(`runServerStartupVacuum`, `persistence/RetentionStorage.ts:301`). `server.ts:195`
wraps it around the SQLite layer, so every server consumer of the database
waits for it and nothing is served meanwhile. CLI subcommands that open the live
server's database never run it: `t3 pair` uses `EnvironmentAuth.runtimeLayer`
(`cli/pair.ts:445`, `auth/EnvironmentAuth.ts:1130`), and project and auth commands
use the plain layer as well. The shared SQLite setup only creates the state table
and sets the `auto_vacuum` pragma, which writes nothing to an existing database.
New databases are created incremental, because the pragma is set before WAL
mode initialises the file.

With `auto`, the default, the VACUUM runs when free pages are at least 64 MB and
at least 10 % of the file. It can never stop startup:

- **Disk preflight.** VACUUM writes a temp copy of the live data, in the
  directory SQLite uses (`SQLITE_TMPDIR`, then `TMPDIR`, `/var/tmp`, `/usr/tmp`,
  `/tmp`). It then writes about the live size again through the WAL. Each copy
  needs 1.2 × live + 64 MB of available space; twice that when the temp
  directory shares the database's filesystem. Too little space, or space that
  cannot be read, skips the VACUUM with a warning.
- **Failure.** A failed VACUUM rolls back and leaves the database intact. It is
  logged, startup continues, and the failure time is recorded. `auto` then does
  not retry for 24 hours; `force` retries at once.

The intended rollout is:

1. Deploy. The first pass prunes. Free pages are reused, so the file stops
   growing, but it does not shrink yet. The pass log says so.
2. Restart the service once. The startup VACUUM compacts what is left, which is
   now small. From then on every pass shrinks the file itself.

Measurements on a copy of a real 550 MB database (106 k events, 60 k
activities, 106 k receipts), on a laptop NVMe disk:

- **First pass:** 11.8 s. It deleted 100,785 events (189 MB of payload), 85,666
  receipts and 50,141 activities (142 MB). The longest event-loop stall was
  35 ms. The second pass took 8 ms.
- **Startup VACUUM after pruning, including the disk preflight:** 0.2 s. The
  file went from 549 MB to 52 MB.
- **Forced VACUUM of the unpruned 550 MB:** 2.1–2.3 s.
- **Incremental reclaim of 50 MB:** 0.3 s, with a longest stall of 6 ms.
- **Creating the retention index:** 87 ms on 106 k events.

Scaled linearly, a forced VACUUM of the unpruned 3 GB devbox file would block
startup for about 12 s on this disk. It needs the live size again as free space
twice: once for the temporary copy and once for the WAL. On the devbox's disk,
which is likely slower, allow a minute or more. Restarting after the first
pass avoids almost all of that.

## Risks and open points

- **Future upstream migrations that backfill from `orchestration_events`.**
  `024` and `046` did this. They would see only retained events. The startup
  guard does not detect this; review new migrations when rebasing.
- **Activity history of older turns is gone.** The work log of turns older
  than 3 days, beyond the newest 200 activities, shows messages but no tool
  calls. An unarchived thread keeps only protected and newest-of-kind activities.
- **Timestamps ahead of the clock.** One event whose `occurred_at` is in the
  future stops event pruning at that event until the timestamp ages out. A
  client clock that is far ahead would cause this.
- **Backups must be consistent snapshots.** VACUUM and incremental vacuum
  rewrite pages through the WAL, so a backup has to use SQLite's online backup
  API or `VACUUM INTO`, never `cp` of the live file. The devbox already
  complies: `t3-state-db` opens the database `mode=ro` and copies it with
  Python's `sqlite3.Connection.backup` into a local temp file, then publishes
  that file to NFS. The backups shrink once the file does.
