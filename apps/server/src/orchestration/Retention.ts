/**
 * Fork: event-log retention (see RETENTION.md for the policy and its
 * evidence). Projections are authoritative once written; the event log is only
 * replayed from each projector's cursor, by WS resume (guarded by the
 * pruned-through floor), and by a few point lookups this pass keeps intact.
 *
 * A pass deletes in bounded batches, each its own short write transaction on
 * the single SQLite connection, so turns keep flowing while it runs, then
 * returns free pages to the filesystem with incremental vacuum.
 *
 * @module OrchestrationRetention
 */
import { WORKTREE_SETUP_ACTIVITY_KIND } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ensureOrchestrationTurnDeliveriesSchema } from "../persistence/Layers/OrchestrationTurnDeliveries.ts";
import {
  loadRetentionConfig,
  raiseEventsPrunedThrough,
  readEventsPrunedThrough,
  reclaimFreePages,
  type RetentionConfig,
} from "../persistence/RetentionStorage.ts";
import { forkParked } from "../serverActivation.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";

const DAY_MS = 86_400_000;
const AGE_SCAN_CHUNK = 2_000;
const ATTACHMENT_CLEANUP_PROJECTOR = "projection.attachment-cleanup";

/**
 * Activity kinds read by request lifecycle accounting (pending approval and
 * user-input counts, answer routing, attachment retention) or by startup
 * reconciliation. They are small and rare; they are never pruned.
 */
const PROTECTED_ACTIVITY_KINDS: ReadonlySet<string> = new Set([
  "approval.requested",
  "approval.resolved",
  "provider.approval.respond.failed",
  "user-input.requested",
  "user-input.resolved",
  "provider.user-input.respond.failed",
  "user-input.answer-submitted",
  WORKTREE_SETUP_ACTIVITY_KIND,
]);

export interface RetentionPassResult {
  readonly events: {
    readonly deleted: number;
    readonly bytes: number;
    /** DELETE statements issued; each removes at most `batchSize` rows. */
    readonly batches: number;
    readonly prunedThrough: number;
    readonly skipped?: string;
  };
  readonly receipts: { readonly deleted: number; readonly batches: number };
  readonly activities: {
    readonly deleted: number;
    readonly bytes: number;
    readonly batches: number;
    /** Old tasks on retired threads left intact because they exceed taskRowCap. */
    readonly tasksOverCap: number;
  };
  readonly deliveries: { readonly deleted: number; readonly batches: number };
  readonly reclaimedBytes: number;
  readonly remainingFreeBytes: number;
  readonly autoVacuum: number;
}

/**
 * Indexes that keep every retention statement a bounded seek (RETENTION.md,
 * "Bounded work"). Created with IF NOT EXISTS when the layer is built, before
 * the server is activated; like the fork's tables they are not migrations.
 */
export const ensureRetentionIndexes = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    // "Is there a newer event of this type in this stream?"
    yield* sql`
      CREATE INDEX IF NOT EXISTS idx_orch_events_retention_stream_type
      ON orchestration_events(aggregate_kind, stream_id, event_type, sequence)
    `;
    // Keyset walk of a thread's activities, newest first, and its Nth newest.
    yield* sql`
      CREATE INDEX IF NOT EXISTS idx_projection_thread_activities_retention_created
      ON projection_thread_activities(thread_id, created_at, activity_id)
    `;
    // Distinct kinds of a thread and the newest activity of each.
    yield* sql`
      CREATE INDEX IF NOT EXISTS idx_projection_thread_activities_retention_kind
      ON projection_thread_activities(thread_id, kind, created_at, activity_id)
    `;
    // Distinct tasks of a thread and the newest row of each. The expression
    // must match the queries below character for character.
    yield* sql`
      CREATE INDEX IF NOT EXISTS idx_projection_thread_activities_retention_task
      ON projection_thread_activities(thread_id, json_extract(payload_json, '$.taskId'), created_at)
      WHERE json_extract(payload_json, '$.taskId') IS NOT NULL
    `;
    // Terminal deliveries by age, and pending ones by status.
    yield* sql`
      CREATE INDEX IF NOT EXISTS idx_orch_turn_deliveries_retention_status
      ON orchestration_turn_deliveries(status, resolved_at)
    `;
  });

type TaskId = string | number;

/**
 * Every statement a pass runs. Each one is a bounded, index-backed seek or a
 * LIMITed page; none scales with the size of a table. Exported so tests can
 * check their query plans.
 */
export const retentionStatements = (sql: SqlClient.SqlClient) => ({
  projectorCursors: () =>
    sql<{ readonly projector: string; readonly lastAppliedSequence: number }>`
      SELECT projector, last_applied_sequence AS "lastAppliedSequence" FROM projection_state
    `,
  eventHead: () =>
    sql<{ readonly head: number }>`
      SELECT COALESCE(MAX(sequence), 0) AS head FROM orchestration_events
    `,
  eventAgePage: (afterSequence: number, throughSequence: number, limit: number) =>
    sql<{ readonly sequence: number; readonly occurredAt: string }>`
      SELECT sequence, occurred_at AS "occurredAt"
      FROM orchestration_events
      WHERE sequence > ${afterSequence} AND sequence <= ${throughSequence}
      ORDER BY sequence ASC
      LIMIT ${limit}
    `,
  // Kept below the cutoff:
  // - the newest event of each (stream, type): stream_version continuity,
  //   hasEventAfter(thread.created) and the per-thread detail watermark;
  // - the turn-start-requested event of a pending conditional delivery,
  //   which the provider reactor re-reads to (re)drive it;
  // - reverts/deletions the bootstrap attachment cleanup has not replayed.
  // The last sequence of the next page of candidates: the page is bounded by
  // rows inspected, not by rows that turn out to be deletable.
  eventPageEnd: (afterSequence: number, cutoff: number, limit: number) =>
    sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events
      WHERE sequence > ${afterSequence} AND sequence <= ${cutoff}
      ORDER BY sequence ASC
      LIMIT 1 OFFSET ${limit - 1}
    `,
  deleteEventRange: (afterSequence: number, throughSequence: number, cleanupCursor: number) =>
    sql<{ readonly sequence: number; readonly bytes: number }>`
      DELETE FROM orchestration_events
      WHERE sequence IN (
        SELECT candidate.sequence
        FROM orchestration_events AS candidate
        WHERE candidate.sequence > ${afterSequence}
          AND candidate.sequence <= ${throughSequence}
          AND EXISTS (
            SELECT 1 FROM orchestration_events AS newer
            WHERE newer.aggregate_kind = candidate.aggregate_kind
              AND newer.stream_id = candidate.stream_id
              AND newer.event_type = candidate.event_type
              AND newer.sequence > candidate.sequence
          )
          AND candidate.sequence NOT IN (
            SELECT requested_sequence FROM orchestration_turn_deliveries
            WHERE status IN ('accepted', 'attempted', 'unresolved')
          )
          AND NOT (
            candidate.event_type IN ('thread.reverted', 'thread.deleted')
            AND candidate.sequence > ${cleanupCursor}
          )
      )
      RETURNING sequence, octet_length(payload_json) AS bytes
    `,
  oldestReceipts: (limit: number) =>
    sql<{ readonly commandId: string; readonly acceptedAt: string }>`
      SELECT command_id AS "commandId", accepted_at AS "acceptedAt"
      FROM orchestration_command_receipts
      ORDER BY result_sequence ASC
      LIMIT ${limit}
    `,
  deleteReceipts: (commandIds: ReadonlyArray<string>, cutoffIso: string) =>
    sql`
      DELETE FROM orchestration_command_receipts
      WHERE ${sql.in("command_id", commandIds)} AND accepted_at < ${cutoffIso}
    `,
  nextActivityThread: (afterThreadId: string) =>
    sql<{ readonly threadId: string }>`
      SELECT thread_id AS "threadId" FROM projection_thread_activities
      WHERE thread_id > ${afterThreadId}
      ORDER BY thread_id ASC
      LIMIT 1
    `,
  threadRetired: (threadId: string) =>
    sql<{ readonly retired: number }>`
      SELECT (archived_at IS NOT NULL OR deleted_at IS NOT NULL) AS retired
      FROM projection_threads WHERE thread_id = ${threadId}
    `,
  nthNewestActivity: (threadId: string, offset: number) =>
    sql<{ readonly createdAt: string }>`
      SELECT created_at AS "createdAt" FROM projection_thread_activities
      WHERE thread_id = ${threadId}
      ORDER BY created_at DESC, activity_id DESC
      LIMIT 1 OFFSET ${offset}
    `,
  anyActivityBefore: (threadId: string, boundary: string) =>
    sql<{ readonly one: number }>`
      SELECT 1 AS one FROM projection_thread_activities
      WHERE thread_id = ${threadId} AND created_at < ${boundary}
      LIMIT 1
    `,
  nextKind: (threadId: string, afterKind: string) =>
    sql<{ readonly kind: string }>`
      SELECT kind FROM projection_thread_activities
      WHERE thread_id = ${threadId} AND kind > ${afterKind}
      ORDER BY kind ASC
      LIMIT 1
    `,
  newestOfKind: (threadId: string, kind: string) =>
    sql<{ readonly activityId: string }>`
      SELECT activity_id AS "activityId" FROM projection_thread_activities
      WHERE thread_id = ${threadId} AND kind = ${kind}
      ORDER BY created_at DESC, activity_id DESC
      LIMIT 1
    `,
  // The initial key (boundary, '') selects created_at < boundary.
  oldActivityPage: (
    threadId: string,
    beforeCreatedAt: string,
    beforeActivityId: string,
    limit: number,
  ) =>
    sql<{
      readonly activityId: string;
      readonly kind: string;
      readonly createdAt: string;
      readonly taskId: TaskId | null;
    }>`
      SELECT activity_id AS "activityId", kind, created_at AS "createdAt",
        json_extract(payload_json, '$.taskId') AS "taskId"
      FROM projection_thread_activities
      WHERE thread_id = ${threadId}
        AND (created_at, activity_id) < (${beforeCreatedAt}, ${beforeActivityId})
      ORDER BY created_at DESC, activity_id DESC
      LIMIT ${limit}
    `,
  deleteActivities: (activityIds: ReadonlyArray<string>, boundary: string) =>
    sql<{ readonly bytes: number }>`
      DELETE FROM projection_thread_activities
      WHERE ${sql.in("activity_id", activityIds)} AND created_at < ${boundary}
      RETURNING octet_length(payload_json) AS bytes
    `,
  nextTask: (threadId: string, afterTaskId: TaskId | null) =>
    afterTaskId === null
      ? sql<{ readonly taskId: TaskId }>`
          SELECT json_extract(payload_json, '$.taskId') AS "taskId"
          FROM projection_thread_activities
          WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.taskId') IS NOT NULL
          ORDER BY json_extract(payload_json, '$.taskId') ASC
          LIMIT 1
        `
      : sql<{ readonly taskId: TaskId }>`
          SELECT json_extract(payload_json, '$.taskId') AS "taskId"
          FROM projection_thread_activities
          WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.taskId') > ${afterTaskId}
          ORDER BY json_extract(payload_json, '$.taskId') ASC
          LIMIT 1
        `,
  newestTaskRow: (threadId: string, taskId: TaskId) =>
    sql<{ readonly createdAt: string }>`
      SELECT created_at AS "createdAt" FROM projection_thread_activities
      WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.taskId') = ${taskId}
      ORDER BY created_at DESC
      LIMIT 1
    `,
  // Bounded size check: counts at most `cap + 1` rows of the task.
  taskRowCount: (threadId: string, taskId: TaskId, cap: number) =>
    sql<{ readonly rows: number }>`
      SELECT COUNT(*) AS rows FROM (
        SELECT 1 FROM projection_thread_activities
        WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.taskId') = ${taskId}
        LIMIT ${cap + 1}
      )
    `,
  deleteTaskRows: (threadId: string, taskId: TaskId) =>
    sql<{ readonly bytes: number }>`
      DELETE FROM projection_thread_activities
      WHERE thread_id = ${threadId} AND json_extract(payload_json, '$.taskId') = ${taskId}
      RETURNING octet_length(payload_json) AS bytes
    `,
  deleteDeliveryPage: (cutoffIso: string, limit: number) =>
    sql<{ readonly commandId: string }>`
      DELETE FROM orchestration_turn_deliveries
      WHERE command_id IN (
        SELECT command_id FROM orchestration_turn_deliveries
        WHERE status IN ('started', 'failed', 'abandoned') AND resolved_at < ${cutoffIso}
        LIMIT ${limit}
      )
      RETURNING command_id AS "commandId"
    `,
});

const isoBefore = (nowMs: number, days: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(nowMs - Math.max(0, days) * DAY_MS));

/**
 * One retention pass. `nowMs` is injected so tests can age rows without
 * touching the clock.
 */
export const runRetentionPass = Effect.fn("OrchestrationRetention.runPass")(function* (
  config: RetentionConfig,
  nowMs: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const statements = retentionStatements(sql);
  const batchSize = Math.max(1, Math.floor(config.batchSize));
  const pause =
    config.batchPauseMs > 0 ? Effect.sleep(Duration.millis(config.batchPauseMs)) : Effect.yieldNow;
  const nowIso = DateTime.formatIso(DateTime.makeUnsafe(nowMs));
  // Many small statements in a row would still hold the event loop, so the
  // activity walk also pauses after every `workSliceMs` of work.
  let lastPauseAt = yield* Clock.currentTimeMillis;
  const maybePause = Effect.gen(function* () {
    if ((yield* Clock.currentTimeMillis) - lastPauseAt < config.workSliceMs) return;
    yield* pause;
    lastPauseAt = yield* Clock.currentTimeMillis;
  });

  // ---- orchestration_events ------------------------------------------------
  const pruneEvents = Effect.gen(function* () {
    const states = yield* statements.projectorCursors();
    const cursorByProjector = new Map(
      states.map((state) => [state.projector, Number(state.lastAppliedSequence)]),
    );
    const projectorCursors = Object.values(ORCHESTRATION_PROJECTOR_NAMES).map((name) =>
      cursorByProjector.get(name),
    );
    const prunedThrough = yield* readEventsPrunedThrough(sql);
    // Every projector must have applied an event before it may be pruned.
    if (projectorCursors.some((cursor) => cursor === undefined)) {
      return {
        deleted: 0,
        bytes: 0,
        batches: 0,
        prunedThrough,
        skipped: "projector-state-missing",
      };
    }
    const minProjectorCursor = Math.min(...(projectorCursors as ReadonlyArray<number>));
    const [{ head } = { head: 0 }] = yield* statements.eventHead();
    const hardCutoff = Math.min(minProjectorCursor, Number(head) - config.eventsKeepRecent);
    if (hardCutoff <= 0) return { deleted: 0, bytes: 0, batches: 0, prunedThrough };

    // Age boundary: the first event (in sequence order) that is not yet old.
    // occurred_at comes from command timestamps, so it is only roughly
    // monotonic; stopping at the first young event never prunes a young one.
    const ageCutoffIso = isoBefore(nowMs, config.eventsMaxAgeDays);
    let cutoff = hardCutoff;
    let scanCursor = 0;
    scan: while (true) {
      const rows = yield* statements.eventAgePage(scanCursor, hardCutoff, AGE_SCAN_CHUNK);
      for (const row of rows) {
        if (row.occurredAt >= ageCutoffIso) {
          cutoff = Number(row.sequence) - 1;
          break scan;
        }
      }
      const last = rows.at(-1);
      if (last === undefined || rows.length < AGE_SCAN_CHUNK) break;
      scanCursor = Number(last.sequence);
      yield* pause;
    }
    if (cutoff <= 0) return { deleted: 0, bytes: 0, batches: 0, prunedThrough };

    // Publish the floor before deleting. WS resume re-checks it after reading
    // (ReplayGuard), so correctness does not depend on the grace period; the
    // grace only spares in-flight replays a snapshot fallback.
    if (cutoff > prunedThrough) {
      yield* raiseEventsPrunedThrough(sql, cutoff, nowIso);
      if (config.floorGraceMs > 0) yield* Effect.sleep(Duration.millis(config.floorGraceMs));
    }
    const cleanupCursor = cursorByProjector.get(ATTACHMENT_CLEANUP_PROJECTOR) ?? 0;

    let deleted = 0;
    let bytes = 0;
    let batches = 0;
    let cursor = 0;
    while (true) {
      // Inspect at most batchSize candidates per page and advance past them
      // whether or not any were deletable, so long protected runs cost one
      // bounded page each.
      const page = yield* sql.withTransaction(
        Effect.gen(function* () {
          const [end] = yield* statements.eventPageEnd(cursor, cutoff, batchSize);
          const through = end === undefined ? cutoff : Number(end.sequence);
          const rows = yield* statements.deleteEventRange(cursor, through, cleanupCursor);
          return { through, rows, last: end === undefined };
        }),
      );
      batches += 1;
      for (const row of page.rows) {
        deleted += 1;
        bytes += Number(row.bytes ?? 0);
      }
      cursor = page.through;
      if (page.last) break;
      yield* pause;
    }
    return { deleted, bytes, batches, prunedThrough: Math.max(prunedThrough, cutoff) };
  });

  // ---- orchestration_command_receipts -------------------------------------
  // A receipt only deduplicates a retried command id. Clients retry within
  // seconds and the mailbox within its own restart, so a window of days is
  // ample. Conditional starts keep a second guard: their delivery row (kept
  // at least as long) makes a re-dispatch fail on its primary key.
  const pruneReceipts = Effect.gen(function* () {
    const cutoffIso = isoBefore(nowMs, config.receiptsMaxAgeDays);
    let deleted = 0;
    let batches = 0;
    while (true) {
      const { rows, stale } = yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* statements.oldestReceipts(batchSize);
          const stale: string[] = [];
          for (const row of rows) {
            if (row.acceptedAt >= cutoffIso) break;
            stale.push(row.commandId);
          }
          if (stale.length > 0) yield* statements.deleteReceipts(stale, cutoffIso);
          return { rows, stale };
        }),
      );
      if (stale.length > 0) {
        deleted += stale.length;
        batches += 1;
      }
      if (stale.length < rows.length || rows.length < batchSize) break;
      yield* pause;
    }
    return { deleted, batches };
  });

  // ---- projection_thread_activities ---------------------------------------
  // Every statement here is an index seek or a page of at most batchSize rows;
  // threads, kinds and tasks are enumerated by skip-scans over the retention
  // indexes, never by grouping a whole thread or table.
  const pruneActivities = Effect.gen(function* () {
    const ageCutoffIso = isoBefore(nowMs, config.activitiesMaxAgeDays);
    let deleted = 0;
    let bytes = 0;
    let batches = 0;
    let tasksOverCap = 0;
    // A thread without a projection row is gone; treat it as retired.
    const isRetired = (id: string) =>
      statements
        .threadRetired(id)
        .pipe(Effect.map(([state]) => state === undefined || Number(state.retired) !== 0));
    const tally = (removed: ReadonlyArray<{ readonly bytes: number }>) =>
      Effect.gen(function* () {
        if (removed.length > 0) {
          deleted += removed.length;
          batches += 1;
          for (const row of removed) bytes += Number(row.bytes ?? 0);
        }
        yield* maybePause;
      });

    let threadId = "";
    while (true) {
      const [next] = yield* statements.nextActivityThread(threadId);
      if (next === undefined) break;
      threadId = next.threadId;
      yield* maybePause;
      const retired = yield* isRetired(threadId);
      let threadStateChanged = false;
      const keep = retired
        ? config.archivedActivitiesKeepPerThread
        : config.activitiesKeepPerThread;
      let boundary = ageCutoffIso;
      if (keep > 0) {
        const [nth] = yield* statements.nthNewestActivity(threadId, keep - 1);
        if (nth === undefined) continue;
        if (nth.createdAt < boundary) boundary = nth.createdAt;
      }
      if ((yield* statements.anyActivityBefore(threadId, boundary)).length === 0) continue;

      // Activities without a task id keep their newest row per kind (latest
      // context-window usage, delivery evidence, ...).
      const newestOfKind = new Set<string>();
      let kind = "";
      while (true) {
        const [nextKind] = yield* statements.nextKind(threadId, kind);
        if (nextKind === undefined) break;
        kind = nextKind.kind;
        const [newest] = yield* statements.newestOfKind(threadId, kind);
        if (newest !== undefined) newestOfKind.add(newest.activityId);
        yield* maybePause;
      }

      // Walk old rows newest-first, one page per transaction. Rows are judged
      // on the state they have when deleted: provider updates rewrite rows in
      // place under stable ids, and a refreshed row is young again.
      // Rows with a task id are left to the task rule below.
      let beforeCreatedAt = boundary;
      let beforeActivityId = "";
      while (true) {
        const page = yield* sql.withTransaction(
          Effect.gen(function* () {
            // The boundary depends on whether the thread is retired; if that
            // changed since it was computed, leave the thread for the next pass.
            if ((yield* isRetired(threadId)) !== retired) {
              return { rows: [], removed: [], stateChanged: true };
            }
            const rows = yield* statements.oldActivityPage(
              threadId,
              beforeCreatedAt,
              beforeActivityId,
              batchSize,
            );
            const doomed = rows
              .filter(
                (row) =>
                  row.taskId === null &&
                  !PROTECTED_ACTIVITY_KINDS.has(row.kind) &&
                  !newestOfKind.has(row.activityId),
              )
              .map((row) => row.activityId);
            const removed =
              doomed.length === 0 ? [] : yield* statements.deleteActivities(doomed, boundary);
            return { rows, removed, stateChanged: false };
          }),
        );
        yield* tally(page.removed);
        if (page.stateChanged) {
          threadStateChanged = true;
          break;
        }
        const last = page.rows.at(-1);
        if (last === undefined || page.rows.length < batchSize) break;
        beforeCreatedAt = last.createdAt;
        beforeActivityId = last.activityId;
      }

      // Task lifecycle, progress and usage rows (everything carrying a task
      // id: task.*, tool.progress, and the stable-id progress/usage rows) are
      // all-or-nothing per task. The client fold and title recovery need the
      // whole sequence, so a task is dropped only on a retired thread and only
      // when every one of its rows is older than the boundary, and then
      // entirely, in one transaction.
      if (retired && !threadStateChanged) {
        let taskId: TaskId | null = null;
        while (true) {
          const [nextTask]: ReadonlyArray<{ readonly taskId: TaskId }> = yield* statements.nextTask(
            threadId,
            taskId,
          );
          if (nextTask === undefined) break;
          taskId = nextTask.taskId;
          const currentTask = taskId;
          // Everything that makes the task eligible is re-checked inside the
          // deleting transaction: the thread is still retired (it may have
          // been unarchived since), every row is still old, and the task is
          // small enough to delete in one bounded statement.
          const outcome = yield* sql.withTransaction(
            Effect.gen(function* () {
              if (!(yield* isRetired(threadId))) return "thread-live" as const;
              const [newest] = yield* statements.newestTaskRow(threadId, currentTask);
              if (newest === undefined || newest.createdAt >= boundary) return "young" as const;
              const [count] = yield* statements.taskRowCount(
                threadId,
                currentTask,
                config.taskRowCap,
              );
              if (Number(count?.rows ?? 0) > config.taskRowCap) return "over-cap" as const;
              return yield* statements.deleteTaskRows(threadId, currentTask);
            }),
          );
          if (outcome === "thread-live") break;
          if (outcome === "over-cap") {
            tasksOverCap += 1;
            yield* Effect.logWarning("retention: task left intact (more rows than the cap)", {
              threadId,
              taskId: currentTask,
              taskRowCap: config.taskRowCap,
            });
            yield* maybePause;
            continue;
          }
          yield* tally(outcome === "young" ? [] : outcome);
        }
      }
    }
    return { deleted, bytes, batches, tasksOverCap };
  });

  // ---- orchestration_turn_deliveries --------------------------------------
  // Pending records (accepted / attempted / unresolved) are what
  // reconciliation and the idle check read; they are never pruned. Terminal
  // ones are kept at least as long as receipts (see pruneReceipts).
  const pruneDeliveries = Effect.gen(function* () {
    const cutoffIso = isoBefore(
      nowMs,
      Math.max(config.deliveriesMaxAgeDays, config.receiptsMaxAgeDays),
    );
    let deleted = 0;
    let batches = 0;
    while (true) {
      const rows = yield* statements.deleteDeliveryPage(cutoffIso, batchSize);
      deleted += rows.length;
      batches += 1;
      if (rows.length < batchSize) break;
      yield* pause;
    }
    return { deleted, batches };
  });

  const events = yield* pruneEvents;
  const receipts = yield* pruneReceipts;
  const activities = yield* pruneActivities;
  const deliveries = yield* pruneDeliveries;
  const reclaimed = yield* reclaimFreePages(sql, {
    pagesPerBatch: config.vacuumPagesPerBatch,
    pause,
  });
  return {
    events,
    receipts,
    activities,
    deliveries,
    ...reclaimed,
  } satisfies RetentionPassResult;
});

/**
 * Runs a pass once the server is activated (projections are caught up: the
 * engine bootstraps them during its own construction, which this layer waits
 * for) and then every `intervalMinutes`.
 */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* loadRetentionConfig;
    // Depend on the engine only for ordering: its construction replays the
    // projections, so the first pass sees caught-up projector cursors.
    yield* OrchestrationEngineService;
    if (!config.enabled) {
      yield* Effect.logInfo("retention: disabled (T3CODE_RETENTION_ENABLED=false)");
      return;
    }
    const sql = yield* SqlClient.SqlClient;
    yield* ensureOrchestrationTurnDeliveriesSchema(sql);
    yield* ensureRetentionIndexes(sql);

    const pass = Effect.gen(function* () {
      const startedAt = yield* Clock.currentTimeMillis;
      const result = yield* runRetentionPass(config, startedAt).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      yield* Effect.logInfo("retention: pass finished", {
        durationMs: (yield* Clock.currentTimeMillis) - startedAt,
        eventsDeleted: result.events.deleted,
        eventPayloadBytes: result.events.bytes,
        eventBatches: result.events.batches,
        eventsPrunedThrough: result.events.prunedThrough,
        ...(result.events.skipped ? { eventsSkipped: result.events.skipped } : {}),
        receiptsDeleted: result.receipts.deleted,
        activitiesDeleted: result.activities.deleted,
        activityPayloadBytes: result.activities.bytes,
        activityBatches: result.activities.batches,
        tasksOverCap: result.activities.tasksOverCap,
        deliveriesDeleted: result.deliveries.deleted,
        reclaimedBytes: result.reclaimedBytes,
        freeBytes: result.remainingFreeBytes,
        ...(result.autoVacuum !== 2 && result.remainingFreeBytes > 0
          ? {
              note: "auto_vacuum is not incremental; free pages are reused but the file only shrinks after the one-time startup VACUUM (T3CODE_RETENTION_STARTUP_VACUUM)",
            }
          : {}),
      });
    }).pipe(
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause),
        (cause) => Effect.logWarning("retention: pass failed", { cause: Cause.pretty(cause) }),
      ),
    );

    yield* forkParked(
      pass.pipe(
        Effect.repeat(Schedule.spaced(Duration.minutes(Math.max(1, config.intervalMinutes)))),
      ),
    );
  }),
);
