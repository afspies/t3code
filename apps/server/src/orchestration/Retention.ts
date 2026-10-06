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
import { taskTitleWhitespace } from "../persistence/Layers/ProjectionThreadActivities.ts";
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

/** Kinds the client's subagent fold and task-title recovery read per task id. */
const TASK_ACTIVITY_KINDS = [
  "task.started",
  "task.progress",
  "task.updated",
  "task.completed",
  "tool.progress",
] as const;

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
  };
  readonly deliveries: { readonly deleted: number; readonly batches: number };
  readonly reclaimedBytes: number;
  readonly remainingFreeBytes: number;
  readonly autoVacuum: number;
}

/** Index that makes "is there a newer event of this type in this stream" a seek. */
export const ensureRetentionIndexes = (sql: SqlClient.SqlClient) =>
  sql`
    CREATE INDEX IF NOT EXISTS idx_orch_events_retention_stream_type
    ON orchestration_events(aggregate_kind, stream_id, event_type, sequence)
  `.pipe(Effect.asVoid);

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
  const batchSize = Math.max(1, Math.floor(config.batchSize));
  const pause =
    config.batchPauseMs > 0 ? Effect.sleep(Duration.millis(config.batchPauseMs)) : Effect.yieldNow;
  const nowIso = DateTime.formatIso(DateTime.makeUnsafe(nowMs));

  // ---- orchestration_events ------------------------------------------------
  const pruneEvents = Effect.gen(function* () {
    const states = yield* sql<{ readonly projector: string; readonly lastAppliedSequence: number }>`
      SELECT projector, last_applied_sequence AS "lastAppliedSequence" FROM projection_state
    `;
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
    const [{ head } = { head: 0 }] = yield* sql<{ readonly head: number }>`
      SELECT COALESCE(MAX(sequence), 0) AS head FROM orchestration_events
    `;
    const hardCutoff = Math.min(minProjectorCursor, Number(head) - config.eventsKeepRecent);
    if (hardCutoff <= 0) return { deleted: 0, bytes: 0, batches: 0, prunedThrough };

    // Age boundary: the first event (in sequence order) that is not yet old.
    // occurred_at comes from command timestamps, so it is only roughly
    // monotonic; stopping at the first young event never prunes a young one.
    const ageCutoffIso = isoBefore(nowMs, config.eventsMaxAgeDays);
    let cutoff = hardCutoff;
    let scanCursor = 0;
    scan: while (true) {
      const rows = yield* sql<{ readonly sequence: number; readonly occurredAt: string }>`
        SELECT sequence, occurred_at AS "occurredAt"
        FROM orchestration_events
        WHERE sequence > ${scanCursor} AND sequence <= ${hardCutoff}
        ORDER BY sequence ASC
        LIMIT ${AGE_SCAN_CHUNK}
      `;
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

    // Publish the floor before deleting so a WS resume that checks it from now
    // on gets a snapshot; the grace period lets a replay that passed the check
    // just before finish reading.
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
      // Kept below the cutoff:
      // - the newest event of each (stream, type): stream_version continuity,
      //   hasEventAfter(thread.created) and the per-thread detail watermark;
      // - the turn-start-requested event of a pending conditional delivery,
      //   which the provider reactor re-reads to (re)drive it;
      // - reverts/deletions the bootstrap attachment cleanup has not replayed.
      const rows = yield* sql<{ readonly sequence: number; readonly bytes: number }>`
        DELETE FROM orchestration_events
        WHERE sequence IN (
          SELECT candidate.sequence
          FROM orchestration_events AS candidate
          WHERE candidate.sequence > ${cursor}
            AND candidate.sequence <= ${cutoff}
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
          ORDER BY candidate.sequence ASC
          LIMIT ${batchSize}
        )
        RETURNING sequence, octet_length(payload_json) AS bytes
      `;
      batches += 1;
      for (const row of rows) {
        deleted += 1;
        bytes += Number(row.bytes ?? 0);
        cursor = Math.max(cursor, Number(row.sequence));
      }
      if (rows.length < batchSize) break;
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
          const rows = yield* sql<{ readonly commandId: string; readonly acceptedAt: string }>`
            SELECT command_id AS "commandId", accepted_at AS "acceptedAt"
            FROM orchestration_command_receipts
            ORDER BY result_sequence ASC
            LIMIT ${batchSize}
          `;
          const stale: string[] = [];
          for (const row of rows) {
            if (row.acceptedAt >= cutoffIso) break;
            stale.push(row.commandId);
          }
          if (stale.length > 0) {
            yield* sql`
              DELETE FROM orchestration_command_receipts
              WHERE ${sql.in("command_id", stale)} AND accepted_at < ${cutoffIso}
            `;
          }
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
  const pruneActivities = Effect.gen(function* () {
    const ageCutoffIso = isoBefore(nowMs, config.activitiesMaxAgeDays);
    // A thread without a projection row is gone; treat it as retired.
    const threadRows = yield* sql<{ readonly threadId: string; readonly retired: number }>`
      SELECT activities.thread_id AS "threadId",
        CASE
          WHEN threads.thread_id IS NULL
            OR threads.archived_at IS NOT NULL
            OR threads.deleted_at IS NOT NULL
          THEN 1 ELSE 0
        END AS retired
      FROM (SELECT DISTINCT thread_id FROM projection_thread_activities) AS activities
      LEFT JOIN projection_threads AS threads ON threads.thread_id = activities.thread_id
    `;
    let deleted = 0;
    let bytes = 0;
    let batches = 0;
    for (const { threadId, retired } of threadRows) {
      const keep =
        Number(retired) === 0
          ? config.activitiesKeepPerThread
          : config.archivedActivitiesKeepPerThread;
      let boundary = ageCutoffIso;
      if (keep > 0) {
        const [nth] = yield* sql<{ readonly createdAt: string }>`
          SELECT created_at AS "createdAt" FROM projection_thread_activities
          WHERE thread_id = ${threadId}
          ORDER BY created_at DESC, activity_id DESC
          LIMIT 1 OFFSET ${keep - 1}
        `;
        if (nth === undefined) continue;
        if (nth.createdAt < boundary) boundary = nth.createdAt;
      }
      const [candidate] = yield* sql<{ readonly one: number }>`
        SELECT 1 AS one FROM projection_thread_activities
        WHERE thread_id = ${threadId} AND created_at < ${boundary}
        LIMIT 1
      `;
      if (candidate === undefined) continue;

      // Task lifecycles are kept per task id, not per kind: for each task the
      // first row (it fixes the agent roster classification the client fold
      // makes), the newest row of each task kind (terminal evidence and last
      // state, in their original order) and the newest titled row (what title
      // recovery reads). Keeping the newest row per kind across tasks would
      // pair one task's start with another's completion.
      const taskKeep = yield* sql<{ readonly activityId: string }>`
        SELECT activity_id AS "activityId" FROM (
          SELECT activity_id, titled,
            ROW_NUMBER() OVER (
              PARTITION BY task_id ORDER BY created_at ASC, activity_id ASC
            ) AS first_rank,
            ROW_NUMBER() OVER (
              PARTITION BY task_id, kind ORDER BY created_at DESC, activity_id DESC
            ) AS kind_rank,
            ROW_NUMBER() OVER (
              PARTITION BY task_id, titled
              ORDER BY sequence DESC, created_at DESC, activity_id DESC
            ) AS title_rank
          FROM (
            SELECT activity_id, kind, created_at, sequence,
              json_extract(payload_json, '$.taskId') AS task_id,
              kind IN ('task.started', 'task.progress') AND length(trim(
                CASE
                  WHEN json_type(payload_json, '$.title') = 'text'
                    THEN json_extract(payload_json, '$.title')
                  WHEN kind = 'task.started' AND json_type(payload_json, '$.detail') = 'text'
                    THEN json_extract(payload_json, '$.detail')
                  ELSE ''
                END,
                ${taskTitleWhitespace}
              )) > 0 AS titled
            FROM projection_thread_activities
            WHERE thread_id = ${threadId} AND ${sql.in("kind", TASK_ACTIVITY_KINDS)}
          ) AS task_rows
          WHERE task_id IS NOT NULL
        ) AS ranked
        WHERE first_rank = 1 OR kind_rank = 1 OR (titled = 1 AND title_rank = 1)
      `;
      const keptTaskRows = new Set(taskKeep.map((row) => row.activityId));
      // Other activities keep their newest row per kind (latest context-window
      // usage, delivery evidence). Kinds present in the retained window need
      // nothing older.
      const retainedKinds = yield* sql<{ readonly kind: string }>`
        SELECT DISTINCT kind FROM projection_thread_activities
        WHERE thread_id = ${threadId} AND created_at >= ${boundary}
      `;
      const seenKinds = new Set(retainedKinds.map((row) => row.kind));
      // Walk old rows newest-first with a keyset cursor. Each batch selects
      // and deletes in one transaction: provider updates rewrite rows in
      // place under stable ids (task progress), so a row is judged on the
      // state it has when it is deleted. A refreshed row is young again and
      // drops out of the walk. The initial key (boundary, '') selects
      // created_at < boundary.
      let afterCreatedAt = boundary;
      let afterActivityId = "";
      while (true) {
        const batch = yield* sql.withTransaction(
          Effect.gen(function* () {
            const rows = yield* sql<{
              readonly activityId: string;
              readonly kind: string;
              readonly createdAt: string;
              readonly taskId: string | number | null;
            }>`
              SELECT activity_id AS "activityId", kind, created_at AS "createdAt",
                CASE WHEN ${sql.in("kind", TASK_ACTIVITY_KINDS)}
                  THEN json_extract(payload_json, '$.taskId')
                END AS "taskId"
              FROM projection_thread_activities
              WHERE thread_id = ${threadId}
                AND (
                  created_at < ${afterCreatedAt}
                  OR (created_at = ${afterCreatedAt} AND activity_id < ${afterActivityId})
                )
              ORDER BY created_at DESC, activity_id DESC
              LIMIT ${batchSize}
            `;
            const doomed: string[] = [];
            for (const row of rows) {
              if (PROTECTED_ACTIVITY_KINDS.has(row.kind)) continue;
              if (row.taskId !== null && row.taskId !== undefined) {
                if (!keptTaskRows.has(row.activityId)) doomed.push(row.activityId);
                continue;
              }
              const firstOfKind = !seenKinds.has(row.kind);
              seenKinds.add(row.kind);
              if (!firstOfKind) doomed.push(row.activityId);
            }
            const removed =
              doomed.length === 0
                ? []
                : yield* sql<{ readonly bytes: number }>`
                    DELETE FROM projection_thread_activities
                    WHERE ${sql.in("activity_id", doomed)} AND created_at < ${boundary}
                    RETURNING octet_length(payload_json) AS bytes
                  `;
            return { rows, removed };
          }),
        );
        if (batch.removed.length > 0) {
          deleted += batch.removed.length;
          batches += 1;
          for (const row of batch.removed) bytes += Number(row.bytes ?? 0);
        }
        const last = batch.rows.at(-1);
        if (last === undefined || batch.rows.length < batchSize) break;
        afterCreatedAt = last.createdAt;
        afterActivityId = last.activityId;
        yield* pause;
      }
    }
    return { deleted, bytes, batches };
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
      const rows = yield* sql<{ readonly commandId: string }>`
        DELETE FROM orchestration_turn_deliveries
        WHERE command_id IN (
          SELECT command_id FROM orchestration_turn_deliveries
          WHERE status IN ('started', 'failed', 'abandoned')
            AND COALESCE(resolved_at, accepted_at) < ${cutoffIso}
          LIMIT ${batchSize}
        )
        RETURNING command_id AS "commandId"
      `;
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
