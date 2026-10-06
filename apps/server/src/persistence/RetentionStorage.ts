/**
 * Storage side of the fork's event-log retention (see RETENTION.md).
 *
 * Owns the retention configuration, the `orchestration_retention_state` table
 * that records how far the event log has been pruned, and the one-time startup
 * VACUUM that switches an existing database to incremental auto-vacuum. The
 * pruning policy itself lives in `../orchestration/Retention.ts`.
 *
 * Like `orchestration_turn_deliveries`, the state table is created with
 * `CREATE TABLE IF NOT EXISTS` and NOT through a numbered migration: Effect's
 * Migrator only runs ids above the latest applied one, so a fork migration
 * would either be skipped or collide with upstream's next id
 * (COMPANION-BUILD.md, "Why the table lives outside the migration ledger").
 *
 * @module RetentionStorage
 */
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { PersistenceSqlError, toPersistenceSqlError } from "./Errors.ts";

export const RetentionStartupVacuumMode = ["auto", "off", "force"] as const;
export type RetentionStartupVacuumMode = (typeof RetentionStartupVacuumMode)[number];

export interface RetentionConfig {
  /** Off switch for pruning, the hourly pass and the startup VACUUM. */
  readonly enabled: boolean;
  readonly intervalMinutes: number;
  /** Events older than this (and already applied by every projector) are pruned. */
  readonly eventsMaxAgeDays: number;
  /** The newest N events are never pruned, whatever their age. */
  readonly eventsKeepRecent: number;
  /** Command receipts (dispatch idempotency) older than this are pruned. */
  readonly receiptsMaxAgeDays: number;
  /** Activities older than this AND outside the newest N of their thread are pruned. */
  readonly activitiesMaxAgeDays: number;
  readonly activitiesKeepPerThread: number;
  /** The same N for archived and deleted threads. */
  readonly archivedActivitiesKeepPerThread: number;
  /** Terminal turn-delivery records older than this are pruned (never below the receipt window). */
  readonly deliveriesMaxAgeDays: number;
  /** Rows deleted per statement; each batch is its own short write transaction. */
  readonly batchSize: number;
  readonly batchPauseMs: number;
  /** Wait between publishing a new pruned-through floor and deleting below it. */
  readonly floorGraceMs: number;
  /** Free pages returned to the OS per incremental-vacuum transaction. */
  readonly vacuumPagesPerBatch: number;
  readonly startupVacuum: RetentionStartupVacuumMode;
}

export const RetentionConfigFromEnv = Config.all({
  enabled: Config.Boolean("T3CODE_RETENTION_ENABLED").pipe(Config.withDefault(true)),
  intervalMinutes: Config.Number("T3CODE_RETENTION_INTERVAL_MINUTES").pipe(Config.withDefault(60)),
  eventsMaxAgeDays: Config.Number("T3CODE_RETENTION_EVENTS_DAYS").pipe(Config.withDefault(3)),
  eventsKeepRecent: Config.Int("T3CODE_RETENTION_EVENTS_KEEP_RECENT").pipe(
    Config.withDefault(5_000),
  ),
  receiptsMaxAgeDays: Config.Number("T3CODE_RETENTION_RECEIPTS_DAYS").pipe(Config.withDefault(7)),
  activitiesMaxAgeDays: Config.Number("T3CODE_RETENTION_ACTIVITIES_DAYS").pipe(
    Config.withDefault(3),
  ),
  activitiesKeepPerThread: Config.Int("T3CODE_RETENTION_ACTIVITIES_KEEP_PER_THREAD").pipe(
    Config.withDefault(200),
  ),
  archivedActivitiesKeepPerThread: Config.Int(
    "T3CODE_RETENTION_ARCHIVED_ACTIVITIES_KEEP_PER_THREAD",
  ).pipe(Config.withDefault(0)),
  deliveriesMaxAgeDays: Config.Number("T3CODE_RETENTION_DELIVERIES_DAYS").pipe(
    Config.withDefault(30),
  ),
  batchSize: Config.Int("T3CODE_RETENTION_BATCH_SIZE").pipe(Config.withDefault(500)),
  batchPauseMs: Config.Int("T3CODE_RETENTION_BATCH_PAUSE_MS").pipe(Config.withDefault(10)),
  floorGraceMs: Config.Int("T3CODE_RETENTION_FLOOR_GRACE_MS").pipe(Config.withDefault(5_000)),
  vacuumPagesPerBatch: Config.Int("T3CODE_RETENTION_VACUUM_PAGES_PER_BATCH").pipe(
    Config.withDefault(512),
  ),
  startupVacuum: Config.Literals(
    RetentionStartupVacuumMode,
    "T3CODE_RETENTION_STARTUP_VACUUM",
  ).pipe(Config.withDefault("auto" as const)),
}) satisfies Config.Config<RetentionConfig>;

/** A bad retention variable is an operator error; fail startup loudly. */
export const loadRetentionConfig: Effect.Effect<RetentionConfig> =
  Effect.orDie(RetentionConfigFromEnv);

const allowPartialReplayConfig = Config.Boolean("T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY").pipe(
  Config.withDefault(false),
);

const EVENTS_PRUNED_THROUGH = "events_pruned_through";

export const ensureRetentionStateSchema = (sql: SqlClient.SqlClient) =>
  sql`
    CREATE TABLE IF NOT EXISTS orchestration_retention_state (
      name TEXT PRIMARY KEY,
      value INTEGER NOT NULL,
      updated_at TEXT NOT NULL
    )
  `.pipe(Effect.asVoid);

/**
 * SQL scalar for the pruned-through floor: every event with a sequence at or
 * below it may be missing (a few are deliberately kept), every event above it
 * is intact. Replaying from a cursor below the floor would silently skip
 * events, so callers must fall back to a snapshot instead.
 */
export const eventsPrunedThroughSql = (sql: SqlClient.SqlClient) =>
  sql`COALESCE((
    SELECT value FROM orchestration_retention_state WHERE name = ${EVENTS_PRUNED_THROUGH}
  ), 0)`;

export const readEventsPrunedThrough = (sql: SqlClient.SqlClient) =>
  sql<{ readonly value: number }>`SELECT ${eventsPrunedThroughSql(sql)} AS value`.pipe(
    Effect.map((rows) => Number(rows[0]?.value ?? 0)),
  );

/** Raise (never lower) the floor. Commit this before deleting anything below it. */
export const raiseEventsPrunedThrough = (sql: SqlClient.SqlClient, sequence: number, at: string) =>
  sql`
    INSERT INTO orchestration_retention_state (name, value, updated_at)
    VALUES (${EVENTS_PRUNED_THROUGH}, ${sequence}, ${at})
    ON CONFLICT (name) DO UPDATE SET
      value = MAX(value, excluded.value),
      updated_at = excluded.updated_at
  `.pipe(Effect.asVoid);

/**
 * Projection bootstrap replays each projector from its own cursor. A projector
 * whose cursor is below the floor (a projector added by a later upstream
 * release starts at 0) would rebuild from an incomplete log and silently
 * produce wrong state, so startup stops instead. Setting
 * T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY=true accepts the partial rebuild.
 */
export const assertProjectorsCoverPrunedEvents = Effect.fn("assertProjectorsCoverPrunedEvents")(
  function* (
    sql: SqlClient.SqlClient,
    cursors: ReadonlyArray<{ readonly projector: string; readonly lastAppliedSequence: number }>,
  ) {
    const prunedThrough = yield* readEventsPrunedThrough(sql).pipe(
      Effect.mapError(toPersistenceSqlError("ProjectionPipeline.bootstrap:readRetentionFloor")),
    );
    const behind = cursors.filter((cursor) => cursor.lastAppliedSequence < prunedThrough);
    if (behind.length === 0) return;
    const detail = `projectors ${behind
      .map((cursor) => `${cursor.projector}@${cursor.lastAppliedSequence}`)
      .join(", ")} would replay events that retention pruned through sequence ${prunedThrough}`;
    if (yield* Effect.orDie(allowPartialReplayConfig)) {
      yield* Effect.logWarning(`retention: partial projection replay allowed; ${detail}`);
      return;
    }
    return yield* new PersistenceSqlError({
      operation: "ProjectionPipeline.bootstrap:retentionFloor",
      detail: `${detail}. Restore a backup taken before pruning, or set T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY=true to rebuild from the retained events only.`,
    });
  },
);

export interface DatabasePageStats {
  readonly pageSize: number;
  readonly pageCount: number;
  readonly freelistCount: number;
  /** 0 = none, 1 = full, 2 = incremental. */
  readonly autoVacuum: number;
}

const pragmaNumber = (sql: SqlClient.SqlClient, pragma: string) =>
  sql
    .unsafe<Record<string, unknown>>(`PRAGMA ${pragma}`)
    .pipe(Effect.map((rows) => Number(Object.values(rows[0] ?? {})[0] ?? 0)));

export const readDatabasePageStats = (sql: SqlClient.SqlClient) =>
  Effect.all({
    pageSize: pragmaNumber(sql, "page_size"),
    pageCount: pragmaNumber(sql, "page_count"),
    freelistCount: pragmaNumber(sql, "freelist_count"),
    autoVacuum: pragmaNumber(sql, "auto_vacuum"),
  }) satisfies Effect.Effect<DatabasePageStats, unknown>;

const AUTO_VACUUM_INCREMENTAL = 2;
// "auto" converts only once pruning has left a meaningful amount of free space.
const STARTUP_VACUUM_MIN_FREE_BYTES = 64 * 1024 * 1024;
const STARTUP_VACUUM_MIN_FREE_FRACTION = 0.1;

/**
 * Runs before WAL mode and migrations. auto_vacuum can only change on an empty database or
 * through VACUUM, so this makes brand-new databases incremental from the start
 * and is a no-op (pending until the next VACUUM) on existing ones.
 */
export const prepareRetentionStorageBeforeMigrations = Effect.gen(function* () {
  const config = yield* loadRetentionConfig;
  if (!config.enabled) return;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA auto_vacuum = INCREMENTAL`;
});

/**
 * Runs after migrations, before any layer reads or serves the database.
 * Creates the retention state table and, when configured, performs the
 * one-time VACUUM that converts an existing database to incremental
 * auto-vacuum (afterwards free pages are returned with incremental_vacuum).
 * VACUUM rewrites the whole file and holds the only connection for its
 * duration, which is why it runs here and never while serving.
 */
export const prepareRetentionStorageAfterMigrations = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureRetentionStateSchema(sql);
  const config = yield* loadRetentionConfig;
  if (!config.enabled || config.startupVacuum === "off") return;

  const before = yield* readDatabasePageStats(sql);
  if (before.autoVacuum === AUTO_VACUUM_INCREMENTAL) return;
  const freeBytes = before.freelistCount * before.pageSize;
  const freeFraction = before.pageCount === 0 ? 0 : before.freelistCount / before.pageCount;
  const worthIt =
    freeBytes >= STARTUP_VACUUM_MIN_FREE_BYTES && freeFraction >= STARTUP_VACUUM_MIN_FREE_FRACTION;
  if (config.startupVacuum === "auto" && !worthIt) {
    if (freeBytes > 0) {
      yield* Effect.logInfo("retention: startup VACUUM skipped (too little free space)", {
        freeBytes,
        fileBytes: before.pageCount * before.pageSize,
      });
    }
    return;
  }

  yield* Effect.logInfo("retention: one-time VACUUM to enable incremental auto-vacuum", {
    fileBytes: before.pageCount * before.pageSize,
    freeBytes,
  });
  const startedAt = yield* Clock.currentTimeMillis;
  yield* sql`PRAGMA auto_vacuum = INCREMENTAL`;
  yield* sql`VACUUM`;
  const after = yield* readDatabasePageStats(sql);
  yield* Effect.logInfo("retention: startup VACUUM finished", {
    durationMs: (yield* Clock.currentTimeMillis) - startedAt,
    fileBytesBefore: before.pageCount * before.pageSize,
    fileBytesAfter: after.pageCount * after.pageSize,
    autoVacuum: after.autoVacuum,
  });
});

/**
 * Returns free pages to the filesystem on an incremental auto-vacuum database,
 * in short transactions. `PRAGMA incremental_vacuum(N)` reports one row per
 * freed page but no columns, so the sqlite client's run() path steps it once
 * and frees a single page; issuing `incremental_vacuum(1)` repeatedly frees
 * exactly one page per call under either stepping behaviour.
 */
export const reclaimFreePages = Effect.fn("reclaimFreePages")(function* (
  sql: SqlClient.SqlClient,
  options: { readonly pagesPerBatch: number; readonly pause: Effect.Effect<void> },
) {
  const before = yield* readDatabasePageStats(sql);
  if (before.autoVacuum !== AUTO_VACUUM_INCREMENTAL) {
    return {
      reclaimedBytes: 0,
      remainingFreeBytes: before.freelistCount * before.pageSize,
      autoVacuum: before.autoVacuum,
    };
  }
  let freelist = before.freelistCount;
  const pagesPerBatch = Math.max(1, Math.floor(options.pagesPerBatch));
  while (freelist > 0) {
    const pages = Math.min(freelist, pagesPerBatch);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        for (let index = 0; index < pages; index += 1) {
          yield* sql`PRAGMA incremental_vacuum(1)`;
        }
      }),
    );
    const next = yield* pragmaNumber(sql, "freelist_count");
    if (next >= freelist) break;
    freelist = next;
    if (freelist > 0) yield* options.pause;
  }
  const after = yield* readDatabasePageStats(sql);
  return {
    reclaimedBytes: Math.max(0, before.pageCount - after.pageCount) * after.pageSize,
    remainingFreeBytes: after.freelistCount * after.pageSize,
    autoVacuum: after.autoVacuum,
  };
});
