// @effect-diagnostics nodeBuiltinImport:off
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { expect } from "vite-plus/test";

// Test-only: the client's subagent fold, to prove pruning leaves it unchanged.
import { foldSubagentActivities } from "../../../../packages/client-runtime/src/state/subagentRuntime.ts";

import { ServerConfig } from "../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import {
  ensureOrchestrationTurnDeliveriesSchema,
  OrchestrationTurnDeliveryRepositoryLive,
} from "../persistence/Layers/OrchestrationTurnDeliveries.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../persistence/Layers/Sqlite.ts";
import {
  runServerStartupVacuum,
  sqliteTempDirectoryFor,
  type RetentionConfig,
} from "../persistence/RetentionStorage.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationTurnDeliveryRepository } from "../persistence/Services/OrchestrationTurnDeliveries.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { materializeIntactReplay } from "./ReplayGuard.ts";
import { ensureRetentionIndexes, retentionStatements, runRetentionPass } from "./Retention.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";

// Commands carry these timestamps as their events' occurred_at. The pass runs
// at PASS_NOW, so OLD rows are past every 3-day window and YOUNG rows are not.
const OLD = "2026-01-01T00:00:00.000Z";
const YOUNG = "2026-01-09T00:00:00.000Z";
const PASS_NOW = Date.parse("2026-01-10T00:00:00.000Z");
const at = (base: string, seconds: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(Date.parse(base) + seconds * 1_000));

const projectId = ProjectId.make("project-1");
const liveThread = ThreadId.make("thread-live");
const archivedThread = ThreadId.make("thread-archived");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" };
const BIG = "x".repeat(40_000);

const config: RetentionConfig = {
  enabled: true,
  intervalMinutes: 60,
  eventsMaxAgeDays: 3,
  eventsKeepRecent: 0,
  receiptsMaxAgeDays: 3,
  activitiesMaxAgeDays: 3,
  activitiesKeepPerThread: 4,
  archivedActivitiesKeepPerThread: 0,
  deliveriesMaxAgeDays: 3,
  batchSize: 4,
  batchPauseMs: 0,
  taskRowCap: 2_000,
  workSliceMs: 20,
  floorGraceMs: 0,
  vacuumPagesPerBatch: 64,
  startupVacuum: "off",
};

// One engine "process" over a database file; providing it twice is a restart.
function makeLayer(databasePath: string) {
  return Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
    ),
    OrchestrationProjectionSnapshotQueryLive,
    OrchestrationTurnDeliveryRepositoryLive,
  ).pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provideMerge(makeSqlitePersistenceLive(databasePath)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-retention-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
}

const system = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const snapshotQuery = yield* ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  const deliveries = yield* OrchestrationTurnDeliveryRepository;
  const receipts = yield* OrchestrationCommandReceiptRepository;
  return {
    engine,
    snapshotQuery,
    sql,
    deliveries,
    receipts,
    dispatch: (command: OrchestrationCommand) => engine.dispatch(command),
    shell: (threadId: ThreadId) =>
      snapshotQuery.getThreadShellById(threadId).pipe(Effect.map(Option.getOrThrow)),
    detail: (threadId: ThreadId) =>
      snapshotQuery.getThreadDetailById(threadId).pipe(Effect.map(Option.getOrThrow)),
    /** The raw projection row, so archived threads can be compared too. */
    threadRow: (threadId: ThreadId) =>
      sql`SELECT * FROM projection_threads WHERE thread_id = ${threadId}`.pipe(
        Effect.map((rows) => rows[0]),
      ),
    pass: (overrides: Partial<RetentionConfig> = {}) =>
      ensureRetentionIndexes(sql).pipe(
        Effect.andThen(runRetentionPass({ ...config, ...overrides }, PASS_NOW)),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
  };
});

type System = Effect.Success<typeof system>;

const boot = <A, E>(databasePath: string, body: (system: System) => Effect.Effect<A, E>) =>
  system.pipe(Effect.flatMap(body), Effect.provide(makeLayer(databasePath)));

const freshDatabasePath = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    directory: NodeOS.tmpdir(),
    prefix: "t3-retention-",
  });
  return NodePath.join(directory, "state.sqlite");
});

const seedThread = (system: System, threadId: ThreadId) =>
  system.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`cmd-create-${threadId}`),
    threadId,
    projectId,
    title: threadId,
    modelSelection,
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    runtimeMode: "full-access",
    branch: null,
    worktreePath: null,
    createdAt: OLD,
  });

const seedProject = (system: System) =>
  system.dispatch({
    type: "project.create",
    commandId: CommandId.make("cmd-project"),
    projectId,
    title: "Project",
    workspaceRoot: "/tmp/retention-project",
    defaultModelSelection: modelSelection,
    createdAt: OLD,
  });

const appendActivity = (
  system: System,
  threadId: ThreadId,
  id: string,
  kind: string,
  createdAt: string,
  payload: Record<string, unknown> = {},
) =>
  system.dispatch({
    type: "thread.activity.append",
    commandId: CommandId.make(`cmd-${id}`),
    threadId,
    activity: {
      id: EventId.make(id),
      tone: "info",
      kind,
      summary: kind,
      payload,
      turnId: null,
      createdAt,
    },
    createdAt,
  });

const sessionSet = (system: System, commandId: string, createdAt: string) =>
  system.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(commandId),
    threadId: liveThread,
    session: {
      threadId: liveThread,
      status: "ready",
      providerName: "codex",
      runtimeMode: "full-access",
      activeTurnId: null,
      lastError: null,
      updatedAt: createdAt,
    },
    createdAt,
  });

const conditionalStart = (system: System, commandId: string, createdAt: string) =>
  system.dispatch({
    type: "thread.turn.start",
    commandId: CommandId.make(commandId),
    threadId: liveThread,
    message: {
      messageId: MessageId.make(`msg-${commandId}`),
      role: "user",
      text: "automated",
      attachments: [],
    },
    runtimeMode: "full-access",
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    createdAt,
    onlyIfIdle: true,
  });

const activityIds = (system: System, threadId: ThreadId) =>
  system.sql<{ readonly id: string }>`
    SELECT activity_id AS id FROM projection_thread_activities
    WHERE thread_id = ${threadId} ORDER BY activity_id
  `.pipe(Effect.map((rows) => rows.map((row) => row.id)));

/**
 * History: the live thread gets an accepted (still pending) conditional start,
 * old activities with large payloads, an open user-input request, a resolved
 * approval and repeated session updates; an archived thread gets old
 * activities; then the live thread receives young activities.
 */
const seedHistory = (system: System) =>
  Effect.gen(function* () {
    yield* seedProject(system);
    yield* seedThread(system, liveThread);
    const pending = yield* conditionalStart(system, "cmd-pending-start", at(OLD, 1));
    // An ordinary start right after it supersedes the pending start's
    // turn-start-requested event, so only the delivery keep-rule protects it.
    yield* system.dispatch({
      type: "thread.turn.start",
      commandId: CommandId.make("cmd-ordinary-start"),
      threadId: liveThread,
      message: {
        messageId: MessageId.make("msg-ordinary"),
        role: "user",
        text: "manual",
        attachments: [],
      },
      runtimeMode: "full-access",
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      createdAt: at(OLD, 2),
    });
    let second = 10;
    for (let index = 0; index < 8; index += 1) {
      yield* appendActivity(
        system,
        liveThread,
        `old-tool-${index}`,
        "tool.completed",
        at(OLD, second++),
        {
          output: BIG,
        },
      );
    }
    yield* appendActivity(
      system,
      liveThread,
      "old-context-1",
      "context-window.updated",
      at(OLD, second++),
    );
    yield* appendActivity(
      system,
      liveThread,
      "old-context-2",
      "context-window.updated",
      at(OLD, second++),
    );
    yield* appendActivity(
      system,
      liveThread,
      "approval-req",
      "approval.requested",
      at(OLD, second++),
      {
        requestId: "approval-1",
      },
    );
    yield* appendActivity(
      system,
      liveThread,
      "approval-done",
      "approval.resolved",
      at(OLD, second++),
      {
        requestId: "approval-1",
      },
    );
    yield* appendActivity(
      system,
      liveThread,
      "input-req",
      "user-input.requested",
      at(OLD, second++),
      {
        requestId: "input-1",
      },
    );
    yield* sessionSet(system, "cmd-session-1", at(OLD, second++));
    yield* sessionSet(system, "cmd-session-2", at(OLD, second++));
    yield* appendActivity(system, liveThread, "old-task", "task.completed", at(OLD, second++));

    yield* seedThread(system, archivedThread);
    for (let index = 0; index < 5; index += 1) {
      yield* appendActivity(
        system,
        archivedThread,
        `arch-tool-${index}`,
        "tool.completed",
        at(OLD, second++),
        {
          output: BIG,
        },
      );
    }
    yield* appendActivity(
      system,
      archivedThread,
      "arch-context",
      "context-window.updated",
      at(OLD, second++),
    );
    yield* appendActivity(
      system,
      archivedThread,
      "arch-approval",
      "approval.requested",
      at(OLD, second++),
      {
        requestId: "approval-2",
      },
    );
    yield* system.dispatch({
      type: "thread.archive",
      commandId: CommandId.make("cmd-archive"),
      threadId: archivedThread,
    });

    for (let index = 0; index < 3; index += 1) {
      yield* appendActivity(
        system,
        liveThread,
        `young-tool-${index}`,
        "tool.completed",
        at(YOUNG, index),
      );
    }
    return { pendingSequence: pending.sequence };
  });

effectIt.layer(NodeServices.layer)("orchestration retention", (it) => {
  it.effect(
    "prunes old history in bounded batches while projections, keep-rules and restart survive",
    () =>
      Effect.gen(function* () {
        const databasePath = yield* freshDatabasePath;
        const before = yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            const { pendingSequence } = yield* seedHistory(system);
            return {
              pendingSequence,
              head: yield* system.engine.latestSequence,
              shell: yield* system.shell(liveThread),
              archivedRow: yield* system.threadRow(archivedThread),
              detail: yield* system.detail(liveThread),
            };
          }),
        );
        expect(before.shell.hasPendingUserInput).toBe(true);

        const prunedThrough = yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            const result = yield* system.pass();
            const { prunedThrough, deleted, batches } = result.events;

            // Everything before the first young event was eligible; at least
            // the large old activity events are gone, in bounded batches.
            expect(deleted).toBeGreaterThan(20);
            // Each page inspects at most batchSize candidates.
            expect(deleted).toBeLessThanOrEqual(batches * config.batchSize);
            expect(batches).toBeGreaterThanOrEqual(Math.ceil(deleted / config.batchSize));
            expect(prunedThrough).toBeGreaterThanOrEqual(before.pendingSequence);
            expect(prunedThrough).toBeLessThan(before.head);

            // Below the floor only the keep-set survives: the newest event of
            // each (stream, type) and the pending delivery's requested event.
            const survivors = yield* system.sql<{
              readonly sequence: number;
              readonly type: string;
              readonly superseded: number;
            }>`
              SELECT e.sequence, e.event_type AS type,
                EXISTS (
                  SELECT 1 FROM orchestration_events n
                  WHERE n.aggregate_kind = e.aggregate_kind AND n.stream_id = e.stream_id
                    AND n.event_type = e.event_type AND n.sequence > e.sequence
                ) AS superseded
              FROM orchestration_events e
              WHERE e.sequence <= ${prunedThrough}
            `;
            for (const survivor of survivors) {
              if (survivor.superseded === 1) {
                expect(survivor.sequence).toBe(before.pendingSequence);
                expect(survivor.type).toBe("thread.turn-start-requested");
              }
            }
            const pendingSurvivor = survivors.find(
              (row) => row.sequence === before.pendingSequence,
            );
            // Superseded by the ordinary start, yet kept for the pending delivery.
            expect(pendingSurvivor?.superseded).toBe(1);
            // Events above the floor are untouched.
            const [aboveFloor] = yield* system.sql<{ readonly above: number }>`
              SELECT COUNT(*) AS above FROM orchestration_events WHERE sequence > ${prunedThrough}
            `;
            expect(aboveFloor?.above).toBe(before.head - prunedThrough);

            // Live thread: newest 4 (3 young + old-task), protected request
            // kinds, and the newest old activity of each other kind.
            expect(yield* activityIds(system, liveThread)).toEqual(
              [
                "approval-done",
                "approval-req",
                "input-req",
                "old-context-2",
                "old-task",
                "young-tool-0",
                "young-tool-1",
                "young-tool-2",
              ].toSorted(),
            );
            // Archived thread keeps no window, only protected and newest-of-kind.
            expect(yield* activityIds(system, archivedThread)).toEqual([
              "arch-approval",
              "arch-context",
              "arch-tool-4",
            ]);
            expect(result.activities.deleted).toBe(9 + 4);

            // Projections are authoritative: the shells do not move.
            expect(yield* system.shell(liveThread)).toEqual(before.shell);
            expect(yield* system.threadRow(archivedThread)).toEqual(before.archivedRow);
            const detail = yield* system.detail(liveThread);
            expect(detail.messages).toEqual(before.detail.messages);
            expect(detail.session).toEqual(before.detail.session);
            expect(detail.latestTurn).toEqual(before.detail.latestTurn);

            // A fresh database is incremental, so the freed pages go back.
            expect(result.autoVacuum).toBe(2);
            expect(result.reclaimedBytes).toBeGreaterThan(500_000);
            expect(result.remainingFreeBytes).toBe(0);

            // Resuming below the floor is reported as unreplayable; at the
            // floor the remaining range is intact and replayable.
            const head = yield* system.engine.latestSequence;
            const belowThread = yield* system.engine.getThreadReplayStats({
              threadId: liveThread,
              fromSequenceExclusive: 0,
              toSequenceInclusive: head,
              maxEvents: 1_000,
            });
            expect(belowThread.rangePruned).toBe(true);
            const atFloorThread = yield* system.engine.getThreadReplayStats({
              threadId: liveThread,
              fromSequenceExclusive: prunedThrough,
              toSequenceInclusive: head,
              maxEvents: 1_000,
            });
            expect(atFloorThread.rangePruned).toBe(false);
            const belowShell = yield* system.snapshotQuery.getEventReplayStats({
              fromSequenceExclusive: prunedThrough - 1,
              toSequenceInclusive: head,
            });
            expect(belowShell.rangePruned).toBe(true);
            const atFloorShell = yield* system.snapshotQuery.getEventReplayStats({
              fromSequenceExclusive: prunedThrough,
              toSequenceInclusive: head,
            });
            expect(atFloorShell.rangePruned).toBe(false);
            expect(atFloorShell.eventCount).toBe(head - prunedThrough);

            // A second pass finds nothing left to prune.
            const again = yield* system.pass();
            expect(again.events.deleted).toBe(0);
            expect(again.activities.deleted).toBe(0);
            return prunedThrough;
          }),
        );

        // Restart: projections bootstrap from their cursors (no replay of the
        // pruned range), and the pending conditional start can still be
        // re-driven from its requested event.
        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            expect(yield* system.shell(liveThread)).toEqual(before.shell);
            expect(yield* system.threadRow(archivedThread)).toEqual(before.archivedRow);
            const pending = Option.getOrThrow(
              yield* system.deliveries.getByCommandId(CommandId.make("cmd-pending-start")),
            );
            expect(pending.status).toBe("accepted");
            const [requested] = yield* Stream.runCollect(
              system.engine.readEvents(pending.requestedSequence - 1, 1),
            );
            expect(requested?.type).toBe("thread.turn-start-requested");
            expect(requested?.commandId).toBe("cmd-pending-start");
            // New events keep extending each stream after pruning.
            yield* appendActivity(system, liveThread, "after-restart", "tool.completed", YOUNG);
            expect(yield* system.engine.latestSequence).toBeGreaterThan(prunedThrough);
          }),
        );
      }),
  );

  it.effect("refuses to rebuild a projector from a pruned log unless allowed", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedHistory(system);
          const result = yield* system.pass();
          expect(result.events.deleted).toBeGreaterThan(0);
          // A projector this database has never run (a later upstream
          // release) starts from sequence 0.
          yield* system.sql`
            DELETE FROM projection_state WHERE projector = 'projection.thread-activities'
          `;
        }),
      );

      const refused = yield* Effect.exit(boot(databasePath, () => Effect.void));
      expect(Exit.isFailure(refused)).toBe(true);
      if (Exit.isFailure(refused)) {
        expect(Cause.pretty(refused.cause)).toContain(
          "ProjectionPipeline.bootstrap:retentionFloor",
        );
      }

      yield* boot(databasePath, (system) =>
        system.shell(liveThread).pipe(Effect.map((shell) => expect(shell.id).toBe(liveThread))),
      ).pipe(
        Effect.provide(
          ConfigProvider.layer(
            ConfigProvider.fromEnv({ env: { T3CODE_RETENTION_ALLOW_PARTIAL_REPLAY: "true" } }),
          ),
        ),
      );
    }),
  );

  it.effect("keeps receipts inside the window so a retried command stays deduplicated", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, liveThread);
          yield* sessionSet(system, "cmd-old", at(OLD, 1));
          yield* conditionalStart(system, "cmd-delivered", at(OLD, 2));
          yield* system.deliveries.resolve({
            commandId: CommandId.make("cmd-delivered"),
            status: "started",
            turnId: null,
            detail: null,
            resolvedAt: at(OLD, 3),
          });
          const young = yield* sessionSet(system, "cmd-young", YOUNG);

          const result = yield* system.pass({ batchSize: 2 });
          expect(result.receipts.deleted).toBe(4);
          expect(result.deliveries.deleted).toBe(1);

          const receipt = (commandId: string) =>
            system.receipts.getByCommandId({ commandId: CommandId.make(commandId) });
          expect(Option.isNone(yield* receipt("cmd-old"))).toBe(true);
          expect(Option.isSome(yield* receipt("cmd-young"))).toBe(true);
          expect(
            Option.isNone(yield* system.deliveries.getByCommandId(CommandId.make("cmd-delivered"))),
          ).toBe(true);

          // Inside the window a retry is answered from the receipt.
          const head = yield* system.engine.latestSequence;
          const retried = yield* sessionSet(system, "cmd-young", YOUNG);
          expect(retried.sequence).toBe(young.sequence);
          expect(yield* system.engine.latestSequence).toBe(head);
        }),
      );
    }),
  );

  it.effect("skips event pruning until every projector has a cursor", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, liveThread);
          yield* system.sql`DELETE FROM projection_state WHERE projector = 'projection.threads'`;
          const result = yield* system.pass();
          expect(result.events).toMatchObject({ deleted: 0, skipped: "projector-state-missing" });
        }),
      );
    }),
  );
});

/** Polls until `condition` holds, letting forked fibers run in between. */
const waitUntil = <E, R>(condition: Effect.Effect<boolean, E, R>, advanceClock = false) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 10_000; attempt += 1) {
      if (yield* condition) return;
      if (advanceClock) yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
    }
    throw new Error("condition never held");
  });

/** Advances the test clock in steps until the fiber finishes. */
const drive = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.gen(function* () {
    for (let step = 0; step < 10_000; step += 1) {
      if (fiber.pollUnsafe() !== undefined) break;
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
    }
    return yield* Fiber.join(fiber);
  });

const readFloor = (system: System) =>
  system.sql<{ readonly value: number }>`
    SELECT value FROM orchestration_retention_state WHERE name = 'events_pruned_through'
  `.pipe(Effect.map((rows) => Number(rows[0]?.value ?? 0)));

const eventCount = (system: System) =>
  system.sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM orchestration_events`.pipe(
    Effect.map((rows) => Number(rows[0]?.n ?? 0)),
  );

/** The thread-subscription replay exactly as ws.ts runs it after its first check. */
const guardedThreadReplay = (
  system: System,
  range: {
    readonly threadId: ThreadId;
    readonly fromSequenceExclusive: number;
    readonly toSequenceInclusive: number;
  },
) =>
  materializeIntactReplay({
    read: system.engine.readThreadEvents({ ...range, limit: 1_000 }),
    stillIntact: system.engine
      .getThreadReplayStats({ ...range, maxEvents: 1_000 })
      .pipe(Effect.map((stats) => stats.rangePruned !== true)),
  });

effectIt.layer(NodeServices.layer)("retention races and restarts", (it) => {
  it.effect("a prune between the replay check and the read falls back to a snapshot", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedHistory(system);
          const head = yield* system.engine.latestSequence;
          const range = {
            threadId: liveThread,
            fromSequenceExclusive: 1,
            toSequenceInclusive: head,
          };

          // The subscriber's first check passes: nothing is pruned yet.
          const checked = yield* system.engine.getThreadReplayStats({
            ...range,
            maxEvents: 1_000,
          });
          expect(checked.rangePruned).toBe(false);

          // Retention starts with a real (non-zero) grace period.
          const pass = yield* Effect.forkChild(system.pass({ floorGraceMs: 5_000 }));
          yield* waitUntil(readFloor(system).pipe(Effect.map((floor) => floor > 1)));
          // Inside the grace period nothing is deleted yet, but a replay that
          // reads now is already refused.
          expect(yield* eventCount(system)).toBe(head);
          expect(Option.isNone(yield* guardedThreadReplay(system, range))).toBe(true);

          // The subscriber stalls past the grace period; the prune completes.
          const result = yield* drive(pass);
          expect(result.events.deleted).toBeGreaterThan(0);

          // Reading the range now really misses events...
          const raw = yield* Stream.runCollect(
            system.engine.readThreadEvents({ ...range, limit: 1_000 }),
          );
          expect(raw.length).toBeLessThan(checked.eventCount);
          // ...and the guarded replay notices and falls back to a snapshot.
          expect(Option.isNone(yield* guardedThreadReplay(system, range))).toBe(true);
          // A cursor at the floor still replays everything after it.
          const floor = yield* readFloor(system);
          const intact = yield* guardedThreadReplay(system, {
            ...range,
            fromSequenceExclusive: floor,
          });
          expect(Option.isSome(intact)).toBe(true);
        }),
      );
    }),
  );

  it.effect("a pass interrupted mid-deletion leaves a consistent database to resume from", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      const before = yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedHistory(system);
          const head = yield* system.engine.latestSequence;
          const shell = yield* system.shell(liveThread);
          // Small batches with a pause between them; interrupt after the first.
          const pass = yield* Effect.forkChild(system.pass({ batchSize: 2, batchPauseMs: 1_000 }));
          yield* waitUntil(eventCount(system).pipe(Effect.map((count) => count < head)), true);
          yield* Fiber.interrupt(pass);
          const remaining = yield* eventCount(system);
          expect(remaining).toBeLessThan(head);
          expect(remaining).toBeGreaterThan(head - 10);
          return { head, shell };
        }),
      );

      // Restart: the floor survived, projections are intact, the next pass finishes.
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          expect(yield* system.shell(liveThread)).toEqual(before.shell);
          const stale = yield* system.engine.getThreadReplayStats({
            threadId: liveThread,
            fromSequenceExclusive: 1,
            toSequenceInclusive: before.head,
            maxEvents: 1_000,
          });
          expect(stale.rangePruned).toBe(true);
          const resumed = yield* system.pass();
          expect(resumed.events.deleted).toBeGreaterThan(0);
          expect((yield* system.pass()).events.deleted).toBe(0);
          expect(yield* system.shell(liveThread)).toEqual(before.shell);
        }),
      );
    }),
  );

  it.effect("keeps task lifecycle and usage rows all-or-nothing per task", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, liveThread);
          yield* seedThread(system, archivedThread);
          let second = 0;
          const row = (
            threadId: ThreadId,
            id: string,
            kind: string,
            payload: Record<string, unknown>,
            createdAt = at(OLD, second++),
          ) => appendActivity(system, threadId, id, kind, createdAt, payload);
          const agent = { agentKind: "agent" };

          // Reopened then failed: started → completed → updated(running) →
          // updated(failed) folds to failed on its second run.
          yield* row(liveThread, "d-start", "task.started", { taskId: "D", ...agent, detail: "D" });
          yield* row(liveThread, "d-done", "task.completed", { taskId: "D", status: "completed" });
          yield* row(liveThread, "d-running", "task.updated", { taskId: "D", status: "running" });
          yield* row(liveThread, "d-failed", "task.updated", { taskId: "D", status: "failed" });
          // Usage and progress live in separate stable-id rows; the older
          // usage row carries the only token count.
          yield* row(liveThread, "u-start", "task.started", { taskId: "U", ...agent, detail: "U" });
          yield* row(liveThread, `task-usage:${liveThread}:U`, "task.progress", {
            taskId: "U",
            usageSnapshot: true,
            typedUsage: { totalTokens: 12_000 },
          });
          yield* row(liveThread, `task-progress:${liveThread}:U`, "task.progress", {
            taskId: "U",
            summary: "still going",
          });
          yield* row(liveThread, `tool-progress:${liveThread}:U`, "tool.progress", {
            taskId: "U",
            toolName: "Read",
          });
          // B starts, A starts and finishes, B finishes; C is background work.
          yield* row(liveThread, "b-start", "task.started", { taskId: "B", ...agent, detail: "B" });
          yield* row(liveThread, "a-start", "task.started", { taskId: "A", ...agent, detail: "A" });
          yield* row(liveThread, "a-done", "task.completed", { taskId: "A", status: "completed" });
          yield* row(liveThread, "c-start", "task.started", { taskId: "C", detail: "shell" });
          yield* row(liveThread, "b-done", "task.completed", { taskId: "B", status: "completed" });
          // Ordinary old output, which the pass should still remove.
          for (let index = 0; index < 3; index += 1) {
            yield* row(liveThread, `old-output-${index}`, "tool.completed", { output: index });
          }
          for (let index = 0; index < 4; index += 1) {
            yield* row(liveThread, `young-${index}`, "tool.completed", {}, at(YOUNG, index));
          }

          // Archived thread: E is entirely old, F has one young row.
          yield* row(archivedThread, "e-start", "task.started", { taskId: "E", ...agent });
          yield* row(archivedThread, "e-done", "task.completed", {
            taskId: "E",
            status: "completed",
          });
          yield* row(archivedThread, "f-start", "task.started", { taskId: "F", ...agent });
          yield* row(
            archivedThread,
            "f-done",
            "task.completed",
            { taskId: "F", status: "completed" },
            at(YOUNG, 9),
          );
          yield* system.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("cmd-archive"),
            threadId: archivedThread,
          });

          const folded = Effect.gen(function* () {
            const detail = yield* system.detail(liveThread);
            return foldSubagentActivities(detail.activities, { sessionLive: true }).map(
              (subagent) => ({
                id: subagent.id,
                status: subagent.status,
                activationCount: subagent.activationCount,
                usage: subagent.usage,
                title: subagent.title,
                progress: subagent.progress,
                lastToolName: subagent.lastToolName,
              }),
            );
          });
          const before = yield* folded;
          const byId = new Map(before.map((subagent) => [subagent.id, subagent]));
          expect(byId.get("D")).toMatchObject({ status: "failed", activationCount: 2 });
          expect(byId.get("U")?.usage?.totalTokens).toBe(12_000);
          expect(byId.get("A")?.status).toBe("completed");
          expect(byId.get("B")?.status).toBe("completed");
          expect(byId.has("C")).toBe(false);

          const result = yield* system.pass({ batchSize: 2 });
          // Live thread: only the ordinary old output goes; every task row stays.
          expect(yield* folded).toEqual(before);
          const live = yield* activityIds(system, liveThread);
          expect(live.filter((id) => id.startsWith("old-output"))).toEqual([]);
          // 13 task rows + 4 young rows.
          expect(live).toHaveLength(17);
          // Archived thread: E goes as a whole, F stays as a whole.
          expect(yield* activityIds(system, archivedThread)).toEqual(["f-done", "f-start"]);
          expect(result.activities.deleted).toBe(3 + 2);
        }),
      );
    }),
  );

  it.effect("an archived thread unarchived mid-pass keeps its old task rows", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, archivedThread);
          // Two old rows of one kind (the older is deletable) and an old task.
          yield* appendActivity(system, archivedThread, "old-a", "tool.completed", at(OLD, 0));
          yield* appendActivity(system, archivedThread, "old-b", "tool.completed", at(OLD, 1));
          yield* appendActivity(system, archivedThread, "x-start", "task.started", at(OLD, 2), {
            taskId: "X",
            agentKind: "agent",
          });
          yield* appendActivity(system, archivedThread, "x-done", "task.completed", at(OLD, 3), {
            taskId: "X",
            status: "completed",
          });
          yield* system.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("cmd-archive"),
            threadId: archivedThread,
          });
          // Pause after every step, so the unarchive lands after the pass has
          // judged the thread retired and before it deletes the task.
          const pass = yield* Effect.forkChild(
            system.pass({ batchSize: 10, batchPauseMs: 1_000, workSliceMs: 0 }),
          );
          yield* waitUntil(
            activityIds(system, archivedThread).pipe(Effect.map((ids) => !ids.includes("old-a"))),
            true,
          );
          expect(yield* activityIds(system, archivedThread)).toContain("x-start");
          yield* system.dispatch({
            type: "thread.unarchive",
            commandId: CommandId.make("cmd-unarchive"),
            threadId: archivedThread,
          });
          yield* drive(pass);
          expect(yield* activityIds(system, archivedThread)).toEqual([
            "old-b",
            "x-done",
            "x-start",
          ]);
        }),
      );
    }),
  );

  it.effect("leaves an old task larger than the row cap intact", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, archivedThread);
          let second = 0;
          for (let index = 0; index < 4; index += 1) {
            yield* appendActivity(
              system,
              archivedThread,
              `big-${index}`,
              "task.updated",
              at(OLD, second++),
              {
                taskId: "BIG",
              },
            );
          }
          for (let index = 0; index < 2; index += 1) {
            yield* appendActivity(
              system,
              archivedThread,
              `small-${index}`,
              "task.updated",
              at(OLD, second++),
              {
                taskId: "SMALL",
              },
            );
          }
          yield* system.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("cmd-archive"),
            threadId: archivedThread,
          });
          const result = yield* system.pass({ taskRowCap: 3 });
          expect(result.activities.tasksOverCap).toBe(1);
          expect(yield* activityIds(system, archivedThread)).toEqual([
            "big-0",
            "big-1",
            "big-2",
            "big-3",
          ]);
        }),
      );
    }),
  );

  it.effect("pages events by rows inspected, across a long protected run", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, liveThread);
          const insert = (stream: string, version: number, id: string) =>
            system.sql`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
                command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
              ) VALUES (
                ${id}, 'thread', ${stream}, ${version}, 'thread.activity-appended', ${OLD},
                NULL, NULL, NULL, 'server', '{}', '{}'
              )
            `;
          // 300 events that are each the newest of their stream: all kept.
          for (let index = 0; index < 300; index += 1) {
            yield* insert(`protected-${index}`, 0, `protected-${index}`);
          }
          // Then five events of one stream: the four older ones are deletable.
          for (let version = 0; version < 5; version += 1) {
            yield* insert("tail", version, `tail-${version}`);
          }
          yield* system.sql`
            UPDATE projection_state
            SET last_applied_sequence = (SELECT MAX(sequence) FROM orchestration_events)
          `;
          const result = yield* system.pass({ batchSize: 50 });
          // At least 300 / 50 pages, each inspecting at most 50 events.
          expect(result.events.batches).toBeGreaterThanOrEqual(7);
          const tail = yield* system.sql<{ readonly id: string }>`
            SELECT event_id AS id FROM orchestration_events WHERE stream_id = 'tail'
          `;
          expect(tail.map((row) => row.id)).toEqual(["tail-4"]);
          const [kept] = yield* system.sql<{ readonly n: number }>`
            SELECT COUNT(*) AS n FROM orchestration_events WHERE stream_id LIKE 'protected-%'
          `;
          expect(kept?.n).toBe(300);
        }),
      );
    }),
  );

  it.effect("an activity refreshed in place during a pass is not deleted", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedProject(system);
          yield* seedThread(system, liveThread);
          // The oldest row reuses a stable id, like task progress does.
          yield* appendActivity(system, liveThread, "stable", "tool.updated", at(OLD, 0), { n: 0 });
          for (let index = 1; index <= 8; index += 1) {
            yield* appendActivity(
              system,
              liveThread,
              `old-${index}`,
              "tool.updated",
              at(OLD, index),
            );
          }
          for (let index = 0; index < 4; index += 1) {
            yield* appendActivity(
              system,
              liveThread,
              `young-${index}`,
              "tool.updated",
              at(YOUNG, index),
            );
          }
          // Pause after every page, so the refresh lands mid-walk.
          const pass = yield* Effect.forkChild(
            system.pass({ batchSize: 2, batchPauseMs: 1_000, workSliceMs: 0 }),
          );
          // After the first batch, the provider refreshes the stable row.
          yield* waitUntil(
            activityIds(system, liveThread).pipe(Effect.map((ids) => !ids.includes("old-8"))),
            true,
          );
          // The walk has not reached the stable row yet.
          expect(yield* activityIds(system, liveThread)).toContain("old-1");
          yield* appendActivity(system, liveThread, "stable", "tool.updated", at(YOUNG, 10), {
            n: 1,
          });
          yield* drive(pass);
          const remaining = yield* activityIds(system, liveThread);
          expect(remaining).toContain("stable");
          expect(remaining.filter((id) => id.startsWith("old-"))).toEqual([]);
        }),
      );
    }),
  );
});

effectIt.layer(NodeServices.layer)("retention startup VACUUM", (it) => {
  // A database created before the fork change: auto_vacuum NONE, with free pages.
  const makeLegacyDatabase = (databasePath: string, rows = 400) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE filler (body TEXT NOT NULL)`;
      yield* sql`CREATE TABLE kept (body TEXT NOT NULL)`;
      yield* sql`INSERT INTO kept (body) VALUES ('still here')`;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          for (let index = 0; index < rows; index += 1) {
            yield* sql`INSERT INTO filler (body) VALUES (${BIG})`;
          }
        }),
      );
      yield* sql`DELETE FROM filler`;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: databasePath })));

  const pragma = (sql: SqlClient.SqlClient, name: string) =>
    sql
      .unsafe<Record<string, number>>(`PRAGMA ${name}`)
      .pipe(Effect.map((rows) => Number(Object.values(rows[0] ?? {})[0])));

  /** Opens the database the way every process does, then runs `body`. */
  const withDatabase = <A, E>(
    databasePath: string,
    env: Record<string, string>,
    body: (sql: SqlClient.SqlClient) => Effect.Effect<A, E, SqlClient.SqlClient>,
  ) =>
    SqlClient.SqlClient.pipe(
      Effect.flatMap(body),
      Effect.provide(makeSqlitePersistenceLive(databasePath)),
      Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
    );

  const plenty = () => Effect.succeed({ availableBytes: 1e15, device: 1 });

  it.effect("plain database opens (CLI subcommands) never VACUUM, even when forced", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* makeLegacyDatabase(databasePath);
      const stats = yield* withDatabase(
        databasePath,
        { T3CODE_RETENTION_STARTUP_VACUUM: "force" },
        (sql) =>
          Effect.all({
            autoVacuum: pragma(sql, "auto_vacuum"),
            free: pragma(sql, "freelist_count"),
          }),
      );
      expect(stats.autoVacuum).toBe(0);
      expect(stats.free).toBeGreaterThan(0);
    }),
  );

  it.effect("server startup converts an existing database when forced", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* makeLegacyDatabase(databasePath);
      const outcome = yield* withDatabase(
        databasePath,
        { T3CODE_RETENTION_STARTUP_VACUUM: "force" },
        (sql) =>
          runServerStartupVacuum(databasePath).pipe(
            Effect.flatMap((outcome) =>
              Effect.all({
                outcome: Effect.succeed(outcome),
                autoVacuum: pragma(sql, "auto_vacuum"),
                free: pragma(sql, "freelist_count"),
              }),
            ),
          ),
      );
      expect(outcome).toEqual({ outcome: "vacuumed", autoVacuum: 2, free: 0 });
    }),
  );

  it.effect("leaves a database with little free space alone in auto mode", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* makeLegacyDatabase(databasePath);
      const outcome = yield* withDatabase(databasePath, {}, () =>
        runServerStartupVacuum(databasePath, { diskSpace: plenty }),
      );
      expect(outcome).toBe("not-worth-it");
    }),
  );

  it.effect("skips the VACUUM when the disk cannot hold the copies", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* makeLegacyDatabase(databasePath);
      const env = { T3CODE_RETENTION_STARTUP_VACUUM: "force" };
      // Same filesystem for database and temp: room for one copy (live size plus
      // headroom, about 65 MB here), not two.
      const live = 100 * 1024 * 1024;
      const oneCopy = () => Effect.succeed({ availableBytes: live, device: 7 });
      const result = yield* withDatabase(databasePath, env, (sql) =>
        Effect.all({
          outcome: runServerStartupVacuum(databasePath, { diskSpace: oneCopy }),
          unreadable: runServerStartupVacuum(databasePath, {
            diskSpace: () => Effect.succeed(null),
          }),
          autoVacuum: pragma(sql, "auto_vacuum"),
          free: pragma(sql, "freelist_count"),
        }),
      );
      expect(result.outcome).toBe("insufficient-disk");
      expect(result.unreadable).toBe("insufficient-disk");
      expect(result.autoVacuum).toBe(0);
      expect(result.free).toBeGreaterThan(0);
    }),
  );

  it.effect("survives a failing VACUUM, keeps the data and backs off in auto mode", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      // Enough free space (> 64 MB, > 10 %) for auto mode to want a VACUUM.
      yield* makeLegacyDatabase(databasePath, 1_800);
      // A real SQLite failure: VACUUM cannot run inside a transaction.
      const failingVacuum = (sql: SqlClient.SqlClient) => sql.withTransaction(sql`VACUUM`);
      const first = yield* withDatabase(databasePath, {}, (sql) =>
        Effect.all({
          outcome: runServerStartupVacuum(databasePath, {
            diskSpace: plenty,
            vacuum: failingVacuum,
          }),
          kept: sql<{ readonly body: string }>`SELECT body FROM kept`,
          autoVacuum: pragma(sql, "auto_vacuum"),
        }),
      );
      expect(first).toEqual({ outcome: "failed", kept: [{ body: "still here" }], autoVacuum: 0 });
      // The next boot does not retry within a day...
      const second = yield* withDatabase(databasePath, {}, () =>
        runServerStartupVacuum(databasePath, { diskSpace: plenty }),
      );
      expect(second).toBe("backing-off");
      // ...unless forced.
      const forced = yield* withDatabase(
        databasePath,
        { T3CODE_RETENTION_STARTUP_VACUUM: "force" },
        () => runServerStartupVacuum(databasePath, { diskSpace: plenty }),
      );
      expect(forced).toBe("vacuumed");
    }),
  );
});

effectIt.layer(NodeServices.layer)("retention bounded work", (it) => {
  it.effect("every retention statement is an index seek or a LIMITed ordered page", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* ensureOrchestrationTurnDeliveriesSchema(sql);
      yield* ensureRetentionIndexes(sql);
      const statements = retentionStatements(sql);
      const all = {
        eventAgePage: statements.eventAgePage(0, 10, 5),
        eventPageEnd: statements.eventPageEnd(0, 10, 5),
        deleteEventRange: statements.deleteEventRange(0, 10, 0),
        taskRowCount: statements.taskRowCount("t", "a", 2_000),
        oldestReceipts: statements.oldestReceipts(5),
        deleteReceipts: statements.deleteReceipts(["a"], OLD),
        nextActivityThread: statements.nextActivityThread(""),
        threadRetired: statements.threadRetired("t"),
        nthNewestActivity: statements.nthNewestActivity("t", 199),
        anyActivityBefore: statements.anyActivityBefore("t", OLD),
        nextKind: statements.nextKind("t", ""),
        newestOfKind: statements.newestOfKind("t", "k"),
        oldActivityPage: statements.oldActivityPage("t", OLD, "", 5),
        deleteActivities: statements.deleteActivities(["a"], OLD),
        firstTask: statements.nextTask("t", null),
        nextTask: statements.nextTask("t", "a"),
        newestTaskRow: statements.newestTaskRow("t", "a"),
        deleteTaskRows: statements.deleteTaskRows("t", "a"),
        deleteDeliveryPage: statements.deleteDeliveryPage(OLD, 5),
      };
      for (const [name, statement] of Object.entries(all)) {
        const [text, params] = statement.compile();
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${text}`,
          params,
        );
        for (const { detail } of plan) {
          // No table or index is scanned end to end and nothing is sorted in
          // a temp b-tree. The one ordered index scan stops at its LIMIT.
          const scan = detail.startsWith("SCAN ");
          // Scanning a LIMITed subquery's own result is bounded by its LIMIT.
          const allowedScan =
            detail.startsWith("SCAN (subquery-") ||
            (name === "oldestReceipts" &&
              detail ===
                "SCAN orchestration_command_receipts USING INDEX idx_orch_command_receipts_sequence");
          expect({ name, detail, bounded: !scan || allowedScan }).toEqual({
            name,
            detail,
            bounded: true,
          });
          expect(detail).not.toContain("TEMP B-TREE");
        }
      }
    }).pipe(Effect.provide(SqlitePersistenceMemory)),
  );

  it.effect("picks SQLite's temp directory: writable and searchable, in SQLite's order", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped({
        directory: NodeOS.tmpdir(),
        prefix: "t3-retention-tmp-",
      });
      const writeOnly = NodePath.join(root, "write-only");
      const usable = NodePath.join(root, "usable");
      const other = NodePath.join(root, "other");
      for (const directory of [writeOnly, usable, other]) {
        yield* fileSystem.makeDirectory(directory);
      }
      // Writable but not searchable: SQLite's access(dir, W_OK | X_OK) rejects it.
      yield* fileSystem.chmod(writeOnly, 0o200);
      const missing = NodePath.join(root, "missing");
      yield* Effect.sync(() => {
        expect(sqliteTempDirectoryFor({ SQLITE_TMPDIR: writeOnly, TMPDIR: usable })).toBe(usable);
        expect(sqliteTempDirectoryFor({ SQLITE_TMPDIR: usable, TMPDIR: other })).toBe(usable);
        expect(sqliteTempDirectoryFor({ SQLITE_TMPDIR: missing, TMPDIR: other })).toBe(other);
        expect(sqliteTempDirectoryFor({ SQLITE_TMPDIR: "", TMPDIR: other })).toBe(other);
      }).pipe(Effect.ensuring(Effect.orDie(fileSystem.chmod(writeOnly, 0o700))));
    }),
  );
});
