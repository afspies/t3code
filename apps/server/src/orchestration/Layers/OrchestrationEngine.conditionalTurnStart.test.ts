// @effect-diagnostics nodeBuiltinImport:off
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  ApprovalRequestId,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type OrchestrationCommand,
  type OrchestrationSession,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { expect } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationTurnDeliveryRepositoryLive } from "../../persistence/Layers/OrchestrationTurnDeliveries.ts";
import { makeSqlitePersistenceLive } from "../../persistence/Layers/Sqlite.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { OrchestrationTurnDeliveryRepository } from "../../persistence/Services/OrchestrationTurnDeliveries.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationCommandConflictError, type OrchestrationDispatchError } from "../Errors.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

const isConflictError = Schema.is(OrchestrationCommandConflictError);
const NOW = "2026-01-01T00:00:00.000Z";
const projectId = ProjectId.make("project-1");
const threadId = ThreadId.make("thread-1");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5-codex" };

// One engine "process" over a database file. Providing it twice in a test is a
// server restart: the second boot projects its command read model from the
// database with empty message/activity arrays.
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
    Layer.provide(makeSqlitePersistenceLive(databasePath)),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-conditional-turn-start-test-" }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
}

const system = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const snapshotQuery = yield* ProjectionSnapshotQuery;
  const deliveries = yield* OrchestrationTurnDeliveryRepository;
  const receipts = yield* OrchestrationCommandReceiptRepository;
  return {
    engine,
    deliveries,
    receipts,
    dispatch: (command: OrchestrationCommand) => engine.dispatch(command),
    dispatchExit: (command: OrchestrationCommand) => Effect.exit(engine.dispatch(command)),
    shell: snapshotQuery.getThreadShellById(threadId).pipe(Effect.map(Option.getOrThrow)),
    thread: snapshotQuery.getThreadDetailById(threadId).pipe(Effect.map(Option.getOrThrow)),
    record: (commandId: string) => deliveries.getByCommandId(CommandId.make(commandId)),
    receipt: (commandId: string) =>
      receipts.getByCommandId({ commandId: CommandId.make(commandId) }),
  };
});

type System = Effect.Success<typeof system>;

const boot = <A, E>(databasePath: string, body: (system: System) => Effect.Effect<A, E>) =>
  system.pipe(Effect.flatMap(body), Effect.provide(makeLayer(databasePath)));

const freshDatabasePath = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    directory: NodeOS.tmpdir(),
    prefix: "t3-cond-start-",
  });
  return NodePath.join(directory, "state.sqlite");
});

function conflictOf(
  exit: Exit.Exit<{ sequence: number }, OrchestrationDispatchError>,
): OrchestrationCommandConflictError["conflict"] {
  if (Exit.isSuccess(exit)) {
    throw new Error("expected a conflict rejection, command was accepted");
  }
  const error = Cause.squash(exit.cause);
  if (!isConflictError(error)) {
    throw new Error(`expected OrchestrationCommandConflictError, got ${String(error)}`);
  }
  return error.conflict;
}

function conditionalStart(
  commandId: string,
  messageId: string,
  createdAt = NOW,
): Extract<OrchestrationCommand, { type: "thread.turn.start" }> {
  return {
    type: "thread.turn.start",
    commandId: CommandId.make(commandId),
    threadId,
    message: { messageId: MessageId.make(messageId), role: "user", text: "quiet", attachments: [] },
    runtimeMode: "full-access",
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    createdAt,
    onlyIfIdle: true,
  };
}

function ordinaryStart(
  commandId: string,
  messageId: string,
  createdAt = NOW,
): Extract<OrchestrationCommand, { type: "thread.turn.start" }> {
  return {
    type: "thread.turn.start",
    commandId: CommandId.make(commandId),
    threadId,
    message: { messageId: MessageId.make(messageId), role: "user", text: "hi", attachments: [] },
    runtimeMode: "full-access",
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    createdAt,
  };
}

function sessionSet(
  commandId: string,
  session: Partial<OrchestrationSession>,
): Extract<OrchestrationCommand, { type: "thread.session.set" }> {
  return {
    type: "thread.session.set",
    commandId: CommandId.make(commandId),
    threadId,
    session: {
      threadId,
      status: "ready",
      providerName: "codex",
      runtimeMode: "full-access",
      activeTurnId: null,
      lastError: null,
      updatedAt: NOW,
      ...session,
    },
    createdAt: NOW,
  };
}

const seedThread = (system: System) =>
  Effect.gen(function* () {
    yield* system.dispatch({
      type: "project.create",
      commandId: CommandId.make("cmd-project"),
      projectId,
      title: "Project",
      workspaceRoot: "/tmp/conditional-project",
      defaultModelSelection: modelSelection,
      createdAt: NOW,
    });
    yield* system.dispatch({
      type: "thread.create",
      commandId: CommandId.make("cmd-thread"),
      threadId,
      projectId,
      title: "Thread",
      modelSelection,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      runtimeMode: "full-access",
      branch: null,
      worktreePath: null,
      createdAt: NOW,
    });
  });

effectIt.layer(NodeServices.layer)("OrchestrationEngine conditional turn start", (effectIt) => {
  effectIt.effect(
    "accepts exactly one of two concurrent conditional starts and persists its delivery record",
    () =>
      Effect.gen(function* () {
        const databasePath = yield* freshDatabasePath;
        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            yield* seedThread(system);
            const [first, second] = yield* Effect.all(
              [
                system.dispatchExit(conditionalStart("cmd-a", "msg-a")),
                system.dispatchExit(conditionalStart("cmd-b", "msg-b")),
              ],
              { concurrency: "unbounded" },
            );
            const accepted = Exit.isSuccess(first) ? first : second;
            const rejected = Exit.isSuccess(first) ? second : first;
            expect(Exit.isSuccess(accepted)).toBe(true);
            const winnerId = Exit.isSuccess(first) ? "cmd-a" : "cmd-b";
            const loserId = winnerId === "cmd-a" ? "cmd-b" : "cmd-a";
            const winnerMessageId = winnerId === "cmd-a" ? "msg-a" : "msg-b";
            const loserMessageId = winnerId === "cmd-a" ? "msg-b" : "msg-a";
            expect(conflictOf(rejected)).toEqual({
              reason: "turn_start_not_idle",
              commandType: "thread.turn.start",
              threadId,
              idleBlocker: "pending-turn-start",
              activeTurnId: null,
              pendingMessageId: winnerMessageId,
            });

            // Exactly one message landed; the loser appended nothing.
            expect((yield* system.thread).messages.map((message) => message.id)).toEqual([
              winnerMessageId,
            ]);

            // Durable record for the winner, none for the loser.
            expect(Option.getOrThrow(yield* system.record(winnerId))).toMatchObject({
              status: "accepted",
              threadId,
              turnId: null,
              attemptedAt: null,
            });
            expect(Option.isNone(yield* system.record(loserId))).toBe(true);

            // The rejection is not receipted: the identical loser command is
            // decided fresh once the winner's record stops blocking the thread.
            expect(Option.isNone(yield* system.receipt(loserId))).toBe(true);
            yield* system.deliveries.resolve({
              commandId: CommandId.make(winnerId),
              status: "failed",
              turnId: null,
              detail: "test",
              resolvedAt: NOW,
            });
            // The winner's message is still "recent" for the legacy queued
            // rule, so decide the retry two minutes later.
            const retried = yield* system.dispatchExit(
              conditionalStart(loserId, loserMessageId, "2026-01-01T00:03:00.000Z"),
            );
            expect(Exit.isSuccess(retried)).toBe(true);
            expect((yield* system.thread).messages).toHaveLength(2);
          }),
        );
      }),
  );

  effectIt.effect(
    "replays an accepted conditional start idempotently without a second record or message",
    () =>
      Effect.gen(function* () {
        const databasePath = yield* freshDatabasePath;
        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            yield* seedThread(system);
            const first = yield* system.dispatch(conditionalStart("cmd-a", "msg-a"));
            const replay = yield* system.dispatch(conditionalStart("cmd-a", "msg-a"));
            expect(replay.sequence).toBe(first.sequence);
            expect((yield* system.thread).messages).toHaveLength(1);
            expect(Option.getOrThrow(yield* system.record("cmd-a"))).toMatchObject({
              status: "accepted",
              requestedSequence: first.sequence,
            });
            expect(yield* system.engine.latestSequence).toBe(first.sequence);
          }),
        );
      }),
  );

  effectIt.effect.each([
    {
      label: "session starting",
      session: { status: "starting" },
      idleBlocker: "session-starting",
    },
    {
      label: "session running with an active turn",
      session: { status: "running", activeTurnId: TurnId.make("turn-1") },
      idleBlocker: "turn-active",
    },
  ] as const)("rejects while $label without appending anything", ({ session, idleBlocker }) =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedThread(system);
          yield* system.dispatch(sessionSet("cmd-session", session));
          const exit = yield* system.dispatchExit(conditionalStart("cmd-a", "msg-a"));
          expect(conflictOf(exit)).toMatchObject({ reason: "turn_start_not_idle", idleBlocker });
          expect((yield* system.thread).messages).toHaveLength(0);
          expect(Option.isNone(yield* system.record("cmd-a"))).toBe(true);
          expect(Option.isNone(yield* system.receipt("cmd-a"))).toBe(true);
        }),
      );
    }),
  );

  effectIt.effect("rejects a pending legacy start and an archived thread", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedThread(system);
          yield* system.dispatch(ordinaryStart("cmd-legacy", "msg-legacy"));
          expect(
            conflictOf(yield* system.dispatchExit(conditionalStart("cmd-a", "msg-a"))),
          ).toMatchObject({ idleBlocker: "pending-turn-start" });
          yield* system.dispatch({
            type: "thread.archive",
            commandId: CommandId.make("cmd-archive"),
            threadId,
          });
          expect(
            conflictOf(
              yield* system.dispatchExit(
                conditionalStart("cmd-b", "msg-b", "2026-01-01T00:05:00.000Z"),
              ),
            ),
          ).toMatchObject({ idleBlocker: "thread-archived" });
        }),
      );
    }),
  );

  effectIt.effect("keeps rejecting after a restart from durable pending approval state alone", () =>
    Effect.gen(function* () {
      const databasePath = yield* freshDatabasePath;
      const requestId = ApprovalRequestId.make("approval-1");
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          yield* seedThread(system);
          yield* system.dispatch(sessionSet("cmd-session", { status: "ready" }));
          yield* system.dispatch({
            type: "thread.activity.append",
            commandId: CommandId.make("cmd-approval"),
            threadId,
            activity: {
              id: EventId.make("activity-approval"),
              tone: "approval",
              kind: "approval.requested",
              summary: "Approval requested",
              payload: { requestId, command: "rm -rf" },
              turnId: TurnId.make("turn-1"),
              createdAt: NOW,
            },
            createdAt: NOW,
          });
        }),
      );

      // Second boot: the command read model has no activities, only the
      // durable shell still knows about the open request.
      yield* boot(databasePath, (system) =>
        Effect.gen(function* () {
          expect((yield* system.shell).hasPendingApprovals).toBe(true);
          expect(
            conflictOf(yield* system.dispatchExit(conditionalStart("cmd-a", "msg-a"))),
          ).toMatchObject({ reason: "turn_start_not_idle", idleBlocker: "pending-approval" });
          expect(Option.isNone(yield* system.record("cmd-a"))).toBe(true);
        }),
      );
    }),
  );

  effectIt.effect(
    "keeps rejecting after a restart while a delivery is unresolved, until it is explicitly abandoned",
    () =>
      Effect.gen(function* () {
        const databasePath = yield* freshDatabasePath;
        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            yield* seedThread(system);
            yield* system.dispatch(conditionalStart("cmd-a", "msg-a"));
            yield* system.deliveries.claimAttempt({
              commandId: CommandId.make("cmd-a"),
              attemptedAt: NOW,
            });
            yield* system.deliveries.resolve({
              commandId: CommandId.make("cmd-a"),
              status: "unresolved",
              turnId: null,
              detail: "restart",
              resolvedAt: NOW,
            });
          }),
        );

        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            const later = "2026-01-01T01:00:00.000Z";
            expect(
              conflictOf(yield* system.dispatchExit(conditionalStart("cmd-b", "msg-b", later))),
            ).toMatchObject({
              idleBlocker: "pending-turn-start",
              pendingMessageId: "msg-a",
              pendingDeliveryAttemptedAt: NOW,
            });
            // An ordinary start is allowed and does not touch the unknown outcome.
            yield* system.dispatch(ordinaryStart("cmd-human", "msg-human", later));
            expect(Option.getOrThrow(yield* system.record("cmd-a")).status).toBe("unresolved");
            // Only the explicit resolution releases the thread, and only once.
            expect(
              yield* system.deliveries.abandon({
                commandId: CommandId.make("cmd-a"),
                detail: "operator gave up",
                abandonedAt: later,
              }),
            ).toBe(true);
            expect(
              yield* system.deliveries.abandon({
                commandId: CommandId.make("cmd-a"),
                detail: "twice",
                abandonedAt: later,
              }),
            ).toBe(false);
            expect(Option.getOrThrow(yield* system.record("cmd-a"))).toMatchObject({
              status: "abandoned",
              detail: "operator gave up",
            });
            const muchLater = "2026-01-01T02:00:00.000Z";
            expect(
              Exit.isSuccess(
                yield* system.dispatchExit(conditionalStart("cmd-c", "msg-c", muchLater)),
              ),
            ).toBe(true);
          }),
        );
      }),
  );

  effectIt.effect(
    "rejects targeted interrupts for a missing or different active turn without receipting them",
    () =>
      Effect.gen(function* () {
        const databasePath = yield* freshDatabasePath;
        yield* boot(databasePath, (system) =>
          Effect.gen(function* () {
            yield* seedThread(system);
            const interrupt = (commandId: string, turnId?: string) =>
              system.dispatchExit({
                type: "thread.turn.interrupt",
                commandId: CommandId.make(commandId),
                threadId,
                ...(turnId === undefined ? {} : { turnId: TurnId.make(turnId) }),
                createdAt: NOW,
              });

            yield* system.dispatch(sessionSet("cmd-session-ready", { status: "ready" }));
            expect(conflictOf(yield* interrupt("cmd-i1", "turn-1"))).toEqual({
              reason: "turn_interrupt_stale",
              commandType: "thread.turn.interrupt",
              threadId,
              requestedTurnId: "turn-1",
              activeTurnId: null,
            });

            yield* system.dispatch(
              sessionSet("cmd-session-running", {
                status: "running",
                activeTurnId: TurnId.make("turn-2"),
              }),
            );
            expect(conflictOf(yield* interrupt("cmd-i2", "turn-1"))).toMatchObject({
              reason: "turn_interrupt_stale",
              requestedTurnId: "turn-1",
              activeTurnId: "turn-2",
            });
            expect(Exit.isSuccess(yield* interrupt("cmd-i3", "turn-2"))).toBe(true);
            expect(Exit.isSuccess(yield* interrupt("cmd-i4"))).toBe(true);
            // Stale rejections were not receipted; the accepted ones were.
            expect(Option.isNone(yield* system.receipt("cmd-i1"))).toBe(true);
            expect(Option.isNone(yield* system.receipt("cmd-i2"))).toBe(true);
            expect(Option.isSome(yield* system.receipt("cmd-i3"))).toBe(true);
          }),
        );
      }),
  );
});
