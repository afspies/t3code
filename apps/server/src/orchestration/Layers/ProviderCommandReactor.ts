import { withWorkspaceLease } from "../../workspace/workspaceLease.ts";
import {
  type ChatAttachment,
  CommandId,
  EventId,
  type ModelSelection,
  type OrchestrationEvent,
  ProviderDriverKind,
  type ProjectId,
  type OrchestrationSession,
  ThreadId,
  type ProviderSession,
  type RuntimeMode,
  type TurnId,
} from "@t3tools/contracts";
import { assistantCitationsToPlainText } from "@t3tools/shared/assistantCitations";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import { isTemporaryWorktreeBranch, WORKTREE_BRANCH_PREFIX } from "@t3tools/shared/git";
import * as Cache from "effect/Cache";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

import { resolveThreadWorkspaceCwd } from "../../checkpointing/Utils.ts";
import { increment, orchestrationEventsProcessedTotal } from "../../observability/Metrics.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  ProviderWorkspaceMissingError,
} from "../../provider/Errors.ts";
import type { ProviderServiceError } from "../../provider/Errors.ts";
import type {
  OrchestrationEventStoreError,
  OrchestrationTurnDeliveryRepositoryError,
} from "../../persistence/Errors.ts";
import { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { ProviderAuthService } from "../../provider/Services/ProviderAuthService.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import { OrchestrationTurnDeliveryRepository } from "../../persistence/Services/OrchestrationTurnDeliveries.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderCommandReactor,
  type ProviderCommandReactorShape,
} from "../Services/ProviderCommandReactor.ts";
import { forkParked, ServerActivation } from "../../serverActivation.ts";
import {
  formatThreadTitleContext,
  type ThreadTitleMessage,
} from "../../textGeneration/ThreadTitleContext.ts";
import { canReplaceThreadTitle, DEFAULT_THREAD_TITLE } from "../threadTitles.ts";
import {
  resolveSourceControlWriterModelSelection,
  ServerSettingsService,
} from "../../serverSettings.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { VcsStatusBroadcaster } from "../../vcs/VcsStatusBroadcaster.ts";
import { GitWorkflowService } from "../../git/GitWorkflowService.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
const isProviderAdapterProcessError = Schema.is(ProviderAdapterProcessError);
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isProviderAdapterValidationError = Schema.is(ProviderAdapterValidationError);
const isProviderWorkspaceMissingError = Schema.is(ProviderWorkspaceMissingError);
const isProviderDriverKind = Schema.is(ProviderDriverKind);

type ProviderIntentEvent = Extract<
  OrchestrationEvent,
  {
    type:
      | "thread.meta-updated"
      | "thread.runtime-mode-set"
      | "thread.turn-start-requested"
      | "thread.turn-interrupt-requested"
      | "thread.approval-response-requested"
      | "thread.user-input-response-requested"
      | "thread.session-stop-requested"
      | "thread.settled"
      | "thread.session-set";
  }
>;

function toNonEmptyProviderInput(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : undefined;
}

const isCompactCommandMessage = (message: ThreadTitleMessage): boolean =>
  message.role === "user" &&
  (message.attachments?.length ?? 0) === 0 &&
  message.text.trim().toLowerCase() === "/compact";
function mapProviderSessionStatusToOrchestrationStatus(
  status: "connecting" | "ready" | "running" | "error" | "closed",
): OrchestrationSession["status"] {
  switch (status) {
    case "connecting":
      return "starting";
    case "running":
      return "running";
    case "error":
      return "error";
    case "closed":
      return "stopped";
    case "ready":
    default:
      return "ready";
  }
}

const turnStartKeyForEvent = (event: ProviderIntentEvent): string =>
  event.commandId !== null ? `command:${event.commandId}` : `event:${event.eventId}`;

// Conditional (onlyIfIdle) delivery is limited to adapters whose sendTurn
// returns at provider admission with a correlated turn id, so holding the
// per-thread admission gate across the call is exact and cheap. Long-return
// adapters (Cursor, Antigravity, OpenCode) keep legacy behaviour and are
// refused before any provider contact.
const CONDITIONAL_DELIVERY_DRIVERS: ReadonlySet<string> = new Set(["codex", "claudeAgent"]);

const HANDLED_TURN_START_KEY_MAX = 10_000;
const HANDLED_TURN_START_KEY_TTL = Duration.minutes(30);
const DEFAULT_RUNTIME_MODE: RuntimeMode = "full-access";

function providerErrorLabel(value: string | undefined): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : "unknown";
}

export function providerErrorLabelFromInstanceHint(input: {
  readonly instanceId?: string | undefined;
  readonly modelSelectionInstanceId?: string | undefined;
  readonly sessionProvider?: string | undefined;
}): string {
  return providerErrorLabel(
    input.instanceId ?? input.modelSelectionInstanceId ?? input.sessionProvider,
  );
}

function findProviderAdapterRequestError(
  cause: Cause.Cause<ProviderServiceError>,
): ProviderAdapterRequestError | undefined {
  const failReason = cause.reasons.find(Cause.isFailReason);
  return isProviderAdapterRequestError(failReason?.error) ? failReason.error : undefined;
}

function isUnknownPendingApprovalRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending approval request") ||
      detail.includes("unknown pending permission request") ||
      detail.includes("unknown pending codex approval request")
    );
  }
  const message = Cause.pretty(cause).toLowerCase();
  return (
    message.includes("unknown pending approval request") ||
    message.includes("unknown pending permission request") ||
    message.includes("unknown pending codex approval request")
  );
}

function isUnknownPendingUserInputRequestError(cause: Cause.Cause<ProviderServiceError>): boolean {
  const error = findProviderAdapterRequestError(cause);
  if (error) {
    const detail = error.detail.toLowerCase();
    return (
      detail.includes("unknown pending user-input request") ||
      detail.includes("unknown pending user input request") ||
      detail.includes("unknown pending codex user input request")
    );
  }
  const message = Cause.pretty(cause).toLowerCase();
  return (
    message.includes("unknown pending user-input request") ||
    message.includes("unknown pending user input request") ||
    message.includes("unknown pending codex user input request")
  );
}

function stalePendingRequestDetail(
  requestKind: "approval" | "user-input",
  requestId: string,
): string {
  return `Stale pending ${requestKind} request: ${requestId}. Provider callback state does not survive app restarts or recovered sessions. Restart the turn to continue.`;
}

function buildGeneratedWorktreeBranchName(raw: string): string {
  const normalized = raw
    .trim()
    .toLowerCase()
    .replace(/^refs\/heads\//, "")
    .replace(/['"`]/g, "");

  const withoutPrefix = normalized.startsWith(`${WORKTREE_BRANCH_PREFIX}/`)
    ? normalized.slice(`${WORKTREE_BRANCH_PREFIX}/`.length)
    : normalized;

  const branchFragment = withoutPrefix
    .replace(/[^a-z0-9/_-]+/g, "-")
    .replace(/\/+/g, "/")
    .replace(/-+/g, "-")
    .replace(/^[./_-]+|[./_-]+$/g, "")
    .slice(0, 64)
    .replace(/[./_-]+$/g, "");

  const safeFragment = branchFragment.length > 0 ? branchFragment : "update";
  return `${WORKTREE_BRANCH_PREFIX}/${safeFragment}`;
}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const providerAuthService = yield* ProviderAuthService;
  const providerService = yield* ProviderService;
  const providerRegistry = yield* ProviderRegistry;
  const gitWorkflow = yield* GitWorkflowService;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster;
  const textGeneration = yield* TextGeneration;
  const serverSettingsService = yield* ServerSettingsService;
  const terminalManager = yield* TerminalManager.TerminalManager;
  /** Environment settings with the thread's project overrides applied. */
  const projectSettingsForThread = Effect.fnUntraced(function* (threadId: ThreadId) {
    const settings = yield* serverSettingsService.getSettings;
    if (Object.keys(settings.projectSettingsOverrides).length === 0) return settings;
    const thread = yield* projectionSnapshotQuery
      .getThreadShellById(threadId)
      .pipe(Effect.orElseSucceed(() => Option.none()));
    return resolveProjectSettings(settings, Option.isSome(thread) ? thread.value.projectId : null)
      .settings;
  });
  const turnDeliveryRepository = yield* OrchestrationTurnDeliveryRepository;
  const serverCommandId = (tag: string) =>
    crypto.randomUUIDv4.pipe(Effect.map((uuid) => CommandId.make(`server:${tag}:${uuid}`)));
  const serverEventId = () => crypto.randomUUIDv4.pipe(Effect.map(EventId.make));
  const handledTurnStartKeys = yield* Cache.make<string, true>({
    capacity: HANDLED_TURN_START_KEY_MAX,
    timeToLive: HANDLED_TURN_START_KEY_TTL,
    lookup: () => Effect.succeed(true),
  });

  const hasHandledTurnStartRecently = (key: string) =>
    Cache.getOption(handledTurnStartKeys, key).pipe(
      Effect.flatMap((cached) =>
        Cache.set(handledTurnStartKeys, key, true).pipe(Effect.as(Option.isSome(cached))),
      ),
    );

  const threadModelSelections = new Map<string, ModelSelection>();
  const compactingThreadIds = new Set<ThreadId>();
  type QueuedTurnStart = Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }>;
  // Turn starts received while a thread compacts, replayed in order once its session is restored.
  const turnsAfterCompaction = new Map<ThreadId, Array<QueuedTurnStart>>();
  // Replay command id → the queued turn start it re-requests. `sent` settles once the replay's
  // provider send finishes, which is what lets the next queued turn follow it in order.
  const resumedTurnStarts = new Map<
    CommandId,
    {
      readonly event: QueuedTurnStart;
      readonly queued: Array<QueuedTurnStart>;
      readonly sent: Deferred.Deferred<void>;
    }
  >();
  // Per-thread provider admission gate. Every start on a thread (ordinary or
  // conditional) runs its final busy re-check, durable attempt claim and
  // provider sendTurn under this permit, so an ordinary start cannot slip in
  // between a conditional start's idle check and its provider call.
  const threadAdmissionGates = new Map<ThreadId, Semaphore.Semaphore>();
  const threadAdmissionGate = (threadId: ThreadId) =>
    Effect.sync(() => {
      let gate = threadAdmissionGates.get(threadId);
      if (gate === undefined) {
        gate = Semaphore.makeUnsafe(1);
        threadAdmissionGates.set(threadId, gate);
      }
      return gate;
    });
  const withThreadAdmission = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    threadAdmissionGate(threadId).pipe(Effect.flatMap((gate) => gate.withPermit(effect)));
  const stoppingThreadIds = new Set<ThreadId>();

  const appendProviderFailureActivity = (input: {
    readonly threadId: ThreadId;
    readonly kind:
      | "provider.turn.start.failed"
      | "provider.turn.interrupt.failed"
      | "provider.approval.respond.failed"
      | "provider.user-input.respond.failed"
      | "provider.session.stop.failed";
    readonly summary: string;
    readonly detail: string;
    readonly turnId: TurnId | null;
    readonly createdAt: string;
    readonly requestId?: string;
    readonly payload?: Record<string, unknown>;
  }) =>
    Effect.all({
      commandId: serverCommandId("provider-failure-activity"),
      eventId: serverEventId(),
    }).pipe(
      Effect.flatMap(({ commandId, eventId }) =>
        orchestrationEngine.dispatch({
          type: "thread.activity.append",
          commandId,
          threadId: input.threadId,
          activity: {
            id: eventId,
            tone: "error",
            kind: input.kind,
            summary: input.summary,
            payload: {
              detail: input.detail,
              ...(input.requestId ? { requestId: input.requestId } : {}),
              ...input.payload,
            },
            turnId: input.turnId,
            createdAt: input.createdAt,
          },
          createdAt: input.createdAt,
        }),
      ),
    );

  const cancelTurnsAfterCompaction = Effect.fn("cancelTurnsAfterCompaction")(function* (
    threadId: ThreadId,
    detail: string,
  ) {
    const queued = turnsAfterCompaction.get(threadId) ?? [];
    turnsAfterCompaction.delete(threadId);
    for (const event of queued) {
      yield* appendProviderFailureActivity({
        threadId,
        kind: "provider.turn.start.failed",
        summary: "Queued message was not sent",
        detail,
        turnId: null,
        createdAt: DateTime.formatIso(yield* DateTime.now),
        requestId: event.payload.messageId,
      }).pipe(Effect.ignore({ log: true, message: "failed to report canceled queued message" }));
    }
  });

  const resumeTurnsAfterCompaction = Effect.fn("resumeTurnsAfterCompaction")(function* (
    threadId: ThreadId,
  ) {
    const queued = turnsAfterCompaction.get(threadId) ?? [];
    while (queued.length > 0 && turnsAfterCompaction.get(threadId) === queued) {
      const event = queued[0]!;
      const turnStart = yield* projectionSnapshotQuery.getTurnStartMessage({
        threadId,
        messageId: event.payload.messageId,
      });
      if (turnsAfterCompaction.get(threadId) !== queued) return;
      // In flight from here on: a cancellation reports it when the replay runs, not from the queue.
      queued.shift();
      if (Option.isNone(turnStart)) continue;
      // Reissue the durable request after restoration clears compaction's
      // pending slot. Reusing the message id preserves a single user bubble.
      const commandId = yield* serverCommandId("after-compaction");
      const sent = yield* Deferred.make<void>();
      resumedTurnStarts.set(commandId, { event, queued, sent });
      const { messageId, ...request } = event.payload;
      yield* orchestrationEngine
        .dispatch({
          type: "thread.turn.start",
          commandId,
          ...request,
          message: {
            messageId,
            role: "user",
            text: turnStart.value.message.text,
            attachments: turnStart.value.message.attachments ?? [],
          },
        })
        .pipe(
          Effect.onError(() =>
            Effect.sync(() => {
              resumedTurnStarts.delete(commandId);
              queued.unshift(event);
            }),
          ),
        );
      yield* Deferred.await(sent);
      resumedTurnStarts.delete(commandId);
    }
    if (turnsAfterCompaction.get(threadId) === queued) turnsAfterCompaction.delete(threadId);
  });

  // Delivery evidence for conditional (onlyIfIdle) starts. Observers correlate
  // by payload.commandId / requestId (= the user messageId), never by time.
  const appendTurnDeliveryActivity = (input: {
    readonly threadId: ThreadId;
    readonly kind:
      | "provider.turn.start.attempted"
      | "provider.turn.started"
      | "provider.turn.start.unresolved";
    readonly summary: string;
    readonly payload: Record<string, unknown>;
    readonly turnId: TurnId | null;
    readonly createdAt: string;
  }) =>
    Effect.all({
      commandId: serverCommandId("turn-delivery-activity"),
      eventId: serverEventId(),
    }).pipe(
      Effect.flatMap(({ commandId, eventId }) =>
        orchestrationEngine.dispatch({
          type: "thread.activity.append",
          commandId,
          threadId: input.threadId,
          activity: {
            id: eventId,
            tone: input.kind === "provider.turn.start.unresolved" ? "error" : "info",
            kind: input.kind,
            summary: input.summary,
            payload: input.payload,
            turnId: input.turnId,
            createdAt: input.createdAt,
          },
          createdAt: input.createdAt,
        }),
      ),
      Effect.asVoid,
    );

  const formatFailureDetail = (cause: Cause.Cause<unknown>): string => {
    const failReason = cause.reasons.find(Cause.isFailReason);
    if (isProviderAdapterRequestError(failReason?.error)) {
      return failReason.error.detail;
    }
    if (isProviderAdapterProcessError(failReason?.error)) {
      return failReason.error.detail;
    }
    if (isProviderAdapterValidationError(failReason?.error)) {
      return failReason.error.issue;
    }
    if (isProviderWorkspaceMissingError(failReason?.error)) {
      return failReason.error.message;
    }
    return Cause.pretty(cause);
  };

  const setThreadSession = (input: {
    readonly threadId: ThreadId;
    readonly session: OrchestrationSession;
    readonly createdAt: string;
  }) =>
    serverCommandId("provider-session-set").pipe(
      Effect.flatMap((commandId) =>
        orchestrationEngine.dispatch({
          type: "thread.session.set",
          commandId,
          threadId: input.threadId,
          session: input.session,
          createdAt: input.createdAt,
        }),
      ),
    );

  const setThreadSessionErrorOnTurnStartFailure = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly detail: string;
    readonly createdAt: string;
  }) {
    const thread = yield* resolveThreadShell(input.threadId);
    if (!thread) {
      return;
    }
    const session = thread.session;
    yield* setThreadSession({
      threadId: input.threadId,
      session: {
        ...(session ?? {
          threadId: input.threadId,
          providerName: null,
          providerInstanceId: thread.modelSelection.instanceId,
          runtimeMode: thread.runtimeMode,
        }),
        status: session?.status === "stopped" ? "stopped" : "error",
        activeTurnId: null,
        lastError: input.detail,
        updatedAt: input.createdAt,
      },
      createdAt: input.createdAt,
    });
  });

  const restoreCompaction = Effect.fnUntraced(function* (threadId: ThreadId, fromRunning = false) {
    if (stoppingThreadIds.has(threadId)) {
      compactingThreadIds.delete(threadId);
      return;
    }
    const thread = yield* resolveThreadShell(threadId);
    if (!thread?.session) return;
    if (
      thread.session.status !== "starting" &&
      thread.session.status !== "ready" &&
      (!fromRunning || thread.session.status !== "running")
    )
      return;
    const completedAt = DateTime.formatIso(yield* DateTime.now);
    if (stoppingThreadIds.has(threadId)) {
      compactingThreadIds.delete(threadId);
      return;
    }
    yield* setThreadSession({
      threadId,
      session: {
        ...thread.session,
        status: "ready",
        activeTurnId: null,
        lastError: null,
        updatedAt: completedAt,
      },
      createdAt: completedAt,
    });
  });

  const resolveProject = Effect.fnUntraced(function* (projectId: ProjectId) {
    return yield* projectionSnapshotQuery
      .getProjectShellById(projectId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  /**
   * Recreates a thread's worktree from its branch when the directory has
   * disappeared. Provider sessions resume into the persisted cwd, so a missing
   * worktree makes every later turn fail as a bogus "session not found".
   * Ordinary starts retain the legacy best-effort behavior; conditional starts
   * fail before provider admission when repair cannot complete.
   */
  const ensureThreadWorktree = Effect.fnUntraced(function* (
    thread: {
      readonly id: ThreadId;
      readonly projectId: ProjectId;
      readonly branch: string | null;
      readonly worktreePath: string | null;
    },
    failOnError = false,
  ) {
    const { worktreePath, branch } = thread;
    if (!worktreePath || !branch) {
      return;
    }
    const exists = yield* fileSystem.exists(worktreePath).pipe(Effect.orElseSucceed(() => true));
    if (exists) {
      return;
    }
    const project = yield* resolveProject(thread.projectId);
    if (!project) {
      return;
    }
    const cwd = project.workspaceRoot;
    yield* Effect.logWarning("provider command reactor recreating missing worktree", {
      threadId: thread.id,
      worktreePath,
      branch,
    });
    // A directory deleted without `git worktree remove` leaves an admin entry
    // that makes `git worktree add` refuse the path; prune clears it.
    // Best effort like the rest of this recovery: a settings read failure
    // falls back to the checkout's t3.json.
    const submodules = yield* projectSettingsForThread(thread.id).pipe(
      Effect.map((settings) => settings.worktreeSubmodules),
      Effect.orElseSucceed(() => null),
    );
    yield* gitWorkflow.pruneWorktrees({ cwd }).pipe(
      Effect.andThen(
        gitWorkflow.createWorktree({ cwd, refName: branch, path: worktreePath }, { submodules }),
      ),
      Effect.catchCauseIf(
        (cause) => !Cause.hasInterruptsOnly(cause) && !failOnError,
        (cause) =>
          Effect.logWarning("provider command reactor failed to recreate worktree", {
            threadId: thread.id,
            worktreePath,
            cause: Cause.pretty(cause),
          }),
      ),
    );
  });

  const resolveThreadShell = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadShellById(threadId)
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const resolveThreadDetail = Effect.fnUntraced(function* (threadId: ThreadId) {
    return yield* projectionSnapshotQuery
      .getThreadDetailById(threadId, { activityKinds: [] })
      .pipe(Effect.map(Option.getOrUndefined));
  });

  const rejectStartedThreadModelChangeIfRequired = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly currentModelSelection: ModelSelection;
    readonly requestedModelSelection: ModelSelection | undefined;
  }) {
    const requestedModelSelection = input.requestedModelSelection;
    if (
      requestedModelSelection === undefined ||
      (input.currentModelSelection.instanceId === requestedModelSelection.instanceId &&
        input.currentModelSelection.model === requestedModelSelection.model)
    ) {
      return;
    }
    const providers = yield* providerRegistry.getProviders;
    const requiresNewThread =
      providers.find((snapshot) => snapshot.instanceId === input.currentModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true ||
      providers.find((snapshot) => snapshot.instanceId === requestedModelSelection.instanceId)
        ?.requiresNewThreadForModelChange === true;
    if (!requiresNewThread) {
      return;
    }
    return yield* new ProviderAdapterRequestError({
      provider: providerErrorLabelFromInstanceHint({
        instanceId: String(requestedModelSelection.instanceId),
        modelSelectionInstanceId: String(input.currentModelSelection.instanceId),
      }),
      method: "thread.turn.start",
      detail: `Thread '${input.threadId}' cannot switch models after the conversation has started. Start a new thread to use '${requestedModelSelection.model}'.`,
    });
  });

  const ensureSessionForThread = Effect.fn("ensureSessionForThread")(function* (
    threadId: ThreadId,
    createdAt: string,
    options?: {
      readonly modelSelection?: ModelSelection;
      readonly pendingTurnStart?: boolean;
      // First-turn prompt seed. A manual title that still equals this seed was
      // written by the client's auto-title, not a user rename.
      readonly titleSeed?: string;
    },
  ) {
    const thread = yield* resolveThreadShell(threadId);
    if (!thread) {
      return yield* Effect.die(new Error(`Thread '${threadId}' was not found in read model.`));
    }

    const desiredRuntimeMode = thread.runtimeMode;
    const requestedModelSelection = options?.modelSelection;
    const resolveActiveSession = (threadId: ThreadId) =>
      providerService
        .listSessions()
        .pipe(Effect.map((sessions) => sessions.find((session) => session.threadId === threadId)));

    const activeSession = yield* resolveActiveSession(threadId);
    const activeThreadSession =
      thread.session !== null && thread.session.status !== "stopped" && activeSession
        ? thread.session
        : null;
    if (
      activeThreadSession !== null &&
      activeSession !== undefined &&
      (activeThreadSession.providerInstanceId === undefined ||
        activeSession.providerInstanceId === undefined)
    ) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(activeThreadSession.providerName ?? undefined),
        method: "thread.turn.start",
        detail: `Thread '${threadId}' has an active provider session without a provider instance id.`,
      });
    }
    const currentInstanceId =
      activeThreadSession !== null &&
      activeSession !== undefined &&
      activeSession.providerInstanceId !== undefined
        ? activeSession.providerInstanceId
        : thread.modelSelection.instanceId;
    const desiredModelSelection = requestedModelSelection ?? thread.modelSelection;
    const desiredInstanceId = desiredModelSelection.instanceId;
    const currentInfo = yield* providerService.getInstanceInfo(currentInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(currentInstanceId),
              modelSelectionInstanceId: String(thread.modelSelection.instanceId),
              sessionProvider: thread.session?.providerName ?? undefined,
            }),
            method: "thread.turn.start",
            detail: `Thread '${threadId}' references unknown provider instance '${currentInstanceId}'. The instance is not configured in this build.`,
          }),
      ),
    );
    const desiredInfo = yield* providerService.getInstanceInfo(desiredInstanceId).pipe(
      Effect.mapError(
        () =>
          new ProviderAdapterRequestError({
            provider: providerErrorLabelFromInstanceHint({
              instanceId: String(desiredModelSelection.instanceId),
            }),
            method: "thread.turn.start",
            detail: `Requested provider instance '${desiredInstanceId}' is not configured in this build.`,
          }),
      ),
    );
    const desiredDriverKind = desiredInfo.driverKind;
    if (!isProviderDriverKind(desiredDriverKind)) {
      return yield* new ProviderAdapterRequestError({
        provider: providerErrorLabel(String(desiredDriverKind)),
        method: "thread.turn.start",
        detail: `Requested provider instance '${desiredInstanceId}' uses unknown provider driver '${desiredDriverKind}'. The driver is not installed in this build.`,
      });
    }
    const preferredProvider: ProviderDriverKind = desiredDriverKind;
    if (options?.pendingTurnStart === true && thread.session?.status !== "running") {
      yield* setThreadSession({
        threadId,
        session: {
          threadId,
          status: "starting",
          providerName: activeSession?.provider ?? preferredProvider,
          providerInstanceId: activeSession?.providerInstanceId ?? desiredInstanceId,
          runtimeMode: desiredRuntimeMode,
          activeTurnId: null,
          lastError: null,
          updatedAt: createdAt,
        },
        createdAt,
      });
    }
    if (thread.session !== null) {
      yield* rejectStartedThreadModelChangeIfRequired({
        threadId,
        currentModelSelection:
          activeSession?.model !== undefined
            ? {
                ...thread.modelSelection,
                instanceId: currentInstanceId,
                model: activeSession.model,
              }
            : thread.modelSelection,
        requestedModelSelection,
      });
    }
    if (
      thread.session !== null &&
      requestedModelSelection !== undefined &&
      requestedModelSelection.instanceId !== currentInstanceId
    ) {
      if (currentInfo.driverKind !== desiredInfo.driverKind) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' is bound to driver '${currentInfo.driverKind}' and cannot switch to '${desiredInfo.driverKind}'.`,
        });
      }
      if (
        currentInfo.continuationIdentity.continuationKey !==
        desiredInfo.continuationIdentity.continuationKey
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: preferredProvider,
          method: "thread.turn.start",
          detail: `Thread '${threadId}' cannot switch from instance '${currentInstanceId}' to '${desiredInstanceId}' because their provider resume state is incompatible.`,
        });
      }
    }
    const project = yield* resolveProject(thread.projectId);
    const effectiveCwd = resolveThreadWorkspaceCwd({
      thread,
      projects: project ? [project] : [],
    });
    const refreshWorkspaceSnapshot = effectiveCwd
      ? providerRegistry
          .refreshWorkspaceSnapshot({ instanceId: desiredInstanceId, cwd: effectiveCwd })
          .pipe(Effect.forkDetach)
      : Effect.void;
    // OpenCode skips SessionPrompt.ensureTitle when session.create already has
    // a title. Prompt seeds and "New thread" are not user titles, so omit them
    // and let the provider generate one. A real rename is source "manual" and
    // differs from the first-turn prompt seed (the web client writes that seed
    // through thread.meta.update, which also marks the title manual).
    const manualTitle = thread.titleState?.source === "manual" ? thread.title.trim() : "";
    const promptSeed = options?.titleSeed?.trim();
    const sessionTitle =
      manualTitle.length > 0 && manualTitle !== promptSeed ? thread.title : undefined;

    const startProviderSession = (input?: {
      readonly resumeCursor?: unknown;
      readonly provider?: ProviderDriverKind;
    }) =>
      providerService
        .startSession(threadId, {
          threadId,
          ...(preferredProvider ? { provider: preferredProvider } : {}),
          providerInstanceId: desiredInstanceId,
          ...(effectiveCwd ? { cwd: effectiveCwd } : {}),
          ...(sessionTitle ? { title: sessionTitle } : {}),
          modelSelection: desiredModelSelection,
          ...(input?.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
          runtimeMode: desiredRuntimeMode,
        })
        .pipe(Effect.tap(() => refreshWorkspaceSnapshot));

    const bindSessionToThread = (session: ProviderSession) =>
      Effect.gen(function* () {
        if (session.providerInstanceId === undefined) {
          return yield* new ProviderAdapterRequestError({
            provider: providerErrorLabel(session.provider),
            method: "thread.turn.start",
            detail: `Provider session '${session.threadId}' started without a provider instance id.`,
          });
        }
        yield* setThreadSession({
          threadId,
          session: {
            threadId,
            status:
              options?.pendingTurnStart === true && session.status === "ready"
                ? "starting"
                : mapProviderSessionStatusToOrchestrationStatus(session.status),
            providerName: session.provider,
            providerInstanceId: session.providerInstanceId,
            runtimeMode: desiredRuntimeMode,
            // Provider turn ids are not orchestration turn ids.
            activeTurnId: null,
            lastError: session.lastError ?? null,
            updatedAt: session.updatedAt,
          },
          createdAt,
        });
      });

    const existingSessionThreadId =
      thread.session && thread.session.status !== "stopped" && activeSession ? thread.id : null;
    if (existingSessionThreadId) {
      const runtimeModeChanged = thread.runtimeMode !== thread.session?.runtimeMode;
      const cwdChanged = effectiveCwd !== activeSession?.cwd;
      const sessionModelSwitch = (yield* providerService.getCapabilities(desiredInstanceId))
        .sessionModelSwitch;
      const modelChanged =
        requestedModelSelection !== undefined &&
        requestedModelSelection.model !== activeSession?.model;
      const instanceChanged =
        requestedModelSelection !== undefined &&
        activeSession?.providerInstanceId !== requestedModelSelection.instanceId;
      const shouldRestartForModelChange = modelChanged && sessionModelSwitch === "unsupported";
      const previousModelSelection = threadModelSelections.get(threadId);
      const shouldRestartForModelSelectionChange =
        preferredProvider === "claudeAgent" &&
        requestedModelSelection !== undefined &&
        !Equal.equals(previousModelSelection, requestedModelSelection);

      if (
        !runtimeModeChanged &&
        !cwdChanged &&
        !instanceChanged &&
        !shouldRestartForModelChange &&
        !shouldRestartForModelSelectionChange
      ) {
        yield* refreshWorkspaceSnapshot;
        return existingSessionThreadId;
      }

      const resumeCursor = shouldRestartForModelChange
        ? undefined
        : (activeSession?.resumeCursor ?? undefined);
      yield* Effect.logInfo("provider command reactor restarting provider session", {
        threadId,
        existingSessionThreadId,
        currentProvider: activeSession?.provider,
        currentInstanceId,
        desiredInstanceId,
        desiredProvider: desiredModelSelection.instanceId,
        currentRuntimeMode: thread.session?.runtimeMode,
        desiredRuntimeMode: thread.runtimeMode,
        runtimeModeChanged,
        previousCwd: activeSession?.cwd,
        desiredCwd: effectiveCwd,
        cwdChanged,
        modelChanged,
        instanceChanged,
        shouldRestartForModelChange,
        shouldRestartForModelSelectionChange,
        hasResumeCursor: resumeCursor !== undefined,
      });
      const restartedSession = yield* startProviderSession(
        resumeCursor !== undefined ? { resumeCursor } : undefined,
      );
      yield* Effect.logInfo("provider command reactor restarted provider session", {
        threadId,
        previousSessionId: existingSessionThreadId,
        restartedSessionThreadId: restartedSession.threadId,
        provider: restartedSession.provider,
        runtimeMode: restartedSession.runtimeMode,
        cwd: restartedSession.cwd,
      });
      yield* bindSessionToThread(restartedSession);
      return restartedSession.threadId;
    }

    const startedSession = yield* startProviderSession(undefined);
    yield* bindSessionToThread(startedSession);
    return startedSession.threadId;
  });

  const buildSendTurnRequestForThread = Effect.fnUntraced(function* (input: {
    readonly threadId: ThreadId;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
    readonly modelSelection?: ModelSelection;
    readonly interactionMode?: "default" | "plan";
    readonly createdAt: string;
    readonly titleSeed?: string;
  }) {
    const thread = yield* resolveThreadShell(input.threadId);
    if (!thread) {
      return yield* Effect.die(
        new Error(`Thread '${input.threadId}' was not found in read model.`),
      );
    }
    yield* ensureSessionForThread(input.threadId, input.createdAt, {
      ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
      ...(input.titleSeed !== undefined ? { titleSeed: input.titleSeed } : {}),
      pendingTurnStart: true,
    });
    if (input.modelSelection !== undefined) {
      threadModelSelections.set(input.threadId, input.modelSelection);
    }
    const normalizedInput = toNonEmptyProviderInput(input.messageText);
    const normalizedAttachments = input.attachments ?? [];
    const activeSession = yield* providerService
      .listSessions()
      .pipe(
        Effect.map((sessions) => sessions.find((session) => session.threadId === input.threadId)),
      );
    const sessionModelSwitch =
      activeSession === undefined
        ? "in-session"
        : activeSession.providerInstanceId === undefined
          ? yield* new ProviderAdapterRequestError({
              provider: providerErrorLabel(activeSession.provider),
              method: "thread.turn.start",
              detail: `Active provider session '${activeSession.threadId}' is missing a provider instance id.`,
            })
          : (yield* providerService.getCapabilities(activeSession.providerInstanceId))
              .sessionModelSwitch;
    const requestedModelSelection =
      input.modelSelection ?? threadModelSelections.get(input.threadId) ?? thread.modelSelection;
    const modelForTurn =
      sessionModelSwitch === "unsupported" && input.modelSelection === undefined
        ? activeSession?.model !== undefined
          ? {
              ...requestedModelSelection,
              model: activeSession.model,
            }
          : requestedModelSelection
        : input.modelSelection;

    return {
      threadId: input.threadId,
      ...(normalizedInput ? { input: normalizedInput } : {}),
      ...(normalizedAttachments.length > 0 ? { attachments: normalizedAttachments } : {}),
      ...(modelForTurn !== undefined ? { modelSelection: modelForTurn } : {}),
      ...(input.interactionMode !== undefined ? { interactionMode: input.interactionMode } : {}),
    };
  });

  const maybeGenerateAndRenameWorktreeBranchForFirstTurn = Effect.fn(
    "maybeGenerateAndRenameWorktreeBranchForFirstTurn",
  )(function* (input: {
    readonly threadId: ThreadId;
    readonly branch: string | null;
    readonly worktreePath: string | null;
    readonly messageText: string;
    readonly attachments?: ReadonlyArray<ChatAttachment>;
  }) {
    if (!input.branch || !input.worktreePath) {
      return;
    }
    if (!isTemporaryWorktreeBranch(input.branch)) {
      return;
    }

    const oldBranch = input.branch;
    const cwd = input.worktreePath;
    const attachments = input.attachments ?? [];
    yield* Effect.gen(function* () {
      const settings = yield* projectSettingsForThread(input.threadId);
      const modelSelection =
        settings.sourceControlWriterModelSelection === null
          ? settings.textGenerationModelSelection
          : resolveSourceControlWriterModelSelection(
              settings,
              yield* providerRegistry.getProviders,
            );

      const generated = yield* textGeneration.generateBranchName({
        cwd,
        message: input.messageText,
        ...(attachments.length > 0 ? { attachments } : {}),
        modelSelection,
      });
      if (!generated) return;

      const targetBranch = buildGeneratedWorktreeBranchName(generated.branch);
      if (targetBranch === oldBranch) return;

      const renamed = yield* gitWorkflow.renameBranch({ cwd, oldBranch, newBranch: targetBranch });
      yield* orchestrationEngine.dispatch({
        type: "thread.meta.update",
        commandId: yield* serverCommandId("worktree-branch-rename"),
        threadId: input.threadId,
        branch: renamed.branch,
        worktreePath: cwd,
      });
      yield* vcsStatusBroadcaster.refreshStatus(cwd).pipe(Effect.ignoreCause({ log: true }));
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("provider command reactor failed to generate or rename worktree branch", {
          threadId: input.threadId,
          cwd,
          oldBranch,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  });

  const maybeGenerateThreadTitleForFirstTurn = Effect.fn("maybeGenerateThreadTitleForFirstTurn")(
    function* (input: {
      readonly threadId: ThreadId;
      readonly cwd: string;
      readonly messageText: string;
      readonly attachments?: ReadonlyArray<ChatAttachment>;
      readonly titleSeed?: string;
      readonly expectedTitle: string;
      readonly expectedVersion: CommandId | null;
    }) {
      const attachments = input.attachments ?? [];
      yield* Effect.gen(function* () {
        const { textGenerationModelSelection: modelSelection } = yield* projectSettingsForThread(
          input.threadId,
        );

        const generated = yield* textGeneration
          .generateThreadTitle({
            cwd: input.cwd,
            message: input.messageText,
            ...(attachments.length > 0 ? { attachments } : {}),
            modelSelection,
          })
          .pipe(
            Effect.retry({
              times: 2,
              schedule: Schedule.exponential("2 seconds"),
            }),
          );
        if (!generated) return;

        const thread = yield* resolveThreadShell(input.threadId);
        if (!thread) return;
        if (!canReplaceThreadTitle(thread.title, input.titleSeed)) {
          return;
        }

        yield* orchestrationEngine.dispatch({
          type: "thread.title.generate.complete",
          commandId: yield* serverCommandId("thread-title-rename"),
          threadId: input.threadId,
          title: generated.title === DEFAULT_THREAD_TITLE ? input.expectedTitle : generated.title,
          expectedTitle: input.expectedTitle,
          expectedVersion: input.expectedVersion,
          needsRefinement:
            generated.needsRefinement === true || generated.title === DEFAULT_THREAD_TITLE,
        });
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider command reactor failed to generate or rename thread title", {
            threadId: input.threadId,
            cwd: input.cwd,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    },
  );

  const maybeRefineThreadTitle = Effect.fn("maybeRefineThreadTitle")(function* (
    threadId: ThreadId,
  ) {
    const thread = yield* resolveThreadShell(threadId);
    if (
      !thread?.titleState?.needsRefinement ||
      thread.titleState.source !== "generated" ||
      thread.titleRegeneration != null ||
      thread.latestTurn?.state !== "completed" ||
      thread.session?.status !== "ready"
    )
      return;
    const detail = yield* resolveThreadDetail(threadId);
    if (!detail || detail.messages.filter((message) => message.role === "user").length !== 1)
      return;
    yield* orchestrationEngine.dispatch({
      type: "thread.title.refine",
      commandId: yield* serverCommandId("thread-title-refine"),
      threadId,
      expectedVersion: thread.titleState.version,
    });
  });

  const regenerateThreadTitle = Effect.fn("regenerateThreadTitle")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>,
    requestId: CommandId,
  ) {
    if (event.payload.regenerateTitle !== true) {
      return { _tag: "Superseded" } as const;
    }

    const thread = yield* resolveThreadDetail(event.payload.threadId);
    if (!thread || thread.titleRegeneration?.requestId !== requestId) {
      return { _tag: "Superseded" } as const;
    }

    const { message, attachments } = formatThreadTitleContext(thread.messages);
    if (message.length === 0) {
      return { _tag: "Completed", title: undefined } as const;
    }

    const previousTitle = event.payload.previousTitle ?? thread.title;
    if (thread.title !== previousTitle) {
      return { _tag: "Superseded" } as const;
    }
    const project = yield* resolveProject(thread.projectId);
    const cwd =
      resolveThreadWorkspaceCwd({
        thread,
        projects: project ? [project] : [],
      }) ?? process.cwd();
    const { textGenerationModelSelection: modelSelection } = resolveProjectSettings(
      yield* serverSettingsService.getSettings,
      thread.projectId,
    ).settings;
    const generated = yield* textGeneration.generateThreadTitle({
      cwd,
      message,
      previousTitle,
      ...(attachments.length > 0 ? { attachments } : {}),
      modelSelection,
    });
    if (generated.title === DEFAULT_THREAD_TITLE || generated.title === previousTitle) {
      return { _tag: "Completed", title: undefined } as const;
    }

    const latestThread = yield* resolveThreadShell(event.payload.threadId);
    if (
      !latestThread ||
      latestThread.titleRegeneration?.requestId !== requestId ||
      latestThread.title !== previousTitle
    ) {
      return { _tag: "Superseded" } as const;
    }

    return { _tag: "Completed", title: generated.title } as const;
  });
  const dispatchThreadTitleRegenerationCompletion = Effect.fn(
    "dispatchThreadTitleRegenerationCompletion",
  )(function* (input: {
    readonly threadId: ThreadId;
    readonly requestId: CommandId;
    readonly title?: string;
  }) {
    yield* orchestrationEngine.dispatch({
      type: "thread.title.regeneration.complete",
      commandId: yield* serverCommandId("thread-title-regeneration-complete"),
      threadId: input.threadId,
      requestId: input.requestId,
      ...(input.title !== undefined ? { title: input.title } : {}),
    });
  });
  const findPendingThreadTitles = Effect.fn("findPendingThreadTitles")(function* () {
    const readModel = yield* projectionSnapshotQuery.getCommandReadModel();
    return {
      interruptedRegenerations: readModel.threads.flatMap((thread) => {
        const requestId = thread.titleRegeneration?.requestId;
        return requestId === undefined ? [] : [{ threadId: thread.id, requestId }];
      }),
      refinementThreadIds: readModel.threads
        .filter((thread) => thread.titleState?.needsRefinement)
        .map((thread) => thread.id),
    };
  });
  const clearInterruptedThreadTitleRegenerations = Effect.fn(
    "clearInterruptedThreadTitleRegenerations",
  )(function* (
    interrupted: ReadonlyArray<{ readonly threadId: ThreadId; readonly requestId: CommandId }>,
  ) {
    yield* Effect.forEach(
      interrupted,
      ({ threadId, requestId }) => {
        return dispatchThreadTitleRegenerationCompletion({
          threadId,
          requestId,
        }).pipe(
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause)) {
              return Effect.interrupt;
            }
            return Effect.logWarning(
              "provider command reactor failed to clear interrupted title regeneration",
              {
                threadId,
                cause: Cause.pretty(cause),
              },
            );
          }),
        );
      },
      { discard: true },
    );
  });
  const processThreadTitleRegenerationSafely = Effect.fn("processThreadTitleRegenerationSafely")(
    function* (event: Extract<ProviderIntentEvent, { type: "thread.meta-updated" }>) {
      if (event.payload.regenerateTitle !== true) {
        return;
      }

      const requestId = event.payload.titleRegeneration?.requestId ?? event.commandId;
      if (requestId === null) {
        return;
      }
      const result = yield* regenerateThreadTitle(event, requestId).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) =>
            Effect.logWarning("provider command reactor failed to regenerate thread title", {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.as({ _tag: "Completed", title: undefined } as const)),
        ),
      );
      if (result._tag === "Superseded") {
        return;
      }

      const completion = {
        threadId: event.payload.threadId,
        requestId,
        ...(result.title !== undefined ? { title: result.title } : {}),
      };
      yield* dispatchThreadTitleRegenerationCompletion(completion).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) =>
            Effect.logWarning("provider command reactor retrying title regeneration completion", {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            }).pipe(Effect.andThen(dispatchThreadTitleRegenerationCompletion(completion))),
        ),
      );
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) =>
            Effect.logWarning("provider command reactor failed to complete title regeneration", {
              threadId: event.payload.threadId,
              cause: Cause.pretty(cause),
            }),
        ),
      ),
  );
  const threadTitleRegenerationWorker = yield* makeDrainableWorker(
    processThreadTitleRegenerationSafely,
  );

  const processTurnStartRequested = Effect.fn("processTurnStartRequested")(function* (
    receivedEvent: Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }>,
  ) {
    const resumed =
      receivedEvent.commandId !== null ? resumedTurnStarts.get(receivedEvent.commandId) : undefined;
    const event = resumed ? { ...receivedEvent, payload: resumed.event.payload } : receivedEvent;
    // Conditional starts carry a durable delivery record keyed by the
    // accepting command id. Correlation comes first so every proven
    // pre-attempt exit below settles the record; a restart then never
    // re-drives a start that already reached (or failed before) the provider.
    // They also bypass the volatile handled-key cache: the durable
    // accepted → attempted claim is their single-shot guard, and a deferred
    // record must be processable again when the thread turns idle.
    const deliveryCommandId =
      event.payload.onlyIfIdle === true && event.commandId !== null
        ? CommandId.make(event.commandId)
        : null;
    const key = turnStartKeyForEvent(event);
    if (deliveryCommandId === null && (yield* hasHandledTurnStartRecently(key))) {
      return;
    }

    // The record is the authority: a terminal activity is only appended once
    // the transition is persisted. A write that keeps failing after bounded
    // retries leaves the record where it was (accepted → recovery re-drives;
    // attempted → unresolved at next boot) and never triggers a resend.
    const resolveDelivery = (input: {
      readonly status: "started" | "failed" | "unresolved";
      readonly turnId: TurnId | null;
      readonly detail: string | null;
    }): Effect.Effect<boolean> =>
      deliveryCommandId === null
        ? Effect.succeed(true)
        : DateTime.now.pipe(
            Effect.flatMap((now) =>
              turnDeliveryRepository.resolve({
                commandId: deliveryCommandId,
                status: input.status,
                turnId: input.turnId,
                detail: input.detail,
                resolvedAt: DateTime.formatIso(now),
              }),
            ),
            Effect.retry({ times: 3, schedule: Schedule.exponential("100 millis") }),
            Effect.catchCause((cause) =>
              Effect.logError("failed to persist conditional turn delivery transition", {
                commandId: deliveryCommandId,
                status: input.status,
                cause: Cause.pretty(cause),
              }).pipe(Effect.as(false)),
            ),
          );
    const appendTurnStartFailure = (summary: string, detail: string) =>
      resolveDelivery({ status: "failed", turnId: null, detail }).pipe(
        Effect.flatMap((persisted) =>
          persisted
            ? appendProviderFailureActivity({
                threadId: event.payload.threadId,
                kind: "provider.turn.start.failed",
                summary,
                detail,
                turnId: null,
                createdAt: event.payload.createdAt,
                requestId: event.payload.messageId,
                ...(deliveryCommandId !== null
                  ? { payload: { commandId: deliveryCommandId } }
                  : {}),
              })
            : Effect.void,
        ),
      );

    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      // No thread to attach an activity to; the record alone carries the outcome.
      yield* resolveDelivery({
        status: "failed",
        turnId: null,
        detail: `Thread '${event.payload.threadId}' was not found for turn start request.`,
      });
      return;
    }
    const turnStart = yield* projectionSnapshotQuery.getTurnStartMessage({
      threadId: thread.id,
      messageId: event.payload.messageId,
    });
    if (Option.isNone(turnStart) || turnStart.value.message.role !== "user") {
      yield* appendTurnStartFailure(
        "Provider turn start failed",
        `User message '${event.payload.messageId}' was not found for turn start request.`,
      );
      return;
    }
    const { message, hasOtherUserMessages } = turnStart.value;
    if (resumed && turnsAfterCompaction.get(event.payload.threadId) !== resumed.queued) {
      return yield* appendTurnStartFailure(
        "Queued message was not sent",
        "The queued message was canceled before it could resume. Send it again to continue.",
      );
    }

    // After the durable attempt claim, a failing sendTurn is not proof that
    // nothing reached the provider (Codex can lose the response after
    // admission; Claude answers after local queueing). The record therefore
    // becomes `unresolved`, never `failed`, and is never re-sent by anyone.
    const appendTurnStartUnresolved = (detail: string, attemptedAt: string) =>
      resolveDelivery({ status: "unresolved", turnId: null, detail }).pipe(
        Effect.flatMap((persisted) =>
          persisted
            ? appendTurnDeliveryActivity({
                threadId: event.payload.threadId,
                kind: "provider.turn.start.unresolved",
                summary: "Turn delivery unresolved",
                payload: {
                  requestId: event.payload.messageId,
                  commandId: deliveryCommandId,
                  attemptedAt,
                  detail,
                },
                turnId: null,
                createdAt: event.payload.createdAt,
              })
            : Effect.void,
        ),
      );
    const handleTurnStartFailure = (cause: Cause.Cause<unknown>, attemptedAt?: string) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.void;
      }
      const detail = formatFailureDetail(cause);
      const settleDelivery =
        deliveryCommandId !== null && attemptedAt !== undefined
          ? appendTurnStartUnresolved(
              `Provider call failed after the attempt was recorded; whether the turn ran is unknown: ${detail}`,
              attemptedAt,
            )
          : appendTurnStartFailure("Provider turn start failed", detail);
      return settleDelivery.pipe(
        Effect.catchCause((settlementCause) =>
          Effect.logWarning("provider command reactor failed to append turn start outcome", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(settlementCause),
          }),
        ),
        Effect.andThen(
          setThreadSessionErrorOnTurnStartFailure({
            threadId: event.payload.threadId,
            detail,
            createdAt: event.payload.createdAt,
          }).pipe(
            Effect.catchCause((sessionCause) =>
              Effect.logWarning("provider command reactor failed to update failed turn session", {
                eventType: event.type,
                threadId: event.payload.threadId,
                cause: Cause.pretty(sessionCause),
              }),
            ),
          ),
        ),
        Effect.asVoid,
      );
    };

    const recoverTurnStartFailure = (cause: Cause.Cause<unknown>, attemptedAt?: string) =>
      handleTurnStartFailure(cause, attemptedAt).pipe(
        Effect.catchCause((recoveryCause) =>
          Effect.logWarning("provider command reactor failed to recover turn start failure", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(recoveryCause),
            originalCause: Cause.pretty(cause),
          }),
        ),
      );

    // Conditional starts are delivered verbatim: no native account command or
    // compaction interception, so the delivery record always maps to one
    // provider turn (or one failure).
    const authCommandHandled = yield* Effect.gen(function* () {
      if (deliveryCommandId !== null) {
        return false;
      }
      // Native account commands belong to the thread's existing provider session.
      const instanceId =
        thread.session?.providerInstanceId ??
        event.payload.modelSelection?.instanceId ??
        thread.modelSelection.instanceId;
      const handled = yield* providerAuthService.tryHandlePromptCommand({
        instanceId,
        text: message.text,
        hasAttachments: (message.attachments?.length ?? 0) > 0,
      });
      if (!handled) {
        return false;
      }

      const instanceInfo = yield* providerService.getInstanceInfo(instanceId);
      yield* setThreadSession({
        threadId: thread.id,
        session: {
          threadId: thread.id,
          status: "stopped",
          providerName: instanceInfo.driverKind,
          providerInstanceId: instanceId,
          runtimeMode: thread.runtimeMode,
          activeTurnId: null,
          lastError: null,
          updatedAt: event.payload.createdAt,
        },
        createdAt: event.payload.createdAt,
      });
      yield* orchestrationEngine.dispatch({
        type: "thread.activity.append",
        commandId: yield* serverCommandId("provider-sign-out"),
        threadId: thread.id,
        activity: {
          id: yield* serverEventId(),
          tone: "info",
          kind: "provider.auth.signed-out",
          summary: "Provider signed out",
          payload: { providerInstanceId: instanceId },
          turnId: null,
          createdAt: event.payload.createdAt,
        },
        createdAt: event.payload.createdAt,
      });
      return true;
    }).pipe(Effect.catchCause((cause) => recoverTurnStartFailure(cause).pipe(Effect.as(true))));
    if (authCommandHandled) {
      return;
    }

    // Resolve the driver once: it decides whether conditional delivery is
    // supported and whether the admission gate must span the provider call.
    const effectiveInstanceId =
      thread.session?.providerInstanceId ??
      event.payload.modelSelection?.instanceId ??
      thread.modelSelection.instanceId;
    const driverKind = yield* providerService.getInstanceInfo(effectiveInstanceId).pipe(
      Effect.map((info) => String(info.driverKind)),
      Effect.orElseSucceed(() => null),
    );
    const holdsAdmissionAcrossSend =
      driverKind !== null && CONDITIONAL_DELIVERY_DRIVERS.has(driverKind);
    if (deliveryCommandId !== null && !holdsAdmissionAcrossSend) {
      // Proven pre-attempt failure: nothing was sent, the thread stays idle.
      return yield* appendTurnStartFailure(
        "Provider turn start failed",
        `Conditional turn starts are not supported for provider driver '${driverKind ?? "unknown"}' (supported: codex, claudeAgent).`,
      );
    }

    if (deliveryCommandId === null) {
      yield* ensureThreadWorktree(thread);
    }

    const isCompactCommand = deliveryCommandId === null && isCompactCommandMessage(message);
    if (!hasOtherUserMessages && !isCompactCommand) {
      const project = yield* resolveProject(thread.projectId);
      const generationCwd =
        resolveThreadWorkspaceCwd({
          thread,
          projects: project ? [project] : [],
        }) ?? process.cwd();
      const generationInput = {
        messageText: assistantCitationsToPlainText(message.text),
        ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
        ...(event.payload.titleSeed !== undefined ? { titleSeed: event.payload.titleSeed } : {}),
      };

      yield* maybeGenerateAndRenameWorktreeBranchForFirstTurn({
        threadId: event.payload.threadId,
        branch: thread.branch,
        worktreePath: thread.worktreePath,
        ...generationInput,
      }).pipe(Effect.forkScoped);

      if (
        thread.titleState?.source !== "manual" &&
        canReplaceThreadTitle(thread.title, event.payload.titleSeed)
      ) {
        yield* maybeGenerateThreadTitleForFirstTurn({
          threadId: event.payload.threadId,
          cwd: generationCwd,
          expectedTitle: thread.title,
          expectedVersion: thread.titleState?.version ?? null,
          ...generationInput,
        }).pipe(Effect.forkScoped);
      }
    }

    let compactionSessionEnsured = false;
    const handleCompactionFailure = (cause: Cause.Cause<unknown>) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.void;
      }
      const detail = formatFailureDetail(cause);
      if (!compactionSessionEnsured) {
        return setThreadSessionErrorOnTurnStartFailure({
          threadId: event.payload.threadId,
          detail,
          createdAt: event.payload.createdAt,
        }).pipe(
          Effect.flatMap(() => appendTurnStartFailure("Context compaction failed", detail)),
          Effect.asVoid,
        );
      }
      return appendTurnStartFailure("Context compaction failed", detail).pipe(
        Effect.ensuring(
          restoreCompaction(event.payload.threadId).pipe(
            Effect.catchCause((restoreCause) =>
              Effect.logWarning("failed to restore provider session after compaction failure", {
                threadId: event.payload.threadId,
                cause: Cause.pretty(restoreCause),
              }),
            ),
          ),
        ),
        Effect.asVoid,
      );
    };
    const recoverCompactionFailure = (cause: Cause.Cause<unknown>) =>
      handleCompactionFailure(cause).pipe(
        Effect.catchCause((recoveryCause) =>
          Effect.logWarning("provider command reactor failed to recover compaction failure", {
            eventType: event.type,
            threadId: event.payload.threadId,
            cause: Cause.pretty(recoveryCause),
            originalCause: Cause.pretty(cause),
          }),
        ),
      );
    if (isCompactCommand) {
      if (!hasOtherUserMessages) {
        return yield* appendTurnStartFailure(
          "Context compaction failed",
          "Context compaction requires an existing conversation.",
        );
      }
      const latestThread = yield* resolveThreadShell(event.payload.threadId);
      if (
        compactingThreadIds.has(event.payload.threadId) ||
        turnsAfterCompaction.has(event.payload.threadId) ||
        latestThread?.session?.status === "starting" ||
        latestThread?.session?.status === "running"
      ) {
        yield* appendTurnStartFailure(
          "Context compaction failed",
          "Context compaction is unavailable while a provider turn is running.",
        );
        return;
      }
      compactingThreadIds.add(event.payload.threadId);
      const clearCompacting = Effect.sync(
        () => void compactingThreadIds.delete(event.payload.threadId),
      );
      yield* Effect.gen(function* () {
        yield* ensureSessionForThread(
          event.payload.threadId,
          event.payload.createdAt,
          event.payload.modelSelection !== undefined
            ? { modelSelection: event.payload.modelSelection, pendingTurnStart: true }
            : { pendingTurnStart: true },
        );
        compactionSessionEnsured = true;
        if (event.payload.modelSelection !== undefined) {
          threadModelSelections.set(event.payload.threadId, event.payload.modelSelection);
        }
        yield* providerService.compactThread(
          event.payload.threadId,
          event.payload.modelSelection,
          event.payload.messageId,
        );
      }).pipe(
        Effect.andThen(restoreCompaction(event.payload.threadId, true)),
        Effect.andThen(clearCompacting),
        Effect.andThen(resumeTurnsAfterCompaction(event.payload.threadId)),
        Effect.catchCause((cause) =>
          recoverCompactionFailure(cause).pipe(
            Effect.ensuring(clearCompacting),
            Effect.andThen(
              cancelTurnsAfterCompaction(
                event.payload.threadId,
                "Context compaction failed. Send this message again to continue.",
              ),
            ),
          ),
        ),
        Effect.forkScoped,
      );
      return;
    }
    // Conditional starts are never queued behind compaction: the replay would
    // re-dispatch under a fresh command id and orphan the durable record.
    // They fall through to the conditional path, whose busy check treats a
    // compacting thread as not idle and leaves the record deferred.
    if (
      !resumed &&
      deliveryCommandId === null &&
      (compactingThreadIds.has(event.payload.threadId) ||
        turnsAfterCompaction.has(event.payload.threadId))
    ) {
      const queued = turnsAfterCompaction.get(event.payload.threadId) ?? [];
      queued.push(event);
      turnsAfterCompaction.set(event.payload.threadId, queued);
      return;
    }
    const buildSendTurnRequest = buildSendTurnRequestForThread({
      threadId: event.payload.threadId,
      messageText: projectComposerContextForProvider({
        text: message.text,
        records: message.context?.records ?? [],
      }),
      ...(message.attachments !== undefined ? { attachments: message.attachments } : {}),
      ...(event.payload.modelSelection !== undefined
        ? { modelSelection: event.payload.modelSelection }
        : {}),
      interactionMode: event.payload.interactionMode,
      createdAt: event.payload.createdAt,
      // Later turns must not reuse the current title as titleSeed. Only the
      // first prompt seed should suppress a not-yet-renamed session title.
      ...(!hasOtherUserMessages && event.payload.titleSeed !== undefined
        ? { titleSeed: event.payload.titleSeed }
        : {}),
    });

    const sendConditionalTurn = Effect.gen(function* () {
      if (deliveryCommandId === null) {
        return;
      }
      // Final authoritative idle check under the gate and BEFORE any session
      // setup (setup itself marks the thread `starting`). If ordinary input
      // won the admission race the thread is busy now: leave the record
      // `accepted` (deferred) and let the next idle session-set re-drive it.
      // The thread's own accepted record is not a blocker.
      const latest = yield* resolveThreadShell(event.payload.threadId);
      if (latest === undefined) {
        yield* resolveDelivery({
          status: "failed",
          turnId: null,
          detail: `Thread '${event.payload.threadId}' disappeared before conditional turn setup.`,
        });
        return;
      }
      const otherPending = yield* turnDeliveryRepository.findPendingForThread(
        event.payload.threadId,
      );
      const busy =
        latest.archivedAt !== null ||
        compactingThreadIds.has(event.payload.threadId) ||
        turnsAfterCompaction.has(event.payload.threadId) ||
        latest.session?.status === "starting" ||
        latest.session?.status === "running" ||
        (latest.session?.activeTurnId ?? null) !== null ||
        latest.latestTurn?.state === "running" ||
        latest.hasPendingApprovals ||
        latest.hasPendingUserInput ||
        (Option.isSome(otherPending) && otherPending.value.commandId !== deliveryCommandId);
      if (busy) {
        yield* Effect.logDebug("conditional turn start deferred; thread is busy", {
          commandId: deliveryCommandId,
          threadId: event.payload.threadId,
        });
        return;
      }
      // Worktree and session setup are a proven pre-attempt phase: any
      // failure here is `failed`, nothing reached the provider.
      const sendTurnRequest = yield* ensureThreadWorktree(thread, true).pipe(
        Effect.andThen(buildSendTurnRequest),
        Effect.asSome,
        Effect.catchCause((cause) => handleTurnStartFailure(cause).pipe(Effect.as(Option.none()))),
      );
      if (Option.isNone(sendTurnRequest)) {
        return;
      }
      // Persist the attempt before the provider call. If the process dies
      // between here and the provider's answer, startup recovery finds
      // `attempted` and reports it unresolved instead of sending again. A
      // lost claim means another handler (live or recovery) owns this start.
      const attemptedAt = DateTime.formatIso(yield* DateTime.now);
      const claimed = yield* turnDeliveryRepository.claimAttempt({
        commandId: deliveryCommandId,
        attemptedAt,
      });
      if (!claimed) {
        yield* Effect.logDebug("conditional turn start already claimed; skipping provider call", {
          commandId: deliveryCommandId,
          threadId: event.payload.threadId,
        });
        return;
      }
      yield* appendTurnDeliveryActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.start.attempted",
        summary: "Turn delivery attempted",
        payload: {
          requestId: event.payload.messageId,
          commandId: deliveryCommandId,
          attemptedAt,
        },
        turnId: null,
        createdAt: attemptedAt,
      });
      yield* providerService.sendTurn(sendTurnRequest.value).pipe(
        Effect.flatMap((result) =>
          resolveDelivery({ status: "started", turnId: result.turnId, detail: null }).pipe(
            Effect.flatMap((persisted) =>
              persisted
                ? DateTime.now.pipe(
                    Effect.flatMap((now) =>
                      appendTurnDeliveryActivity({
                        threadId: event.payload.threadId,
                        kind: "provider.turn.started",
                        summary: "Turn started",
                        payload: {
                          requestId: event.payload.messageId,
                          commandId: deliveryCommandId,
                          turnId: result.turnId,
                        },
                        turnId: result.turnId,
                        createdAt: DateTime.formatIso(now),
                      }),
                    ),
                  )
                : Effect.void,
            ),
          ),
        ),
        Effect.catchCause((cause) => recoverTurnStartFailure(cause, attemptedAt)),
      );
    });

    if (deliveryCommandId !== null) {
      // Forked so the worker keeps serving other threads; the per-thread gate
      // serializes setup + attempt + provider admission for this thread.
      // The fork outlives the workspace lease the caller holds around this
      // handler, so the forked setup takes the lease again itself (storage
      // cleanup must not remove the worktree under it). Lease outside, gate
      // inside: an ordinary start waits for the gate while holding the lease,
      // so the opposite order could deadlock.
      const admitted = withThreadAdmission(event.payload.threadId, sendConditionalTurn);
      yield* (
        thread.worktreePath
          ? withWorkspaceLease(path.resolve(thread.worktreePath), admitted)
          : admitted
      ).pipe(Effect.forkScoped);
      return;
    }

    // Ordinary start. On drivers that support conditional delivery the
    // thread's admission permit is taken here, before setup, and released
    // only when the forked sendTurn returns, so this start cannot interleave
    // with a conditional start's idle check → attempt → send sequence. Setup
    // still runs inline in the worker and the send is still forked, exactly
    // as before, so long-return drivers see no gate at all.
    const gate = holdsAdmissionAcrossSend
      ? yield* threadAdmissionGate(event.payload.threadId)
      : null;
    if (gate !== null) {
      yield* gate.take(1);
    }
    const releaseGate = gate === null ? Effect.void : gate.release(1).pipe(Effect.asVoid);
    const sendTurnRequest = yield* buildSendTurnRequest.pipe(
      Effect.asSome,
      Effect.catchCause((cause) => handleTurnStartFailure(cause).pipe(Effect.as(Option.none()))),
    );

    if (Option.isNone(sendTurnRequest)) {
      yield* releaseGate;
      return;
    }

    const send = providerService.sendTurn(sendTurnRequest.value).pipe(
      Effect.asVoid,
      Effect.catchCause((cause) => recoverTurnStartFailure(cause)),
    );
    // The forked send settles `sent` from here on, so drop the entry the post-processing hook uses.
    if (resumed && event.commandId !== null) resumedTurnStarts.delete(event.commandId);
    yield* send.pipe(
      Effect.ensuring(releaseGate),
      Effect.ensuring(resumed ? Deferred.succeed(resumed.sent, undefined) : Effect.void),
      Effect.forkScoped,
    );
  });

  const processTurnInterruptRequested = Effect.fn("processTurnInterruptRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.turn-interrupt-requested" }>,
  ) {
    yield* cancelTurnsAfterCompaction(
      event.payload.threadId,
      "Context compaction was interrupted. Send this message again to continue.",
    );
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }
    const session = thread.session;
    if (!session || session.status === "stopped") {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.interrupt.failed",
        summary: "Provider turn interrupt failed",
        detail: "No active provider session is bound to this thread.",
        turnId: event.payload.turnId ?? null,
        createdAt: event.payload.createdAt,
      });
    }
    // The decider already rejected stale targets, but the active turn can
    // change between that decision and this handler. Re-check against the
    // latest projection so a successor turn (and its child agents) is never
    // interrupted on behalf of a finished one.
    if (event.payload.turnId !== undefined && session.activeTurnId !== event.payload.turnId) {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.turn.interrupt.failed",
        summary: "Provider turn interrupt skipped",
        detail: `Turn '${event.payload.turnId}' is no longer the active turn (active: ${session.activeTurnId ?? "none"}).`,
        turnId: event.payload.turnId,
        createdAt: event.payload.createdAt,
        payload: { requestedTurnId: event.payload.turnId, activeTurnId: session.activeTurnId },
      });
    }

    const recoverInterruptFailure = (cause: Cause.Cause<unknown>) => {
      if (Cause.hasInterruptsOnly(cause)) {
        return Effect.interrupt;
      }

      const detail = formatFailureDetail(cause);
      return Effect.gen(function* () {
        const latestThread = yield* resolveThreadShell(event.payload.threadId);
        const latestSession = latestThread?.session;
        // A targeted interrupt only ever acts on its own turn: once the
        // active id differs, INCLUDING a null successor that is still
        // starting, the fallback stop would hit the successor, so stop here.
        if (
          !latestSession ||
          latestSession.status === "stopped" ||
          latestSession.status === "ready" ||
          (event.payload.turnId !== undefined &&
            latestSession.activeTurnId !== event.payload.turnId)
        ) {
          return;
        }

        yield* providerService.stopSession({ threadId: event.payload.threadId }).pipe(
          Effect.catchCause((stopCause) => {
            if (Cause.hasInterruptsOnly(stopCause)) {
              return Effect.interrupt;
            }
            return Effect.logWarning(
              "provider command reactor failed to stop session after interrupt failure",
              {
                threadId: event.payload.threadId,
                cause: Cause.pretty(stopCause),
                originalCause: Cause.pretty(cause),
              },
            );
          }),
        );
        const stoppedThread = yield* resolveThreadShell(event.payload.threadId);
        const stoppedSession = stoppedThread?.session;
        if (
          !stoppedSession ||
          stoppedSession.status === "stopped" ||
          stoppedSession.status === "ready" ||
          (event.payload.turnId !== undefined &&
            stoppedSession.activeTurnId !== event.payload.turnId)
        ) {
          return;
        }

        yield* setThreadSession({
          threadId: event.payload.threadId,
          session: {
            ...stoppedSession,
            status: "stopped",
            activeTurnId: null,
            lastError: detail,
            updatedAt: event.payload.createdAt,
          },
          createdAt: event.payload.createdAt,
        });
        yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.turn.interrupt.failed",
          summary: "Provider turn interrupt failed",
          detail,
          turnId: event.payload.turnId ?? null,
          createdAt: event.payload.createdAt,
        });
      });
    };

    // Pass the exact target so adapters that key their active turn by this id
    // (Codex runtime, OpenCode) can refuse a stale one before touching child
    // agents. Adapters that ignore the id keep interrupting by session.
    yield* providerService
      .interruptTurn({
        threadId: event.payload.threadId,
        ...(event.payload.turnId !== undefined ? { turnId: event.payload.turnId } : {}),
      })
      .pipe(Effect.catchCause(recoverInterruptFailure));
  });

  const processApprovalResponseRequested = Effect.fn("processApprovalResponseRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.approval-response-requested" }>,
  ) {
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }
    const hasSession = thread.session && thread.session.status !== "stopped";
    if (!hasSession) {
      return yield* appendProviderFailureActivity({
        threadId: event.payload.threadId,
        kind: "provider.approval.respond.failed",
        summary: "Provider approval response failed",
        detail: "No active provider session is bound to this thread.",
        turnId: null,
        createdAt: event.payload.createdAt,
        requestId: event.payload.requestId,
      });
    }

    yield* providerService
      .respondToRequest({
        threadId: event.payload.threadId,
        requestId: event.payload.requestId,
        decision: event.payload.decision,
      })
      .pipe(
        Effect.catchCause((cause) =>
          appendProviderFailureActivity({
            threadId: event.payload.threadId,
            kind: "provider.approval.respond.failed",
            summary: "Provider approval response failed",
            detail: isUnknownPendingApprovalRequestError(cause)
              ? stalePendingRequestDetail("approval", event.payload.requestId)
              : Cause.pretty(cause),
            turnId: null,
            createdAt: event.payload.createdAt,
            requestId: event.payload.requestId,
          }),
        ),
      );
  });

  const processUserInputResponseRequested = Effect.fn("processUserInputResponseRequested")(
    function* (
      event: Extract<ProviderIntentEvent, { type: "thread.user-input-response-requested" }>,
    ) {
      const thread = yield* resolveThreadShell(event.payload.threadId);
      if (!thread) {
        return;
      }
      const hasSession = thread.session && thread.session.status !== "stopped";
      if (!hasSession) {
        return yield* appendProviderFailureActivity({
          threadId: event.payload.threadId,
          kind: "provider.user-input.respond.failed",
          summary: "Provider user input response failed",
          detail: "No active provider session is bound to this thread.",
          turnId: null,
          createdAt: event.payload.createdAt,
          requestId: event.payload.requestId,
        });
      }

      yield* providerService
        .respondToUserInput({
          threadId: event.payload.threadId,
          requestId: event.payload.requestId,
          answers: event.payload.answers,
          ...(event.payload.attachmentsByQuestionId
            ? { attachmentsByQuestionId: event.payload.attachmentsByQuestionId }
            : {}),
        })
        .pipe(
          Effect.catchCause((cause) =>
            appendProviderFailureActivity({
              threadId: event.payload.threadId,
              kind: "provider.user-input.respond.failed",
              summary: "Provider user input response failed",
              detail: isUnknownPendingUserInputRequestError(cause)
                ? stalePendingRequestDetail("user-input", event.payload.requestId)
                : Cause.pretty(cause),
              turnId: null,
              createdAt: event.payload.createdAt,
              requestId: event.payload.requestId,
            }),
          ),
        );
    },
  );

  const processSessionStopRequested = Effect.fn("processSessionStopRequested")(function* (
    event: Extract<ProviderIntentEvent, { type: "thread.session-stop-requested" }>,
  ) {
    const thread = yield* resolveThreadShell(event.payload.threadId);
    if (!thread) {
      return;
    }

    const now = event.payload.createdAt;
    const wasCompacting = compactingThreadIds.has(thread.id);
    stoppingThreadIds.add(thread.id);
    const clearStopping = Effect.sync(() => void stoppingThreadIds.delete(thread.id));
    yield* cancelTurnsAfterCompaction(
      thread.id,
      "The session was stopped during context compaction. Send this message again to continue.",
    ).pipe(
      Effect.andThen(
        thread.session && thread.session.status !== "stopped"
          ? providerService.stopSession({ threadId: thread.id })
          : Effect.void,
      ),
      Effect.matchCauseEffect({
        onFailure: (cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.interrupt;
          }
          const detail = formatFailureDetail(cause);
          return Effect.sync(() => {
            stoppingThreadIds.delete(thread.id);
            return wasCompacting && !compactingThreadIds.has(thread.id);
          }).pipe(
            Effect.flatMap((compactionSettled) =>
              compactionSettled ? restoreCompaction(thread.id) : Effect.void,
            ),
            Effect.andThen(
              appendProviderFailureActivity({
                threadId: thread.id,
                kind: "provider.session.stop.failed",
                summary: "Provider session stop failed",
                detail,
                turnId: null,
                createdAt: now,
              }),
            ),
          );
        },
        onSuccess: () =>
          setThreadSession({
            threadId: thread.id,
            session: {
              threadId: thread.id,
              status: "stopped",
              providerName: thread.session?.providerName ?? null,
              ...(thread.session?.providerInstanceId !== undefined
                ? { providerInstanceId: thread.session.providerInstanceId }
                : {}),
              runtimeMode: thread.session?.runtimeMode ?? DEFAULT_RUNTIME_MODE,
              activeTurnId: null,
              lastError: thread.session?.lastError ?? null,
              updatedAt: now,
            },
            createdAt: now,
          }),
      }),
      Effect.ensuring(clearStopping),
    );
  });

  // Re-enqueue the persisted turn-start-requested event for an `accepted`
  // record. The handler's durable claim makes duplicates harmless.
  const findConditionalStartEvent: (delivery: {
    readonly commandId: CommandId;
    readonly requestedSequence: number;
  }) => Effect.Effect<
    Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }>,
    OrchestrationEventStoreError
  > = Effect.fnUntraced(function* (delivery) {
    const events = yield* Stream.runCollect(
      orchestrationEngine.readEvents(delivery.requestedSequence - 1, 1),
    );
    const requested = Array.from(events).find(
      (
        candidate,
      ): candidate is Extract<ProviderIntentEvent, { type: "thread.turn-start-requested" }> =>
        candidate.sequence === delivery.requestedSequence &&
        candidate.type === "thread.turn-start-requested" &&
        candidate.commandId === delivery.commandId,
    );
    if (requested === undefined) {
      return yield* Effect.die(
        new Error(
          `Conditional turn start '${delivery.commandId}' has no matching event at sequence ${delivery.requestedSequence}.`,
        ),
      );
    }
    return requested;
  });

  const enqueueConditionalStart = Effect.fnUntraced(function* (delivery: {
    readonly commandId: CommandId;
    readonly requestedSequence: number;
  }) {
    yield* worker.enqueue(yield* findConditionalStartEvent(delivery));
  });

  const redriveAcceptedConditionalStarts: (
    threadId: ThreadId,
  ) => Effect.Effect<
    void,
    OrchestrationEventStoreError | OrchestrationTurnDeliveryRepositoryError
  > = Effect.fnUntraced(function* (threadId) {
    const pending = yield* turnDeliveryRepository.findPendingForThread(threadId);
    if (Option.isNone(pending) || pending.value.status !== "accepted") {
      return;
    }
    yield* enqueueConditionalStart(pending.value);
  });

  const processDomainEvent = Effect.fn("processDomainEvent")(function* (
    event: ProviderIntentEvent,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration.event_type": event.type,
      "orchestration.thread_id": event.payload.threadId,
      ...(event.commandId ? { "orchestration.command_id": event.commandId } : {}),
    });
    yield* increment(orchestrationEventsProcessedTotal, {
      eventType: event.type,
    });
    switch (event.type) {
      case "thread.meta-updated":
        if (event.payload.regenerateTitle) yield* threadTitleRegenerationWorker.enqueue(event);
        else if (event.payload.titleState?.needsRefinement)
          yield* maybeRefineThreadTitle(event.payload.threadId);
        return;
      case "thread.session-set": {
        const session = event.payload.session;
        if (session.status === "ready") yield* maybeRefineThreadTitle(event.payload.threadId);
        // A deferred conditional start (record still `accepted` because
        // ordinary input won the admission race) is re-driven once the
        // thread reports idle. The durable claim keeps this single-shot.
        if (
          session.status === "starting" ||
          session.status === "running" ||
          session.activeTurnId !== null
        ) {
          return;
        }
        yield* redriveAcceptedConditionalStarts(event.payload.threadId);
        return;
      }
      case "thread.runtime-mode-set": {
        const thread = yield* resolveThreadShell(event.payload.threadId);
        if (!thread?.session || thread.session.status === "stopped") {
          return;
        }
        const cachedModelSelection = threadModelSelections.get(event.payload.threadId);
        const resume = ensureSessionForThread(
          event.payload.threadId,
          event.occurredAt,
          cachedModelSelection !== undefined ? { modelSelection: cachedModelSelection } : {},
        );
        yield* thread.worktreePath
          ? withWorkspaceLease(path.resolve(thread.worktreePath), resume)
          : resume;
        return;
      }
      case "thread.turn-start-requested": {
        const thread = yield* resolveThreadShell(event.payload.threadId);
        yield* thread?.worktreePath
          ? withWorkspaceLease(path.resolve(thread.worktreePath), processTurnStartRequested(event))
          : processTurnStartRequested(event);
        return;
      }
      case "thread.turn-interrupt-requested":
        yield* processTurnInterruptRequested(event);
        return;
      case "thread.approval-response-requested":
        yield* processApprovalResponseRequested(event);
        return;
      case "thread.user-input-response-requested":
        yield* processUserInputResponseRequested(event);
        return;
      case "thread.session-stop-requested":
        yield* processSessionStopRequested(event);
        return;
      case "thread.settled": {
        const thread = yield* projectionSnapshotQuery.getThreadShellById(event.payload.threadId);
        // A thread re-engaged before this event ran keeps its shells and session.
        if (Option.isNone(thread) || thread.value.settledOverride !== "settled") {
          return;
        }
        // Idle shells close so they stop holding the worktree. A terminal that
        // runs a command (a dev server, an editor) stays for the user to close.
        yield* terminalManager.closeIdle({ threadId: event.payload.threadId });
        if (thread.value.session == null || thread.value.session.status === "stopped") {
          return;
        }
        yield* orchestrationEngine.dispatch({
          type: "thread.session.stop",
          commandId: CommandId.make(`session-stop-for-settle:${event.commandId ?? event.eventId}`),
          threadId: event.payload.threadId,
          createdAt: event.occurredAt,
          onlyIfSettled: true,
        });
        return;
      }
    }
  });

  const processDomainEventSafely = (event: ProviderIntentEvent) =>
    processDomainEvent(event).pipe(
      // A replay that returned before forking its send still holds its entry; settle it so
      // the compaction queue moves on. Forked sends drop the entry first and settle it themselves.
      Effect.ensuring(
        Effect.suspend(() => {
          const resumed = event.commandId !== null && resumedTurnStarts.get(event.commandId);
          return resumed ? Deferred.succeed(resumed.sent, undefined) : Effect.void;
        }),
      ),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning("provider command reactor failed to process event", {
          eventType: event.type,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const worker = yield* makeDrainableWorker(processDomainEventSafely);

  const start: ProviderCommandReactorShape["start"] = Effect.fn("start")(function* () {
    const pendingTitles = yield* findPendingThreadTitles().pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning("provider command reactor failed to find pending thread titles", {
          failureKind: Cause.hasDies(cause) ? "defect" : "failure",
          reasonCount: cause.reasons.length,
        }).pipe(Effect.as({ interruptedRegenerations: [], refinementThreadIds: [] }));
      }),
    );
    const processEvent = Effect.fn("processEvent")(function* (event: OrchestrationEvent) {
      if (
        (event.type === "thread.meta-updated" &&
          (event.payload.regenerateTitle === true ||
            event.payload.titleState?.needsRefinement === true)) ||
        event.type === "thread.runtime-mode-set" ||
        event.type === "thread.turn-start-requested" ||
        event.type === "thread.turn-interrupt-requested" ||
        event.type === "thread.approval-response-requested" ||
        event.type === "thread.user-input-response-requested" ||
        event.type === "thread.session-stop-requested" ||
        event.type === "thread.session-set" ||
        event.type === "thread.settled"
      ) {
        return yield* worker.enqueue(event);
      }
    });

    // Startup fence. Nothing has been processed by this reactor yet, so any
    // `attempted` record is a pre-boot attempt whose provider outcome is
    // unknown: mark it unresolved BEFORE subscribing, so a current-boot
    // attempt can never be mistaken for one. `accepted` records are then
    // re-driven through the durable claim after the live subscription exists.
    // The fence fails closed: if the durable scan cannot complete after
    // bounded retries, start() fails rather than advertising a reactor that
    // would silently ignore pre-boot deliveries.
    const withRecoveryRetry = <A, E>(effect: Effect.Effect<A, E>) =>
      effect.pipe(
        Effect.retry({ times: 3, schedule: Schedule.exponential("100 millis") }),
        Effect.orDie,
      );
    const attemptedBeforeBoot = yield* withRecoveryRetry(
      turnDeliveryRepository.listByStatus("attempted"),
    );
    for (const delivery of attemptedBeforeBoot) {
      const detail =
        "The server restarted while the provider call was in flight; whether the turn ran is unknown.";
      const resolvedAt = DateTime.formatIso(yield* DateTime.now);
      const resolved = yield* withRecoveryRetry(
        turnDeliveryRepository.resolve({
          commandId: delivery.commandId,
          status: "unresolved",
          turnId: null,
          detail,
          resolvedAt,
        }),
      );
      if (!resolved) continue;
      yield* appendTurnDeliveryActivity({
        threadId: delivery.threadId,
        kind: "provider.turn.start.unresolved",
        summary: "Turn delivery unresolved",
        payload: {
          requestId: delivery.messageId,
          commandId: delivery.commandId,
          attemptedAt: delivery.attemptedAt,
          detail,
        },
        turnId: null,
        createdAt: resolvedAt,
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("failed to append unresolved delivery activity", {
            commandId: delivery.commandId,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    }

    // Subscribe before returning, even while event handling waits for server activation.
    const domainEvents = yield* orchestrationEngine.subscribeDomainEvents;
    yield* forkParked(Stream.runForEach(domainEvents, processEvent));

    const recoverConditionalTurnStarts = Effect.gen(function* () {
      const accepted = yield* withRecoveryRetry(turnDeliveryRepository.listByStatus("accepted"));
      return yield* Effect.forEach(accepted, (delivery) =>
        withRecoveryRetry(findConditionalStartEvent(delivery)),
      );
    });
    // Earlier events do not replay. Clear interrupted requests by their captured
    // IDs, then schedule persisted refinements after subscribing to their events.
    const recoverTitles = clearInterruptedThreadTitleRegenerations(
      pendingTitles.interruptedRegenerations,
    ).pipe(
      Effect.andThen(
        Effect.forEach(pendingTitles.refinementThreadIds, maybeRefineThreadTitle, {
          discard: true,
        }),
      ),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.interrupt;
        }
        return Effect.logWarning(
          "provider command reactor failed to recover pending thread titles",
          {
            failureKind: Cause.hasDies(cause) ? "defect" : "failure",
            reasonCount: cause.reasons.length,
          },
        );
      }),
    );
    // Validate every accepted record while startup can still fail. Enqueueing
    // remains activation-gated, but scan/correlation failures reach start().
    const recoveredConditionalStarts = yield* recoverConditionalTurnStarts;
    const enqueueRecoveredConditionalStarts = Effect.forEach(
      recoveredConditionalStarts,
      worker.enqueue,
      { discard: true },
    );
    const activation = yield* ServerActivation;
    if (activation === undefined) {
      yield* recoverTitles;
      yield* enqueueRecoveredConditionalStarts;
    } else {
      yield* forkParked(recoverTitles);
      yield* forkParked(enqueueRecoveredConditionalStarts);
    }
  });

  return {
    start,
    drain: Effect.gen(function* () {
      yield* worker.drain;
      yield* threadTitleRegenerationWorker.drain;
    }),
  } satisfies ProviderCommandReactorShape;
});

export const ProviderCommandReactorLive = Layer.effect(ProviderCommandReactor, make);
