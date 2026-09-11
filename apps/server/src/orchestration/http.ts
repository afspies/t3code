import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  type EnvironmentInternalError,
  type EnvironmentOrchestrationConflictError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { projectThreadDetailSnapshot } from "./ActivityPayloadProjection.ts";
import { isOrchestrationCommandConflictError } from "./Errors.ts";
import { cleanupFailedUploadedAttachments, normalizeDispatchCommand } from "./Normalizer.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  failEnvironmentOrchestrationConflict,
  requireEnvironmentScope,
} from "../auth/http.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import { OrchestrationTurnDeliveryRepository } from "../persistence/Services/OrchestrationTurnDeliveries.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const projectCloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
    const turnDeliveryRepository = yield* OrchestrationTurnDeliveryRepository;

    return handlers
      .handle(
        "snapshot",
        Effect.fn("environment.orchestration.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          // Serve the lightweight command read model (thread bodies empty)
          // instead of the fully hydrated snapshot. Hydrating every message
          // and activity payload in the database has OOM-killed servers, and
          // the route's only consumer (the project CLI) reads projects alone —
          // UI clients load the shell and per-thread snapshots instead.
          return yield* projectionSnapshotQuery
            .getCommandReadModel()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery
            .getShellSnapshot()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* projectionSnapshotQuery
            .getThreadDetailSnapshot(
              args.params.threadId,
              args.payload.turnLimit === undefined
                ? undefined
                : {
                    turnLimit: args.payload.turnLimit,
                    ...(args.payload.beforeCursor !== undefined
                      ? { beforeCursor: args.payload.beforeCursor }
                      : {}),
                  },
            )
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_thread_snapshot_failed", cause),
              ),
            );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return projectThreadDetailSnapshot(
            snapshot.value,
            args.payload.reasoningMessages === "true",
          );
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          yield* ProjectCloneTracker.rejectCommandsDuringClone(
            projectCloneTracker,
            args.payload,
          ).pipe(
            Effect.catch((cause) =>
              failEnvironmentInternal("orchestration_dispatch_failed", cause),
            ),
          );
          const normalizedCommand = yield* normalizeDispatchCommand(args.payload).pipe(
            Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")),
          );
          const result = yield* orchestrationEngine.dispatch(normalizedCommand).pipe(
            Effect.tapError(() =>
              cleanupFailedUploadedAttachments(args.payload, normalizedCommand),
            ),
            Effect.catch(
              (
                cause,
              ): Effect.Effect<
                never,
                EnvironmentInternalError | EnvironmentOrchestrationConflictError
              > =>
                // Conflict rejections are transient and unreceipted; give
                // automated callers a 409 they can act on instead of a 500.
                isOrchestrationCommandConflictError(cause)
                  ? failEnvironmentOrchestrationConflict(cause.conflict)
                  : failEnvironmentInternal("orchestration_dispatch_failed", cause),
            ),
          );
          yield* ProjectCloneTracker.discardCloneForDeletedProject(
            projectCloneTracker,
            normalizedCommand,
          );
          return result;
        }),
      )
      .handle(
        "turnDelivery",
        Effect.fn("environment.orchestration.turnDelivery")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const delivery = yield* turnDeliveryRepository
            .getByCommandId(args.params.commandId)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_turn_delivery_failed", cause),
              ),
            );
          if (Option.isNone(delivery)) {
            return yield* failEnvironmentNotFound("turn_delivery_not_found");
          }
          return delivery.value;
        }),
      )
      .handle(
        "resolveTurnDelivery",
        Effect.fn("environment.orchestration.resolveTurnDelivery")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const existing = yield* turnDeliveryRepository
            .getByCommandId(args.params.commandId)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_turn_delivery_failed", cause),
              ),
            );
          if (Option.isNone(existing)) {
            return yield* failEnvironmentNotFound("turn_delivery_not_found");
          }
          const abandonedAt = DateTime.formatIso(yield* DateTime.now);
          const abandoned = yield* turnDeliveryRepository
            .abandon({
              commandId: args.params.commandId,
              detail: `${existing.value.detail ?? "Outcome unknown."} Abandoned by operator${args.payload.note === undefined ? "" : `: ${args.payload.note}`}.`,
              abandonedAt,
            })
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_turn_delivery_failed", cause),
              ),
            );
          if (!abandoned) {
            return yield* failEnvironmentInvalidRequest("turn_delivery_not_unresolved");
          }
          const updated = yield* turnDeliveryRepository
            .getByCommandId(args.params.commandId)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_turn_delivery_failed", cause),
              ),
            );
          return Option.isSome(updated) ? updated.value : existing.value;
        }),
      );
  }),
);
