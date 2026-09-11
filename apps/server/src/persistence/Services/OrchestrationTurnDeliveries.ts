/**
 * OrchestrationTurnDeliveryRepository - durable delivery records for
 * conditional (onlyIfIdle) turn starts.
 *
 * The engine inserts `accepted` in the same transaction as the command's
 * events. The provider command reactor claims `attempted` immediately before
 * the provider call and resolves `started`/`failed` from the call's outcome.
 * Startup recovery re-drives `accepted` rows and marks leftover `attempted`
 * rows `unresolved`, since their provider outcome is unknown.
 *
 * @module OrchestrationTurnDeliveryRepository
 */
import {
  CommandId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  OrchestrationTurnDelivery,
  OrchestrationTurnDeliveryStatus,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { OrchestrationTurnDeliveryRepositoryError } from "../Errors.ts";

export const InsertAcceptedTurnDeliveryInput = Schema.Struct({
  commandId: CommandId,
  threadId: ThreadId,
  messageId: MessageId,
  requestedSequence: NonNegativeInt,
  acceptedAt: IsoDateTime,
});
export type InsertAcceptedTurnDeliveryInput = typeof InsertAcceptedTurnDeliveryInput.Type;

export const ClaimTurnDeliveryAttemptInput = Schema.Struct({
  commandId: CommandId,
  attemptedAt: IsoDateTime,
});
export type ClaimTurnDeliveryAttemptInput = typeof ClaimTurnDeliveryAttemptInput.Type;

export const ResolveTurnDeliveryInput = Schema.Struct({
  commandId: CommandId,
  status: Schema.Literals(["started", "failed", "unresolved"]),
  turnId: Schema.NullOr(TurnId),
  detail: Schema.NullOr(Schema.String),
  resolvedAt: IsoDateTime,
});
export type ResolveTurnDeliveryInput = typeof ResolveTurnDeliveryInput.Type;

export const AbandonTurnDeliveryInput = Schema.Struct({
  commandId: CommandId,
  detail: Schema.String,
  abandonedAt: IsoDateTime,
});
export type AbandonTurnDeliveryInput = typeof AbandonTurnDeliveryInput.Type;

export interface OrchestrationTurnDeliveryRepositoryShape {
  /** Insert the `accepted` row. Called inside the engine's command transaction. */
  readonly insertAccepted: (
    input: InsertAcceptedTurnDeliveryInput,
  ) => Effect.Effect<void, OrchestrationTurnDeliveryRepositoryError>;

  readonly getByCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<
    Option.Option<OrchestrationTurnDelivery>,
    OrchestrationTurnDeliveryRepositoryError
  >;

  /**
   * Latest conditional start on the thread that still blocks idle:
   * `accepted`, `attempted`, or `unresolved`. An unresolved attempt keeps
   * blocking until an operator explicitly abandons it; ordinary starts never
   * clear it.
   */
  readonly findPendingForThread: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Option.Option<OrchestrationTurnDelivery>,
    OrchestrationTurnDeliveryRepositoryError
  >;

  /** Explicit reconciliation: `unresolved` → `abandoned`. False when the row was not unresolved. */
  readonly abandon: (
    input: AbandonTurnDeliveryInput,
  ) => Effect.Effect<boolean, OrchestrationTurnDeliveryRepositoryError>;

  /**
   * Move `accepted` → `attempted`. Returns false when the row was not
   * `accepted`, which is how a live handler and startup recovery agree on a
   * single attempt without coordinating in memory.
   */
  readonly claimAttempt: (
    input: ClaimTurnDeliveryAttemptInput,
  ) => Effect.Effect<boolean, OrchestrationTurnDeliveryRepositoryError>;

  /** Terminal transition. Only rows still `accepted` or `attempted` change. */
  readonly resolve: (
    input: ResolveTurnDeliveryInput,
  ) => Effect.Effect<boolean, OrchestrationTurnDeliveryRepositoryError>;

  readonly listByStatus: (
    status: OrchestrationTurnDeliveryStatus,
  ) => Effect.Effect<
    ReadonlyArray<OrchestrationTurnDelivery>,
    OrchestrationTurnDeliveryRepositoryError
  >;
}

export class OrchestrationTurnDeliveryRepository extends Context.Service<
  OrchestrationTurnDeliveryRepository,
  OrchestrationTurnDeliveryRepositoryShape
>()("t3/persistence/Services/OrchestrationTurnDeliveries/OrchestrationTurnDeliveryRepository") {}
