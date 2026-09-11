import { OrchestrationConflict, ThreadId } from "@t3tools/contracts";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Schema from "effect/Schema";

import type { ProjectionRepositoryError } from "../persistence/Errors.ts";

export class OrchestrationCommandJsonParseError extends Schema.TaggedErrorClass<OrchestrationCommandJsonParseError>()(
  "OrchestrationCommandJsonParseError",
  {
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Invalid orchestration command JSON: ${this.detail}`;
  }
}

export class OrchestrationCommandDecodeError extends Schema.TaggedErrorClass<OrchestrationCommandDecodeError>()(
  "OrchestrationCommandDecodeError",
  {
    issue: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Invalid orchestration command payload: ${this.issue}`;
  }
}

export class OrchestrationCommandInvariantError extends Schema.TaggedErrorClass<OrchestrationCommandInvariantError>()(
  "OrchestrationCommandInvariantError",
  {
    commandType: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Orchestration command invariant failed (${this.commandType}): ${this.detail}`;
  }
}

export class OrchestrationThreadSettleBlockedError extends Schema.TaggedErrorClass<OrchestrationThreadSettleBlockedError>()(
  "OrchestrationThreadSettleBlockedError",
  {
    threadId: ThreadId,
  },
) {
  override get message(): string {
    return "This thread still needs attention. Resolve or interrupt it first, then try again.";
  }
}

/**
 * Transient rejection: the command's precondition (thread idle, target turn
 * still active) does not hold right now. Deliberately NOT part of
 * OrchestrationCommandRejection: the engine writes no receipt for it, so the
 * identical command can be re-sent once the state changes.
 */
export class OrchestrationCommandConflictError extends Schema.TaggedErrorClass<OrchestrationCommandConflictError>()(
  "OrchestrationCommandConflictError",
  {
    conflict: OrchestrationConflict,
  },
) {
  override get message(): string {
    const conflict = this.conflict;
    return conflict.reason === "turn_start_not_idle"
      ? `Thread '${conflict.threadId}' is not idle (${conflict.idleBlocker}).`
      : `Turn '${conflict.requestedTurnId}' is not the active turn on thread '${conflict.threadId}' (active: ${conflict.activeTurnId ?? "none"}).`;
  }
}
export const isOrchestrationCommandConflictError = Schema.is(OrchestrationCommandConflictError);

export const OrchestrationCommandRejection = Schema.Union([
  OrchestrationCommandInvariantError,
  OrchestrationThreadSettleBlockedError,
]);
export type OrchestrationCommandRejection = typeof OrchestrationCommandRejection.Type;
export const isOrchestrationCommandRejection = Schema.is(OrchestrationCommandRejection);

export class OrchestrationCommandPreviouslyRejectedError extends Schema.TaggedErrorClass<OrchestrationCommandPreviouslyRejectedError>()(
  "OrchestrationCommandPreviouslyRejectedError",
  {
    commandId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Command previously rejected (${this.commandId}): ${this.detail}`;
  }
}

export class OrchestrationCommandIdConflictError extends Schema.TaggedErrorClass<OrchestrationCommandIdConflictError>()(
  "OrchestrationCommandIdConflictError",
  {
    commandId: Schema.String,
    receiptAggregateKind: Schema.String,
    receiptAggregateId: Schema.String,
    commandAggregateKind: Schema.String,
    commandAggregateId: Schema.String,
  },
) {
  override get message(): string {
    return `Command id '${this.commandId}' already used for ${this.receiptAggregateKind} '${this.receiptAggregateId}'; refusing to replay its receipt for ${this.commandAggregateKind} '${this.commandAggregateId}'.`;
  }
}

export class OrchestrationProjectorDecodeError extends Schema.TaggedErrorClass<OrchestrationProjectorDecodeError>()(
  "OrchestrationProjectorDecodeError",
  {
    eventType: Schema.String,
    issue: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Projector decode failed for ${this.eventType}: ${this.issue}`;
  }
}

export class OrchestrationListenerCallbackError extends Schema.TaggedErrorClass<OrchestrationListenerCallbackError>()(
  "OrchestrationListenerCallbackError",
  {
    listener: Schema.Literals(["read-model", "domain-event"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Orchestration ${this.listener} listener failed: ${this.detail}`;
  }
}

export type OrchestrationDispatchError =
  | ProjectionRepositoryError
  | OrchestrationCommandRejection
  | OrchestrationCommandConflictError
  | OrchestrationCommandIdConflictError
  | OrchestrationCommandPreviouslyRejectedError
  | OrchestrationProjectorDecodeError
  | OrchestrationListenerCallbackError;

export type OrchestrationEngineError =
  | OrchestrationDispatchError
  | OrchestrationCommandJsonParseError
  | OrchestrationCommandDecodeError;

export function toOrchestrationCommandDecodeError(error: Schema.SchemaError) {
  return new OrchestrationCommandDecodeError({
    issue: SchemaIssue.makeFormatterDefault()(error.issue),
    cause: error,
  });
}

export function toProjectorDecodeError(eventType: string) {
  return (error: Schema.SchemaError): OrchestrationProjectorDecodeError =>
    new OrchestrationProjectorDecodeError({
      eventType,
      issue: SchemaIssue.makeFormatterDefault()(error.issue),
      cause: error,
    });
}

export function toOrchestrationJsonParseError(cause: unknown) {
  return new OrchestrationCommandJsonParseError({
    detail: `Failed to parse orchestration command JSON`,
    cause,
  });
}

export function toListenerCallbackError(listener: "read-model" | "domain-event") {
  return (cause: unknown): OrchestrationListenerCallbackError =>
    new OrchestrationListenerCallbackError({
      listener,
      detail: `Failed to invoke orchestration ${listener} listener`,
      cause,
    });
}
