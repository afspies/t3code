import {
  CommandId,
  OrchestrationTurnDelivery,
  OrchestrationTurnDeliveryStatus,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  ClaimTurnDeliveryAttemptInput,
  InsertAcceptedTurnDeliveryInput,
  OrchestrationTurnDeliveryRepository,
  ResolveTurnDeliveryInput,
  AbandonTurnDeliveryInput,
  type OrchestrationTurnDeliveryRepositoryShape,
} from "../Services/OrchestrationTurnDeliveries.ts";

const ChangedRow = Schema.Struct({ commandId: CommandId });

const makeOrchestrationTurnDeliveryRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const selectColumns = sql`
    command_id AS "commandId",
    thread_id AS "threadId",
    message_id AS "messageId",
    status,
    turn_id AS "turnId",
    detail,
    requested_sequence AS "requestedSequence",
    accepted_at AS "acceptedAt",
    attempted_at AS "attemptedAt",
    resolved_at AS "resolvedAt"
  `;

  const insertAcceptedRow = SqlSchema.void({
    Request: InsertAcceptedTurnDeliveryInput,
    execute: (input) =>
      sql`
        INSERT INTO orchestration_turn_deliveries (
          command_id, thread_id, message_id, status, turn_id, detail,
          requested_sequence, accepted_at, attempted_at, resolved_at
        )
        VALUES (
          ${input.commandId}, ${input.threadId}, ${input.messageId}, 'accepted', NULL, NULL,
          ${input.requestedSequence}, ${input.acceptedAt}, NULL, NULL
        )
      `,
  });

  const findByCommandId = SqlSchema.findOneOption({
    Request: CommandId,
    Result: OrchestrationTurnDelivery,
    execute: (commandId) =>
      sql`
        SELECT ${selectColumns}
        FROM orchestration_turn_deliveries
        WHERE command_id = ${commandId}
      `,
  });

  const findPendingRowForThread = SqlSchema.findOneOption({
    Request: ThreadId,
    Result: OrchestrationTurnDelivery,
    execute: (threadId) =>
      sql`
        SELECT ${selectColumns}
        FROM orchestration_turn_deliveries
        WHERE thread_id = ${threadId} AND status IN ('accepted', 'attempted', 'unresolved')
        ORDER BY requested_sequence DESC
        LIMIT 1
      `,
  });

  const abandonRows = SqlSchema.findAll({
    Request: AbandonTurnDeliveryInput,
    Result: ChangedRow,
    execute: (input) =>
      sql`
        UPDATE orchestration_turn_deliveries
        SET status = 'abandoned', detail = ${input.detail}, resolved_at = ${input.abandonedAt}
        WHERE command_id = ${input.commandId} AND status = 'unresolved'
        RETURNING command_id AS "commandId"
      `,
  });

  // RETURNING makes the conditional UPDATE its own claim: an empty result
  // means another handler (live or recovery) already moved the row.
  const claimAttemptRows = SqlSchema.findAll({
    Request: ClaimTurnDeliveryAttemptInput,
    Result: ChangedRow,
    execute: (input) =>
      sql`
        UPDATE orchestration_turn_deliveries
        SET status = 'attempted', attempted_at = ${input.attemptedAt}
        WHERE command_id = ${input.commandId} AND status = 'accepted'
        RETURNING command_id AS "commandId"
      `,
  });

  const resolveRows = SqlSchema.findAll({
    Request: ResolveTurnDeliveryInput,
    Result: ChangedRow,
    execute: (input) =>
      sql`
        UPDATE orchestration_turn_deliveries
        SET status = ${input.status},
            turn_id = ${input.turnId},
            detail = ${input.detail},
            resolved_at = ${input.resolvedAt}
        WHERE command_id = ${input.commandId} AND status IN ('accepted', 'attempted')
        RETURNING command_id AS "commandId"
      `,
  });

  const findByStatus = SqlSchema.findAll({
    Request: OrchestrationTurnDeliveryStatus,
    Result: OrchestrationTurnDelivery,
    execute: (status) =>
      sql`
        SELECT ${selectColumns}
        FROM orchestration_turn_deliveries
        WHERE status = ${status}
        ORDER BY requested_sequence ASC
      `,
  });

  const insertAccepted: OrchestrationTurnDeliveryRepositoryShape["insertAccepted"] = (input) =>
    insertAcceptedRow(input).pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationTurnDeliveryRepository.insertAccepted:query"),
      ),
    );

  const getByCommandId: OrchestrationTurnDeliveryRepositoryShape["getByCommandId"] = (commandId) =>
    findByCommandId(commandId).pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationTurnDeliveryRepository.getByCommandId:query"),
      ),
    );

  const findPendingForThread: OrchestrationTurnDeliveryRepositoryShape["findPendingForThread"] = (
    threadId,
  ) =>
    findPendingRowForThread(threadId).pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationTurnDeliveryRepository.findPendingForThread:query"),
      ),
    );

  const abandon: OrchestrationTurnDeliveryRepositoryShape["abandon"] = (input) =>
    abandonRows(input).pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError(toPersistenceSqlError("OrchestrationTurnDeliveryRepository.abandon:query")),
    );

  const claimAttempt: OrchestrationTurnDeliveryRepositoryShape["claimAttempt"] = (input) =>
    claimAttemptRows(input).pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError(
        toPersistenceSqlError("OrchestrationTurnDeliveryRepository.claimAttempt:query"),
      ),
    );

  const resolve: OrchestrationTurnDeliveryRepositoryShape["resolve"] = (input) =>
    resolveRows(input).pipe(
      Effect.map((rows) => rows.length > 0),
      Effect.mapError(toPersistenceSqlError("OrchestrationTurnDeliveryRepository.resolve:query")),
    );

  const listByStatus: OrchestrationTurnDeliveryRepositoryShape["listByStatus"] = (status) =>
    findByStatus(status).pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationTurnDeliveryRepository.listByStatus:query"),
      ),
    );

  return {
    insertAccepted,
    getByCommandId,
    findPendingForThread,
    abandon,
    claimAttempt,
    resolve,
    listByStatus,
  } satisfies OrchestrationTurnDeliveryRepositoryShape;
});

export const OrchestrationTurnDeliveryRepositoryLive = Layer.effect(
  OrchestrationTurnDeliveryRepository,
  makeOrchestrationTurnDeliveryRepository,
);
