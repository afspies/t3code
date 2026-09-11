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

// This fork-only table is created here, at layer construction, and NOT via a
// numbered entry in `../Migrations.ts`. Effect's Migrator only runs entries
// whose id is greater than the latest id recorded in `effect_sql_migrations`,
// so a fork migration numbered below upstream's newest id is skipped on a
// database that upstream has already migrated, and one numbered above it
// shadows the next upstream migration with the same id (this fork's former
// `050_OrchestrationTurnDeliveries` collided with upstream's
// `050_ProjectionThreadPullRequests`). Keeping the fork out of the ledger
// entirely makes the table independent of upstream's numbering; both
// statements are idempotent, so databases that already carry the table from
// the old numbered migration are left untouched. The schema below must stay
// identical to that migration's.
const ensureOrchestrationTurnDeliveriesSchema = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
) {
  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_turn_deliveries (
      command_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      status TEXT NOT NULL,
      turn_id TEXT,
      detail TEXT,
      requested_sequence INTEGER NOT NULL,
      accepted_at TEXT NOT NULL,
      attempted_at TEXT,
      resolved_at TEXT
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_orch_turn_deliveries_thread_status
    ON orchestration_turn_deliveries(thread_id, status)
  `;
});

const makeOrchestrationTurnDeliveryRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* ensureOrchestrationTurnDeliveriesSchema(sql).pipe(
    Effect.mapError(toPersistenceSqlError("OrchestrationTurnDeliveryRepository.ensureSchema")),
  );

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
