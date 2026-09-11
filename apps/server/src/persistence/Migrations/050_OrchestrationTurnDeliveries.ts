import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Effect from "effect/Effect";

// Durable delivery records for conditional (onlyIfIdle) turn starts. One row
// per accepted command; the reactor advances status before and after the
// provider call so a restart can tell unattempted from attempted-but-unknown.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

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
