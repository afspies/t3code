/**
 * Fork (RETENTION.md): race-free WS resume against event-log retention.
 *
 * Retention commits a raised pruned-through floor before it deletes anything
 * at or below it, and the floor never falls. A replay therefore reads its
 * (already bounded) range first and checks the floor afterwards: if the range
 * still starts at or above the floor, no delete that could touch it had begun
 * before the read finished, so the read is complete. Otherwise the caller
 * falls back to a snapshot. This holds however long the subscriber stalls
 * between its first check and the read.
 *
 * @module ReplayGuard
 */
import type { OrchestrationEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

export const materializeIntactReplay = <E1, R1, E2, R2>(input: {
  /** The bounded replay read (callers cap it by count and payload bytes first). */
  readonly read: Stream.Stream<OrchestrationEvent, E1, R1>;
  /** Re-checks the floor after the read; false means the range may be pruned. */
  readonly stillIntact: Effect.Effect<boolean, E2, R2>;
}): Effect.Effect<Option.Option<ReadonlyArray<OrchestrationEvent>>, E1 | E2, R1 | R2> =>
  Effect.gen(function* () {
    const events = Array.from(yield* Stream.runCollect(input.read));
    return (yield* input.stillIntact) ? Option.some(events) : Option.none();
  });
