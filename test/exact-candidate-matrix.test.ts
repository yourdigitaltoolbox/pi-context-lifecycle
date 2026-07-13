import { describe, expect, it } from "vitest";
import { assertProductionReleaseLaneOrder, type ProductionReleaseLaneExpectation } from "../src/testing/exact-candidate-matrix.js";
import type { ExactCandidateProbeReceipt } from "../src/testing/exact-candidate-probe.js";

function expected(...entries: ProductionReleaseLaneExpectation[]): readonly ProductionReleaseLaneExpectation[] {
  return entries;
}

describe("exact candidate production release receipts", () => {
  it("proves reply-before-unsolicited from Remote Pi lane-tagged transitions instead of their shared custom type", () => {
    const observations: readonly ExactCandidateProbeReceipt[] = [
      { consumer: "remote-pi", id: "reply", outcome: "released", lane: "mesh-reply" },
      { consumer: "remote-pi", id: "unsolicited", outcome: "released", lane: "mesh-unsolicited" },
    ];

    expect(() => assertProductionReleaseLaneOrder(observations, expected(
      { consumer: "remote-pi", id: "reply", laneId: "mesh-reply" },
      { consumer: "remote-pi", id: "unsolicited", laneId: "mesh-unsolicited" },
    ))).not.toThrow();
  });

  it("proves failure then both success releases from pi-subagents lane receipts", () => {
    const observations: readonly ExactCandidateProbeReceipt[] = [
      // Delivery Promise settlement is intentionally reversed here. The
      // production dispatch witness still proves failure before the success batch.
      { consumer: "pi-subagents", id: "during-success", outcome: "released", laneId: "subagent-success", dispatchSequence: 1 },
      { consumer: "pi-subagents", id: "after-success", outcome: "released", laneId: "subagent-success", dispatchSequence: 1 },
      { consumer: "pi-subagents", id: "during-failure", outcome: "released", laneId: "failure-attention-decision", dispatchSequence: 0 },
    ];

    expect(() => assertProductionReleaseLaneOrder(observations, expected(
      { consumer: "pi-subagents", id: "during-failure", laneId: "failure-attention-decision" },
      { consumer: "pi-subagents", id: "during-success", laneId: "subagent-success" },
      { consumer: "pi-subagents", id: "after-success", laneId: "subagent-success" },
    ))).not.toThrow();
  });

  it("fails closed for a same-custom-type lane reordering or absent lane receipt", () => {
    expect(() => assertProductionReleaseLaneOrder([
      { consumer: "remote-pi", id: "reply", outcome: "released", lane: "mesh-unsolicited" },
      { consumer: "remote-pi", id: "unsolicited", outcome: "released", lane: "mesh-reply" },
    ], expected(
      { consumer: "remote-pi", id: "reply", laneId: "mesh-reply" },
      { consumer: "remote-pi", id: "unsolicited", laneId: "mesh-unsolicited" },
    ))).toThrow(/redacted release receipt/);
    expect(() => assertProductionReleaseLaneOrder([
      { consumer: "pi-subagents", id: "failure", outcome: "released", dispatchSequence: 0 },
    ], expected(
      { consumer: "pi-subagents", id: "failure", laneId: "failure-attention-decision" },
    ))).toThrow(/redacted release receipt/);
    expect(() => assertProductionReleaseLaneOrder([
      { consumer: "pi-subagents", id: "failure", outcome: "released", laneId: "failure-attention-decision" },
    ], expected(
      { consumer: "pi-subagents", id: "failure", laneId: "failure-attention-decision" },
    ))).toThrow(/dispatchSequence/);
  });
});
