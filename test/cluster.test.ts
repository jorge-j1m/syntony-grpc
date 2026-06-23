import { NodeStatus } from "syntony";
import { afterEach, describe, expect, it } from "vitest";
import { SyntonyNode, type SyntonyNodeOptions } from "../src/index.js";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array | undefined) => (b ? new TextDecoder().decode(b) : undefined);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor(predicate: () => boolean, timeoutMs = 6000, stepMs = 25): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await sleep(stepMs);
  }
  throw new Error("waitFor timed out");
}

// Fast gossip timings so the cluster converges quickly under test.
const FAST: SyntonyNodeOptions = {
  address: "127.0.0.1:0",
  probeIntervalMs: 150,
  pingTimeoutMs: 300,
  disseminateIntervalMs: 30,
  antiEntropyIntervalMs: 120,
  rpcTimeoutMs: 800,
  joinTimeoutMs: 1500,
};

const started: SyntonyNode[] = [];
async function launch(opts: SyntonyNodeOptions): Promise<SyntonyNode> {
  const node = new SyntonyNode({ ...FAST, ...opts });
  await node.start();
  started.push(node);
  return node;
}

afterEach(async () => {
  await Promise.all(started.splice(0).map((n) => n.stop()));
});

describe("SyntonyNode over real gRPC", () => {
  it("is ready and addressable after start", async () => {
    const node = await launch({});
    expect(node.ready).toBe(true);
    expect(node.address).toMatch(/^127\.0\.0\.1:\d+$/);
    expect(node.typeId).toMatch(/^node_/);
  });

  it("a joining node learns the seed and converges on its data", async () => {
    const seed = await launch({});
    seed.set("region", enc("us-east"));

    const peer = await launch({ seeds: [seed.address] });

    // The joiner sees the seed in its member list...
    expect(peer.members().map((m) => m.typeIdStr)).toContain(seed.typeId);
    // ...and pulls existing state during the initial sync (§9.7, §10.2).
    await waitFor(() => dec(peer.get("region")) === "us-east");
    expect(dec(peer.get("region"))).toBe("us-east");
  });

  it("propagates a write made after join to every peer", async () => {
    const seed = await launch({});
    const a = await launch({ seeds: [seed.address] });
    const b = await launch({ seeds: [seed.address] });

    // A write on the last node reaches the seed and the other peer.
    b.set("new_checkout", enc("true"));

    await waitFor(
      () => dec(seed.get("new_checkout")) === "true" && dec(a.get("new_checkout")) === "true",
    );
    expect(dec(seed.get("new_checkout"))).toBe("true");
    expect(dec(a.get("new_checkout"))).toBe("true");
  });

  it("converges concurrent writes to different keys across the cluster", async () => {
    const seed = await launch({});
    const a = await launch({ seeds: [seed.address] });
    const b = await launch({ seeds: [seed.address] });
    await waitFor(() => a.members().length === 3 && b.members().length === 3, 8000);

    seed.set("k_seed", enc("0"));
    a.set("k_a", enc("1"));
    b.set("k_b", enc("2"));

    const allHave = (n: SyntonyNode) =>
      dec(n.get("k_seed")) === "0" && dec(n.get("k_a")) === "1" && dec(n.get("k_b")) === "2";
    await waitFor(() => [seed, a, b].every(allHave), 8000);
    for (const n of [seed, a, b]) expect(allHave(n)).toBe(true);
  });

  it("detects and confirms a failed node (SWIM, §9)", async () => {
    const seed = await launch({});
    const victim = await launch({ seeds: [seed.address] });
    await waitFor(() => seed.members().some((m) => m.typeIdStr === victim.typeId));

    const victimId = victim.typeId;
    await victim.stop();
    started.splice(started.indexOf(victim), 1);

    // SWIM suspects, then confirms FAILED after the suspicion timeout (~3s).
    await waitFor(
      () => seed.members().some((m) => m.typeIdStr === victimId && m.status === NodeStatus.FAILED),
      10000,
    );
    const record = seed.members().find((m) => m.typeIdStr === victimId)!;
    expect(record.status).toBe(NodeStatus.FAILED);
  });

  it("rejects a peer presenting the wrong cluster secret (§12)", async () => {
    const seed = await launch({ clusterSecret: "correct-horse" });
    seed.set("secret_flag", enc("yes"));

    const intruder = await launch({ seeds: [seed.address], clusterSecret: "wrong-secret" });

    // The intruder's Join is rejected, so it learns no peers and gets no data.
    await sleep(800);
    expect(intruder.members()).toHaveLength(1);
    expect(intruder.get("secret_flag")).toBeUndefined();
  });
});
