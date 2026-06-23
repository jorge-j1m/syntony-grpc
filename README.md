# syntony-grpc

> Real gRPC transport for [Syntony](https://www.npmjs.com/package/syntony) — turns
> the embedded CRDT primitive into a live, decentralized cluster of peer nodes.

`syntony` on its own is an in-process state primitive. **`syntony-grpc` gives it a
network.** Each `SyntonyNode` runs a gRPC server, joins the cluster through a seed,
and replicates state to its peers — with SWIM-style membership and failure
detection, epidemic gossip, eager delta dissemination, and digest anti-entropy.
There is no central server: every node is an equal peer.

> **Status: v0 — for early adopters validating the concept.** It works: real
> sockets, real convergence, real failure detection. TLS for the peer channel is
> not wired yet (the channel is plaintext with an optional shared-secret check —
> run it on a trusted network), and the timing/observability knobs are minimal.

## Install

```bash
npm install syntony-grpc
```

## Quickstart

```ts
import { SyntonyNode } from "syntony-grpc";

// First node — it's its own seed.
const seed = new SyntonyNode({ address: "0.0.0.0:7000", label: "seed" });
await seed.start();

// Other nodes join through any reachable seed.
const node = new SyntonyNode({
  address: "0.0.0.0:7000",
  advertiseAddress: "10.0.0.2:7000", // how peers reach you (defaults to the bound host:port)
  seeds: ["10.0.0.1:7000"],
  label: "web-1",
});
await node.start(); // resolves once joined AND initially synced (readiness gating)

node.set("region", new TextEncoder().encode("us-east"));
node.get("region"); // local, instant; converges across the fleet in the background

await node.stop();
```

`start()` deliberately resolves only after the node has joined and pulled initial
state, so a node never serves empty values during a rolling deploy (§9.7).

### With the flag product

`syntony-grpc` composes with [`syntony-flag`](https://www.npmjs.com/package/syntony-flag):
wrap the node's `core` with `Flags`.

```ts
import { SyntonyNode } from "syntony-grpc";
import { Flags } from "syntony-flag";

const node = new SyntonyNode({ seeds: ["10.0.0.1:7000"] });
await node.start();

const flags = new Flags(node.core);
if (flags.getBool("new_checkout", false)) renderNewCheckout(); // default at the call site
flags.set("new_checkout", true); // toggled from any instance, replicates fleet-wide
```

## Run the demo

```bash
npm run demo
```

Starts a 3-node cluster in one process, toggles a flag on the seed, and prints it
propagating to convergence:

```
Admin request lands on the seed and toggles new_checkout = true…
  t+ 100ms   true on 3/3 nodes (converged)
```

## Options

| Option | Default | Meaning |
|---|---|---|
| `address` | `0.0.0.0:0` | gRPC bind address (`:0` = ephemeral port) |
| `advertiseAddress` | bound host:port | address peers use to reach this node |
| `seeds` | `[]` | seed peers to join through (any one suffices) |
| `label` | id-seeded mnemonic | human label for dashboards |
| `clusterSecret` | none | shared peer-mesh secret, checked constant-time (§12) |
| `probeIntervalMs` | `1000` | SWIM protocol period |
| `disseminateIntervalMs` | `100` | eager delta push interval |
| `antiEntropyIntervalMs` | `1000` | digest reconciliation interval |
| `pingTimeoutMs` | `500` | direct-probe deadline |
| `indirectProbes` | `3` | indirect-probe fan-out `k` |

## How it works

Three background loops drive the same engine/membership APIs the in-process
`MemoryHub` exercises in tests, but over gRPC:

- **Failure detection** — each period, probe one peer (round-robin); on a missed
  ack, ask `k` peers to probe it indirectly; still nothing → suspect, then confirm
  FAILED after the suspicion timeout.
- **Eager dissemination** — local writes are drained and pushed to peers promptly.
- **Anti-entropy** — periodically exchange a per-key version digest with a random
  peer and pull anything newer or missing. This is the completeness backstop; the
  idempotent CRDT merge makes redelivery and reordering harmless.

Every RPC piggybacks a bounded batch of membership updates, so cluster state
spreads epidemically without a dedicated channel.

## License

Apache-2.0.
