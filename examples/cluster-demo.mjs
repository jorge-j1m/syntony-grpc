// A runnable 3-node Syntony cluster, in one process, over real gRPC.
//
//   npm run demo
//
// Watch a feature flag toggled on one node ripple out to the others — no flag
// server, no coordination, just peers gossiping over the network.

import { SyntonyNode } from "../dist/index.js";
import { Flags, fleetState } from "syntony-flag";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const tune = { disseminateIntervalMs: 50, antiEntropyIntervalMs: 200, probeIntervalMs: 300, pingTimeoutMs: 300 };

async function main() {
  console.log("Starting a 3-node Syntony cluster over gRPC…\n");

  const seed = new SyntonyNode({ address: "127.0.0.1:0", label: "seed", ...tune });
  await seed.start();
  console.log(`  seed    ${seed.address}   ${seed.typeId}`);

  const web = new SyntonyNode({ address: "127.0.0.1:0", label: "web", seeds: [seed.address], ...tune });
  await web.start();
  console.log(`  web     ${web.address}   ${web.typeId}`);

  const worker = new SyntonyNode({ address: "127.0.0.1:0", label: "worker", seeds: [seed.address], ...tune });
  await worker.start();
  console.log(`  worker  ${worker.address}   ${worker.typeId}`);

  const nodes = [seed, web, worker];
  const cores = nodes.map((n) => n.core);
  const flags = cores.map((c) => new Flags(c));

  console.log("\nEvery instance returns its call-site default before the flag exists:");
  for (let i = 0; i < nodes.length; i++) {
    console.log(`  ${nodes[i].label.padEnd(6)} new_checkout = ${flags[i].getBool("new_checkout", false)}`);
  }

  console.log("\nAdmin request lands on the seed and toggles new_checkout = true…\n");
  flags[0].set("new_checkout", true);

  for (let t = 1; t <= 30; t++) {
    await sleep(100);
    const view = fleetState(cores).flags.find((f) => f.key === "new_checkout");
    if (view) console.log(`  t+${String(t * 100).padStart(4)}ms   ${view.summary}`);
    if (view?.converged) break;
  }

  console.log("\nFinal read on each instance:");
  for (let i = 0; i < nodes.length; i++) {
    console.log(`  ${nodes[i].label.padEnd(6)} new_checkout = ${flags[i].getBool("new_checkout", false)}`);
  }

  await Promise.all(nodes.map((n) => n.stop()));
  console.log("\nCluster stopped. ✅");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
