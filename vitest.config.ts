import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts", "src/**/*.test.ts"],
    environment: "node",
    // Real gRPC servers bind sockets and converge over a few gossip rounds.
    testTimeout: 20000,
    hookTimeout: 20000,
  },
});
