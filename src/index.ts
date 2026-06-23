/**
 * syntony-grpc — a real gRPC transport for Syntony.
 *
 * Turns the embedded {@link Syntony} CRDT primitive into a live, decentralized
 * cluster of peer nodes: each {@link SyntonyNode} runs a gRPC server, joins via
 * seeds, and replicates state over the network with SWIM membership, gossip, and
 * anti-entropy.
 */
export { SyntonyNode, type SyntonyNodeOptions } from "./node.js";

// Re-exported from the core so a consumer needs a single import.
export { Syntony, NodeStatus, type NodeState, type Entry } from "syntony";
