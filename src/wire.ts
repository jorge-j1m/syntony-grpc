import { NodeStatus, type Delta, type Entry, type Hlc, type NodeState } from "syntony";

/**
 * Conversions between the gRPC wire shapes (the plain objects @grpc/proto-loader
 * produces, with `keepCase: true` and `longs: String`) and Syntony's domain
 * types. Two invariants the rest of the transport relies on:
 *   - every 64-bit field crosses the wire as a decimal string and is parsed to
 *     `bigint` here, never as a JS number (the float64 trap, §7.2);
 *   - every `bytes` field is normalized to a real `Uint8Array` (proto-loader
 *     hands us `Buffer`s, which are a subclass but compare differently).
 */
export const PROTOCOL_VERSION = 1;

export interface ProtoHlc {
  physical: string;
  logical: string;
}
export interface ProtoEntry {
  value: Uint8Array;
  deleted: boolean;
  hlc: ProtoHlc;
  writer: Uint8Array;
}
export interface ProtoNodeState {
  node_id: Uint8Array;
  typeid_str: string;
  label: string;
  grpc_address: string;
  status: number | string;
  incarnation: string;
}
export interface ProtoHeader {
  protocol_version: number;
  gossip: ProtoNodeState[];
}

function u8(bytes: Uint8Array | Buffer | undefined): Uint8Array {
  if (bytes === undefined) return new Uint8Array(0);
  return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength).slice();
}

export function hlcToProto(h: Hlc): ProtoHlc {
  return { physical: h.physical.toString(), logical: h.logical.toString() };
}

export function hlcFromProto(p: ProtoHlc): Hlc {
  return { physical: BigInt(p.physical ?? "0"), logical: BigInt(p.logical ?? "0") };
}

export function entryToProto(e: Entry): ProtoEntry {
  return { value: u8(e.value), deleted: e.deleted, hlc: hlcToProto(e.hlc), writer: u8(e.writer) };
}

export function entryFromProto(p: ProtoEntry): Entry {
  return { value: u8(p.value), deleted: Boolean(p.deleted), hlc: hlcFromProto(p.hlc), writer: u8(p.writer) };
}

const STATUS_BY_NAME: Record<string, NodeStatus> = {
  ALIVE: NodeStatus.ALIVE,
  SUSPECT: NodeStatus.SUSPECT,
  FAILED: NodeStatus.FAILED,
  LEFT: NodeStatus.LEFT,
};

/** Coerce a proto enum value (numeric or string name) to {@link NodeStatus}. */
export function statusFromProto(status: number | string): NodeStatus {
  if (typeof status === "number") return status as NodeStatus;
  return STATUS_BY_NAME[status] ?? NodeStatus.ALIVE;
}

export function nodeStateToProto(n: NodeState): ProtoNodeState {
  return {
    node_id: u8(n.nodeId),
    typeid_str: n.typeIdStr,
    label: n.label,
    grpc_address: n.grpcAddress,
    status: n.status,
    incarnation: n.incarnation.toString(),
  };
}

export function nodeStateFromProto(p: ProtoNodeState): NodeState {
  return {
    nodeId: u8(p.node_id),
    typeIdStr: p.typeid_str,
    label: p.label,
    grpcAddress: p.grpc_address,
    status: statusFromProto(p.status),
    incarnation: BigInt(p.incarnation ?? "0"),
  };
}

export function deltaToProto(delta: Delta): Record<string, ProtoEntry> {
  const out: Record<string, ProtoEntry> = {};
  for (const [key, entry] of delta) out[key] = entryToProto(entry);
  return out;
}

export function deltaFromProto(obj: Record<string, ProtoEntry> | undefined): Delta {
  const out: Delta = new Map();
  for (const [key, entry] of Object.entries(obj ?? {})) out.set(key, entryFromProto(entry));
  return out;
}

export function digestToProto(digest: Map<string, Hlc>): Record<string, ProtoHlc> {
  const out: Record<string, ProtoHlc> = {};
  for (const [key, hlc] of digest) out[key] = hlcToProto(hlc);
  return out;
}

export function digestFromProto(obj: Record<string, ProtoHlc> | undefined): Map<string, Hlc> {
  const out = new Map<string, Hlc>();
  for (const [key, hlc] of Object.entries(obj ?? {})) out.set(key, hlcFromProto(hlc));
  return out;
}

/** Build the envelope header carried by every RPC (§11.2, §9.5). */
export function makeHeader(gossip: NodeState[]): ProtoHeader {
  return { protocol_version: PROTOCOL_VERSION, gossip: gossip.map(nodeStateToProto) };
}
