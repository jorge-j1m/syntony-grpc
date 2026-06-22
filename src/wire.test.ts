import { NodeStatus, type Entry, type Hlc, type NodeState } from "syntony";
import { describe, expect, it } from "vitest";
import {
  PROTOCOL_VERSION,
  deltaFromProto,
  deltaToProto,
  digestFromProto,
  digestToProto,
  entryFromProto,
  entryToProto,
  hlcFromProto,
  hlcToProto,
  makeHeader,
  nodeStateFromProto,
  nodeStateToProto,
  statusFromProto,
} from "./wire.js";

const hlc = (p: number, l: number): Hlc => ({ physical: BigInt(p), logical: BigInt(l) });
const id = (b: number) => new Uint8Array(16).fill(b);

const entry = (over: Partial<Entry> & { hlc: Hlc; writer: Uint8Array }): Entry => ({
  value: over.value ?? new Uint8Array([1, 2, 3]),
  deleted: over.deleted ?? false,
  hlc: over.hlc,
  writer: over.writer,
});

const node = (b: number): NodeState => ({
  nodeId: id(b),
  typeIdStr: `node_${b}`,
  label: `n${b}`,
  grpcAddress: `127.0.0.1:${7000 + b}`,
  status: NodeStatus.ALIVE,
  incarnation: 4n,
});

describe("HLC wire (uint64 as decimal string, §7.2)", () => {
  it("encodes 64-bit fields as strings", () => {
    const proto = hlcToProto(hlc(5, 7));
    expect(proto).toEqual({ physical: "5", logical: "7" });
  });

  it("round-trips through proto and back to BigInt", () => {
    const original = { physical: 18446744073709551615n, logical: 9n };
    expect(hlcFromProto(hlcToProto(original))).toEqual(original);
  });
});

describe("Entry wire", () => {
  it("round-trips a live entry", () => {
    const e = entry({ hlc: hlc(3, 1), writer: id(0xaa) });
    expect(entryFromProto(entryToProto(e))).toEqual(e);
  });

  it("round-trips a tombstone with empty value", () => {
    const e = entry({ hlc: hlc(9, 0), writer: id(0xbb), deleted: true, value: new Uint8Array() });
    expect(entryFromProto(entryToProto(e))).toEqual(e);
  });

  it("normalizes incoming Buffer bytes to Uint8Array", () => {
    const proto = { value: Buffer.from([7, 8]), deleted: false, hlc: hlcToProto(hlc(1, 0)), writer: Buffer.from(id(1)) };
    const e = entryFromProto(proto);
    expect(e.value).toBeInstanceOf(Uint8Array);
    expect(Array.from(e.value)).toEqual([7, 8]);
  });
});

describe("NodeState wire", () => {
  it("round-trips including status and incarnation", () => {
    const n = node(2);
    expect(nodeStateFromProto(nodeStateToProto(n))).toEqual(n);
  });

  it("coerces status whether proto-loader yields a number or an enum name", () => {
    expect(statusFromProto(2)).toBe(NodeStatus.FAILED);
    expect(statusFromProto("SUSPECT")).toBe(NodeStatus.SUSPECT);
  });

  it("defaults a missing incarnation to 0", () => {
    const partial = { node_id: id(1), typeid_str: "node_1", label: "n1", grpc_address: "x", status: 0 } as any;
    expect(nodeStateFromProto(partial).incarnation).toBe(0n);
  });
});

describe("delta and digest maps", () => {
  it("round-trips a delta map", () => {
    const delta = new Map<string, Entry>([
      ["k1", entry({ hlc: hlc(5, 0), writer: id(1) })],
      ["k2", entry({ hlc: hlc(6, 0), writer: id(2), deleted: true, value: new Uint8Array() })],
    ]);
    const back = deltaFromProto(deltaToProto(delta));
    expect(back).toEqual(delta);
  });

  it("round-trips a digest map", () => {
    const digest = new Map<string, Hlc>([["a", hlc(1, 2)], ["b", hlc(3, 0)]]);
    expect(digestFromProto(digestToProto(digest))).toEqual(digest);
  });

  it("treats a missing map as empty", () => {
    expect(deltaFromProto(undefined).size).toBe(0);
    expect(digestFromProto(undefined).size).toBe(0);
  });
});

describe("makeHeader", () => {
  it("stamps the protocol version and serializes gossip", () => {
    const header = makeHeader([node(1), node(2)]);
    expect(header.protocol_version).toBe(PROTOCOL_VERSION);
    expect(header.gossip).toHaveLength(2);
    expect(header.gossip[0]!.typeid_str).toBe("node_1");
  });
});
