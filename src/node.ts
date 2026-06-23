import { randomBytes, timingSafeEqual } from "node:crypto";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { Syntony, type NodeState } from "syntony";
import {
  PROTOCOL_VERSION,
  deltaFromProto,
  deltaToProto,
  digestFromProto,
  digestToProto,
  makeHeader,
  nodeStateFromProto,
  nodeStateToProto,
  type ProtoHeader,
} from "./wire.js";

const PROTO_PATH = fileURLToPath(new URL("../proto/syntony.proto", import.meta.url));
const SECRET_KEY = "x-syntony-secret";
const GOSSIP_LIMIT = 8;

// Load the wire protocol once. keepCase preserves the snake_case proto field
// names; longs:String surfaces every uint64 as a decimal string (§7.2).
const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  defaults: true,
  oneofs: true,
});
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const v1 = (grpc.loadPackageDefinition(packageDef) as any).syntony.v1;

export interface SyntonyNodeOptions {
  /** Address to bind the gRPC server to. Default `0.0.0.0:0` (an ephemeral port). */
  address?: string;
  /** Address peers should use to reach this node. Defaults to the bound host:port. */
  advertiseAddress?: string;
  /** Seed peers to join through (§9.7). Any one reachable seed suffices. */
  seeds?: string[];
  /** Human label (§8.4); defaults to the id-seeded mnemonic. */
  label?: string;
  /** Fixed 16-byte node id; a UUIDv7 is generated if omitted (§8). */
  nodeId?: Uint8Array;
  /** Shared cluster secret for peer-mesh auth (§12.1); checked constant-time. */
  clusterSecret?: string;
  now?: () => bigint;
  randomBytes?: (n: number) => Uint8Array;
  /** SWIM protocol period — probe interval (§9.1). Default 1000ms. */
  probeIntervalMs?: number;
  /** Direct-probe deadline (§9.1). Default 500ms. */
  pingTimeoutMs?: number;
  /** Eager delta dissemination interval (§10.1). Default 100ms. */
  disseminateIntervalMs?: number;
  /** Anti-entropy interval (§10.2). Default 1000ms. */
  antiEntropyIntervalMs?: number;
  /** Replication RPC deadline. Default 2000ms. */
  rpcTimeoutMs?: number;
  /** Indirect-probe fan-out `k` (§9.1). Default 3. */
  indirectProbes?: number;
  /** Join RPC deadline. Default 3000ms. */
  joinTimeoutMs?: number;
  /** Optional structured logger. */
  logger?: (message: string) => void;
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * A live Syntony node with a real gRPC transport. It wraps the embedded
 * {@link Syntony} core (engine + membership + identity) and drives it over the
 * network: a gRPC server answering the Membership and Replication services, a
 * pool of clients to peers, seed-based join with readiness gating (§9.7), and
 * three background loops — SWIM failure detection (§9.3), eager delta
 * dissemination (§10.1), and digest anti-entropy (§10.2). Every RPC piggybacks
 * membership gossip (§9.5).
 *
 * Reads and writes go straight to local memory and never block; propagation
 * happens in the background once {@link start} has joined the cluster.
 */
export class SyntonyNode {
  readonly #core: Syntony;
  readonly #options: SyntonyNodeOptions;
  readonly #bindAddress: string;
  readonly #bindHost: string;
  #advertiseAddress: string | undefined;
  readonly #secret: string | undefined;
  readonly #secretBuf: Buffer | undefined;

  #server: grpc.Server | undefined;
  readonly #membershipClients = new Map<string, grpc.Client>();
  readonly #replicationClients = new Map<string, grpc.Client>();
  #timers: NodeJS.Timeout[] = [];
  #ready = false;
  #started = false;

  constructor(options: SyntonyNodeOptions = {}) {
    this.#options = options;
    this.#bindAddress = options.address ?? "0.0.0.0:0";
    const colon = this.#bindAddress.lastIndexOf(":");
    this.#bindHost = colon > 0 ? this.#bindAddress.slice(0, colon) : "0.0.0.0";
    this.#advertiseAddress = options.advertiseAddress;
    this.#secret = options.clusterSecret;
    this.#secretBuf = options.clusterSecret ? Buffer.from(options.clusterSecret) : undefined;
    this.#core = new Syntony({
      nodeId: options.nodeId,
      label: options.label,
      grpcAddress: options.advertiseAddress ?? this.#bindAddress,
      now: options.now,
      randomBytes: options.randomBytes ?? ((n) => randomBytes(n)),
    });
  }

  /** The embedded Syntony core (for `new Flags(node.core)` and introspection). */
  get core(): Syntony {
    return this.#core;
  }

  /** True once the node has joined and completed its initial state sync (§9.7). */
  get ready(): boolean {
    return this.#ready;
  }

  /** The address peers use to reach this node (known after {@link start}). */
  get address(): string {
    return this.#advertiseAddress ?? this.#bindAddress;
  }

  get nodeId(): Uint8Array {
    return this.#core.nodeId;
  }
  get typeId(): string {
    return this.#core.typeId;
  }
  get label(): string {
    return this.#core.label;
  }

  /**
   * Bind the gRPC server, join the cluster via seeds, pull initial state, then
   * start the background loops. Resolves only once the node is ready to serve
   * non-empty reads (§9.7) — a rolling deploy stays a non-event.
   */
  async start(): Promise<void> {
    if (this.#started) return;
    this.#started = true;

    this.#buildServer();
    const port = await this.#bind();
    if (!this.#advertiseAddress) {
      const host =
        this.#bindHost === "0.0.0.0" || this.#bindHost === "::" || this.#bindHost === ""
          ? "127.0.0.1"
          : this.#bindHost;
      this.#advertiseAddress = `${host}:${port}`;
    }
    this.#core.membership.self.grpcAddress = this.#advertiseAddress;
    // @grpc/grpc-js (>= 1.10) begins serving inside bindAsync; no start() needed.

    await this.#join();
    await this.#initialSync();
    this.#ready = true;
    this.#startLoops();
  }

  /** Stop the loops, close peer connections, and shut the server down. */
  async stop(): Promise<void> {
    for (const timer of this.#timers) clearInterval(timer);
    this.#timers = [];
    this.#ready = false;
    this.#started = false;
    for (const client of this.#membershipClients.values()) this.#safeClose(client);
    for (const client of this.#replicationClients.values()) this.#safeClose(client);
    this.#membershipClients.clear();
    this.#replicationClients.clear();
    const server = this.#server;
    if (!server) return;
    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };
      server.tryShutdown(() => finish());
      setTimeout(() => {
        try {
          server.forceShutdown();
        } catch {
          /* ignore */
        }
        finish();
      }, 1500).unref?.();
    });
  }

  // ── Public data API (delegates to the core, §14) ──────────────────────────

  get(key: string): Uint8Array | undefined {
    return this.#core.get(key);
  }
  set(key: string, value: Uint8Array): void {
    this.#core.set(key, value);
  }
  delete(key: string): void {
    this.#core.delete(key);
  }
  has(key: string): boolean {
    return this.#core.has(key);
  }
  keys(): string[] {
    return this.#core.keys();
  }
  members(): NodeState[] {
    return this.#core.members();
  }
  self(): NodeState {
    return this.#core.self();
  }

  // ── Server ────────────────────────────────────────────────────────────────

  #buildServer(): void {
    const server = new grpc.Server();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const unauth = (cb: grpc.sendUnaryData<any>) =>
      cb({ code: grpc.status.UNAUTHENTICATED, message: "invalid cluster secret" }, null);

    server.addService(v1.Membership.service, {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Ping: (call: any, cb: grpc.sendUnaryData<any>) => {
        if (!this.#authorized(call.metadata)) return unauth(cb);
        this.#applyHeader(call.request.header);
        cb(null, { header: this.#header() });
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      PingReq: (call: any, cb: grpc.sendUnaryData<any>) => {
        if (!this.#authorized(call.metadata)) return unauth(cb);
        this.#applyHeader(call.request.header);
        const target = this.#core.membership.get(new Uint8Array(call.request.target_node_id));
        if (!target) {
          cb(null, { header: this.#header(), acked: false });
          return;
        }
        this.#ping(target)
          .then((ok) => cb(null, { header: this.#header(), acked: ok }))
          .catch(() => cb(null, { header: this.#header(), acked: false }));
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      Join: (call: any, cb: grpc.sendUnaryData<any>) => {
        if (!this.#authorized(call.metadata)) return unauth(cb);
        this.#applyHeader(call.request.header);
        if (call.request.self) this.#core.membership.apply(nodeStateFromProto(call.request.self));
        cb(null, {
          header: this.#header(),
          members: this.#core.membership.members().map(nodeStateToProto),
        });
      },
    });

    server.addService(v1.Replication.service, {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      PushDeltas: (call: any, cb: grpc.sendUnaryData<any>) => {
        if (!this.#authorized(call.metadata)) return unauth(cb);
        this.#applyHeader(call.request.header);
        this.#core.engine.applyRemote(deltaFromProto(call.request.entries));
        cb(null, { header: this.#header() });
      },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      SyncDigest: (call: any, cb: grpc.sendUnaryData<any>) => {
        if (!this.#authorized(call.metadata)) return unauth(cb);
        this.#applyHeader(call.request.header);
        const diff = this.#core.engine.diffAgainst(digestFromProto(call.request.versions));
        cb(null, { header: this.#header(), entries: deltaToProto(diff) });
      },
    });

    this.#server = server;
  }

  #bind(): Promise<number> {
    return new Promise((resolve, reject) => {
      (this.#server as grpc.Server).bindAsync(
        this.#bindAddress,
        grpc.ServerCredentials.createInsecure(),
        (err, port) => (err ? reject(err) : resolve(port)),
      );
    });
  }

  #authorized(metadata: grpc.Metadata): boolean {
    if (!this.#secretBuf) return true;
    const provided = metadata.get(SECRET_KEY)[0];
    if (typeof provided !== "string") return false;
    const buf = Buffer.from(provided);
    return buf.length === this.#secretBuf.length && timingSafeEqual(buf, this.#secretBuf);
  }

  // ── Join & initial sync ─────────────────────────────────────────────────────

  async #join(): Promise<void> {
    const seeds = this.#options.seeds ?? [];
    if (seeds.length === 0) return;
    const selfProto = nodeStateToProto(this.#core.self());
    const timeout = this.#options.joinTimeoutMs ?? 3000;
    for (const seed of seeds) {
      if (seed === this.#advertiseAddress) continue;
      try {
        const client = this.#membershipClient(seed);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const res: any = await this.#unary(client, "Join", { header: this.#header(), self: selfProto }, timeout);
        this.#applyHeader(res.header);
        for (const member of res.members ?? []) this.#core.membership.apply(nodeStateFromProto(member));
        this.#log(`joined cluster via ${seed}`);
        return;
      } catch (err) {
        this.#log(`join via ${seed} failed: ${(err as Error).message}`);
      }
    }
    this.#log("no seed reachable; starting standalone");
  }

  async #initialSync(): Promise<void> {
    const peer = this.#randomAlivePeers(1)[0];
    if (!peer) return;
    try {
      await this.#antiEntropyWith(peer);
    } catch (err) {
      this.#log(`initial sync failed: ${(err as Error).message}`);
    }
  }

  // ── Background loops ────────────────────────────────────────────────────────

  #startLoops(): void {
    const probe = setInterval(() => void this.#probeOnce().catch(() => {}), this.#options.probeIntervalMs ?? 1000);
    const push = setInterval(() => void this.#disseminateOnce().catch(() => {}), this.#options.disseminateIntervalMs ?? 100);
    const sync = setInterval(() => void this.#antiEntropyOnce().catch(() => {}), this.#options.antiEntropyIntervalMs ?? 1000);
    for (const timer of [probe, push, sync]) {
      timer.unref?.();
      this.#timers.push(timer);
    }
  }

  async #probeOnce(): Promise<void> {
    for (const confirmed of this.#core.membership.tick()) {
      this.#log(`confirmed failed: ${confirmed.typeIdStr}`);
      this.#dropClients(confirmed.grpcAddress);
    }
    const target = this.#core.membership.nextProbeTarget();
    if (!target) return;
    if (await this.#ping(target)) return;

    const relays = this.#randomAlivePeers(this.#options.indirectProbes ?? 3, target.nodeId);
    const acks = await Promise.all(relays.map((relay) => this.#pingReq(relay, target).catch(() => false)));
    if (acks.some(Boolean)) return;

    this.#core.membership.suspect(target.nodeId);
    this.#log(`suspect ${target.typeIdStr}`);
  }

  async #disseminateOnce(): Promise<void> {
    const delta = this.#core.engine.drainOutbound();
    if (delta.size === 0) return;
    const entries = deltaToProto(delta);
    await Promise.all(
      this.#alivePeers().map(async (peer) => {
        try {
          const client = this.#replicationClient(peer.grpcAddress);
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const res: any = await this.#unary(client, "PushDeltas", { header: this.#header(), entries }, this.#rpcTimeout());
          this.#applyHeader(res.header);
        } catch {
          // Best-effort eager push (§10.1); anti-entropy heals anything missed.
        }
      }),
    );
  }

  async #antiEntropyOnce(): Promise<void> {
    const peer = this.#randomAlivePeers(1)[0];
    if (!peer) return;
    try {
      await this.#antiEntropyWith(peer);
    } catch {
      /* transient; next round retries */
    }
  }

  async #antiEntropyWith(peer: NodeState): Promise<void> {
    const client = this.#replicationClient(peer.grpcAddress);
    const req = { header: this.#header(), versions: digestToProto(this.#core.engine.digest()) };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const res: any = await this.#unary(client, "SyncDigest", req, this.#rpcTimeout());
    this.#applyHeader(res.header);
    this.#core.engine.applyRemote(deltaFromProto(res.entries));
  }

  // ── Probing primitives ──────────────────────────────────────────────────────

  async #ping(target: NodeState): Promise<boolean> {
    try {
      const client = this.#membershipClient(target.grpcAddress);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const res: any = await this.#unary(client, "Ping", { header: this.#header() }, this.#options.pingTimeoutMs ?? 500);
      this.#applyHeader(res.header);
      return true;
    } catch {
      return false;
    }
  }

  async #pingReq(relay: NodeState, target: NodeState): Promise<boolean> {
    const client = this.#membershipClient(relay.grpcAddress);
    const req = { header: this.#header(), target_node_id: Buffer.from(target.nodeId) };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const res: any = await this.#unary(client, "PingReq", req, this.#options.pingTimeoutMs ?? 500);
    this.#applyHeader(res.header);
    return Boolean(res.acked);
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  #alivePeers(): NodeState[] {
    return this.#core.membership
      .aliveMembers()
      .filter((m) => !bytesEqual(m.nodeId, this.#core.nodeId) && m.grpcAddress.length > 0);
  }

  #randomAlivePeers(count: number, exclude?: Uint8Array): NodeState[] {
    let peers = this.#alivePeers();
    if (exclude) peers = peers.filter((p) => !bytesEqual(p.nodeId, exclude));
    for (let i = peers.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [peers[i], peers[j]] = [peers[j]!, peers[i]!];
    }
    return peers.slice(0, count);
  }

  #header(): ProtoHeader {
    return makeHeader(this.#core.membership.takeGossip(GOSSIP_LIMIT));
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #applyHeader(header: any): void {
    if (!header) return;
    if (header.protocol_version !== undefined && Number(header.protocol_version) !== PROTOCOL_VERSION) {
      this.#log(`ignoring message with protocol version ${header.protocol_version}`);
      return;
    }
    for (const gossip of header.gossip ?? []) this.#core.membership.apply(nodeStateFromProto(gossip));
  }

  #membershipClient(address: string): grpc.Client {
    let client = this.#membershipClients.get(address);
    if (!client) {
      client = new v1.Membership(address, grpc.credentials.createInsecure());
      this.#membershipClients.set(address, client!);
    }
    return client!;
  }

  #replicationClient(address: string): grpc.Client {
    let client = this.#replicationClients.get(address);
    if (!client) {
      client = new v1.Replication(address, grpc.credentials.createInsecure());
      this.#replicationClients.set(address, client!);
    }
    return client!;
  }

  #dropClients(address: string): void {
    for (const map of [this.#membershipClients, this.#replicationClients]) {
      const client = map.get(address);
      if (client) {
        this.#safeClose(client);
        map.delete(address);
      }
    }
  }

  #safeClose(client: grpc.Client): void {
    try {
      client.close();
    } catch {
      /* ignore */
    }
  }

  #rpcTimeout(): number {
    return this.#options.rpcTimeoutMs ?? 2000;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #unary(client: grpc.Client, method: string, req: any, timeoutMs: number): Promise<any> {
    return new Promise((resolve, reject) => {
      const metadata = new grpc.Metadata();
      if (this.#secret) metadata.set(SECRET_KEY, this.#secret);
      const deadline = new Date(Date.now() + timeoutMs);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (client as any)[method](req, metadata, { deadline }, (err: grpc.ServiceError | null, res: any) =>
        err ? reject(err) : resolve(res),
      );
    });
  }

  #log(message: string): void {
    this.#options.logger?.(message);
  }
}
