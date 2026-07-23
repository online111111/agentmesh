import WebSocket from "ws";
import { encode, decode } from "@msgpack/msgpack";
import * as p from "./protocol.js";

function newId(): string {
  return crypto.randomUUID().replace(/-/g, "");
}

function toWsUrl(url: string): string {
  const u = url.trim();
  if (u.startsWith("https://")) return "wss://" + u.slice(8);
  if (u.startsWith("http://")) return "ws://" + u.slice(7);
  if (u.startsWith("ws://") || u.startsWith("wss://")) return u;
  return "ws://" + u;
}

export class DialError extends Error {}
export class RPCError extends Error {
  code: string;
  constructor(code: string, message = "") {
    super(message ? `${code}: ${message}` : code);
    this.code = code;
  }
}

export class Client {
  private ws: WebSocket;
  agentId: string;
  session: string;
  private onMsg?: (env: p.Envelope, payload: Uint8Array) => void;
  private pending = new Map<string, { resolve: (v: any) => void; reject: (e: any) => void }>();
  private streamQ = new Map<string, { push: (c: any) => void }>();
  private corrToStream = new Map<string, string>();
  private pendingStreamQ = new Map<string, { push: (c: any) => void }>();

  private constructor(ws: WebSocket, agentId: string, session: string) {
    this.ws = ws;
    this.agentId = agentId;
    this.session = session;
    ws.on("message", (data) => this.onRaw(data as Buffer));
  }

  static async dial(hubUrl: string, token: string, agentId: string, caps: string[] = []): Promise<Client> {
    const wsUrl = toWsUrl(hubUrl);
    const ws = new WebSocket(wsUrl);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", reject);
    });
    const helloPayload = encode({
      token,
      agentId,
      caps,
      protocols: [],
      v: p.PROTOCOL_VERSION,
    });
    const frame = p.encodeFrame({ v: p.PROTOCOL_VERSION, type: p.HELLO }, helloPayload);
    ws.send(frame);
    const raw = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new DialError("welcome timeout")), 10000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
      ws.once("error", reject);
    });
    const [env, payload] = p.decodeFrame(new Uint8Array(raw));
    if (env.type === p.ERROR) {
      const ep = decode(payload) as any;
      ws.close();
      throw new DialError(`${ep.code}: ${ep.message || ""}`);
    }
    if (env.type !== p.WELCOME) {
      ws.close();
      throw new DialError(`expected WELCOME got ${env.type}`);
    }
    const welcome = decode(payload) as any;
    return new Client(ws, agentId, String(welcome.session || ""));
  }

  onMessage(h: (env: p.Envelope, payload: Uint8Array) => void) {
    this.onMsg = h;
  }

  async send(dst: string, payload: Uint8Array): Promise<void> {
    const frame = p.encodeFrame(
      { v: p.PROTOCOL_VERSION, type: p.SEND, id: newId(), src: this.agentId, dst },
      payload
    );
    this.ws.send(frame);
  }

  async request(dst: string, payload: Uint8Array, ttlMs = 30000): Promise<{ from: string; payload: Uint8Array; corr: string }> {
    const corr = newId();
    const result = new Promise<{ from: string; payload: Uint8Array; corr: string }>((resolve, reject) => {
      this.pending.set(corr, { resolve, reject });
      setTimeout(() => {
        if (this.pending.has(corr)) {
          this.pending.delete(corr);
          reject(new RPCError("TIMEOUT", "request timed out"));
        }
      }, ttlMs);
    });
    const frame = p.encodeFrame(
      {
        v: p.PROTOCOL_VERSION,
        type: p.REQUEST,
        id: newId(),
        corr,
        src: this.agentId,
        dst,
        ttl: ttlMs,
        hops: 8,
      },
      payload
    );
    this.ws.send(frame);
    return result;
  }

  async *requestStream(dst: string, payload: Uint8Array, ttlMs = 30000): AsyncGenerator<any> {
    const corr = newId();
    const chunks: any[] = [];
    let wake: (() => void) | null = null;
    const push = (c: any) => {
      chunks.push(c);
      if (wake) wake();
    };
    this.pendingStreamQ.set(corr, { push });
    const frame = p.encodeFrame(
      {
        v: p.PROTOCOL_VERSION,
        type: p.REQUEST,
        id: newId(),
        corr,
        src: this.agentId,
        dst,
        ttl: ttlMs,
        hops: 8,
        hdr: { stream: "1" },
      },
      payload
    );
    this.ws.send(frame);
    try {
      while (true) {
        if (chunks.length === 0) {
          await new Promise<void>((r) => {
            wake = r;
            setTimeout(r, ttlMs);
          });
          wake = null;
        }
        if (chunks.length === 0) throw new RPCError("TIMEOUT", "stream timed out");
        const c = chunks.shift();
        yield c;
        if (c.is_end) return;
      }
    } finally {
      this.pendingStreamQ.delete(corr);
    }
  }

  async writeFrame(env: p.Envelope, payload: Uint8Array = new Uint8Array()) {
    if (!env.src) env.src = this.agentId;
    if (!env.v) env.v = p.PROTOCOL_VERSION;
    this.ws.send(p.encodeFrame(env, payload));
  }

  close() {
    this.ws.close();
  }

  private onRaw(data: Buffer) {
    const [env, payload] = p.decodeFrame(new Uint8Array(data));
    if (env.type === p.RESPONSE) {
      const corr = env.corr || "";
      const pend = this.pending.get(corr);
      if (pend) {
        this.pending.delete(corr);
        pend.resolve({ from: env.src || "", payload, corr });
        return;
      }
    }
    if (env.type === p.ERROR) {
      const corr = env.corr || "";
      const pend = this.pending.get(corr);
      if (pend) {
        this.pending.delete(corr);
        const ep = decode(payload) as any;
        pend.reject(new RPCError(String(ep.code || "ERROR"), String(ep.message || "")));
        return;
      }
    }
    if (env.type === p.STREAM_OPEN) {
      const corr = env.corr || "";
      const sid = env.stream || "";
      const q = this.pendingStreamQ.get(corr);
      if (q && sid) {
        this.streamQ.set(sid, q);
        this.corrToStream.set(corr, sid);
      }
      return;
    }
    if (env.type === p.STREAM_DATA) {
      const sid = env.stream || "";
      const q = this.streamQ.get(sid);
      if (q) q.push({ seq: Number(env.hdr?.seq || 0), data: payload, is_end: false });
      return;
    }
    if (env.type === p.STREAM_END) {
      const sid = env.stream || "";
      const q = this.streamQ.get(sid);
      if (q) q.push({ is_end: true, status: env.hdr?.status || "ok", data: new Uint8Array() });
      return;
    }
    if (this.onMsg) this.onMsg(env, payload);
  }
}
