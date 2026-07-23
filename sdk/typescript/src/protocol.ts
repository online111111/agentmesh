import { encode, decode } from "@msgpack/msgpack";

export const PROTOCOL_VERSION = 1;
export const MAX_FRAME_BYTES = 1 << 20;

export const HELLO = 0x01;
export const WELCOME = 0x02;
export const SEND = 0x10;
export const REQUEST = 0x11;
export const RESPONSE = 0x12;
export const CANCEL = 0x13;
export const STREAM_OPEN = 0x20;
export const STREAM_DATA = 0x21;
export const STREAM_END = 0x22;
export const ERROR = 0xff;

export type Envelope = {
  v?: number;
  type: number;
  id?: string;
  corr?: string;
  stream?: string;
  src?: string;
  dst?: string;
  tenant?: string;
  ttl?: number;
  hops?: number;
  hdr?: Record<string, string>;
};

function encodeVarint(n: number): Uint8Array {
  const out: number[] = [];
  while (true) {
    let b = n & 0x7f;
    n >>>= 7;
    if (n) out.push(b | 0x80);
    else {
      out.push(b);
      break;
    }
  }
  return Uint8Array.from(out);
}

function decodeVarint(buf: Uint8Array, off: number): [number, number] {
  let result = 0;
  let shift = 0;
  while (true) {
    if (off >= buf.length) throw new Error("truncated varint");
    const b = buf[off++];
    result |= (b & 0x7f) << shift;
    if (!(b & 0x80)) return [result, off];
    shift += 7;
    if (shift > 35) throw new Error("varint too long");
  }
}

export function encodeEnvelope(env: Envelope): Uint8Array {
  const hdr = env.hdr || {};
  const sorted: Record<string, string> = {};
  for (const k of Object.keys(hdr).sort()) sorted[k] = hdr[k];
  const arr = [
    env.v ?? PROTOCOL_VERSION,
    env.type,
    env.id ?? "",
    env.corr ?? "",
    env.stream ?? "",
    env.src ?? "",
    env.dst ?? "",
    env.tenant ?? "",
    env.ttl ?? 0,
    env.hops ?? 0,
    sorted,
  ];
  return encode(arr);
}

export function decodeEnvelope(data: Uint8Array): Envelope {
  const arr = decode(data) as unknown[];
  if (!Array.isArray(arr) || arr.length !== 11) throw new Error("envelope must be 11-slot array");
  const hdrRaw = (arr[10] as Record<string, string>) || {};
  const hdr: Record<string, string> = {};
  for (const [k, v] of Object.entries(hdrRaw)) hdr[String(k)] = String(v);
  return {
    v: Number(arr[0]),
    type: Number(arr[1]),
    id: String(arr[2] ?? ""),
    corr: String(arr[3] ?? ""),
    stream: String(arr[4] ?? ""),
    src: String(arr[5] ?? ""),
    dst: String(arr[6] ?? ""),
    tenant: String(arr[7] ?? ""),
    ttl: Number(arr[8] ?? 0),
    hops: Number(arr[9] ?? 0),
    hdr,
  };
}

export function encodeFrame(env: Envelope, payload: Uint8Array = new Uint8Array()): Uint8Array {
  const envBytes = encodeEnvelope(env);
  const typ = env.type & 0xff;
  const vi = encodeVarint(envBytes.length);
  const out = new Uint8Array(1 + vi.length + envBytes.length + payload.length);
  out[0] = typ;
  out.set(vi, 1);
  out.set(envBytes, 1 + vi.length);
  out.set(payload, 1 + vi.length + envBytes.length);
  return out;
}

export function decodeFrame(data: Uint8Array): [Envelope, Uint8Array] {
  if (data.length < 2) throw new Error("frame too short");
  const typ = data[0];
  const [envLen, off] = decodeVarint(data, 1);
  if (envLen > MAX_FRAME_BYTES) throw new Error("FRAME_TOO_BIG");
  const end = off + envLen;
  if (end > data.length) throw new Error("truncated envelope");
  const env = decodeEnvelope(data.subarray(off, end));
  env.type = typ;
  return [env, data.subarray(end)];
}

export function encodeEnvFromStruct(env: {
  v: number; type: number; id: string; corr: string; stream: string;
  src: string; dst: string; tenant: string; ttl: number; hops: number;
  hdr?: Record<string, string>;
}): Uint8Array {
  return encodeEnvelope(env);
}
