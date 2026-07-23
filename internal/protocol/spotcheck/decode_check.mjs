#!/usr/bin/env node
// Cross-language wire parity spot-check (TypeScript/Node side) — Task 0.9.
//
// Reads the Go golden testvectors.json and, for a representative subset (a
// normal full-envelope frame, a frame whose hdr has >=2 keys, and the compact
// STREAM_DATA frame), decodes the 11-slot positional msgpack envelope array with
// @msgpack/msgpack and RE-ENCODES it, asserting bytes are byte-identical to the
// Go golden `envHex`.
//
// Proves B2 (positional array, not map) and B3 (hdr key sort) hold across the
// Go and JS encoders before the full P6 SDK exists.
//
// Run:  node decode_check.mjs
//   Requires @msgpack/msgpack resolvable. If not installed locally, set
//   NODE_PATH to a dir containing node_modules/@msgpack/msgpack, e.g.:
//     NODE_PATH=/tmp/tsmsgpack/node_modules node decode_check.mjs
// Exit: 0 on parity, non-zero on any mismatch.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

let encode, decode, ExtensionCodec;
try {
  ({ encode, decode } = require("@msgpack/msgpack"));
} catch (e) {
  console.error("FAIL: @msgpack/msgpack not resolvable. Set NODE_PATH to a dir with node_modules/@msgpack/msgpack");
  console.error(String(e));
  process.exit(2);
}

const HERE = dirname(fileURLToPath(import.meta.url));
const VECTORS = join(HERE, "..", "testvectors.json");

const SLOT_ORDER = ["v", "type", "id", "corr", "stream", "src", "dst", "tenant", "ttl", "hops", "hdr"];
const TARGET_NAMES = new Set(["hdr_multi_unsorted", "stream_data", "request", "utf8_multibyte"]);

function toHex(u8) {
  return Buffer.from(u8).toString("hex");
}

function fromHex(hex) {
  return Uint8Array.from(Buffer.from(hex, "hex"));
}

// Re-encode an envelope as the frozen 11-slot positional array with sorted hdr.
function encodeEnvelope(env) {
  const hdr = env.hdr || {};
  const sorted = {};
  for (const k of Object.keys(hdr).sort()) sorted[k] = hdr[k];
  const arr = [
    env.v,
    env.type,
    env.id,
    env.corr,
    env.stream,
    env.src,
    env.dst,
    env.tenant,
    env.ttl,
    env.hops,
    sorted,
  ];
  // sortKeys ensures any map header (the hdr slot) is deterministic; the array
  // itself is positional so order is already fixed.
  return encode(arr, { sortKeys: true });
}

function main() {
  const gf = JSON.parse(readFileSync(VECTORS, "utf-8"));
  const byName = new Map(gf.vectors.map((v) => [v.name, v]));

  let failures = 0;
  for (const name of [...TARGET_NAMES].sort()) {
    const vec = byName.get(name);
    if (!vec) {
      console.error(`FAIL: golden file missing target vector ${name}`);
      failures++;
      continue;
    }
    const wantHex = vec.envHex;

    // 1) Decode Go golden envelope bytes; must be an 11-element array.
    const decoded = decode(fromHex(wantHex));
    if (!Array.isArray(decoded)) {
      console.error(`FAIL[${name}]: envelope decoded as ${typeof decoded}, expected Array (B2 positional array)`);
      failures++;
      continue;
    }
    if (decoded.length !== 11) {
      console.error(`FAIL[${name}]: envelope array len ${decoded.length} != 11`);
      failures++;
      continue;
    }

    // 2) Re-encode structured env and compare hex.
    const gotHex = toHex(encodeEnvelope(vec.env));
    if (gotHex !== wantHex) {
      console.error(`FAIL[${name}]: env re-encode mismatch\n  got =${gotHex}\n  want=${wantHex}`);
      failures++;
      continue;
    }

    // 3) Sanity: non-hdr slots match structured env.
    for (let i = 0; i < SLOT_ORDER.length; i++) {
      const k = SLOT_ORDER[i];
      if (k === "hdr") continue;
      const got = decoded[i];
      const want = vec.env[k];
      if (got !== want) {
        console.error(`FAIL[${name}]: slot ${i} (${k}) = ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
        failures++;
      }
    }
    console.log(`OK[${name}]: envelope 11-slot array, hdr sorted, bytes match Go golden`);
  }

  if (failures) {
    console.error(`\n${failures} parity failure(s)`);
    process.exit(1);
  }
  console.log("\nTypeScript<->Go wire parity: OK");
}

main();
