import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { encodeEnvFromStruct, decodeFrame } from "../src/protocol.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const vectors = JSON.parse(
  readFileSync(path.join(ROOT, "internal/protocol/testvectors.json"), "utf8")
).vectors as any[];

function toHex(u: Uint8Array): string {
  return Buffer.from(u).toString("hex");
}

describe("golden vectors", () => {
  for (const vec of vectors) {
    it(`env parity: ${vec.name}`, () => {
      const got = toHex(encodeEnvFromStruct(vec.env));
      expect(got).toBe(vec.envHex);
    });
    it(`frame decode: ${vec.name}`, () => {
      const frame = Buffer.from(vec.frameHex, "hex");
      const [env] = decodeFrame(new Uint8Array(frame));
      expect(env.type).toBe(vec.env.type);
    });
  }
});
