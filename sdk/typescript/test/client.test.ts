import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { spawn, ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { Client, DialError } from "../src/client.js";
import * as p from "../src/protocol.js";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const addr = s.address();
      if (addr && typeof addr === "object") {
        const port = addr.port;
        s.close(() => resolve(port));
      } else reject(new Error("no port"));
    });
  });
}

let hub: ChildProcess;
let hubUrl: string;

beforeAll(async () => {
  const port = await freePort();
  hubUrl = `http://127.0.0.1:${port}`;
  hub = spawn("go", ["run", "./cmd/meshd", "serve"], {
    cwd: ROOT,
    env: {
      ...process.env,
      MESH_HOST: "127.0.0.1",
      MESH_PORT: String(port),
      MESH_API_KEYS: "a:ka:alice:default\nb:kb:bob:default",
      MESH_IP_CONN_RATE: "0",
      MESH_AGENT_MSG_RATE: "0",
      ALL_PROXY: "",
      HTTP_PROXY: "",
      HTTPS_PROXY: "",
    },
    stdio: "ignore",
  });
  // wait health
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${hubUrl}/health`);
      if (r.ok) break;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
}, 20000);

afterAll(() => {
  hub?.kill();
});

describe("ts sdk client", () => {
  it("dials and sends", async () => {
    const bob = await Client.dial(hubUrl, "kb", "bob-ts");
    const got = new Promise<Uint8Array>((resolve) => {
      bob.onMessage((env, payload) => {
        if (env.type === p.SEND) resolve(payload);
      });
    });
    const alice = await Client.dial(hubUrl, "ka", "alice-ts");
    await alice.send("bob-ts", new TextEncoder().encode("hello-ts"));
    const payload = await Promise.race([
      got,
      new Promise<Uint8Array>((_, rej) => setTimeout(() => rej(new Error("timeout")), 3000)),
    ]);
    expect(new TextDecoder().decode(payload)).toBe("hello-ts");
    alice.close();
    bob.close();
  });

  it("auth failed", async () => {
    await expect(Client.dial(hubUrl, "wrong", "alice-x")).rejects.toBeInstanceOf(DialError);
  });
});
