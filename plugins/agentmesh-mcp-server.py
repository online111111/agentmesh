#!/usr/bin/env python3
"""AgentMesh MCP Server — bridges Hermes ↔ AgentMesh.

Exposes three tools that Hermes (or any MCP client) can call directly:
  - mesh_agents:   list online agents on the Hub (via HTTP API)
  - mesh_call:     send a REQUEST to an agent and wait for the reply
  - mesh_send:     fire-and-forget SEND to an agent (uses /v1/send API)

Configuration via environment variables:
  MESH_HUB          Hub URL (default: https://hub.example.com)
  MESH_TOKEN        API key token (required)
  MESH_AGENT_ID     This agent's ID (default: hermes)
  MESH_BIN          Path to mesh binary (default: /usr/local/bin/mesh)
"""
import asyncio
import json
import os
import subprocess
import sys
import urllib.request
import urllib.error

from mcp.server import Server
from mcp.types import Tool, TextContent

MESH_BIN = os.environ.get("MESH_BIN", "/usr/local/bin/mesh")
MESH_HUB = os.environ.get("MESH_HUB", "https://hub.example.com")
MESH_TOKEN = os.environ.get("MESH_TOKEN", "")
MESH_AGENT_ID = os.environ.get("MESH_AGENT_ID", "hermes")

server = Server("agentmesh")


def _http_get(url: str, token: str, timeout: int = 15) -> dict:
    """GET an AgentMesh Hub API endpoint, return parsed JSON."""
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detail": e.read().decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        return {"error": str(e)}


def _http_post(url: str, token: str, body: dict, timeout: int = 15) -> dict:
    """POST JSON to a Hub API endpoint."""
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detail": e.read().decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        return {"error": str(e)}


def _mesh_call_cli(to: str, payload: str, ttl_ms: int = 60000) -> dict:
    """Run `mesh call` CLI for synchronous REQUEST/RESPONSE."""
    cmd = [
        MESH_BIN, "call",
        "--hub", MESH_HUB,
        "--token", MESH_TOKEN,
        "--to", to,
        "--ttl-ms", str(ttl_ms),
        "--payload", payload,
    ]
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=int(ttl_ms / 1000) + 10,
        )
        stdout = r.stdout.strip()
        stderr = r.stderr.strip()
        if r.returncode != 0:
            return {"error": f"mesh call exit {r.returncode}", "stderr": stderr, "stdout": stdout}
        # Parse JSON output
        if stdout:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError:
                return {"result": stdout}
        return {"result": "ok"}
    except subprocess.TimeoutExpired:
        return {"error": f"mesh call timed out after {ttl_ms}ms"}
    except FileNotFoundError:
        return {"error": f"mesh binary not found at {MESH_BIN}"}
    except Exception as e:
        return {"error": str(e)}


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="mesh_agents",
            description=(
                "List all agents currently online on the AgentMesh Hub. "
                "Returns their agent IDs and tenants. Use this to discover "
                "who is available to talk to before calling mesh_call."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="mesh_call",
            description=(
                "Send a REQUEST to another agent on the AgentMesh Hub and "
                "wait for its reply (synchronous). The target agent receives "
                "the payload, processes it (e.g. via its LLM handler), and "
                "returns a RESPONSE. Use this for question-answer style "
                "communication. The payload is a string (the question or "
                "message content)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "Target agent ID, e.g. 'bob-peer'"},
                    "payload": {
                        "type": "string",
                        "description": "The message or question to send (plain text).",
                    },
                    "ttl_ms": {
                        "type": "integer",
                        "description": "Timeout in millis (default 60000, max 120000)",
                        "default": 60000,
                    },
                },
                "required": ["to", "payload"],
            },
        ),
        Tool(
            name="mesh_send",
            description=(
                "Send a fire-and-forget message (SEND) to another agent "
                "via the Hub API. No reply is expected. Use this for "
                "notifications or when you don't need a synchronous response."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "to": {"type": "string", "description": "Target agent ID"},
                    "payload": {"type": "string", "description": "The message content to send."},
                },
                "required": ["to", "payload"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if not MESH_TOKEN:
        return [TextContent(type="text", text=json.dumps({"error": "MESH_TOKEN not configured"}, ensure_ascii=False))]

    if name == "mesh_agents":
        # Use curl to avoid Cloudflare bot-blocking on Python urllib
        url = f"{MESH_HUB.rstrip('/')}/v1/agents"
        try:
            r = subprocess.run(
                ["curl", "-s", "--max-time", "15", url,
                 "-H", f"Authorization: Bearer {MESH_TOKEN}"],
                capture_output=True, text=True, timeout=20,
            )
            out = r.stdout.strip()
            result = json.loads(out) if out else {"error": "empty response"}
        except Exception as e:
            result = {"error": str(e)}
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "mesh_call":
        to = arguments.get("to", "")
        payload = arguments.get("payload", "")
        ttl = arguments.get("ttl_ms", 60000)
        ttl = max(1000, min(int(ttl), 120000))
        if not to:
            return [TextContent(type="text", text=json.dumps({"error": "to is required"}, ensure_ascii=False))]
        result = _mesh_call_cli(to, payload, ttl)
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    elif name == "mesh_send":
        to = arguments.get("to", "")
        payload = arguments.get("payload", "")
        if not to:
            return [TextContent(type="text", text=json.dumps({"error": "to is required"}, ensure_ascii=False))]
        url = f"{MESH_HUB.rstrip('/')}/v1/send"
        body = json.dumps({"to": to, "payload": payload, "from": MESH_AGENT_ID})
        try:
            r = subprocess.run(
                ["curl", "-s", "--max-time", "15", "-X", "POST", url,
                 "-H", f"Authorization: Bearer {MESH_TOKEN}",
                 "-H", "Content-Type: application/json",
                 "-d", body],
                capture_output=True, text=True, timeout=20,
            )
            out = r.stdout.strip()
            result = json.loads(out) if out else {"result": "ok"}
        except Exception as e:
            result = {"error": str(e)}
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]

    else:
        return [TextContent(type="text", text=json.dumps({"error": f"unknown tool: {name}"}, ensure_ascii=False))]


async def main():
    from mcp.server.stdio import stdio_server
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
