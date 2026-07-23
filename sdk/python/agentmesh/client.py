"""Async WebSocket client for AgentMesh."""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, AsyncIterator, Callable, Dict, Optional

import websockets

from . import protocol as p


class DialError(Exception):
    pass


class RPCError(Exception):
    def __init__(self, code: str, message: str = ""):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}" if message else code)


def _new_id() -> str:
    # Compact ULID-ish id; hub accepts any unique string.
    return uuid.uuid4().hex


def _to_ws_url(url: str) -> str:
    u = url.strip()
    if u.startswith("https://"):
        return "wss://" + u[len("https://") :]
    if u.startswith("http://"):
        return "ws://" + u[len("http://") :]
    if u.startswith("ws://") or u.startswith("wss://"):
        return u
    return "ws://" + u


class Client:
    def __init__(self, ws, agent_id: str, session: str):
        self._ws = ws
        self.agent_id = agent_id
        self.session = session
        self._on_message: Optional[Callable[[Dict[str, Any], bytes], None]] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._streams: Dict[str, asyncio.Queue] = {}
        self._stream_by_corr: Dict[str, str] = {}
        self._reader_task = asyncio.create_task(self._read_loop())
        self._closed = False

    @classmethod
    async def dial(
        cls,
        hub_url: str,
        token: str,
        agent_id: str,
        caps: Optional[list] = None,
    ) -> "Client":
        ws_url = _to_ws_url(hub_url)
        try:
            ws = await websockets.connect(ws_url, max_size=p.MAX_FRAME_BYTES)
        except Exception as e:
            raise DialError(str(e)) from e
        from .control import marshal_hello

        hello_payload = marshal_hello(token, agent_id, caps or [])
        frame = p.encode_frame({"v": p.PROTOCOL_VERSION, "type": p.HELLO}, hello_payload)
        await ws.send(frame)
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        if isinstance(raw, str):
            raw = raw.encode()
        env, payload = p.decode_frame(raw)
        if env["type"] == p.ERROR:
            from .control import unmarshal_error

            ep = unmarshal_error(payload)
            await ws.close()
            raise DialError(f"{ep.get('code')}: {ep.get('message')}")
        if env["type"] != p.WELCOME:
            await ws.close()
            raise DialError(f"expected WELCOME, got {env['type']}")
        from .control import unmarshal_welcome

        welcome = unmarshal_welcome(payload)
        return cls(ws, agent_id, welcome.get("session", ""))

    def on_message(self, handler: Callable[[Dict[str, Any], bytes], None]) -> None:
        self._on_message = handler

    async def send(self, dst: str, payload: bytes) -> None:
        env = {
            "v": p.PROTOCOL_VERSION,
            "type": p.SEND,
            "id": _new_id(),
            "src": self.agent_id,
            "dst": dst,
        }
        await self._ws.send(p.encode_frame(env, payload))

    async def request(self, dst: str, payload: bytes, ttl_ms: int = 30000) -> Dict[str, Any]:
        corr = _new_id()
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[corr] = fut
        env = {
            "v": p.PROTOCOL_VERSION,
            "type": p.REQUEST,
            "id": _new_id(),
            "corr": corr,
            "src": self.agent_id,
            "dst": dst,
            "ttl": ttl_ms,
            "hops": 8,
        }
        await self._ws.send(p.encode_frame(env, payload))
        try:
            return await asyncio.wait_for(fut, timeout=ttl_ms / 1000.0)
        except asyncio.TimeoutError:
            self._pending.pop(corr, None)
            raise RPCError("TIMEOUT", "request timed out")

    async def request_stream(
        self, dst: str, payload: bytes, ttl_ms: int = 30000
    ) -> AsyncIterator[Dict[str, Any]]:
        corr = _new_id()
        q: asyncio.Queue = asyncio.Queue()
        self._stream_by_corr[corr] = ""
        self._pending_stream_q = getattr(self, "_pending_stream_q", {})
        self._pending_stream_q[corr] = q
        env = {
            "v": p.PROTOCOL_VERSION,
            "type": p.REQUEST,
            "id": _new_id(),
            "corr": corr,
            "src": self.agent_id,
            "dst": dst,
            "ttl": ttl_ms,
            "hops": 8,
            "hdr": {"stream": "1"},
        }
        await self._ws.send(p.encode_frame(env, payload))
        try:
            while True:
                chunk = await asyncio.wait_for(q.get(), timeout=ttl_ms / 1000.0)
                yield chunk
                if chunk.get("is_end"):
                    return
        except asyncio.TimeoutError:
            raise RPCError("TIMEOUT", "stream timed out")
        finally:
            self._pending_stream_q.pop(corr, None)
            sid = self._stream_by_corr.pop(corr, None)
            if sid:
                self._streams.pop(sid, None)

    async def write_frame(self, env: Dict[str, Any], payload: bytes = b"") -> None:
        if not env.get("src"):
            env["src"] = self.agent_id
        if not env.get("v"):
            env["v"] = p.PROTOCOL_VERSION
        await self._ws.send(p.encode_frame(env, payload))

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._reader_task.cancel()
        try:
            await self._ws.close()
        except Exception:
            pass

    async def _read_loop(self) -> None:
        try:
            async for raw in self._ws:
                if isinstance(raw, str):
                    raw = raw.encode()
                env, payload = p.decode_frame(raw)
                typ = env["type"]
                if typ == p.RESPONSE:
                    corr = env.get("corr") or ""
                    fut = self._pending.pop(corr, None)
                    if fut and not fut.done():
                        fut.set_result({"from": env.get("src"), "payload": payload, "corr": corr, "env": env})
                        continue
                if typ == p.ERROR:
                    corr = env.get("corr") or ""
                    fut = self._pending.pop(corr, None)
                    if fut and not fut.done():
                        from .control import unmarshal_error

                        ep = unmarshal_error(payload)
                        fut.set_exception(RPCError(ep.get("code", "ERROR"), ep.get("message", "")))
                        continue
                if typ == p.STREAM_OPEN:
                    corr = env.get("corr") or ""
                    sid = env.get("stream") or ""
                    q = getattr(self, "_pending_stream_q", {}).get(corr)
                    if q is not None and sid:
                        self._streams[sid] = q
                        self._stream_by_corr[corr] = sid
                    continue
                if typ == p.STREAM_DATA:
                    sid = env.get("stream") or ""
                    q = self._streams.get(sid)
                    if q is not None:
                        seq = int((env.get("hdr") or {}).get("seq", "0") or 0)
                        await q.put({"seq": seq, "data": payload, "is_end": False})
                    continue
                if typ == p.STREAM_END:
                    sid = env.get("stream") or ""
                    q = self._streams.get(sid)
                    if q is not None:
                        status = (env.get("hdr") or {}).get("status", "ok")
                        await q.put({"is_end": True, "status": status, "data": b""})
                    continue
                if self._on_message:
                    self._on_message(env, payload)
        except asyncio.CancelledError:
            return
        except Exception:
            return
