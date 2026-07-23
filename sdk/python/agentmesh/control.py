"""Control payload encode/decode (HELLO/WELCOME/ERROR) — map msgpack, not envelope."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import msgpack


def marshal_hello(token: str, agent_id: str, caps: Optional[List[str]] = None, v: int = 1) -> bytes:
    obj = {
        "token": token,
        "agentId": agent_id,
        "caps": caps or [],
        "protocols": [],
        "v": v,
    }
    return msgpack.packb(obj, use_bin_type=True)


def unmarshal_welcome(b: bytes) -> Dict[str, Any]:
    obj = msgpack.unpackb(b, raw=False, strict_map_key=False)
    if not isinstance(obj, dict):
        return {}
    return {
        "session": str(obj.get("session") or ""),
        "heartbeatMs": int(obj.get("heartbeatMs") or 0),
        "features": list(obj.get("features") or []),
    }


def unmarshal_error(b: bytes) -> Dict[str, Any]:
    obj = msgpack.unpackb(b, raw=False, strict_map_key=False)
    if not isinstance(obj, dict):
        return {"code": "ERROR", "message": ""}
    return {
        "code": str(obj.get("code") or "ERROR"),
        "message": str(obj.get("message") or ""),
        "supported": list(obj.get("supported") or []),
    }
