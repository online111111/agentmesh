"""Minimal wire codec matching frozen Go protocol (11-slot positional msgpack)."""
from __future__ import annotations

import struct
from typing import Any, Dict, Optional, Tuple

import msgpack

# Message types (DESIGN §4.3)
HELLO = 0x01
WELCOME = 0x02
PING = 0x03
PONG = 0x04
SEND = 0x10
REQUEST = 0x11
RESPONSE = 0x12
CANCEL = 0x13
ACK = 0x1E
NACK = 0x1F
STREAM_OPEN = 0x20
STREAM_DATA = 0x21
STREAM_END = 0x22
SUBSCRIBE = 0x30
SUBACK = 0x31
UNSUB = 0x32
PUBLISH = 0x33
ERROR = 0xFF

PROTOCOL_VERSION = 1
MAX_FRAME_BYTES = 1 << 20


def _encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _decode_varint(buf: bytes, off: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if off >= len(buf):
            raise ValueError("truncated varint")
        b = buf[off]
        off += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, off
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def encode_envelope(env: Dict[str, Any]) -> bytes:
    hdr = env.get("hdr") or {}
    # Sort hdr keys for deterministic bytes (B3)
    if hdr:
        hdr = {k: hdr[k] for k in sorted(hdr.keys())}
    arr = [
        int(env.get("v", PROTOCOL_VERSION)),
        int(env["type"]),
        str(env.get("id", "")),
        str(env.get("corr", "")),
        str(env.get("stream", "")),
        str(env.get("src", "")),
        str(env.get("dst", "")),
        str(env.get("tenant", "")),
        int(env.get("ttl", 0)),
        int(env.get("hops", 0)),
        hdr,
    ]
    return msgpack.packb(arr, use_bin_type=True)


def decode_envelope(data: bytes) -> Dict[str, Any]:
    arr = msgpack.unpackb(data, raw=False, strict_map_key=False)
    if not isinstance(arr, list) or len(arr) != 11:
        raise ValueError("envelope must be 11-slot array")
    hdr = arr[10] or {}
    if not isinstance(hdr, dict):
        hdr = {}
    return {
        "v": int(arr[0]),
        "type": int(arr[1]),
        "id": str(arr[2] or ""),
        "corr": str(arr[3] or ""),
        "stream": str(arr[4] or ""),
        "src": str(arr[5] or ""),
        "dst": str(arr[6] or ""),
        "tenant": str(arr[7] or ""),
        "ttl": int(arr[8] or 0),
        "hops": int(arr[9] or 0),
        "hdr": {str(k): str(v) for k, v in hdr.items()},
    }


def encode_frame(env: Dict[str, Any], payload: bytes = b"") -> bytes:
    if payload is None:
        payload = b""
    env_bytes = encode_envelope(env)
    if len(env_bytes) > MAX_FRAME_BYTES:
        raise ValueError("FRAME_TOO_BIG")
    typ = int(env["type"]) & 0xFF
    return bytes([typ]) + _encode_varint(len(env_bytes)) + env_bytes + payload


def decode_frame(data: bytes) -> Tuple[Dict[str, Any], bytes]:
    if len(data) < 2:
        raise ValueError("frame too short")
    typ = data[0]
    env_len, off = _decode_varint(data, 1)
    if env_len > MAX_FRAME_BYTES:
        raise ValueError("FRAME_TOO_BIG")
    end = off + env_len
    if end > len(data):
        raise ValueError("truncated envelope")
    env = decode_envelope(data[off:end])
    env["type"] = typ  # type byte is authoritative
    payload = data[end:]
    return env, payload
