"""Golden vector byte-parity: Python encode matches Go envHex/frameHex."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import msgpack
import pytest

ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "internal" / "protocol" / "testvectors.json"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentmesh import protocol as p


def load_vectors():
    with open(VECTORS, encoding="utf-8") as f:
        return json.load(f)["vectors"]


def encode_env_from_struct(env: dict) -> bytes:
    hdr = env.get("hdr") or {}
    sorted_hdr = {k: hdr[k] for k in sorted(hdr.keys())}
    arr = [
        env["v"],
        env["type"],
        env["id"],
        env["corr"],
        env["stream"],
        env["src"],
        env["dst"],
        env["tenant"],
        env["ttl"],
        env["hops"],
        sorted_hdr,
    ]
    return msgpack.packb(arr, use_bin_type=True)


@pytest.mark.parametrize("vec", load_vectors(), ids=lambda v: v["name"])
def test_vector_env_parity(vec):
    want = vec["envHex"]
    got = encode_env_from_struct(vec["env"]).hex()
    assert got == want, f"{vec['name']}: env mismatch\n got={got}\nwant={want}"


@pytest.mark.parametrize("vec", load_vectors(), ids=lambda v: v["name"])
def test_vector_frame_decode(vec):
    frame = bytes.fromhex(vec["frameHex"])
    env, payload = p.decode_frame(frame)
    assert env["type"] == vec["env"]["type"]
    # payload may be empty
    if vec.get("payloadB64"):
        import base64
        assert payload == base64.b64decode(vec["payloadB64"])
