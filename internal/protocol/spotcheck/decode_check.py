#!/usr/bin/env python3
"""Cross-language wire parity spot-check (Python side) — Task 0.9.

Reads the Go golden testvectors.json and, for a representative subset (a normal
full-envelope frame, a frame whose hdr has >=2 keys, and the compact
STREAM_DATA frame), decodes the 11-slot positional msgpack envelope array with
the `msgpack` library and RE-ENCODES it, asserting the bytes are byte-identical
to the Go golden `envHex`.

This proves early (before the full P6 SDK) that Go's positional-array + sorted
hdr encoding is reproducible by Python's msgpack, catching B2 (map vs array) and
B3 (hdr key ordering) drift while changing the wire is still cheap.

Run:  python3 decode_check.py            # resolves ../testvectors.json
Exit: 0 on parity, non-zero on any mismatch.
"""
import base64
import json
import os
import sys

try:
    import msgpack
except ImportError:
    print("FAIL: python 'msgpack' package not installed (pip install msgpack)", file=sys.stderr)
    sys.exit(2)

HERE = os.path.dirname(os.path.abspath(__file__))
VECTORS = os.path.join(HERE, "..", "testvectors.json")

# The 11 frozen positional slots (DESIGN §4.2), in order.
SLOT_ORDER = ["v", "type", "id", "corr", "stream", "src", "dst", "tenant", "ttl", "hops", "hdr"]

# Representative subset required by Task 0.9.
TARGET_NAMES = {"hdr_multi_unsorted", "stream_data", "request", "utf8_multibyte"}


def encode_envelope(env: dict) -> bytes:
    """Re-encode an envelope as the frozen 11-slot positional array.

    - fixarray of exactly 11 elements (never a map),
    - hdr (slot 10) is a map with keys sorted ascending (B3),
    - integers use msgpack's minimal encoding (packer default),
    - strings are UTF-8.
    """
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
    # use_bin_type/strict irrelevant here; sort_keys guarantees map determinism.
    packer = msgpack.Packer(autoreset=True, use_bin_type=True)
    # Manually emit array header + elements so ints stay ints and the map is
    # emitted with sorted keys (msgpack.packb(sort_keys=True) also works).
    return msgpack.packb(arr, use_bin_type=True)


def main() -> int:
    with open(VECTORS, "r", encoding="utf-8") as fh:
        gf = json.load(fh)

    vectors = {v["name"]: v for v in gf["vectors"]}
    missing = TARGET_NAMES - set(vectors.keys())
    if missing:
        print(f"FAIL: golden file missing target vectors: {sorted(missing)}", file=sys.stderr)
        return 1

    failures = 0
    for name in sorted(TARGET_NAMES):
        vec = vectors[name]
        want_env_hex = vec["envHex"]

        # 1) Decode the Go golden envelope bytes and check it is an 11-array.
        env_bytes = bytes.fromhex(want_env_hex)
        decoded = msgpack.unpackb(env_bytes, raw=False, strict_map_key=False)
        if not isinstance(decoded, list):
            print(f"FAIL[{name}]: envelope decoded as {type(decoded).__name__}, expected list (B2: positional array)", file=sys.stderr)
            failures += 1
            continue
        if len(decoded) != 11:
            print(f"FAIL[{name}]: envelope array len {len(decoded)} != 11", file=sys.stderr)
            failures += 1
            continue

        # 2) Re-encode from the structured env in the golden file and compare hex.
        got_env = encode_envelope(vec["env"])
        got_hex = got_env.hex()
        if got_hex != want_env_hex:
            print(f"FAIL[{name}]: env re-encode mismatch\n  got ={got_hex}\n  want={want_env_hex}", file=sys.stderr)
            failures += 1
            continue

        # 3) Sanity: decoded slots match the structured env (skip hdr order).
        expected = [vec["env"][k] if k != "hdr" else None for k in SLOT_ORDER]
        for i, k in enumerate(SLOT_ORDER):
            if k == "hdr":
                continue
            if decoded[i] != expected[i]:
                print(f"FAIL[{name}]: slot {i} ({k}) = {decoded[i]!r}, want {expected[i]!r}", file=sys.stderr)
                failures += 1

        print(f"OK[{name}]: envelope 11-slot array, hdr sorted, bytes match Go golden")

    if failures:
        print(f"\n{failures} parity failure(s)", file=sys.stderr)
        return 1
    print("\nPython<->Go wire parity: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
