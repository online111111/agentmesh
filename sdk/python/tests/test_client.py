import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentmesh import Client, DialError


def free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="module")
def hub_url():
    port = free_port()
    env = os.environ.copy()
    for k in ("ALL_PROXY", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "http_proxy", "https_proxy"):
        env.pop(k, None)
    env.update({
        "MESH_HOST": "127.0.0.1",
        "MESH_PORT": str(port),
        "MESH_API_KEYS": "a:ka:alice:default\nb:kb:bob:default",
        "MESH_IP_CONN_RATE": "0",
        "MESH_AGENT_MSG_RATE": "0",
    })
    meshd = subprocess.Popen(
        ["go", "run", "./cmd/meshd", "serve"],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    # wait health
    import urllib.request
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/health", timeout=1) as r:
                if r.status == 200:
                    break
        except Exception:
            time.sleep(0.1)
    else:
        meshd.kill()
        raise RuntimeError("meshd failed to start")
    yield url
    meshd.terminate()
    try:
        meshd.wait(timeout=3)
    except Exception:
        meshd.kill()


@pytest.mark.asyncio
async def test_dial_and_send(hub_url):
    bob_got = asyncio.Event()
    payload_box = {}

    bob = await Client.dial(hub_url, "kb", "bob-py")
    def on_msg(env, payload):
        if env["type"] == 0x10:  # SEND
            payload_box["data"] = payload
            bob_got.set()
    bob.on_message(on_msg)

    alice = await Client.dial(hub_url, "ka", "alice-py")
    await alice.send("bob-py", b"hello-py")
    await asyncio.wait_for(bob_got.wait(), timeout=3)
    assert payload_box["data"] == b"hello-py"
    await alice.close()
    await bob.close()


@pytest.mark.asyncio
async def test_request_echo(hub_url):
    bob = await Client.dial(hub_url, "kb", "bob-echo-py")
    async def echo_loop():
        # use on_message to reply
        pass
    def on_msg(env, payload):
        if env["type"] == 0x11:  # REQUEST
            asyncio.get_event_loop().create_task(
                bob.write_frame({
                    "type": 0x12,
                    "id": "r",
                    "corr": env["corr"],
                    "dst": env["src"],
                }, payload)
            )
    bob.on_message(on_msg)
    alice = await Client.dial(hub_url, "ka", "alice-req-py")
    await asyncio.sleep(0.05)
    res = await alice.request("bob-echo-py", b"ping", ttl_ms=3000)
    assert res["payload"] == b"ping"
    await alice.close()
    await bob.close()


@pytest.mark.asyncio
async def test_auth_failed(hub_url):
    with pytest.raises(DialError):
        await Client.dial(hub_url, "wrong", "alice-x")
