"""Run real standalone Gateway and Gateway image/state transport smoke checks."""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from importlib.metadata import version

import numpy as np
from PIL import Image as PILImage
import yaml
from websockets.sync.client import connect

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["single", "chain"], required=True)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--output", type=Path, default=ROOT / "build/forge2-acceptance")
    args = parser.parse_args()
    if args.seconds < 6:
        parser.error("--seconds must be at least 6")
    dora = os.environ.get("DORA_BIN") or shutil.which("dora")
    if dora is None:
        parser.error("Dora CLI 1.0.1 is required (install dora-rs-cli or set DORA_BIN)")
    out = (args.output / args.mode).resolve()
    out.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = {"host": "127.0.0.1", "port": port, "joint_order": ["joint1"],
              "image_input_ids": ["image/rgb", "image/depth"] if args.mode == "chain" else [],
              "readiness": {"require_proprio_state": args.mode == "chain", "require_images": False,
                            "require_state_client": False, "require_image_client": False}}
    (out / "gateway.yaml").write_text(yaml.safe_dump(config))
    gateway = {"id": "gateway", "path": sys.executable,
               "args": f"-m forge_gateway --config {out / 'gateway.yaml'}",
               "env": {"PYTHONPATH": str(ROOT / "src")},
               "inputs": {"tick": "dora/timer/millis/20"}, "outputs": ["policy_command"]}
    nodes = [gateway]
    if args.mode == "chain":
        gateway["inputs"].update({"proprio_state": "fixture/state", "image/rgb": "fixture/image", "image/depth": "fixture/depth"})
        nodes.append({"id": "fixture", "path": sys.executable,
                      "args": str(Path(__file__).with_name("image_fixture.py")),
                      "inputs": {"tick": "dora/timer/millis/33"}, "outputs": ["state", "image", "depth"]})
    flow = out / "dataflow.yaml"
    flow.write_text(yaml.safe_dump({"nodes": nodes}, sort_keys=False))
    command = [dora, "run", str(flow), "--stop-after", f"{args.seconds}s"]
    report = {"mode": args.mode, "command": command, "includes_gateway": True,
              "dora_version": subprocess.check_output([dora, "--version"], text=True).strip(),
              "versions": {name: version(name) for name in ["forge-msgs", "forge-common", "forge-tool", "dora-rs", "pillow"]},
              "passed": False}
    process = None
    try:
        with (out / "dora.log").open("w") as log:
            process = subprocess.Popen(command, cwd=out, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 30.0
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=0.5) as response:
                        health = json.load(response)
                    if health.get("ok"):
                        break
                except OSError:
                    pass
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Gateway did not become healthy; inspect dora.log")
                time.sleep(0.1)
            report["health"] = health
            with connect(f"ws://127.0.0.1:{port}/ws/state", open_timeout=2) as ws:
                while True:
                    state = json.loads(ws.recv(timeout=2))
                    if state["current_frame_count"] > 0 and (args.mode == "single" or state["sensors"]["joints"].get("joint1") == 0.25):
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Expected state was not received")
                report["state"] = state
            if args.mode == "chain":
                received = {}
                with connect(f"ws://127.0.0.1:{port}/ws/images", open_timeout=2) as ws:
                    while len(received) < 2:
                        image = json.loads(ws.recv(timeout=2))
                        decoded = np.array(PILImage.open(io.BytesIO(base64.b64decode(image["data"]))))
                        assert decoded.shape[:2] == (48, 64)
                        if image["id"] == "image/rgb":
                            assert np.abs(decoded.astype(int) - np.array([220, 90, 30])).max() <= 5
                        received[image["id"]] = {"shape": list(decoded.shape), "format": image["format"]}
                report["images"] = received
            process.wait(timeout=args.seconds + 10)
        report["exit_code"] = process.returncode
        if process.returncode != 0:
            raise RuntimeError(f"Dora exited with {process.returncode}")
        report["passed"] = True
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))


if __name__ == "__main__":
    main()
