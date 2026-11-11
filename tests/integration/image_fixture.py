"""Deterministic Forge message producer; no hardware or user images."""
import numpy as np
from dora import Node
from forge_msgs import CompressedImage, Image, JointState


def main():
    node = Node()
    color = np.empty((48, 64, 3), dtype=np.uint8)
    color[:] = [220, 90, 30]
    jpeg = CompressedImage.from_numpy(color, format="jpeg", quality=95)
    depth = Image.from_numpy(np.full((48, 64), 1.25, dtype=np.float32), encoding="32FC1")
    state = JointState(name=["joint1"], position=[0.25])
    for event in node:
        if event["type"] == "STOP":
            break
        if event["type"] == "ERROR":
            raise RuntimeError(event.get("error", "Dora error"))
        if event["type"] == "INPUT" and event["id"] == "tick":
            node.send_output("image", jpeg.to_arrow())
            node.send_output("depth", depth.to_arrow())
            node.send_output("state", state.to_arrow())


if __name__ == "__main__":
    main()
