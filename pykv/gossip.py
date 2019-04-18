import argparse
import datetime
import json
import socket

import sys
from threading import Thread, Lock, current_thread
from time import sleep
from typing import Tuple, Dict, List, Any

from attr import dataclass

IP = str
Port = int
PRUNE_TIMEOUT = 20


def flood(sock: socket.socket, addresses: List[Tuple[IP, Port]], data: bytes) -> None:
    if not addresses:
        return
    # print(f"Flooding {data}", end='\n\n')
    for addr in addresses:
        # print(f"  to: {addr}")
        sock.sendto(data, addr)


def encode(x: Any) -> bytes:
    if isinstance(x, dict):
        return json.dumps(x, default=str).encode("utf-8")


@dataclass(frozen=True)
class NodeParams:
    ip: IP
    port: int
    gossip_port: int


def ping_recv_loop(sock: socket.socket, node: "GossipNode") -> None:
    print(f"{current_thread().name} gossip_loop")
    while True:
        data, _, _, sender = sock.recvmsg(1024)  # should be 1500?
        msg = json.loads(data)
        _type = msg["type"]

        # node.recv_ping(sender)

        # print(sender, msg, end="\n\n") # TODO - reduce chatter

        addr = tuple(msg["addr"])
        node_params = NodeParams(
            ip=addr[0], port=msg["port"], gossip_port=msg["gossip_port"]
        )
        node.recv_ping(node_params)

        ttl = msg["ttl"]

        if ttl <= 1:
            # print("TTL up - dropping")
            continue

        new_msg = {**msg, "ttl": ttl - 1}

        addresses = [
            (addr.ip, addr.gossip_port)
            for addr in node
            if addr not in (sender, sock.getsockname())
        ]
        flood(sock, addresses, encode(new_msg))


def prune_loop(node: "GossipNode"):
    print(f"{current_thread().name} prune loop")
    while True:
        sleep(5)
        node.prune()


# WHAT ABOUT THREAD SAFETY?
def ping_send_loop(sock: socket.socket, node: "GossipNode") -> None:
    print(f"{current_thread().name} ping loop")
    while True:
        sleep(5)
        addresses = [addr for addr in node]
        for addr in addresses:
            # print(f"Health check to {addr}", end='\n\n')
            msg = dict(
                ttl=2,
                type="PING",
                addr=sock.getsockname(),
                port=node.port,
                gossip_port=node.gossip_port,
            )
            sock.sendto(encode(msg), (addr.ip, addr.gossip_port))


class GossipNode:
    port: int
    gossip_port: int
    nodes: Dict[NodeParams, datetime.datetime]
    lock: Lock

    def __init__(self, port: int, gossip_port: int):
        self.port = port
        self.gossip_port = gossip_port
        self.nodes = {}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", gossip_port))
        print(self.sock)

        self.lock = Lock()

        self.gossip_worker = Thread(
            name="🗣",
            target=ping_recv_loop,
            kwargs=dict(sock=self.sock, node=self),
            daemon=False,
        )

        self.prune_worker = Thread(
            name="✂", target=prune_loop, kwargs=dict(node=self), daemon=True
        )

        self.ping_worker = Thread(
            name="🏓",
            target=ping_send_loop,
            kwargs=dict(sock=self.sock, node=self),
            daemon=True,
        )

    def __iter__(self):
        with self.lock:
            return iter(list(self.nodes.keys()))

    def recv_ping(self, sender: NodeParams):
        with self.lock:
            # print(f"PING from {sender}", end='\n\n')
            self.nodes[sender] = datetime.datetime.now()

    def start(self) -> None:
        self.gossip_worker.start()
        self.prune_worker.start()
        self.ping_worker.start()

    def prune(self) -> None:
        with self.lock:
            now = datetime.datetime.now()
            threshold = datetime.timedelta(seconds=PRUNE_TIMEOUT)

            stale_nodes = [
                *{addr for addr, dt in self.nodes.items() if now - dt > threshold}
            ]

            # USE LOGGER
            # if stale_nodes:
            print(
                f"""
  fresh: {[(x.port, TTL(now=now, dt=dt)) for x, dt in self.nodes.items() if x not in stale_nodes]}
  stale: {stale_nodes}
"""
            )

            for addr in stale_nodes:
                self.nodes.pop(addr)

    def seed(self, seed_port: int) -> None:
        node_params = NodeParams(
            ip="127.0.0.1", port=-1, gossip_port=seed_port  # TODO - how to bootstrap?
        )
        self.nodes[node_params] = datetime.datetime.now()


def TTL(now, dt) -> float:
    return round(
        (dt + datetime.timedelta(seconds=PRUNE_TIMEOUT) - now).total_seconds(), 1
    )


def main(port, gossip_port):
    gossip_node = GossipNode(port=port, gossip_port=gossip_port)
    gossip_node.start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, required=True)
    parser.add_argument("--gossip-port", "-g", type=int, required=True)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main(**vars(parse_args()))
