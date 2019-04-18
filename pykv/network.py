import socket
from random import choice, choices
from threading import Lock, Thread
from time import sleep
from typing import List

from pykv.util import read_uint32, send_uint32, send_uint16_arr

Port = int


class Network:
    nodes: List[Port]
    mutex: Lock

    def __init__(self):
        self.nodes = []
        self.mutex = Lock()

    def __iter__(self):
        with self.mutex:
            return self.nodes[:]

    def is_empty(self) -> bool:
        with self.mutex:
            return len(self.nodes) == 0

    def choice(self) -> Port:
        with self.mutex:
            return choice(self.nodes)

    def choices(self, k: int) -> List[Port]:
        with self.mutex:
            return [node for node in set(choices(self.nodes, k))]


class StaticNetwork(Network):
    def __init__(self, nodes: List[Port]):
        super().__init__()
        self.nodes.extend(nodes)


def connect(port: Port, ip="127.0.0.1") -> socket.socket:
    sock = socket.socket()
    # todo connection pooling
    sock.connect((ip, port))
    return sock


def request_peers(sock: socket.socket) -> List[Port]:
    read_uint32(sock)


def gossip(gossip_net: "GossipNetwork") -> None:
    while True:
        sleep(5)

        if gossip_net.is_empty():
            continue

        port = gossip_net.choice()
        sock = connect(port)
        ports = [p for p in gossip_net]
        send_uint16_arr(sock, ports)


# is a UDP socket threadsafe?
class GossipNetwork(Network):
    gossip_worker: Thread

    def __init__(self, nodes: List[Port]):
        super().__init__()
        self.nodes.extend(nodes)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.recv(4)

        self.gossip_worker = Thread(
            target=gossip, kwargs=dict(gossip_net=self), daemon=True
        )
        self.gossip_worker.start()
