import argparse
import socket
from argparse import Namespace
from queue import Queue
from socket import AF_INET, SOCK_STREAM
from threading import Thread, current_thread

from pykv.gossip import GossipNode, NodeParams
from pykv.util import read_uint32, read_bytes, send_str


def handle_client(client_sock, replication_log: Queue, kv: dict) -> None:
    while True:
        n = read_uint32(client_sock)

        if n is None:
            print(f"{current_thread().name} DONE")
            break

        data = read_bytes(client_sock, n).decode('utf-8')
        print(f"{current_thread().name} {n} | {data}")

        if data.startswith('set '):
            _, key, val = data.split(' ')
            kv[key] = val
            # TODO - append to log
            replication_log.put(data)
        elif data.startswith('get '):
            _, key = data.split(' ')
            val = kv[key]
            send_str(client_sock, val)
        else:
            print('Unknown command:', data)


def one_off_socket(p: NodeParams) -> socket:
    sock = socket.socket(AF_INET, SOCK_STREAM)
    sock.connect((p.ip, p.gossip_port))
    return sock


# need connection pooling
def replication_broadcast_loop(gossip_node, replication_log: Queue) -> None:
    while True:
        data: bytes = replication_log.get()
        peers = [peer for peer in gossip_node]
        for p in peers:
            # CONTEXT MANAGER
            sock = one_off_socket(p)
            send_str(sock, data)
            sock.close()


def replication_listen_loop(replication_port: int, kv: dict) -> None:
    replication_sock = socket.socket(AF_INET, SOCK_STREAM)
    replication_sock.bind(("0.0.0.0", replication_port))
    replication_sock.listen(5)
    print(f"replication_listen_loop on {replication_sock.getsockname()}")

    while True:
        peer_sock, addr = replication_sock.accept()
        n = read_uint32(peer_sock)
        cmd: str = read_bytes(peer_sock, n).decode("utf-8")

        if not cmd.startswith("set "):
            print("wtf", cmd)
            continue

        chunks = cmd.split(" ")
        assert len(chunks) == 3
        _, key, val = chunks

        print(f"  ({addr}) said to set {key} to {val}")
        kv[key] = val


def main(port: int, gossip_port: int, seed_port: int = None):
    """
    1. Spin up gossip node
    2. Replication log
    3. Start replication loop
    4. Init listen_fd for receiving connections from client
    5. Accept loop; new thread to handle each new client
    """

    gossip_node = GossipNode(port=port, gossip_port=gossip_port)

    if seed_port:
        gossip_node.seed(seed_port)

    gossip_node.start()

    replication_log = Queue()

    kv = {}  # where the magic happens

    Thread(
        target=replication_listen_loop,
        name=f"replication-listen-thread",
        kwargs=dict(replication_port=gossip_port, kv=kv),
        daemon=True,
    ).start()

    Thread(
        target=replication_broadcast_loop,
        name=f"replication-broadcast-thread",
        kwargs=dict(gossip_node=gossip_node, replication_log=replication_log),
        daemon=True,
    ).start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", port))
    sock.listen(5)
    i = 0
    while True:
        client_sock, _ = sock.accept()

        Thread(
            target=handle_client,
            name=f"client-{i}",
            kwargs=dict(
                client_sock=client_sock,
                replication_log=replication_log,
                kv=kv,
            ),
            daemon=True,
        ).start()
        i += 1


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, required=True)
    parser.add_argument("--gossip-port", "-g", type=int, required=True)
    parser.add_argument("--seed-port", "-s", type=int)
    args = parser.parse_args()
    return args


# ipython run -i ${FILE} will run it as main
if __name__ == "__main__":
    main(**vars(parse_args()))
