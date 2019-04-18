import socket
import sys
from typing import List, Optional

from pykv.util import send_uint32, decode_type, read_str


def send_line(sock, line: str) -> int:
    encoded = line.encode("utf-8")
    n = len(encoded)
    send_uint32(sock, n)
    return sock.send(encoded)


def recv_line(sock: socket.socket) -> Optional[str]:
    _type = decode_type(sock.recv(1))

    if _type is None:
        return None

    s = read_str(sock)
    return s


def log(x):
    if x is None:
        print("∅")
    else:
        print(x)


QUITS = ("exit", "quit")


def main(argv: List[str]):
    port = int(argv[1])

    sock = socket.socket()
    sock.connect(("127.0.0.1", port))

    while True:
        try:
            line = input("> ")
        except EOFError:
            line = None

        if not line or line.lower() in QUITS:
            break

        send_line(sock, line)
        x = recv_line(sock)
        log(x)


if __name__ == "__main__":
    main(sys.argv)
