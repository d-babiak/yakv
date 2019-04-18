import socket
import sys
from typing import List

from pykv.util import send_uint32


def send_line(sock, line: str) -> int:
    encoded = line.encode("utf-8")
    n = len(encoded)
    send_uint32(sock, n)
    return sock.send(encoded)


def main(argv: List[str]):
    port = int(argv[1])

    sock = socket.socket()
    sock.connect(("127.0.0.1", port))

    while True:
        try:
            line = input("> ")
        except EOFError:
            line = None

        if not line:
            break
        send_line(sock, line)


if __name__ == "__main__":
    main(sys.argv)
