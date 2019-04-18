import socket
import struct
from typing import List, Union, Optional


def encode_uint32(n: int) -> bytes:
    return struct.pack("!I", n)


def decode_uint32(data: bytes) -> int:
    return struct.unpack("!I", data)[0]


def send_uint32(sock, n) -> int:
    print(n)
    return sock.send(struct.pack("!I", n))


def send_uint16_arr(sock, shorts: List[int]) -> None:
    n = len(shorts)
    assert n < 2 ** 32
    send_uint32(sock, n)
    data = struct.pack(f"!{n}H", *shorts)
    sock.send(data)


def read_uint32(sock: socket.socket) -> Optional[int]:
    # will this work with UDP sockets?
    data: bytes = sock.recv(4)

    if len(data) < 4:
        return None

    return decode_uint32(data)


def read_bytes(sock: socket.socket, n: int) -> bytes:
    data = b""
    while n - len(data) > 0:
        data += sock.recv(n - len(data))
    return data


def send_str(sock: socket.socket, s: Union[str, bytes]) -> None:
    if isinstance(s, str):
        data = s.encode("utf-8")
    elif isinstance(s, bytes):
        data = s
    else:
        raise ValueError(f"Cannot send {s} | types: {type(s)}")
    n = len(s)
    sock.send(encode_uint32(n))
    sock.send(data)


def read_str(sock: socket.socket) -> str:
    n: int = read_uint32(sock)
    data = read_bytes(sock, n)
    return data.decode('utf-8')
