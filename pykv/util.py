import socket
import struct
from typing import List, Union, Optional, Any


def encode_type(x: Any) -> bytes:
    meta = 0 if x is None else 1
    return struct.pack("!B", meta)


def decode_type(b: bytes) -> type:
    assert len(b) == 1
    n = struct.unpack("!B", b)[0]
    switch = {0: None, 1: str}  # type(None)  # 💪
    return switch[n]


def encode_uint32(n: int) -> bytes:
    return struct.pack("!I", n)


def decode_uint32(data: bytes) -> int:
    return struct.unpack("!I", data)[0]


def send_uint32(sock, n) -> int:
    # print(n)
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
    sock.send(encode_type(s))

    if s is None:
        return

    if isinstance(s, str):
        data = s.encode("utf-8")
    elif isinstance(s, bytes):
        data = s
    n = len(data)
    sock.send(encode_uint32(n))
    sock.send(data)


def read_str(sock: socket.socket) -> str:
    n: int = read_uint32(sock)
    data = read_bytes(sock, n)
    # import pdb; pdb.set_trace()
    # print(repr(data))
    return data.decode("utf-8")
