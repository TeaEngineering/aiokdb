import logging
import socket
import struct
from typing import Optional

from aiokdb import KException, KObj, MessageType, TypeEnum, b9, cv, d9


# I don't recommend using, the asyncio interface is way nicer
class KSocket:
    def __init__(self, skt: socket.socket, raise_krr: bool = True):
        self.s = skt
        self.raise_krr = raise_krr

    def k(self, cmd: str, data: Optional[KObj] = None) -> KObj:
        ko = cv(cmd)
        self.s.sendall(b9(ko, msgtype=MessageType.SYNC))
        msgh = self.readexactly(8)
        ver, msgtype, flags, msglen = struct.unpack("<BBHI", msgh)
        print(f"> recv ver={ver} msgtype={msgtype} flags={flags} msglen={msglen}")
        payload = self.readexactly(msglen - 8)

        k = d9(msgh + payload)
        if self.raise_krr and k.t == TypeEnum.KRR:
            raise KException(k.aS())
        return k

    def readexactly(self, sz: int) -> bytes:
        bs = b""
        while len(bs) < sz:
            bs = bs + self.s.recv(sz - len(bs))
        return bs


def khpu(
    host: str = "localhost",
    port: int = 12345,
    auth: str = "kdb:pass",
    ver: int = 3,
    raise_krr: bool = True,
) -> KSocket:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(auth.encode() + struct.pack("<B", ver) + b"\000")
    data = s.recv(1)
    remote_ver = struct.unpack("<B", data)[0]
    if remote_ver != ver:
        raise Exception(f"expected version {ver}, server gave {remote_ver}")
    return KSocket(s, raise_krr=raise_krr)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    h = khpu("localhost", 12345, "kdb:pass")
    result = h.k("2.0+3.0")
    assert result.aF() == 5.0
    print(result)
    result.aJ()
