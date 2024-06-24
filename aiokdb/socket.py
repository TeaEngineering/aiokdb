import logging
import socket
import struct

from . import KException, KObj, MessageType, TypeEnum, b9, cv, d9


# I don't recommend using, the asyncio interface is way nicer
class KSocket:
    def __init__(self, skt, raise_krr=True):
        self.s = skt
        self.raise_krr = raise_krr

    def k(self, cmd, data=None):
        ko = cv(cmd)
        self.s.sendall(b9(ko, msgtype=MessageType.SYNC))
        msgh = self.readexactly(8)
        ver, msgtype, flags, msglen = struct.unpack("<BBHI", msgh)
        print(
            "> recv ver={0} msgtype={1} flags={2} msglen={3}".format(
                ver, msgtype, flags, msglen
            )
        )
        payload = self.readexactly(msglen - 8)

        k = d9(msgh + payload)
        if self.raise_krr and k.t == TypeEnum.KRR:
            raise KException(k.aS())
        return k

    def readexactly(self, sz):
        bs = b""
        while len(bs) < sz:
            bs = bs + self.s.recv(sz - len(bs))
        return bs


def khpu(
    host="localhost",
    port=12345,
    auth="kdb:pass",
    ver=3,
    raise_krr=True,
):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall(auth.encode() + struct.pack("<B", ver) + b"\000")
    data = s.recv(1)
    remote_ver = struct.unpack("<B", data)[0]
    if remote_ver != ver:
        raise Exception("expected version {0}, server gave {1}".format(ver, remote_ver))
    return KSocket(s, raise_krr=raise_krr)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    h = khpu("localhost", 12345, "kdb:pass")
    result = h.k("2.0+3.0")
    assert result.aF() == 5.0
    print(result)
