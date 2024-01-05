import argparse
import asyncio
import logging
import struct
from typing import Any, Optional

from aiokdb import cv
from aiokdb.server import CredentialsException, KdbReader, KdbWriter


# KDB client code
async def open_qipc_connection(
    host: str = "127.0.0.1",
    port: int = 8890,
    user: Optional[str] = None,
    password: Optional[str] = None,
    ver: int = 3,
) -> tuple[KdbReader, KdbWriter]:
    reader, writer = await asyncio.open_connection(host, port)

    auth = ""
    if user:
        auth = auth + user
    if password:
        auth = auth + ":" + password

    writer.write(auth.encode() + struct.pack("<B", ver) + b"\000")
    await writer.drain()

    try:
        data = await reader.readexactly(1)  # negotiated version, if auth correct
        logging.info(f"Received: {data!r}")
        remote_ver = struct.unpack("<B", data)[0]
        if remote_ver != ver:
            raise Exception(f"expected version {ver}, server gave {remote_ver}")
    except asyncio.IncompleteReadError as e:
        raise CredentialsException(e)

    q_reader = KdbReader(reader)
    q_writer = KdbWriter(writer, q_reader, version=remote_ver)
    return q_reader, q_writer


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=8890, type=int)
    parser.add_argument("--user", default="user")
    parser.add_argument("--password")
    args = parser.parse_args()

    async def main(args: Any) -> None:
        r, w = await open_qipc_connection(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        w.write(cv("2+2"))
        mtype, obj = await r.read()
        print(f"Got object {obj}")
        return None

    asyncio.run(main(args))
