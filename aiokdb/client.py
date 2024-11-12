import argparse
import asyncio
import logging
import struct
from typing import Any, Optional, Tuple
from urllib.parse import urlparse

from aiokdb import cv, logger
from aiokdb.server import (
    BaseContext,
    CredentialsException,
    KdbReader,
    KdbWriter,
    reader_to_context_task,
)


class ClientContext(BaseContext):
    pass


background_tasks = set()


# KDB client code
async def open_qipc_connection(
    host: str = "127.0.0.1",
    port: int = 8890,
    user: Optional[str] = None,
    password: Optional[str] = None,
    uri: Optional[str] = None,
    context: Optional[ClientContext] = None,
    ver: int = 3,
) -> Tuple[KdbReader, KdbWriter]:
    if uri:  #  uri takes precedence if provided
        pr = urlparse(uri)
        if pr.hostname:
            host = pr.hostname
        if pr.port:
            port = pr.port
        user = pr.username
        password = pr.password

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
        remote_ver = struct.unpack("<B", data)[0]
        if remote_ver != ver:
            raise Exception(f"expected version {ver}, server gave {remote_ver}")
        logger.debug(f"Connected OK, remote_ver={remote_ver}")
    except asyncio.IncompleteReadError as e:
        raise CredentialsException(e)

    q_reader = KdbReader(reader)
    q_writer = KdbWriter(writer, q_reader, version=remote_ver, context=context)
    if context is not None:
        task = asyncio.create_task(reader_to_context_task(q_writer, q_reader, context))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    return q_reader, q_writer


async def maintain_qipc_connection(uri: Optional[str], context: ClientContext) -> None:
    while True:
        try:
            logging.info("attempting connection")
            qr, qw = await open_qipc_connection(uri=uri, context=context)
            await qw.writer.wait_closed()
            logging.info("connection closed")
        except CredentialsException:
            raise
        except Exception:
            logging.exception("caught exception")
            await asyncio.sleep(10)

    logging.info("retry loop exited?")


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
        while True:
            obj = await w.sync_req(cv(input("Enter query: ")))
            print(f"Got object {obj}")
        return None

    asyncio.run(main(args))
