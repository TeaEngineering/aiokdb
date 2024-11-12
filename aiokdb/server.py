import asyncio
import hmac
import itertools
import logging
import os
import struct
from functools import partial
from typing import Any, Callable, List, Optional, Tuple

from aiokdb import KException, KObj, MessageType, TypeEnum, b9, d9, krr, logger


class CredentialsException(Exception):
    pass


# TypeAlias for Optional KObj callback
OptKcb = Optional[Callable[[KObj], None]]


class KdbReader:
    def __init__(self, reader: asyncio.StreamReader, raise_krr: bool = True):
        self.reader = reader
        self.raise_krr = raise_krr

    async def _read(self) -> Tuple[MessageType, KObj]:
        msgh = await self.reader.readexactly(8)
        ver, msgtype, flags, msglen = struct.unpack("<BBHI", msgh)
        logger.debug(
            f"> recv ver={ver} msgtype={msgtype} flags={flags} msglen={msglen}"
        )
        payload = await self.reader.readexactly(msglen - 8)
        k = d9(msgh + payload)
        return msgtype, k

    async def read(self) -> Tuple[MessageType, KObj]:
        msgtype, k = await self._read()
        if self.raise_krr and k.t == TypeEnum.KRR:
            raise KException(k.aS())
        return msgtype, k


class KdbWriter:
    def __init__(
        self,
        writer: asyncio.StreamWriter,
        kreader: KdbReader,
        version: int = 0,
        qid: Any = None,
        context: Optional["BaseContext"] = None,
    ):
        self.writer = writer
        self.qid = qid
        self.version = version
        self.reader = kreader
        self._context = context
        self._completions: List[asyncio.Future[KObj]] = []

    def write(self, obj: KObj, mt: MessageType = MessageType.SYNC) -> None:
        bs = b9(obj, msgtype=mt)
        logger.debug(f"< sending {bs!r}")
        self.writer.write(bs)

    async def sync_req(self, obj: KObj, ooob: OptKcb = None) -> KObj:
        # responses arrive in the order that requests are sent.
        # The caller can either call write() directly, and then consume
        # from the qreader.read() method 'inline', or they can call this
        # method to wait for the result.
        # if we have a context provided, then there is a reader task that
        # will consume the return values, and forward the RESPONSE messages
        # to us, allowing us to unblock the waiting Future.
        # if there is no context/task, then we will read from the qreader
        # here ourselves.
        #
        # if tasks are calling this method *along* with their *own* read() task,
        # then responses will get muddled up or lost, or raise
        # RuntimeError: readexactly() called while another coroutine is already waiting for incoming data
        #
        # ooob is out-of-order-buffer, to optionally capture any async messages
        # or sync requests found while waiting for our response, if a context
        # has not been setup (deprecated - use a context)
        f: asyncio.Future[KObj] = asyncio.Future()
        self._completions.append(f)
        self.write(obj, MessageType.SYNC)
        if self._context is None:
            # with a lock...
            while True:
                msgtype, k = await self.reader._read()
                if msgtype == MessageType.RESPONSE:
                    self.on_response(k)
                    break
                elif ooob is not None:
                    ooob(k)
                else:
                    logging.warning(
                        f"{self.qid} recieved {msgtype} whilst awaiting response, no context available."
                    )

        return await f

    def on_response(self, k: KObj) -> None:
        # if this raises IndexError a RESPONSE message has been recieved
        # when we did not make a SYNC request.
        f = self._completions.pop(0)

        # handle KRR through the Future exception system
        if self.reader.raise_krr and k.t == TypeEnum.KRR:
            f.set_exception(KException(k.aS()))
        else:
            f.set_result(k)

    async def async_msg(self, obj: KObj) -> None:
        # this method is a shortcut to avoid having to import MessageType
        self.write(obj, MessageType.ASYNC)

    def close(self) -> None:
        self.writer.close()

    async def wait_closed(self) -> None:
        return await self.writer.wait_closed()


class BaseContext:
    async def on_sync_request(self, cmd: KObj, dotzw: KdbWriter) -> KObj:  # .z.pg
        # kdb clients usually present RPC to server as a string, evaluated
        # with value, although it is possible to have arbitary objects here
        raise Exception("nyi handling")

    async def on_async_message(self, cmd: KObj, dotzw: KdbWriter) -> None:  # .z.ps
        pass

    async def sync_response(self, cmd: KObj, dotzw: KdbWriter) -> None:
        pass

    async def writer_available(self, dotzw: KdbWriter) -> None:
        # .z.po
        pass

    def writer_closing(self, dotzw: KdbWriter) -> None:
        # .z.pc
        pass


class ServerContext(BaseContext):
    # TODO: wrap clients in a class for timers/callbacks?

    def __init__(self, password: Optional[str] = None):
        self.password = password

    def check_login(self, user: str, password: Optional[str]) -> bool:  # .z.pw
        if self.password is None:
            return True
        # Resists timing attacks on comparison but still leaks password length
        if password is None:
            return False
        return hmac.compare_digest(self.password, password)

    async def start_tasks(self) -> None:
        pass


connection_counter = itertools.count()


async def process_login(
    qid: str,
    context: ServerContext,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> Tuple[KdbReader, KdbWriter]:
    data = await reader.readuntil(separator=b"\000")
    # format is user(:password){ver}\000  where ver is an optional single byte
    # values currently in use are 1,2,3 ie. all non-printable ascii.
    # very old client/servers did not send a protocol version
    if len(data) < 2:
        raise Exception(f"{qid} bad login - insufficent bytes before null")
    if data[-2] < 32:
        ver = data[-2]
        auth = data[0:-2].decode()
    else:
        ver = 0
        auth = data[0:-1].decode()
    password: Optional[str] = None
    if ":" in auth:
        user, password = auth.split(":", maxsplit=1)
    else:
        user = auth
    pwstars = "*" * len(password) if password is not None else "None"
    logging.info(f"{qid} process_login ver={ver} user={user} password={pwstars}")

    if not context.check_login(user, password):
        raise CredentialsException("login check failed")
    writer.write(b"\x03")
    await writer.drain()

    q_reader = KdbReader(reader)
    q_writer = KdbWriter(writer, q_reader, version=ver, qid=qid, context=context)
    return q_reader, q_writer


async def reader_to_context_task(
    q_writer: KdbWriter, q_reader: KdbReader, context: BaseContext
) -> None:
    # use a new task for this notification as it might await the completion of
    # a future that we later dispatch via. q_writer.on_response(...)
    task = asyncio.create_task(context.writer_available(q_writer))
    try:
        while not q_writer.writer.is_closing():
            mtype, cmd = await q_reader._read()
            if mtype == MessageType.SYNC:
                logging.info(f"{q_writer.qid} command {cmd}")
                try:
                    q_writer.write(
                        await context.on_sync_request(cmd, q_writer),
                        MessageType.RESPONSE,
                    )
                except asyncio.TimeoutError as e:
                    logging.info("Sync command had timeout, continue")
                    q_writer.write(krr(str(e)), MessageType.RESPONSE)
                except Exception as e:
                    logging.warning(
                        f"sync command {cmd} resulted in exception {repr(e)}",
                        exc_info=True,
                    )
                    q_writer.write(krr(str(e)), MessageType.RESPONSE)

            elif mtype == MessageType.ASYNC:
                try:
                    await context.on_async_message(cmd, q_writer)
                except asyncio.TimeoutError:
                    logging.info("Async command had timeout, continue")
                except Exception as e:
                    logging.warning(
                        f"async command {cmd} resulted in exception {repr(e)}",
                        exc_info=True,
                    )
            elif mtype == MessageType.RESPONSE:
                try:
                    q_writer.on_response(cmd)
                except Exception as e:
                    logging.warning(
                        f"response processing resulted in exception: {repr(e)}",
                        exc_info=True,
                    )
            else:
                raise Exception(f"{q_writer.qid} Unexpected incoming message type")

    except asyncio.exceptions.IncompleteReadError:
        # reader is closed, we have nothing more to send
        q_writer.close()

    finally:
        task.cancel()
        await task

        context.writer_closing(q_writer)


async def handle_connection(
    context: ServerContext, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    qid: str = f"q-{next(connection_counter)}"
    disconnect_log_level = logging.DEBUG
    try:
        logging.debug(f"{qid} new connection")
        q_reader, q_writer = await asyncio.wait_for(
            process_login(qid, context, reader, writer), timeout=10
        )
        disconnect_log_level = logging.INFO
        await reader_to_context_task(q_writer, q_reader, context)
    except asyncio.TimeoutError:
        logging.info(f"{qid} closed - login timeout")
    except asyncio.exceptions.IncompleteReadError:
        # When kdb process timeout via .timer.timeoutSyncCall
        logging.log(disconnect_log_level, f"{qid} connection reached end of stream")
    except BrokenPipeError:
        logging.info(f"{qid} connection ended BrokenPipeError")
    except CredentialsException:
        logging.info(f"{qid} login credentials incorrect, closed")
    finally:
        try:
            # throws RuntimeError in test teardown
            writer.close()
        except RuntimeError:
            pass


async def start_qserver(
    port: int, context: ServerContext, periodic: bool = False
) -> Any:
    logging.info(f"opening KDB-q IPC server on port {port}")
    if periodic:
        await context.start_tasks()

    return await asyncio.start_server(partial(handle_connection, context), "", port)


async def main(qpassword: str, qport: int) -> None:
    context = ServerContext(qpassword)
    server = await start_qserver(qport, context)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    import argparse

    parser = argparse.ArgumentParser()
    QPORT_DEFAULT = os.environ.get("QPORT", 8890)
    QPASSWORD_DEFAULT = os.environ.get("QPASSWORD", None)
    parser.add_argument(
        "--qport",
        default=QPORT_DEFAULT,
        help="port to listen for kdb-q IPC connections",
    )
    parser.add_argument(
        "--qpassword",
        default=QPASSWORD_DEFAULT,
        help="password for kdb-q IPC connections",
    )
    args = parser.parse_args()

    asyncio.run(main(args.qpassword, args.qport))
