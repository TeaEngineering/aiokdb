import argparse
import asyncio
import logging
import os
import traceback
from typing import Any, Optional

from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import PromptSession

from aiokdb import TypeEnum, cv
from aiokdb.client import (
    ClientContext,
    KdbWriter,
    maintain_qipc_connection,
)
from aiokdb.format import AsciiFormatter


class CliClientContext(ClientContext):
    def __init__(self) -> None:
        self.writer: Optional[KdbWriter] = None

    async def writer_available(self, dotzw: KdbWriter) -> None:
        self.writer = dotzw

    def writer_closing(self, dotzw: KdbWriter) -> None:
        self.writer = None


async def main(args: Any) -> None:
    history = FileHistory(os.path.expanduser("~/.aiokdb-cli-history"))
    session: Any = PromptSession("(eval) > ", history=history)
    fmt = AsciiFormatter(height=args.height)

    password = args.password
    if password is None:
        password = await session.prompt_async("Password:", is_password=True)

    cc = CliClientContext()

    task = asyncio.create_task(
        maintain_qipc_connection(
            uri=f"kdb://{args.user}:{password}@{args.host}:{args.port}", context=cc
        )
    )

    # Run echo loop. Read text from stdin, and reply it back.
    while True:
        try:
            inp = await session.prompt_async("q)", is_password=False)
            if inp == "":
                continue
            if not cc.writer:
                print("Writer not connected, wait for re-connect")
            else:
                output = await cc.writer.sync_req(cv(inp))
                if output.t != TypeEnum.NIL:
                    print(fmt.format(output))
        except KeyboardInterrupt:
            return
        except EOFError:
            break
        except Exception:
            traceback.print_exc(limit=-2)

    task.cancel()
    await task

    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=8890, type=int)
    parser.add_argument("--user", default="user")
    parser.add_argument("--password")
    parser.add_argument("--height", default=10, type=int)
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()
    debug_level = {True: logging.DEBUG, False: logging.INFO}
    logging.basicConfig(level=debug_level[args.debug])
    with patch_stdout():
        asyncio.run(main(args))
