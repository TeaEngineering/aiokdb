import argparse
import asyncio
import logging
import os
import traceback
from typing import Any

from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import PromptSession

from aiokdb import TypeEnum, cv
from aiokdb.client import open_qipc_connection
from aiokdb.format import AsciiFormatter


async def main(args: Any) -> None:
    r, w = await open_qipc_connection(
        host=args.host, port=args.port, user=args.user, password=args.password
    )

    history = FileHistory(os.path.expanduser("~/.aiokdb-cli-history"))
    session: Any = PromptSession("(eval) > ", history=history)
    fmt = AsciiFormatter(height=args.height)

    # Run echo loop. Read text from stdin, and reply it back.
    while True:
        try:
            inp = await session.prompt_async()
            if inp == "":
                continue
            output = await w.sync_req(cv(inp))
            if output.t != TypeEnum.NIL:
                print(fmt.format(output))
        except KeyboardInterrupt:
            return
        except EOFError:
            break
        except Exception:
            traceback.print_exc(limit=-2)
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
