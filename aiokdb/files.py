import os
import pathlib
from typing import TypeVar

from aiokdb import KObj, _d9_unpackfrom

PathLike = TypeVar("PathLike", str, pathlib.Path)


def kfromfile(filename: PathLike) -> KObj:
    with open(filename, "rb") as f:
        # theres no length header since files have a size
        rb = f.read()
        if rb[0:2] == b"\xff\x01":
            k, _ = _d9_unpackfrom(rb, 2)
            return k
        elif rb[0:2] == b"\xfd ":
            raise Exception("Unsupported v2 QDB serialisation format")
        else:
            raise Exception(f"Unknown serialisation format {rb[0:2]}")


def ktofile(k: KObj, filename: PathLike) -> None:
    # writing directly in-place is dangerous, and can leave corrupt data if we crash
    # or are sent a signal mid-write. Write to a temporary file and then rename once
    # closed
    temporary_filename = f"{filename}$"
    with open(temporary_filename, "wb") as f:
        f.write(b"\xff\x01")
        f.write(k._databytes())
    os.rename(temporary_filename, filename)
