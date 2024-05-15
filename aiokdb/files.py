import os
import pathlib
from typing import TypeVar

from aiokdb import KObj, _d9_unpackfrom

PathLike = TypeVar("PathLike", str, pathlib.Path)


def kfromfile(filename: PathLike) -> KObj:
    with open(filename, "rb") as f:
        # theres no length header since files have a size
        rb = f.read()
        assert rb[0:2] == b"\xff\x01"
        k, _ = _d9_unpackfrom(rb, 2)
        return k


def ktofile(k: KObj, filename: PathLike) -> None:
    # writing directly in-place is dangerous, and can leave corrupt data if we crash
    # or are sent a signal mid-write. Write to a temporary file and then rename once
    # closed
    temporary_filename = f"{filename}$"
    with open(temporary_filename, "wb") as f:
        f.write(b"\xff\x01")
        f.write(k._databytes())
    os.rename(temporary_filename, filename)
