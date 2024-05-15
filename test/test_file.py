from pathlib import Path

from aiokdb import TypeEnum, kj, kk, ks
from aiokdb.files import kfromfile, ktofile


def test_qdb(tmp_path: Path) -> None:
    p = kk(kj(5), ks("hello"))
    filename = tmp_path / "test.qdb"

    ktofile(p, filename)

    k = kfromfile(filename)
    assert k.t == TypeEnum.K
    assert k.kK()[0].aJ() == 5
    assert k.kK()[1].aS() == "hello"
