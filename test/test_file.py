from pathlib import Path

from aiokdb import TypeEnum, fromfile, kj, kk, ks, writefile


def test_qdb(tmp_path: Path) -> None:
    p = kk(kj(5), ks("hello"))
    d = tmp_path / "test.qdb"

    with open(d, "wb") as f:
        writefile(p, f)

    with open(d, "rb") as f:
        k = fromfile(f)
        assert k.t == TypeEnum.K
        assert k.kK()[0].aJ() == 5
        assert k.kK()[1].aS() == "hello"
