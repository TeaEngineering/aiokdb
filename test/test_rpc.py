import os
from urllib.parse import urlparse

import pytest

from aiokdb import Infs, KObj, Nulls, TypeEnum, cv
from aiokdb.client import open_qipc_connection
from aiokdb.format import AsciiFormatter

# test requires working KDB's eval in the server process, so can't use our python shim


@pytest.mark.asyncio
async def test_rpc() -> None:
    server_uri = os.environ.get("KDB_PYTEST_SERVICE", None)

    if not server_uri:
        pytest.skip("No KDB_PYTEST_SERVICE provided")
        return

    o = urlparse(server_uri)
    h = o.hostname
    p = o.port
    if h is None or p is None:
        raise Exception("url must provide kdb://hostname:port")
    krd, kwr = await open_qipc_connection(
        host=h, port=p, user=o.username, password=o.password
    )

    fmt = AsciiFormatter()

    async def check_result(cmd: str, t: int) -> KObj:
        k = await kwr.sync_req(cv(cmd))
        assert k.t == t
        fmt.format(k)
        return k

    # atoms
    await check_result("0b", -TypeEnum.KB)
    await check_result("0Ng", -TypeEnum.UU)
    await check_result("0x00", -TypeEnum.KG)
    await check_result("0h", -TypeEnum.KH)
    await check_result("0i", -TypeEnum.KI)
    await check_result("0j", -TypeEnum.KJ)
    await check_result("0e", -TypeEnum.KE)
    await check_result("0.0", -TypeEnum.KF)
    await check_result('"x"', -TypeEnum.KC)
    await check_result("`", -TypeEnum.KS)
    await check_result("2000.01.01D00:00:00.000000000", -TypeEnum.KP)
    await check_result("2000.01m", -TypeEnum.KM)
    await check_result("1984.01.25", -TypeEnum.KD)
    # deprecated floating point
    await check_result("2000.01.01T00:00:00.000", -TypeEnum.KZ)
    await check_result("00:00:00.000000000", -TypeEnum.KN)
    await check_result("23:59", -TypeEnum.KU)
    await check_result("23:59:00", -TypeEnum.KV)
    await check_result("23:59:00.000", -TypeEnum.KT)

    # vectors
    await check_result("enlist 0b", TypeEnum.KB)
    await check_result("enlist 0Ng", TypeEnum.UU)
    await check_result("enlist 0x00", TypeEnum.KG)
    await check_result("enlist 0h", TypeEnum.KH)
    await check_result("enlist 0i", TypeEnum.KI)
    await check_result("enlist 0j", TypeEnum.KJ)
    await check_result("enlist 0e", TypeEnum.KE)
    await check_result("enlist 0.0", TypeEnum.KF)
    await check_result('enlist "x"', TypeEnum.KC)
    await check_result("enlist `", TypeEnum.KS)
    await check_result("enlist 2000.01.01D00:00:00.000", TypeEnum.KP)
    await check_result("enlist 2000.01m", TypeEnum.KM)
    await check_result("enlist 1984.01.25", TypeEnum.KD)
    await check_result("enlist 2000.01.01T00:00:00.000", TypeEnum.KZ)
    await check_result("enlist 00:00:00.000000000", TypeEnum.KN)
    await check_result("enlist 23:59", TypeEnum.KU)
    await check_result("enlist 23:59:00", TypeEnum.KV)
    await check_result("enlist 23:59:00.000", TypeEnum.KT)

    await check_result("`a`b`c!(1 2;3 5;7 11)", TypeEnum.XD)
    await check_result("flip `a`b`c!(1 2;3 5;7 11)", TypeEnum.XT)
    await check_result("`s#4 5 6!6 5 4", TypeEnum.SD)

    # fancy
    await check_result("::", TypeEnum.NIL)
    await check_result("", TypeEnum.NIL)

    # await check_result(('{x+y}', TypeEnum.FN
    # https://code.kx.com/q/basics/datatypes/#functions-iterators-derived-functions
    # q)type each({x+y};neg;-;\;+[;1];<>;,';+/;+\;prev;+/:;+\:;`f 2:`f,1)
    # 100 101 102 103 104 105 106 107 108 109 110 111 112h

    # nulls
    assert (await check_result("0Nh", -TypeEnum.KH)).aH() == Nulls.h
    # await check_result(("0Ng"))).aU() == ku(Nulls.u)
    assert (await check_result("0Ni", -TypeEnum.KI)).aI() == Nulls.i
    assert (await check_result("0Nj", -TypeEnum.KJ)).aJ() == Nulls.j
    # await check_result(("0Ne"))).aE() == Nulls.e
    # await check_result(("0Nf"))).aF() == Nulls.f

    # infs
    assert (await check_result("0Wh", -TypeEnum.KH)).aH() == Infs.h
    assert (await check_result("0Wi", -TypeEnum.KI)).aI() == Infs.i
    assert (await check_result("0Wj", -TypeEnum.KJ)).aJ() == Infs.j
    assert (await check_result("0We", -TypeEnum.KE)).aE() == Infs.e
    assert (await check_result("0w", -TypeEnum.KF)).aF() == Infs.f

    await check_result(
        's:(0b;0Ng;0x00;0h;0i;0j;0e;0.0;"x";`;2000.01.01D;2023.01m;2023.12.23;2000.01.01T;00:00:00.000000000;23:59;23:59:00;23:59:00.000);([]t:type each s;atom:s;vect:enlist each s)',
        TypeEnum.XT,
    )
