import os
from urllib.parse import urlparse

import pytest

from aiokdb import TypeEnum, cv
from aiokdb.client import open_qipc_connection

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

    # atoms
    assert (await kwr.sync_req(cv("0b"))).t == -TypeEnum.KB
    assert (await kwr.sync_req(cv("0Ng"))).t == -TypeEnum.UU
    assert (await kwr.sync_req(cv("0x00"))).t == -TypeEnum.KG
    assert (await kwr.sync_req(cv("0h"))).t == -TypeEnum.KH
    assert (await kwr.sync_req(cv("0i"))).t == -TypeEnum.KI
    assert (await kwr.sync_req(cv("0j"))).t == -TypeEnum.KJ
    assert (await kwr.sync_req(cv("0e"))).t == -TypeEnum.KE
    assert (await kwr.sync_req(cv("0.0"))).t == -TypeEnum.KF
    assert (await kwr.sync_req(cv('"x"'))).t == -TypeEnum.KC
    assert (await kwr.sync_req(cv("`"))).t == -TypeEnum.KS
    assert (await kwr.sync_req(cv("2000.01.01D00:00:00.000000000"))).t == -TypeEnum.KP
    assert (await kwr.sync_req(cv("2000.01m"))).t == -TypeEnum.KM
    assert (await kwr.sync_req(cv("1984.01.25"))).t == -TypeEnum.KD
    # deprecated floating point
    assert (await kwr.sync_req(cv("2000.01.01T00:00:00.000"))).t == -TypeEnum.KZ
    assert (await kwr.sync_req(cv("00:00:00.000000000"))).t == -TypeEnum.KN
    assert (await kwr.sync_req(cv("23:59"))).t == -TypeEnum.KU
    assert (await kwr.sync_req(cv("23:59:00"))).t == -TypeEnum.KV
    assert (await kwr.sync_req(cv("23:59:00.000"))).t == -TypeEnum.KT

    # vectors
    assert (await kwr.sync_req(cv("enlist 0b"))).t == TypeEnum.KB
    assert (await kwr.sync_req(cv("enlist 0Ng"))).t == TypeEnum.UU
    assert (await kwr.sync_req(cv("enlist 0x00"))).t == TypeEnum.KG
    assert (await kwr.sync_req(cv("enlist 0h"))).t == TypeEnum.KH
    assert (await kwr.sync_req(cv("enlist 0i"))).t == TypeEnum.KI
    assert (await kwr.sync_req(cv("enlist 0j"))).t == TypeEnum.KJ
    assert (await kwr.sync_req(cv("enlist 0e"))).t == TypeEnum.KE
    assert (await kwr.sync_req(cv("enlist 0.0"))).t == TypeEnum.KF
    assert (await kwr.sync_req(cv('enlist "x"'))).t == TypeEnum.KC
    assert (await kwr.sync_req(cv("enlist `"))).t == TypeEnum.KS
    assert (await kwr.sync_req(cv("enlist 2000.01.01D00:00:00.000"))).t == TypeEnum.KP
    assert (await kwr.sync_req(cv("enlist 2000.01m"))).t == TypeEnum.KM
    assert (await kwr.sync_req(cv("enlist 1984.01.25"))).t == TypeEnum.KD
    assert (await kwr.sync_req(cv("enlist 2000.01.01T00:00:00.000"))).t == TypeEnum.KZ
    assert (await kwr.sync_req(cv("enlist 00:00:00.000000000"))).t == TypeEnum.KN
    assert (await kwr.sync_req(cv("enlist 23:59"))).t == TypeEnum.KU
    assert (await kwr.sync_req(cv("enlist 23:59:00"))).t == TypeEnum.KV
    assert (await kwr.sync_req(cv("enlist 23:59:00.000"))).t == TypeEnum.KT

    assert (await kwr.sync_req(cv("`a`b`c!(1 2;3 5;7 11)"))).t == TypeEnum.XD
    assert (await kwr.sync_req(cv("flip `a`b`c!(1 2;3 5;7 11)"))).t == TypeEnum.XT

    # fancy
    assert (await kwr.sync_req(cv("::"))).t == TypeEnum.NIL
    assert (await kwr.sync_req(cv(""))).t == TypeEnum.NIL

    # assert (await kwr.sync_req(cv('{x+y}'))).t == TypeEnum.FN
    # https://code.kx.com/q/basics/datatypes/#functions-iterators-derived-functions
    # q)type each({x+y};neg;-;\;+[;1];<>;,';+/;+\;prev;+/:;+\:;`f 2:`f,1)
    # 100 101 102 103 104 105 106 107 108 109 110 111 112h
