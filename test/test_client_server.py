import asyncio
from typing import List, Optional

import pytest

from aiokdb import KException, KObj, MessageType, TypeEnum, cv, kj, kNil
from aiokdb.client import open_qipc_connection
from aiokdb.extras import MagicClientContext, MagicServerContext, _string_to_functional
from aiokdb.server import CredentialsException, KdbWriter, ServerContext, start_qserver


@pytest.mark.asyncio
async def test_client_noauth_defaultport() -> None:
    context = ServerContext()
    server = await start_qserver(8890, context)

    client_rd, client_wr = await open_qipc_connection()

    with pytest.raises(KException, match="nyi handling"):
        await client_wr.sync_req(cv("1+2"))

    client_wr.close()
    await client_wr.wait_closed()

    # check supplied auth ignored when noauth configured
    client_rd, client_wr = await open_qipc_connection(user="troy", password="tango")
    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()


# Please manually verify auth changes against KDB.
#
# Python server, KDB client
#   $ python -m qaio.server
#   $ ~/kdb/q
#   q)hopen ":localhost:8890"           INFO:root:q-0 process_login ver=6 user=chrisshucksmith password=None
#   q)hopen ":localhost:8890:blah"      INFO:root:q-1 process_login ver=6 user=blah password=None
#   q)hopen ":localhost:8890:blah:boo"  INFO:root:q-2 process_login ver=6 user=blah password=***
#
# Python client, KDB server
#   $ ~/kdb/q -p 8890
#   q).z.pw:{[user;pswd]1 "user: ",(string user)," password: ",pswd,"\n";1b}
#   $ python -m qaio.client                          q) user: user password:
#   $ python -m qaio.client --user u1                q) user: u1 password:
#   $ python -m qaio.client --user u1 --password p2  q) user: u1 password: p2


@pytest.mark.asyncio
async def test_client_auth_defined_port() -> None:
    # echo back client request
    class EchoServerContext(ServerContext):
        async def on_sync_request(self, cmd: KObj, dotzw: KdbWriter) -> KObj:
            return cmd

    context = EchoServerContext("tangoxray")
    server = await start_qserver(6778, context)

    # check correct auth succeeds
    client_rd, client_wr = await open_qipc_connection(
        port=6778, user="troy", password="tangoxray"
    )
    kr = await client_wr.sync_req(cv("1+2"))
    assert kr.aS() == "1+2"

    client_wr.close()
    await client_wr.wait_closed()

    # check wrong auth fails
    with pytest.raises(CredentialsException):
        client_rd, client_wr = await open_qipc_connection(
            port=6778, user="troy", password="xyz"
        )

    # hopen can be sent without a password
    with pytest.raises(CredentialsException):
        client_rd, client_wr = await open_qipc_connection(
            port=6778, user="xyz", password=None
        )

    # hopen can be sent with empty username (encodes as "")
    with pytest.raises(CredentialsException):
        client_rd, client_wr = await open_qipc_connection(
            port=6778, user=None, password=None
        )

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_client_auth_uri() -> None:
    context = ServerContext("tango")
    server = await start_qserver(6779, context)

    # can connect by url
    client_rd, client_wr = await open_qipc_connection(
        uri="kdb://qq:tango@localhost:6779"
    )

    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_server_async() -> None:
    class AsyncServerContext(ServerContext):
        async def on_sync_request(self, cmd: KObj, dotzw: KdbWriter) -> KObj:
            # async message sent during sync request handling
            dotzw.write(kj(1), MessageType.ASYNC)
            # return value becomes the sync response
            return kj(2)

    context = AsyncServerContext("tangoxray")
    server = await start_qserver(6778, context)

    client_rd, client_wr = await open_qipc_connection(
        port=6778, user="troy", password="tangoxray"
    )

    ooob: List[KObj] = []
    kr = await client_wr.sync_req(cv("1+2"), ooob=ooob.append)
    assert kr.aJ() == 2

    assert len(ooob) == 1
    assert ooob[0].aJ() == 1

    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()


def test_extras_parse_commands() -> None:
    with pytest.raises(ValueError):
        _string_to_functional(kj(4))

    with pytest.raises(ValueError):
        _string_to_functional("bare string not KObj")  # type: ignore

    c = cv("blah[3;45;`silly;0b]")
    k = _string_to_functional(c)

    assert len(k) == 5
    assert k.t == TypeEnum.K
    bits = k.kK()
    assert bits[0].aS() == "blah"
    assert bits[1].aJ() == 3
    assert bits[2].aJ() == 45
    assert bits[3].aS() == "silly"
    assert bits[4].aB() is False

    # KDB doesn't have no argument functions, they are passed [::]
    k = _string_to_functional(cv("regular.function[]"))
    assert len(k) == 2
    assert k.t == TypeEnum.K
    bits = k.kK()
    assert bits[0].aS() == "regular.function"
    assert bits[1] is kNil


@pytest.mark.asyncio
async def test_extras_MagicServerContext() -> None:
    # defining the function within a MagicServerContext is enough to make it callable
    class MyContext(MagicServerContext):
        async def mynamespace__myfunction(self, args: KObj, dotzw: KdbWriter) -> KObj:
            return kj(args.kK()[0].aJ())

        def regular__function(self, args: KObj, dotzw: KdbWriter) -> KObj:
            return kj(34)

    server = await start_qserver(6778, MyContext())
    client_rd, client_wr = await open_qipc_connection(port=6778)

    kr = await client_wr.sync_req(cv(".mynamespace.myfunction[45]"))
    assert kr.aJ() == 45

    kr = await client_wr.sync_req(cv("regular.function[]"))
    assert kr.aJ() == 34

    with pytest.raises(
        KException,
        match="No python function mynamespace__notafunction found from .mynamespace.notafunction",
    ):
        await client_wr.sync_req(cv(".mynamespace.notafunction[45]"))

    assert (await client_wr.sync_req(cv("regular.function[]"))).aJ() == 34

    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_server_calls_client() -> None:
    class TestServerContext(MagicServerContext):
        stored_handle: Optional[KdbWriter] = None

        async def storehandle(self, args: KObj, dotzw: KdbWriter) -> KObj:
            self.stored_handle = dotzw
            return kNil

        async def checkafter(self, args: KObj, dotzw: KdbWriter) -> KObj:
            return kj(32)

    server = await start_qserver(6778, tsc := TestServerContext())

    class TestClientContext(MagicClientContext):
        async def dosums(self, args: KObj, dotzw: KdbWriter) -> KObj:
            return kj(42)

    client_rd, client_wr = await open_qipc_connection(
        port=6778, user="troy", password="tangoxray", context=TestClientContext()
    )

    assert tsc.stored_handle is None
    kr = await client_wr.sync_req(cv("storehandle[]"))
    assert kr == kNil
    assert tsc.stored_handle is not None

    # call client method from the server
    assert (await tsc.stored_handle.sync_req(cv("dosums[]"))).aJ() == 42

    assert (await client_wr.sync_req(cv("checkafter[]"))).aJ() == 32

    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_client_speaks_first() -> None:
    class BannerServerContext(MagicServerContext):
        async def banner(self, args: KObj, dotzw: KdbWriter) -> KObj:
            return kj(32)

    server = await start_qserver(6778, BannerServerContext())

    fut = asyncio.get_running_loop().create_future()

    class TestClientContext(MagicClientContext):
        async def writer_available(self, dotzw: KdbWriter) -> None:
            self.writer = dotzw
            print("connected - sending subscribe")
            try:
                r = await dotzw.sync_req(cv("banner[]"))
                print(f"recieved banner {r.aJ()}")
                fut.set_result(r)
            except Exception:
                print("banner login rejected")

    client_rd, client_wr = await open_qipc_connection(
        port=6778, user="troy", password="tangoxray", context=TestClientContext()
    )

    r = await fut
    assert r.aJ() == 32

    client_wr.close()
    await client_wr.wait_closed()

    server.close()
    await server.wait_closed()
