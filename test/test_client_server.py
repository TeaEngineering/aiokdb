from typing import List

import pytest

from aiokdb import KException, KObj, MessageType, cv, kj
from aiokdb.client import open_qipc_connection
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
