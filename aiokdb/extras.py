import inspect
import re
import uuid
from typing import cast

from aiokdb import KIntArray, KLongArray, KObj, TypeEnum, kb, kj, kk, kNil, ks, ktn, tn
from aiokdb.client import ClientContext
from aiokdb.server import KdbWriter, ServerContext


# these are python friendly constructors for vector types, incomplete
# not sure if there is a better way
def ktni(t: TypeEnum, *ints: int) -> KObj:
    v = ktn(t)
    if t == TypeEnum.KG:
        v.kG().extend(ints)
    elif t == TypeEnum.KH:
        v.kH().extend(ints)
    elif isinstance(v, KIntArray):
        v.kI().extend(ints)
    elif isinstance(v, KLongArray):
        v.kJ().extend(ints)
    else:
        raise ValueError(f"No int array initialiser for {tn(t)}")
    return v


def ktns(*ss: str) -> KObj:
    v = ktn(TypeEnum.KS)
    for s in ss:
        v.appendS(s)
    return v


def ktnu(*uuids: uuid.UUID) -> KObj:
    v = ktn(TypeEnum.UU)
    v.kU().extend(uuids)
    return v


def ktnb(*bools: bool) -> KObj:
    v = ktn(TypeEnum.KB)
    v.kB().extend(bools)
    return v


def ktnf(t: TypeEnum, *floats: float) -> KObj:
    v = ktn(t)
    if t == TypeEnum.KF:
        v.kF().extend(floats)
    elif t == TypeEnum.KE:
        v.kE().extend(floats)
    else:
        raise ValueError(f"No float array initialiser for {tn(t)}")

    return v


# We assume commands of this pattern "func[arg1;arg2;argN]"
# which we convert to a k-array with symbol as the first argument, and remaining
# arguments (badly) converted to k-atoms. KDBs parser is a work of art, and this is
# admittedly a poor immitation.
def _string_to_functional(cmd: KObj) -> KObj:
    if not isinstance(cmd, KObj):
        raise ValueError("Expected KObj")
    if cmd.t != TypeEnum.KC:
        raise ValueError("Expected char vector input")
    c = cmd.aS()
    result = re.match(r"(.+)\[(.*)\]", c)
    if not result:
        raise ValueError("error parsing: {}".format(cmd))

    functional = [ks(result.group(1))]
    if result.group(2):
        cmd_args = result.group(2).split(";")
        print(cmd_args)
        for arg in cmd_args:
            if len(arg) > 1 and arg[0] == "`":
                # symbol
                functional.append(ks(arg[1:]))
            elif len(arg) == 2 and arg[1] == "b":
                # bool atom
                functional.append(kb(False if arg[0] == "0" else True))
            elif arg.isnumeric():
                functional.append(kj(int(arg)))
            else:
                raise ValueError(f"error parsing arg: {arg} in command {c}")
    else:
        functional.append(kNil)
    return kk(*functional)


# this tries to offer basic eval support for function dispatch within a server
# users expect the input to the server to hit eval, which will parse the arguments to
# kdb types, then dispatch to a function.
class MagicContext:
    async def on_sync_request(self, cmd: KObj, dotzw: KdbWriter) -> KObj:  # .z.pg
        # kdb clients usually present RPC to server as a string, evaluated
        # with value, although it is possible to have arbitary objects here
        if cmd.t == TypeEnum.KC:
            cmd = _string_to_functional(cmd)

        # cmd should now be a k, with symbol as first argument
        fn = cmd.kK()[0].aS()
        # map kx function name (containing periods) to python and do getattr
        # this is behind the login check, so I'm not concerned about RPC
        args = kk(*cmd.kK()[1:])

        fnpy = fn.lstrip(".").replace(".", "__")  # drop initial dot, snake dots
        try:
            f = getattr(self, fnpy)
            k = f(args, dotzw)
            if inspect.isawaitable(k):
                return cast(KObj, await k)
            return cast(KObj, k)
        except AttributeError:
            raise ValueError(f"No python function {fnpy} found from {fn}")


class MagicServerContext(MagicContext, ServerContext):
    pass


class MagicClientContext(MagicContext, ClientContext):
    pass
