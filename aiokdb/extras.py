from aiokdb import KIntArray, KLongArray, KObj, TypeEnum, ktn, tn

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


def ktns(t: TypeEnum, *ss: str) -> KObj:
    v = ktn(t)
    if t == TypeEnum.KS:
        for s in ss:
            v.appendS(s)
    else:
        raise ValueError(f"No str array initialiser for {tn(t)}")
    return v
