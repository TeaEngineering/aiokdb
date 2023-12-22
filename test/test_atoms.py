from aiokdb import b9, ki, TypeEnum, ktn


def h2b(hx: str) -> bytes:
    return bytes.fromhex(hx[2:])

def test_atoms_getset():
    assert ki(1).kI() == 1


def test_atoms_b9():
    assert b9(ki(1)) == h2b("0x010000000d000000fa01000000")
    assert b9(ki(-1)) == h2b("0x010000000d000000faffffffff")

def test_vector_b9():
    k = ktn(TypeEnum.KI, 1)
    k.j[0] = 1
    assert b9(k) == h2b("0x010000001200000006000100000001000000")

    k = ktn(TypeEnum.KG)
    k.g.extend(range(5))
    assert b9(k) == h2b('0x01000000130000000400050000000001020304')

