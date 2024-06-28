from aiokdb import d9


# (eval) >  500#0j
# DEBUG:aiokdb:< sending b'\x01\x01\x00\x00\x14\x00\x00\x00\n\x00\x06\x00\x00\x00500#0j'
# DEBUG:aiokdb:> recv ver=1 msgtype=2 flags=1 msglen=53
# DEBUG:aiokdb:decompressing payload ae0f0000c00700f401000000ff00ffff00ff00ff00ff00ff00ff00ff00ff00ff3f00ff00ff00ff00ff00ff008f
# ValueError: compressed payload NYI {flags}
def test_decompress() -> None:
    data = bytes.fromhex(
        "0102010035000000ae0f0000c00700f401000000ff00ffff00ff00ff00ff00ff00ff00ff00ff00ff3f00ff00ff00ff00ff00ff008f"
    )
    k = d9(data)
    assert len(k) == 500
    for i in k.kJ():
        assert i == 0

    # 500#12848484j
    data = bytes.fromhex(
        "0102010052000000ae0f0000000700f4010000640dfec40000000169ffc9ffc4ff00010000ff69ffc9ffc4ff0000000169ffc9ffc4ffff0001000069ffc9ffc4ff0000000169ff1fc9ffc4ff00010000696e"
    )
    k = d9(data)
    assert len(k) == 500
    for i in k.kJ():
        assert i == 12848484

    # (200#12848484j),300#3456
    data = bytes.fromhex(
        "010201005f000000ae0f0000000700f4010000640dfec40000000169ffc9ffc4ff000100003f69ffc9ffc4ff000000016926800dff000100018dff0dff000100018dff0dffff000100018dff0dff000100018dff0dff0f000100018dff0d35"
    )
    k = d9(data)
    assert len(k) == 500
    for i, j in enumerate(k.kJ()):
        if i < 200:
            assert j == 12848484
        else:
            assert j == 3456
