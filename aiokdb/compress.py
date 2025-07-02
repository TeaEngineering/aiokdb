import argparse
import logging
import random
import struct
import sys
from typing import Optional

logger = logging.getLogger(__name__)


def decompress(data: bytes) -> bytes:
    # decompression based on c.java
    # https://github.com/KxSystems/javakdb/blob/master/javakdb/src/main/java/com/kx/c.java#L667
    # which in turn seems quite similar to LZRW1 http://www.ross.net/compression/lzrw1.html
    # however with a much smaller window for the hash function (2 bytes) and only 256 slots

    uncomp_sz = struct.unpack("I", data[0:4])[0] - 8
    logger.debug(f"decompressing: {len(data)} compressed bytes to {uncomp_sz} bytes")

    # compressed stream repeats: control byte (8 single-bit instructions), followed by 8-16 data bytes.
    # Bits of the control byte are examined bit-by-bit from least significant:
    #   control bit clear: copy one literal byte to output.
    #   control bit set: read 2 bytes [index, sz]. Copy from hashed history table entry [index] for sz bytes.
    # After each operation, update the hashpos table provided we have at least 2 bytes output, and using no more
    # than two bytes from the history copy

    dst = bytearray(uncomp_sz)
    hashpos = [0] * 256
    d = 4  # position index into compressed data[]
    s = 0  # write position in dst[] (uncompressed output)
    p = 0  # hash  position in dst[] for hashpos updates
    f = 0  # current instruction byte, examined bit by bit
    i = 0  # bit position within f as a 1-hot value, ie. 1,2,4,8,16,32,64,128

    while s < uncomp_sz:
        if i == 0:
            # out of instruction bits! reload f, reset i, advance d
            # print(f"loading next 8 instructions from d={d}")
            f = data[d]
            d += 1
            i = 1

        # check bit i of f
        if f & i:  # bit set: copy history range
            # {ptr}{sz} copy n bytes (sz+2) from uncompressed history pointer r (ptr)
            r = hashpos[data[d]]
            n = data[d + 1]
            # print(f"instr {i} hist d={d} slot {data[d]}, copy pos={r} len={2+n} to {s}")
            # DO NOT USE SLICE ASSIGNMENT HERE, AS A HISTORY REFERENCE CAN COPY OWN OUTPUT
            # ie. start 8 bytes back and copy 64 bytes, gives 8x repeating 8 bytes
            for m in range(2 + n):
                dst[s + m] = dst[r + m]
            d += 2
            s += 2

        else:  # copy 1 byte from compressed stream to uncomp
            # print(f"instr {i} literal d={d} value {data[d:d+1].hex()} to pos {s}")
            dst[s] = data[d]
            n = 0
            d += 1
            s += 1

        while p < s - 1:
            hashv = dst[p] ^ dst[p + 1]
            # print(f" hash slot {hashv} set to p={p}")
            hashpos[hashv] = p
            p += 1

        # only first two bytes of a copied range are used to update the hash
        # jump s and p over the remaining copied range (n)
        s += n
        if f & i:
            p = s

        # next control bit
        i = (i << 1) & 255

    # Note that cpython is overly restrictive in the uuid.UUID(bytes=...) call and will not
    # allow a bytearray, so now decompression is done convert it once to immutable bytes object.
    #  https://github.com/python/cpython/blob/8edfa0b0b4ae4235bb3262d952c23e7581516d4f/Lib/uuid.py#L188
    return bytes(dst)


def compress(y: bytes) -> Optional[bytes]:
    # if data is not worth compressing return None, else compressed payload
    if len(y) < 8:
        return None

    wr = bytearray(len(y) // 2)

    i = 128  # bit position within f as a 1-hot value, ie. 1,2,4,8,16,32,64,128
    f = 0  # current flags
    h0 = 0
    h = 0
    p = 0
    s0 = 0
    s = 0  # read position in uncompressed y
    t = len(y)
    a = [0] * 256  # hash buckets

    struct.pack_into("I", wr, 0, len(y) + 8)
    d = 4  # write position into wr
    c = 4  # last flags position into wr (sentinal first loop, written to twice)

    while s < t:
        i *= 2
        if i == 256:
            # print(f"flushing flags wBuf[{c}]={f} writepos={d}")
            if d > len(wr) - 17:  # 8x2 data + 1 flags
                logger.debug(
                    f"compressor: {len(y)} bytes not worth compressing (writepos {d})"
                )
                return None
            i = 1
            wr[c] = f  # write back flags
            c = d
            d += 1
            f = 0

        # h0, s0 are hash and position writeback?
        # h current hash
        if s > t - 3:
            literal = True
        else:
            h = y[s] ^ y[s + 1]
            p = a[h]
            # quirk: it doesn't check y[s+1]==y[p+1]. Since h is over two bytes,
            # if hash matches and y[s]==y[p], it cannot be a collision
            literal = (0 == p) or (y[s] != y[p])

        # print(f"operation literal={literal} writepos={d} readpos={s}")
        if 0 < s0:
            a[h0] = s0
            s0 = 0

        if literal:
            # write back the (possibly updated) backref for this hash
            h0 = h
            s0 = s
            # cp the literal. f flag already clear
            wr[d] = y[s]
            d += 1
            s += 1
        else:
            a[h] = s
            f |= i
            p += 2
            s += 2
            # test match length
            r = s
            q = min(s + 255, t) - 1
            # print(f"flags now {f} maxlen {q}")
            while y[p] == y[s] and s < q:
                p += 1
                s += 1
            # print(f'  runlength {s-r}')
            wr[d + 0] = h
            wr[d + 1] = s - r
            d += 2

    wr[c] = f  # flush flags
    logger.debug(f"compressor: {len(y)} uncompressed bytes down to {d} bytes")
    return bytes(wr[0:d])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()
    debug_level = {True: logging.DEBUG, False: logging.INFO}
    logging.basicConfig(level=debug_level[args.debug])

    while True:
        sz = random.randrange(1, 10000)
        spots = random.randrange(0, sz)  # patch in some non-zeros
        data = bytearray(sz)
        for i in range(spots):
            data[random.randrange(1, sz)] = random.randrange(0, 255)

        bs = compress(bytes(data))
        if bs is not None:
            ds = decompress(bs)
            if not ds == data:
                print("mismatch")
                print(data)
                print(bs)
                print(ds)
                sys.exit(-1)
