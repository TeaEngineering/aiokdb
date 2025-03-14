![Python versions](https://img.shields.io/pypi/pyversions/aiokdb.svg) ![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/TeaEngineering/aiokdb/check.yml) [![PyPI version](https://badge.fury.io/py/aiokdb.svg)](https://badge.fury.io/py/aiokdb)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb?ref=badge_shield)

# aiokdb
Python asyncio connector to KDB. Pure python, so does not depend on the `k.h` bindings or kdb shared objects, or numpy/pandas. Fully type hinted to comply with `PEP-561`. No non-core dependencies, and tested on Python 3.8 - 3.12.

## Peer review & motivation

[qPython](https://github.com/exxeleron/qPython) is a widely used library for this task and it maps objects to Pandas Dataframes which might be more suitable for the majority of applications.

This library takes a different approach and aims to replicate using the KDB C-library functions, ie. being 100% explicit about KDB types. It was built working from the publicly documented [Serialisation Examples](https://code.kx.com/q/kb/serialization/) and [C API for kdb+](https://code.kx.com/q/wp/capi/) pages. Users might also need to be familiar with [k.h](https://github.com/KxSystems/ffi/blob/master/include/k.h).

A simple example, using blocking sockets:

```python
# run ./q -p 12345 &

from aiokdb.socket import khpu

h = khpu("localhost", 12345, "kdb:pass")

# if remote returns Exception, it is raised here, unless khpu(..., raise_krr=False)
result = h.k("2.0+3.0")

assert result.aF() == 5.0

result.aJ() # raises ValueError: wrong type KF (-9) for aJ
````

The `result` object is a K-like Python object (a `KObj`), having the usual signed integer type available as `result.type`. Accessors for the primitive types are prefixed with an `a` and check at runtime that the accessor is appropriate for the stored type (`.aI()`, `.aJ()`, `.aH()`, `.aF()` etc.). Atoms store their value to a `bytes` object irrespective of the type, and encode/decode on demand. Atomic values can be set with (`.i(3)`, `.j(12)`, `.ss("hello")`).

Arrays are implemented with subtypes that use [Python's native arrays module](https://docs.python.org/3/library/array.html) for efficient array types. The `MutableSequence` arrays are returned using the usual array accessor functions `.kI()`, `.kB()`, `.kS()` etc.

```
 kdb type  name       python    python    python       python python                     implementation
  n   c               TypeEnum  accessor  setter       create type
---------------------------------------------------------------------------------------------------------------
 -19  t    time       -KT       -         -            -      -                           KObjAtom
 -18  v    second     -KV       -         -            -      -                           KObjAtom
 -17  u    minute     -KU       -         -            -      -                           KObjAtom
 -16  n    timespan   -KN       -         -            -      -                           KObjAtom
 -15  z    datetime   -KZ       -         -            -      -                           KObjAtom
 -14  d    date       -KD       -         -            -      -                           KObjAtom
 -13  m    month      -KM       -         -            -      -                           KObjAtom
 -12  p    timestamp  -KP       -         -            -      -                           KObjAtom
 -11  s    symbol     -KS       .aS()     .ss("sym")   ks()   str                         KSymAtom
 -10  c    char       -KC       .aC()     .c("c")      kc()   str (len 1)                 KObjAtom
  -9  f    float      -KF       .aF()     .f(6.1)      kf()   float                       KObjAtom
  -8  e    real       -KE       .aE()     .f(6.2)      ke()   float                       KObjAtom
  -7  j    long       -KJ       .aJ()     .j(7)        kj()   int                         KObjAtom
  -6  i    int        -KI       .aI()     .i(6)        ki()   int                         KObjAtom
  -5  h    short      -KH       .aH()     .h(5)        kh()   int                         KObjAtom
  -4  x    byte       -KG       .aG()     .g(4)        kg()   int                         KObjAtom
  -2  g    guid       -UU       .aU()     .uu(UUID())  kuu()  uuid.UUID                   KObjAtom
  -1  b    boolean    -KB       .aB()     .b(True)     kb()   bool                        KObjAtom
   0  *    list        K        .kK()     -            kk()   MutableSequence[KObj]       KObjArray
   1  b    boolean     KB       .kB()     -            ktnb() MutableSequence[bool]       KByteArray
   2  g    guid        UU       .kU()     -            ktnu() MutableSequence[uuid.UUID]  KUUIDArray
   4  x    byte        KG       .kG()     -            ktni() MutableSequence[int]        KByteArray
   5  h    short       KH       .kH()     -            ktni() MutableSequence[int]        KShortArray
   6  i    int         KI       .kI()     -            ktni() MutableSequence[int]        KIntArray
   7  j    long        KJ       .kJ()     -            ktni() MutableSequence[int]        KLongArray
   8  e    real        KE       .kE()     -            -      MutableSequence[float]      KFloatArray
   9  f    float       KF       .kF()     -            ktnf() MutableSequence[float]      KDoubleArray
  10  c    char        KC       .kC()     -            cv()   array.array                 KCharArray
  11  s    symbol      KS       .kS()     -            ktns() Sequence[str]               KIntSymArray
  12  p    timestamp   KP       -         -            -      -
  13  m    month       KM       -         -            -      -
  14  d    date        KD       -         -            -      -
  15  z    datetime    KZ       -         -            -      -
  16  n    timespan    KN       -         -            -      -
  17  u    minute      KU       -         -            -      -
  18  v    second      KV       -         -            -      -
  19  t    time        KT       -         -            -      -
  98       flip        XT       .kkey(), .kvalue()     xt()   KObj, KObj                  KFlip
  99       dict        XD       .kkey(), .kvalue()     xd()   KObj, KObj                  KDict
 100       function    FN       -         -            -      -
 101  ::   nil        NIL       -         -            kNil   -
 127       `s#dict     SD       .kkey(), .kvalue()     -      KObj, KObj                  KDict
-128  '    err        KRR       .aS()     .ss()        krr()  str                         KSymAtom
```

Serialisation is handled by the `b9` function, which encodes a `KObj` to a python `bytes`, and the `d9` function which takes a `bytes` and returns a `KObj`.

* Atoms are created by `ka`, `kb`, `ku`, `kg`, `kh`, `ki`, `kj`, `ke`, `kf`, `kc`, `ks`, `kt`, `kd`, `kz`, `ktj`
* Lists with `ktn` and `knk`
* Dictionaries with `xd` and tables with `xt`.

Python manages garbage collection, so none of the reference counting primitives exist, i.e. `k.r` and functions `r1`, `r0` and `m9`, `setm`.

## Asyncio

Both kdb client and server *protocols* are implemented using asyncio, and can be tested back-to-back.
For instance running `python -m aiokdb.server` and then `python -m aiokdb.client` will connect together using KDB IPC. However since there is no _interpreter_ (and the default server does not handle any commands) the server will return an `nyi` error to all queries. To implement a partial protocol for your own application, subclass `aiokdb.server.ServerContext` and implement `on_sync_request()`, `on_async_message()`, and perhaps `check_login()`.

## Command Line Interface

Usable command line client support for connecting to a remote KDB instance (using python `asyncio`, and `prompt_toolkit` for line editing and history) is built into the package:

```bash
$ pip install aiokdb prompt_toolkit
$ ./q -p 12345 &
$ python -m aiokdb.cli --host localhost --port 12345
(eval) > ([s:7 6 0Nj]x:3?0Ng;y:2)
s| x                                    y
-|---------------------------------------
7| 409031f3-b19c-6770-ee84-6e9369c98697 2
6| 52cb20d9-f12c-9963-2829-3c64d8d8cb14 2
 | cddeceef-9ee9-3847-9172-3e3d7ab39b26 2
(eval) > 4 5 6!(`abc`def;til 300;(3 4!`a`b))
4| abc def
5| 0 1 2 ... 297 298 299
6| KDict
(eval) > [ctrl-D]
$
```

Text formatting above is controlled by `aiokdb.format.ASCIIFormatter`, which looks inside a `KObj` to render `XD`, `SD`, `XT` types in tabular form containing atom and vector values. Nested complex types ie. dictionary or table render as `KDict` or `KFlip` constant.

## QDB Files
Ordinary `.qdb` files written with set can be read by `kfromfile` or written by `ktofile`:

```python
>>> from aiokdb.files import kfromfile, ktofile
>>> k = kfromfile('test_qdb0/test.qdb')
>>> k
<aiokdb.KObjArray object at 0x7559136d8230>
>>> from aiokdb.format import AsciiFormatter
>>> fmt = AsciiFormatter()
>>> print(fmt.format(k))
[5, hello]
```

Ordinarily `k` is dictionary representing a KDB namespace containing other objects.

## Tests
The library has extensive test coverage, however de-serialisation of certain (obscure) KObj may not be fully supported yet. PR's welcome. All tests are pure python except for those in `test/test_rpc.py`, which will use a real KDB server to test against if you set the `KDB_PYTEST_SERVICE` environment variable (to a URL of the form `kdb://user:password@hostname:port`), otherwise that test is skipped.

* Formatting with `ruff check .`
* Formatting with `ruff format .`
* Check type annotations with `mypy --strict .`
* Run `pytest .` in the root directory


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FTeaEngineering%2Faiokdb?ref=badge_large)