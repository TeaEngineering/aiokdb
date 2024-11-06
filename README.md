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