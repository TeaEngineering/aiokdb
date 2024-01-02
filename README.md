# aiokdb
Python asyncio connector to KDB.  Pure python, so does not depend on the `k.h` bindings or kdb shared objects, or numpy/pandas.

The unit tests will use a real KDB binary to test against if you have a `QHOME` containing a working interpreter.

The layout of the repository, documentation and code borrows heavily from aioredis-py

## Peer review & motivation

[qPython](https://github.com/exxeleron/qPython) is a widely used library for this task and it maps objects to Pandas Dataframes which might be more suitable for the majority of applications.

This library takes a different approach and aims to replicate using the KDB C-library functions. It was built working from the publically documented [Serialization Examples](https://code.kx.com/q/kb/serialization/) and [C API for kdb+](https://code.kx.com/q/wp/capi/) pages. Users might also need to be familiar with [k.h](https://github.com/KxSystems/ffi/blob/master/include/k.h).

A simple example:

```python
from aiokdb import khpu
# run ./q -p 12345 &

h = khpu("localhost", 12345, "kdb:pass")
result = h.k("2.0+3.0", None) # None can be used where C expects (K)0

# if the remote returns a Q Exception, this gets raised, unless k(..., raise=False)
assert result.f() == 5.0
````

The `result` object is a K-like Python object (a `KObj`), having the usual signed integer type available as `result.type`. Accessors for the primitive types are prefixed with an `a` and check at runtime that the accessor is appropriate for the stored type (`.aI()`, `.aJ()`, `.aH()` etc.). Atoms store their value to a `bytes` object irrespective of the type, and encode/decode on demand. Atomic values can be set with (`.i(3)`, `.j(12)`, `.ss("hello")`).

Arrays are implemented with subtypes that use [Python's native arrays module](https://docs.python.org/3/library/array.html) for efficient array types. The `MutableSequence` arrays are returned using the usual array accessor functions `kI`, `kB`, `kS` etc.

Serialisation is handled by `b9` which returns a python bytes, and `d9` which takes a bytes and returns a K-object.

* Atoms are created by `ka`, `kb`, `ku`, `kg`, `kh`, `ki`, `kj`, `ke`, `kf`, `kc`, `ks`, `kt`, `kd`, `kz`, `ktj`
* Lists with `ktn` and `knk`
* Dictionaries with `xd` and tables with `xt`.

Python manages garbage colleciton of our objects, so none of the refcounting primitives exist, ie. `k.r` and functions `r1`, `r0` and `m9`, `setm` have no equivelent.

## Tests

Run `pytest` in the root directory.
Formatting with `black .`
Check type annotations with `mypy --strict .`


