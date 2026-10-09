import array
from collections.abc import MutableSequence, Sequence
from typing import TYPE_CHECKING, Any, Iterable, Union, overload

from aiokdb.context import KContext

if TYPE_CHECKING:
    BaseBoolMutSeq = MutableSequence[bool]
    BaseSymMutSeq = MutableSequence[str]
else:
    BaseBoolMutSeq = MutableSequence
    BaseSymMutSeq = MutableSequence


class BoolByteAdaptor(BaseBoolMutSeq):
    def __init__(self, data: "MutableSequence[int]"):
        self.data = data

    @overload
    def __getitem__(self, index: int) -> bool: ...
    @overload
    def __getitem__(self, index: slice) -> "MutableSequence[bool]": ...
    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[bool, "MutableSequence[bool]"]:
        if isinstance(index, slice):
            return self.__class__(self.data[index])
        else:
            return {0: False, 1: True}[self.data[index]]

    def __len__(self) -> int:
        return len(self.data)

    @overload
    def __setitem__(self, index: int, value: bool) -> None: ...
    @overload
    def __setitem__(self, index: slice, value: Iterable[bool]) -> None: ...
    def __setitem__(
        self, index: Union[int, slice], value: Union[bool, Iterable[bool]]
    ) -> None:
        if isinstance(index, slice) and isinstance(value, Iterable):
            self.data[index] = array.array("B", [{True: 1, False: 0}[v] for v in value])
        elif isinstance(index, int) and isinstance(value, bool):
            self.data[index] = {True: 1, False: 0}[value]
        else:
            raise TypeError()

    def insert(self, index: int, value: bool) -> None:
        self.data.insert(index, {True: 1, False: 0}[value])

    def __delitem__(self, index: Union[int, slice]) -> None:
        del self.data[index]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Sequence):
            if len(self) == len(other):
                return all([a == b for a, b in zip(self, other)])
            else:
                return False
        raise TypeError()


class SymIntAdaptor(BaseSymMutSeq):
    def __init__(self, data: "MutableSequence[int]", context: KContext):
        self.data = data
        self.context = context

    @overload
    def __getitem__(self, index: int) -> str: ...
    @overload
    def __getitem__(self, index: slice) -> "MutableSequence[str]": ...
    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[str, "MutableSequence[str]"]:
        if isinstance(index, slice):
            return self.__class__(self.data[index], self.context)
        else:
            return self.context.lookup_str(self.data[index])

    def __len__(self) -> int:
        return len(self.data)

    @overload
    def __setitem__(self, index: int, value: str) -> None: ...
    @overload
    def __setitem__(self, index: slice, value: Iterable[str]) -> None: ...
    def __setitem__(
        self, index: Union[int, slice], value: Union[str, Iterable[str]]
    ) -> None:
        if isinstance(index, slice) and isinstance(value, Iterable):
            self.data[index] = array.array("l", [self.context.ss(v) for v in value])
        elif isinstance(index, int) and isinstance(value, str):
            self.data[index] = self.context.ss(value)
        else:
            raise TypeError()

    def insert(self, index: int, value: str) -> None:
        self.data.insert(index, self.context.ss(value))

    def __delitem__(self, index: Union[int, slice]) -> None:
        del self.data[index]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Sequence):
            if len(self) == len(other):
                return all(a == b for a, b in zip(self, other))
            else:
                return False
        raise TypeError()
