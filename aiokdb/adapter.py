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
    def __getitem__(self, i: int) -> bool: ...
    @overload
    def __getitem__(self, s: slice) -> "MutableSequence[bool]": ...
    def __getitem__(self, i: Union[int, slice]) -> Union[bool, "MutableSequence[bool]"]:
        if isinstance(i, slice):
            return self.__class__(self.data[i])
        else:
            return {0: False, 1: True}[self.data[i]]

    def __len__(self) -> int:
        return len(self.data)

    @overload
    def __setitem__(self, index: int, item: bool) -> None: ...
    @overload
    def __setitem__(self, index: slice, item: Iterable[bool]) -> None: ...
    def __setitem__(
        self, index: Union[int, slice], item: Union[bool, Iterable[bool]]
    ) -> None:
        if isinstance(index, slice) and isinstance(item, Iterable):
            self.data[index] = array.array("B", [{True: 1, False: 0}[i] for i in item])
        elif isinstance(index, int) and isinstance(item, bool):
            self.data[index] = {True: 1, False: 0}[item]
        else:
            raise TypeError()

    def insert(self, index: int, item: bool) -> None:
        self.data.insert(index, {True: 1, False: 0}[item])

    def __delitem__(self, item: Union[int, slice]) -> None:
        del self.data[item]

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
    def __getitem__(self, i: int) -> str: ...
    @overload
    def __getitem__(self, s: slice) -> "MutableSequence[str]": ...
    def __getitem__(self, i: Union[int, slice]) -> Union[str, "MutableSequence[str]"]:
        if isinstance(i, slice):
            return self.__class__(self.data[i], self.context)
        else:
            return self.context.lookup_str(self.data[i])

    def __len__(self) -> int:
        return len(self.data)

    @overload
    def __setitem__(self, index: int, item: str) -> None: ...
    @overload
    def __setitem__(self, index: slice, item: Iterable[str]) -> None: ...
    def __setitem__(
        self, index: Union[int, slice], item: Union[str, Iterable[str]]
    ) -> None:
        if isinstance(index, slice) and isinstance(item, Iterable):
            self.data[index] = array.array("l", [self.context.ss(i) for i in item])
        elif isinstance(index, int) and isinstance(item, str):
            self.data[index] = self.context.ss(item)
        else:
            raise TypeError()

    def insert(self, index: int, item: str) -> None:
        self.data.insert(index, self.context.ss(item))

    def __delitem__(self, item: Union[int, slice]) -> None:
        del self.data[item]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Sequence):
            if len(self) == len(other):
                return all(a == b for a, b in zip(self, other))
            else:
                return False
        raise TypeError()
