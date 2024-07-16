from collections.abc import MutableSequence, Sequence
from typing import TYPE_CHECKING, Any, Iterable, Union, overload

if TYPE_CHECKING:
    BaseMutSeq = MutableSequence[bool]
else:
    BaseMutSeq = MutableSequence


class BoolByteAdaptor(BaseMutSeq):
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
            self.data[index] = [{True: 1, False: 0}[i] for i in item]
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
