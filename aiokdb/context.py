from typing import Dict, List


class KContext:
    def __init__(self) -> None:
        self.symbols: Dict[str, int] = {}
        self._symbol_str: List[str] = []
        self._symbol_bytes: List[bytes] = []

    def ss(self, s: str) -> int:
        idx = self.symbols.setdefault(s, len(self.symbols))
        if idx == len(self._symbol_str):
            bs = bytes(s, "utf-8") + b"\x00"
            self._symbol_bytes.append(bs)
            self._symbol_str.append(s)
        return idx

    def lookup_str(self, idx: int) -> str:
        return self._symbol_str[idx]

    def lookup_bytes(self, idx: int) -> bytes:
        return self._symbol_bytes[idx]
