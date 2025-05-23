from typing import Dict, Tuple


class KContext:
    def __init__(self) -> None:
        self.symbols: Dict[str, int] = {}
        self.symbols_enc: Dict[int, Tuple[str, bytes]] = {}

    def ss(self, s: str) -> int:
        bs = bytes(s, "utf-8") + b"\x00"
        idx = self.symbols.setdefault(s, len(self.symbols))
        # TODO this should be a list
        self.symbols_enc[idx] = (s, bs)
        return idx

    def lookup_str(self, idx: int) -> str:
        return self.symbols_enc[idx][0]

    def lookup_bytes(self, idx: int) -> bytes:
        return self.symbols_enc[idx][1]
