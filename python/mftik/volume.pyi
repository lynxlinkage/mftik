from __future__ import annotations

import os

def write(dir: str | os.PathLike[str], name: str, data: bytes | bytearray | memoryview) -> None: ...
def read(
    volume_dir: str | os.PathLike[str], input_dir: str | os.PathLike[str], name: str
) -> bytes | None: ...
