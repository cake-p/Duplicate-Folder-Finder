from dataclasses import dataclass, field
from typing import List


@dataclass
class File:
    dir_index: List[int] = field(default_factory=list)
    file_index: int = 0
    sha512: bytes = b''
    size: int = 0


@dataclass
class Folder:
    sha512: bytes = b''
    size: int = 0
    hash: List[bytes] = field(default_factory=list)
