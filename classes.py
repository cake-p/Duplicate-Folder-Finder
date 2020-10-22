from dataclasses import dataclass, field
from typing import List


@dataclass
class File:
    sha512: bytes(512) = b''
    size: int = 0


@dataclass
class Folder:
    sha512: bytes(512) = b''
    size: int = 0
    hash: List[bytes] = field(default_factory=list)
