from dataclasses import dataclass, field
from typing import List


@dataclass
class File:
    dir_name: str = ''
    file_name: str = ''
    sha512: bytes(512) = b''
    size: int = 0
    atime: int = 0
    mtime: int = 0
    ctime: int = 0


@dataclass
class Folder:
    dir_name: str = ''
    folder_name: str = ''
    sha512: bytes(512) = b''
    size: int = 0
    hash: List[bytes] = field(default_factory=list)
