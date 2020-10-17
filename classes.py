from dataclasses import dataclass, field
from typing import Dict, List


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

    
@dataclass
class Folders:
    folders: Dict[str, Folder] = field(default_factory=dict)

    
@dataclass
class Files:
    files: Dict[str, File] = field(default_factory=dict)
