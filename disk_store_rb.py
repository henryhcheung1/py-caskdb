"""
caskDB red black tree implementation. See disk_store.py for more details.
"""

import os
import time
import typing

from format import encode_kv, decode_kv, decode_header, EntryFormat

class DiskStorageRB:
    TOMBSTONE = '' # value to set

    def __init__(self, file_name: str = "data.db"):
        pass

    def set(self, key: str, value: str) -> None:
        pass

    def get(self, key: str) -> str:
        pass

    def delete(self, key: str) -> bool:
        pass

    def close(self) -> None:

        self.active_file_handle.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
