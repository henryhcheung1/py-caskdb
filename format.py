"""
format module provides encode/decode functions for serialisation and deserialisation
operations

format module is generic, does not have any disk or memory specific code.

The disk storage deals with bytes; you cannot just store a string or object without
converting it to bytes. The programming languages provide abstractions where you don't
have to think about all this when storing things in memory (i.e. RAM). Consider the
following example where you are storing stuff in a hash table:

    books = {}
    books["hamlet"] = "shakespeare"
    books["anna karenina"] = "tolstoy"

In the above, the language deals with all the complexities:

    - allocating space on the RAM so that it can store data of `books`
    - whenever you add data to `books`, convert that to bytes and keep it in the memory
    - whenever the size of `books` increases, move that to somewhere in the RAM so that
      we can add new items

Unfortunately, when it comes to disks, we have to do all this by ourselves, write
code which can allocate space, convert objects to/from bytes and many other operations.

format module provides two functions which help us with serialisation of data.

    encode_kv - takes the key value pair and encodes them into bytes
    decode_kv - takes a bunch of bytes and decodes them into key value pairs

**workshop note**

For the workshop, the functions will have the following signature:

    def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]
    def decode_kv(data: bytes) -> tuple[int, str, str]
"""
import struct

#TODO:
    # - Add CRC cyclic redundancy check

class EntryFormat:
    HEADER_FORMAT = 'III'
    HEADER_SIZE = 12 # bytes
    def __init__(self):
        self.timestamp
        self.key_size
        self.value_size

def encode_header(timestamp: int, key_size: int, value_size: int) -> bytes:

    return struct.pack(EntryFormat.HEADER_FORMAT, timestamp, key_size, value_size)

def encode_kv(timestamp: int, key: str, value: str):# -> tuple[int, bytes]:

    header_bytes = encode_header(timestamp, len(key), len(value))
    kv_bytes = struct.pack(f"{len(key)}s{len(value)}s", key.encode(), value.encode())
    return (len(kv_bytes), header_bytes + kv_bytes)

def decode_kv(data: bytes):# -> tuple[int, str, str]:

    # print(f"decode_kv: data bytes {data}")

    header_bytes = data[:EntryFormat.HEADER_SIZE]
    timestamp, key_size, value_size = decode_header(header_bytes)

    # data_bytes = data[EntryFormat.HEADER_SIZE:]
    data_bytes = data[EntryFormat.HEADER_SIZE:EntryFormat.HEADER_SIZE + key_size + value_size]

    # print(f"decode_kv: {key_size} {value_size}")

    decoded = struct.unpack(f"{key_size}s{value_size}s", data_bytes)
    return (timestamp, decoded[0].decode(), decoded[1].decode())

def decode_header(data: bytes):# -> tuple[int, int, int]:
    
    decoded = struct.unpack(EntryFormat.HEADER_FORMAT, data)
    return (decoded[0], decoded[1], decoded[2]) # timestamp, key size, value size
