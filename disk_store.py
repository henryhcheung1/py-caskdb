"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import os
import time
import typing

from format import encode_kv, decode_kv, decode_header, EntryFormat


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf

class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    """
    active file_name: /etc/data/data.db

    file_id     status      filename
    0           older       0.db
    1           older       1.db
    2           older       2.db
    3           active      data.db

    on db initialization, load all keys / values from directory (i.e. /etc/data/)


    Merge:
    When to start merge process?
        - cron
        - when init dir size exceeds some threshold
        - when database is started
        - manually

    1. sort all files & read files from oldest to newest
    2. create an in memory hashtable that tracks most recent value of keys & values
    3. Once all files are processed, write hashtable to file(s).
        This creates a compaction / reduction of files. Write the hashtable to multiple output
        files if the hashtable key size exceeds the maximum size per file.


    DB Initialization:
    1. pass in initialization directory containing all data files (active & inactive files)
    2. sort all data files in init dir and read from oldest to newest
    3. update key dir. 
        Delete key if tomebstoned value is encountered.
    """

    #TODO: implement legacy bitcask file compaction

    TOMBSTONE = '' # value to set

    # dictionary implementation

    def __init__(self, file_name: str = "data.db"):

        file_path = os.path.split(file_name)

        self.db_directory = file_path[0]
        self.active_file_name = file_name

        self.active_file_handle = open(file_name, 'ab')
        # self.active_file_handle = open(file_name, "ab", 0) # flush after file write, (skip buffering)
        self.key_dir = dict()
        self.active_index = 0
        self.active_file_id = 0

        # initialize keydir
        self.__initialize_keydir()



    def set(self, key: str, value: str) -> None:

        timestamp = int(time.time())

        # serialize key / value to bytes
        encoded_kv = encode_kv(timestamp, key, value)

        # append key / value bytes to active file
        self.active_file_handle.write(encoded_kv[1])

        # update keydir
        self.key_dir[key] = (self.active_file_id, len(value.encode()), self.active_index, timestamp)

        # update pointer location in file
        self.active_index += EntryFormat.HEADER_SIZE + encoded_kv[0]



    def get(self, key: str) -> str:

        if key not in self.key_dir:
            # key not found
            return ''

        # get the key / value location metadata from keydir
        meta = self.key_dir[key]

        # find bitcask file that contains most recent key / value
        filename = self.active_file_name
        if meta[0] == self.active_file_id:
            # value exists in active file, therefore flush active file buffer to disk to allow reading most recent writes
            self.active_file_handle.flush()
            os.fsync(self.active_file_handle.fileno())

        # else:
            # key exists in previously closed immutable file, get file name
            # TODO: finish implementation

        # read file and search for key / value
        with open(filename, 'rb') as f:

            print(f"get: from {filename}")

            data_bytes = f.read()

            # print(f"get: {data_bytes}")

            # kv_bytes_start = meta[2]+EntryFormat.HEADER_SIZE

            # header_bytes = data_bytes[meta[2]:kv_bytes_start]

            # header = decode_header(header_bytes)
            # kv_size_bytes = header[1] + header[2]

            # entry_bytes = data_bytes[meta[2]:kv_bytes_start+kv_size_bytes]

            # kv = decode_kv(entry_bytes)

            kv = decode_kv(data_bytes[meta[2]:])

        return kv[2]


    def delete(self, key: str) -> bool:
        """
        Key to delete.
        Returns true if key exists.
        Returns false if key does not exist.

        tombstone value format
        [timestamp, key_size, value_size, key, value]
        where value is an empty string with value size of length 0
        This assumes keys should not have empty strings for values
        """

        # 1. Remove key from key_dir
        # 2. Write tombstone value to active data file
        # Note: 
        #   - Once merge process begins, most recent tombstoned keys are not added to keydir
        #   - Also on DB initialization, do not store most recent tombstoned keys in keydir

        if key not in self.key_dir:
            # key not found
            return False

        # remove existing key
        del self.key_dir[key]

        timestamp = int(time.time())
        encoded_kv = encode_kv(timestamp, key, '')

        self.active_file_handle.write(encoded_kv[1])

        return True


    def close(self) -> None:

        self.active_file_handle.close()

    def __initialize_keydir(self) -> None:

        # TODO: initiliaze keydir with all legacy immutable files, instead of only the active file
            # sort all files
            # self.__read_file(filename)

        # print("__initialize_keydir")

        file_pointer = 0
        with open(self.active_file_name, 'rb') as f:
            data_bytes = f.read()

        print(data_bytes)

        while file_pointer < len(data_bytes):

            kv = decode_kv(data_bytes[file_pointer:])
            # print(kv)

            if kv[1] in self.key_dir and kv[2] == DiskStorage.TOMBSTONE:
                # tombstone value found in db file, this indicates a key deletion, therefore delete from keydir
                del self.key_dir[kv[1]]

            else:
                self.key_dir[kv[1]] = (self.active_file_name, len(kv[2]), file_pointer, kv[0])

            file_pointer += EntryFormat.HEADER_SIZE + len(kv[1]) + len(kv[2])



    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
