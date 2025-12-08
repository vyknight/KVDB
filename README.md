# Simple Key Value Database - Report

## Compilation and Run Instructions
This project should be compiled with CMake using the CMake options:
`-G Ninja -DCMAKE_BUILD_TYPE_DEBUG -DCMAKE_MAKE_PROGRAM=<path to ninja binary>`

All compilation targets are already in the CMakeList.txt file.

Please use C++20, while I did not use any language features newer than C++17, I cannot guarantee it will compile with an
older version of C++.

By default, running KVDB will start the program in interactive mode, you may use the `help` command to see CLI options. 
For unit tests, run the program with any keyword argument.

## Description of Classes
As a group of 1, I took advantage of the dispensations granted to me and tried to keep things simple. The structure of
KVDB is as the project requirement specified, a `Memtable` contains the data stored in RAM, with `SSTableReader` and
`SSTableWriter` classes to persist flushed Memtables in disk. A `WriteAheadLog` is used to ensure that operations are
recoverable. `SSTableReader` depends on a `BufferPool` to cache recently used pages to improve performance, albeit
without the querying speed improvements B Tree querying would provide. Finally, SSTable files were ordered into levels
by `LevelManager` using `Compactor`, with all functionality wrapped inside a `LSMTree` class to improve querying
performance.

### Memtable
`Memtable.cpp Memtable.h`

A simple class to keep track of data in memory. Data is stored inside an instance of `std::map` (which I'm able to use
thanks to dispensation 1). Though this class comprises simple getters and setters, there are two points of interest:
- `std::map` stores data in order, with strings this means that they're stored in lexicographical order. 
This means we don't have to do any sorting when flushing memtables into SST files.
- Values are represented by the `Entry` struct, the point of which is to provide a separate bool to mark tombstones
instead of using a special character, this is to guarantee that KVDB supports all special characters.

### SSTable Reader and Writer
`SStableReader.cpp SStableReader.h SSTableWriter.cpp SSTableWriter.h`

These classes are responsible for writing and reading SSTable files. Since these files are the foundation of the whole
DB, the design fo the file format has a very large impact on the final performance. The format is as such:

Header
- Magic Number (8B) as a simple file integrity check
- Version number (uint32) to support file format changes
- Entry count (uint32) number of entries in SSTable
- Data offset (uint64) number of bytes before KV data starts.

Key Directory
- Key length (uint32)
- Key (variable size) string value of the key
- Value offset (uint64) position of matching value in data section
- Value length (uint32) length of value
- Tombstone flag (uint8) 1 = deleted, 0 = alive

Value Data Section
- Sequential storage of all values

A directory type structure was chosen so that SSTables can be quickly scanned and eliminated. However, to support longer
keys, the key directory itself is stored sequentially and thus must all be read. Most of the header values are overkill
for this project, but since the size of the header is small relative to the data, and since it doesn't create complexity
for the rest of the database implementation, I've chosen to keep the Magic and Version fields for redundancy.

Another important note is that the SSTable headers don't include a sequence number, instead they're sorted based on
their last modified date. Unix fs last modified is a very reliable way to track of recency, chances of two files being
written at the same unix second is very unlikely. However, this means that the database cannot guarantee that SSTable
files created on different computers (eg. a windows computer and a linux computer) can be used since we don't handle 
file system differences. 

### Write Ahead Log
`WriteAheadLog.cpp WriteAheadLog.h`

This class simply just writes upcoming operations into a file on disk.

### Page
`PageId.cpp PageId.h Page.cpp Page.h`

These classes are used to track values such as hash and modified status for use in the BufferPool.

### BufferPool
`BufferPool.cpp BufferPool.h`

This is a very simple buffer pool that uses LRU for eviction. For collisions, since pages are tracked in a 
`std::unorder_map`, collisions are resolved by the container itself with chaining.

### Interface 
`KVStore.cpp KVStore.h CLI.cpp CLI.h`

KVStore is the internal interface for interacting with the database, while CLI is a wrapper over KVStore that translates
`std::istringstream` streams into KVStore operations. This two layered structure with an austere interface with a 
user-friendly wrapper helps during the debugging process by quickly eliminating the interface as the source of error.

### Compactor
`Compactor.cpp Compactor.h`

This class performs multiway merge on sstables passed to it by the LevelManager. It uses `fs::last_write_time` as a
sequence number to determine which values to keep in case of conflict (see `Compactor::get_file_timestamp()` and 
`Compactor::multiway_merge()`). The compacted value is returned as a `SSTableReader` object.

### Level Manager
`LevelManager.cpp LevelManager.h`

This class is responsible for handling all SSTables in the LSM tree representation of the database. It determines when
compaction is necessary based on number of stored entries in each SSTable. This class is also responsible for finding
candidate SSTables when LSM needs to query out of Memtable. SSTable levels are indicated in filenames, not directory 
structures. This keeps the implementation simple and less error-prone, despite potential querying performance issues 
when many files reside in one file system directory.

### LSM
`LSMTree.cpp LSMTree.h`

This class is implemented almost as a mirror of the `KVStore` classes, the difference being that its operations handle
the possibility of the data being on different SSTable layers, and prioritizes lower layers for querying.

## Project Status

All basic requirements for the project are met. To my knowledge however there is a bug with LSM operations where
putting new values into a recently deleted key cause it to not be found by a subsequent get request. I was not able to
complete any bonus objectives at the time of this report.

I should note that I've experienced some issues with the scanning in which the top range of scans seem exclusive
rather than inclusive because of nuances of lexicographical order.

## Testing

You will find an extensive suite of unit tests in the `/Tests` directory, I've made them after completing each class to
ensure that regression does not occur as newer classes are created and older ones modified. Within each test file, 
individual functions are tested, and then integrations with other classes are tested.

