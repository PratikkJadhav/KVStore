My raw thoughts after reading bitcask paper , and arpit bhayani's blog basically a todolist of what i have to build and what i understood from the paper.

Bitcask:

stores KV pairs in append only file

Input: Put(key , value) , Get(key);
Output: Ok(1) , (OK , val)

DataFiles: -append only logfile that hold kV Pairs + some meta data
           -BitCask instance may contain many datafiles , only one to be active for appending/CRUD and others will be just for reading.
            -contains crc , timestamps , key_size , value_size , key , value
            -if size(DataFile) > threshold , this datafile is closed and new df is created.

KeyDir: -InMem hash table stores all keys present in the bitcask instances and it maps it to the offset where the datafile log entry resides


Operations on BitCask:

Put Key Value: new KV pair is submitted to be stored in the Bitcask ,  the engine first appends it to the active datafile and then creates a new entry in the KeyDir specifying the offset and file where the value is stored

Delete: Deleting a key is a special operation where the engine atomically appends a new entry in the active datafile with value equalling a tombstone value, denoting deletion, and deleting the entry from the in-memory KeyDir.

Read: Reading a KV pair from the store requires the engine to first find the datafile and the offset within it for the given key; which is done using the KeyDir. Once that information is available the engine then performs one disk read from the corresponding datafile at the offset to retrieve the log entry

MERGE and COMPACTION:

The merge process iterates over all the immutable files in the Bitcask and produces a set of datafiles having only live and latest versions of each present key.
