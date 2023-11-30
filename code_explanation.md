## NewKeyValueStore(walFilePath string) (*KeyValueStore, error)

This function creates a new instance of the `KeyValueStore`. It initializes an in-memory key-value store (`data`), opens or creates a Write-Ahead Log (WAL) file for persistent storage, and sets up additional variables to track key lengths and deleted keys.

## CloseWAL()

This method closes the Write-Ahead Log (WAL) file associated with the `KeyValueStore`.

## Get(key string) ([]byte, bool)

Retrieves the value associated with the given key from the in-memory store. If the key is marked as deleted, it returns `nil` and `false`. If the key is not found in memory, it searches through SST files for the key.

## Set(key string, value []byte)

Sets a key-value pair in the in-memory store, writes the operation to the Write-Ahead Log (WAL), and updates key length metrics. If the in-memory store reaches a threshold, it flushes the data to an SSTable file, clears the in-memory store, and clears the WAL.

## Delete(key string) ([]byte, bool)

Deletes a key from the in-memory store and writes the operation to the Write-Ahead Log (WAL). 

## ClearWAL() error

Closes, truncates, and reopens the Write-Ahead Log (WAL) to clear its contents.

## WriteSSTable(filename string) error

Writes the in-memory data to an SSTable file. It includes writing a magic number, entry count, key length metrics, and key-value pairs to the SSTable file.

## writeToWAL(entry map[string]interface{})

Writes a log entry (set or delete operation) to the Write-Ahead Log (WAL) file in JSON format.

## RecoverFromWAL() error

Replays operations from the Write-Ahead Log (WAL) during system startup to recover the state.

## handleSet(kv *KeyValueStore) http.HandlerFunc

Handles the HTTP POST request for setting a key-value pair. It parses the JSON body, extracts the key and value, and updates the in-memory store.

## handleGet(kv *KeyValueStore) http.HandlerFunc

Handles the HTTP GET request for retrieving a key's value. It extracts the key from the URL and calls the `Get` method to retrieve the value.

## handleDelete(kv *KeyValueStore) http.HandlerFunc

Handles the HTTP DELETE request for deleting a key. It extracts the key from the URL and calls the `Delete` method to delete the key.

## SearchSSTFiles(key string) ([]byte, bool)

Searches for a key in SST files from most recent to oldest. It calls the `SearchSSTFile` method for each file.

## SearchSSTFile(key string, sstFile string) ([]byte, bool)

Searches for a key in a specific SST file. It reads the magic number, entry count, key length metrics, and key-value pairs to find the key.

## fileInfoModTime(filename string) time.Time

Returns the modification time of a file.

## main()

The main function initializes the `KeyValueStore`, recovers from the WAL, and starts an HTTP server to handle set, get, and delete requests.
