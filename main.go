package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// KeyValueStore represents the in-memory key-value store.
type KeyValueStore struct {
	data map[string][]byte
	wal  *os.File
	smallestKeyLength int
	largestKeyLength  int
	DeletedKeys map[string]bool
}

// NewKeyValueStore creates a new instance of KeyValueStore.
func NewKeyValueStore(walFilePath string) (*KeyValueStore, error) {
	data := make(map[string][]byte)

	// Open or create the WAL file
	wal, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &KeyValueStore{
		data: data,
		wal:  wal,
		smallestKeyLength: 0,
		largestKeyLength:  0,
		DeletedKeys: make(map[string]bool),
	}, nil
}

// CloseWAL closes the Write-Ahead Log file.
func (kv *KeyValueStore) CloseWAL() {
	kv.wal.Close()
}

// Get retrieves the value associated with the given key.
func (kv *KeyValueStore) Get(key string) ([]byte, bool) {
	value, ok := kv.data[key]

	// Check if the key is marked as deleted
	if kv.DeletedKeys[key] {
		return nil, false
	}else if !ok{
		// Key not found in memory, and not deleted in DeletedKeys, search in SST files
		value, ok = kv.SearchSSTFiles(key)
	}

	return value, ok
}

// Set writes the key-value pair to the in-memory store and the WAL.
func (kv *KeyValueStore) Set(key string, value []byte) {
	// Check if the key is marked as deleted
	if kv.DeletedKeys[key] {
		// Mark it as not deleted, so it won't be flushed
		kv.DeletedKeys[key] = false
		return
	}
	
	// Update smallest and largest key lengths
	keyLength := len(key)
	if kv.smallestKeyLength == 0 || keyLength < kv.smallestKeyLength {
		kv.smallestKeyLength = keyLength
	}
	if keyLength > kv.largestKeyLength {
		kv.largestKeyLength = keyLength
	}


	// Write to the WAL
	logEntry := map[string]interface{}{
		"operation": "set",
		"key":       key,
		"value":     string(value),
	}
	kv.writeToWAL(logEntry)

	// Update the in-memory store
	kv.data[key] = value

	// Check if the map size has reached the threshold
    if len(kv.data) >= 10 {
        // Flush to SSTable
        if err := kv.WriteSSTable("sstable_" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".sst"); err != nil {
            log.Printf("Error flushing to SSTable: %v\n", err)
            return // Exit if flush fails
        }
        // Clear the in-memory store after flushing
        kv.data = make(map[string][]byte)
        kv.DeletedKeys = make(map[string]bool)

        // Clear the Write-Ahead Log
        if err := kv.ClearWAL(); err != nil {
            log.Printf("Error clearing WAL: %v\n", err)
            // Handle the error as needed (e.g., log it or retry)
        }
    }
}

// Delete removes the key and returns its value.
func (kv *KeyValueStore) Delete(key string) ([]byte, bool) {
	value, ok := kv.data[key]
	if ok {
		// Write to the WAL
		logEntry := map[string]interface{}{
			"operation": "delete",
			"key":       key,
			"value":     string(value),
		}
		kv.writeToWAL(logEntry)
		delete(kv.data, key)
		return value, ok
	}else if value, ok = kv.Get(key); ok{
		logEntry := map[string]interface{}{
			"operation": "delete",
			"key":       key,
			"value":     string(value),
		}
		kv.writeToWAL(logEntry)
		kv.DeletedKeys[key] = true
	}
	return value, ok	
}

// ClearWAL closes, truncates, and reopens the Write-Ahead Log.
func (kv *KeyValueStore) ClearWAL() error {
    // Close the Write-Ahead Log
    if err := kv.wal.Close(); err != nil {
        return err
    }

    // Truncate the Write-Ahead Log
    if err := os.Truncate("wal.log", 0); err != nil {
        return err
    }

    // Reopen the Write-Ahead Log
    wal, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }

    // Update the KeyValueStore's WAL reference
    kv.wal = wal

    return nil
}

// WriteSSTable writes the in-memory data to an SSTable file.
func (kv *KeyValueStore) WriteSSTable(filename string) error {
    // Open or create the SSTable file
    sstableFile, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer sstableFile.Close()

    // Write magic number
    if _, err := sstableFile.Write([]byte("SSTB")); err != nil {
        return err
    }

    // Combine keys from data and DeletedKeys maps for sequential writes in SSTable
    var keys []string
    for key := range kv.data {
        keys = append(keys, key)
    }
    for key := range kv.DeletedKeys {
        keys = append(keys, key)
    }
    sort.Strings(keys)

    // Write entry count
    entryCount := len(keys)
    if err := binary.Write(sstableFile, binary.LittleEndian, uint32(entryCount)); err != nil {
        return err
    }

    // Write smallest key length
    if err := binary.Write(sstableFile, binary.LittleEndian, uint32(kv.smallestKeyLength)); err != nil {
        return err
    }

    // Write largest key length
    if err := binary.Write(sstableFile, binary.LittleEndian, uint32(kv.largestKeyLength)); err != nil {
        return err
    }

    // Write key-value pairs to the SSTable
    for _, key := range keys {
        value := kv.data[key]

        // Determine the operation type (set or delete)
        var operationMarker uint16
        if kv.DeletedKeys[key] {
            operationMarker = 1 // Delete operation
        } else {
            operationMarker = 0 // Set operation
        }

        // Write operation marker
        if err := binary.Write(sstableFile, binary.LittleEndian, operationMarker); err != nil {
            return err
        }

        // Write key length
        if err := binary.Write(sstableFile, binary.LittleEndian, uint32(len(key))); err != nil {
            return err
        }

        // Write value length
        if err := binary.Write(sstableFile, binary.LittleEndian, uint32(len(value))); err != nil {
            return err
        }

        // Write key
        if _, err := sstableFile.Write([]byte(key)); err != nil {
            return err
        }

        // Write value
        if _, err := sstableFile.Write(value); err != nil {
            return err
        }
    }

    return nil
}


// writeToWAL writes a log entry to the Write-Ahead Log file.
func (kv *KeyValueStore) writeToWAL(entry map[string]interface{}) {
	// Convert the entry to JSON
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		log.Printf("Error marshaling WAL entry: %v\n", err)
		return
	}

	// Write the JSON entry followed by a newline to the WAL
	if _, err := kv.wal.WriteString(string(entryJSON) + "\n"); err != nil {
		log.Printf("Error writing to WAL: %v\n", err)
	}

	// Flush to ensure the entry is written to disk
	if err := kv.wal.Sync(); err != nil {
		log.Printf("Error syncing WAL: %v\n", err)
	}
}

// RecoverFromWAL replays operations from the Write-Ahead Log during system startup.
func (kv *KeyValueStore) RecoverFromWAL() error {
    // Open the WAL file
    walFile, err := os.Open(kv.wal.Name())
    if err != nil {
        return err
    }
    defer walFile.Close()

    // Create a JSON decoder for the WAL file
    decoder := json.NewDecoder(walFile)

    // Replay operations from the Write-Ahead Log
    for {
        var logEntry map[string]interface{}

        // Decode the next log entry
        if err := decoder.Decode(&logEntry); err == io.EOF {
            // End of file reached
            break
        } else if err != nil {
            return err
        }

        // Perform the operation based on the log entry
        switch operation := logEntry["operation"].(string); operation {
        case "set":
            key := logEntry["key"].(string)
            value := logEntry["value"].(string)
			fmt.Printf("Set operation recovered from WAL - Key: %s, Value: %s\n", key, string(value))
            // Set the key-value pair in memory
			// kv.Set(key, []byte(value))
            kv.data[key] = []byte(value)

        case "delete":
            key := logEntry["key"].(string)
			value := logEntry["value"].(string)
            // Delete the key from memory
			fmt.Printf("Delete operation recovered from WAL - Key: %s, Value: %s\n", key, string(value))
            // kv.Delete(key)
			delete(kv.data, key)
        }
    }

    return nil
}

// handleSet handles the POST request for setting a key-value pair.
func handleSet(kv *KeyValueStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse the JSON body
		var requestBody map[string]string
		err := json.NewDecoder(r.Body).Decode(&requestBody)
		if err != nil {
			http.Error(w, "Error decoding JSON body", http.StatusBadRequest)
			return
		}

		// Extract key and value from the JSON body
		key, ok := requestBody["key"]
		if !ok {
			http.Error(w, "Key not found in JSON body", http.StatusBadRequest)
			return
		}

		value, ok := requestBody["value"]
		if !ok {
			http.Error(w, "Value not found in JSON body", http.StatusBadRequest)
			return
		}

		// Update the in-memory store
		kv.Set(key, []byte(value))

		fmt.Fprintf(w, "OK\n")
	}
}

// SearchSSTFiles searches for the key in SST files from most recent to oldest.
func (kv *KeyValueStore) SearchSSTFiles(key string) ([]byte, bool) {
	// Get a list of SST files in the directory
	files, err := filepath.Glob("sstable_*.sst")
	if err != nil {
		log.Printf("Error listing SST files: %v\n", err)
		return nil, false
	}

	// Sort files based on modification time (most recent to oldest)
	sort.Slice(files, func(i, j int) bool {
		return fileInfoModTime(files[i]).After(fileInfoModTime(files[j]))
	})

	// Iterate through SST files from most recent to oldest
	for _, sstFile := range files {
		// Search for the key in the SST file
		value, ok := kv.SearchSSTFile(key, sstFile)
		if ok {
			return value, true
		}
	}

	return nil, false
}

// SearchSSTFile searches for the key in a specific SST file.
func (kv *KeyValueStore) SearchSSTFile(key string, sstFile string) ([]byte, bool) {
	// Open the SST file
	file, err := os.Open(sstFile)
	if err != nil {
		log.Printf("Error opening SST file %s: %v\n", sstFile, err)
		return nil, false
	}
	defer file.Close()

	// Read magic number
	magicNumber := make([]byte, 4)
	if _, err := file.Read(magicNumber); err != nil {
		log.Printf("Error reading magic number from SST file %s: %v\n", sstFile, err)
		return nil, false
	}
	log.Printf("Magic Number: %s\n", magicNumber)

	// Check if it's a valid SST file
	if string(magicNumber) != "SSTB" {
		log.Printf("Invalid SST file format for file %s\n", sstFile)
		return nil, false
	}

	// Read entry count
	var entryCount uint32
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		log.Printf("Error reading entry count from SST file %s: %v\n", sstFile, err)
		return nil, false
	}
	log.Printf("Entry Count: %d\n", entryCount)

	// Read smallest key length
	var smallestKeyLength uint32
	if err := binary.Read(file, binary.LittleEndian, &smallestKeyLength); err != nil {
		log.Printf("Error reading smallest key length from SST file %s: %v\n", sstFile, err)
		return nil, false
	}
	log.Printf("Smallest Key Length: %d\n", smallestKeyLength)

	// Read largest key length
	var largestKeyLength uint32
	if err := binary.Read(file, binary.LittleEndian, &largestKeyLength); err != nil {
		log.Printf("Error reading largest key length from SST file %s: %v\n", sstFile, err)
		return nil, false
	}
	log.Printf("Largest Key Length: %d\n", largestKeyLength)

	// Iterate through entries in the SST file
	for i := uint32(0); i < entryCount; i++ {
		var operationMarker uint16
		if err := binary.Read(file, binary.LittleEndian, &operationMarker); err != nil {
			log.Printf("Error reading operation marker from SST file %s: %v\n", sstFile, err)
			return nil, false
		}
		log.Printf("Operation Marker: %d\n", operationMarker)

		var keyLength uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLength); err != nil {
			log.Printf("Error reading key length from SST file %s: %v\n", sstFile, err)
			return nil, false
		}
		log.Printf("Key Length: %d\n", keyLength)

		var valueLength uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLength); err != nil {
			log.Printf("Error reading value length from SST file %s: %v\n", sstFile, err)
			return nil, false
		}
		log.Printf("Value Length: %d\n", valueLength)

		keyBytes := make([]byte, keyLength)
		if _, err := file.Read(keyBytes); err != nil {
			log.Printf("Error reading key from SST file %s: %v\n", sstFile, err)
			return nil, false
		}
		log.Printf("Key: %s\n", keyBytes)

		valueBytes := make([]byte, valueLength)
		if _, err := file.Read(valueBytes); err != nil {
			log.Printf("Error reading value from SST file %s: %v\n", sstFile, err)
			return nil, false
		}
		log.Printf("Value: %s\n", valueBytes)

		if string(keyBytes) == key {
			// Check if the key is marked as deleted
			if operationMarker == 1 {
				log.Printf("Key marked as deleted: %s\n", key)
				return []byte("DELETED"), true // Key is marked as deleted
			}

			return valueBytes, true // Key found
		}
	}

	return nil, false // Key not found in this SST file
}

// fileInfoModTime returns the modification time of a file.
func fileInfoModTime(filename string) time.Time {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		log.Printf("Error getting file info for %s: %v\n", filename, err)
		return time.Time{}
	}
	return fileInfo.ModTime()
}

// handleGet handles the GET request for retrieving a key's value.
func handleGet(kv *KeyValueStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		value, ok := kv.Get(key)

		if ok {
			fmt.Fprintf(w, "Value: %s\n", string(value))
		} else {
			fmt.Fprintf(w, "Key not found\n")
		}
	}
}	

// handleDelete handles the DELETE request for deleting a key.
func handleDelete(kv *KeyValueStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		value, ok := kv.Delete(key)
		if ok {
			fmt.Fprintf(w, "Deleted key: %s, Value: %s\n", key, value)
		} else {
			fmt.Fprintf(w, "Key not found\n")
		}
	}
}


func main() {
	walFilePath := "wal.log" 

    kv, err := NewKeyValueStore(walFilePath)
    if err != nil {
        log.Fatal("Error creating KeyValueStore:", err)
    }
    defer kv.CloseWAL()

    // Recover from WAL on system restart
    if err := kv.RecoverFromWAL(); err != nil {
        log.Printf("Error recovering from WAL: %v\n", err)
        // Handle the error as needed (e.g., log it or terminate the application)
    }

    // Start the HTTP server
    router := http.NewServeMux()
    router.HandleFunc("/get", handleGet(kv))
    router.HandleFunc("/set", handleSet(kv))
    router.HandleFunc("/del", handleDelete(kv))

    port := 8080
    log.Printf("Server listening on :%d...\n", port)
    log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), router))
}