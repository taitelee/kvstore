package kv

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// FileWAL is an append-only write-ahead log stored on disk.
type FileWAL struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
}

// OpenWAL opens (or creates) a WAL at the given path.
func OpenWAL(path string) (*FileWAL, error) {
	// Open file with os.OpenFile
	//    - Create if not exists
	//    - Append mode
	//    - Read + write
	// Create a bufio.Writer
	// Return &FileWAL?

	return nil, nil
}


func (w *FileWAL) Append(op Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return nil
}


func (w *FileWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return nil
}
