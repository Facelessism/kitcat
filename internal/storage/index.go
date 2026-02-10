package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const indexPath = ".kitcat/index"

// IndexEntry holds the hash and metadata.
// Short JSON keys keep on-disk index compact.
type IndexEntry struct {
	Hash    string `json:"h"`
	ModTime int64  `json:"m,omitempty"` // Unix timestamp
	Size    int64  `json:"s,omitempty"` // File size in bytes
}

// LoadIndex returns the legacy map[path]hash view.
// Maintains backward compatibility for code that expects the simple form.
func LoadIndex() (map[string]string, error) {
	rawIndex, err := LoadIndexWithMeta()
	if err != nil {
		return nil, err
	}

	simpleIndex := make(map[string]string, len(rawIndex))
	for path, entry := range rawIndex {
		simpleIndex[path] = entry.Hash
	}
	return simpleIndex, nil
}

// LoadIndexWithMeta reads the index file and safely detects old vs new formats.
// Uses json.RawMessage + head-byte sniffing to avoid relying on json.Unmarshal's weak typing.
func LoadIndexWithMeta() (map[string]IndexEntry, error) {
	index := make(map[string]IndexEntry)

	content, err := os.ReadFile(indexPath)
	if os.IsNotExist(err) {
		// No index yet — empty repository state.
		return index, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not read index file: %w", err)
	}
	if len(content) == 0 {
		return index, nil
	}

	// Parse into a map of RawMessage so we can inspect each value exactly.
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(content, &rawMap); err != nil {
		return nil, fmt.Errorf("index file corruption: %w", err)
	}

	for path, rawValue := range rawMap {
		rawValue = bytes.TrimSpace(rawValue)
		if len(rawValue) == 0 {
			continue
		}

		switch rawValue[0] {
		case '"':
			// Legacy format: value is a JSON string containing the hash.
			var hash string
			if err := json.Unmarshal(rawValue, &hash); err != nil {
				return nil, fmt.Errorf("failed to decode legacy entry for %s: %w", path, err)
			}
			// Migrate into new struct form (empty metadata means 're-check later').
			index[path] = IndexEntry{Hash: hash}
		case '{':
			// New format: value is an object matching IndexEntry.
			var entry IndexEntry
			if err := json.Unmarshal(rawValue, &entry); err != nil {
				return nil, fmt.Errorf("failed to decode entry for %s: %w", path, err)
			}
			index[path] = entry
		default:
			// Unknown/garbage entry — warn and skip instead of crashing repo.
			fmt.Printf("warning: unknown index format for %s, skipping\n", path)
		}
	}

	return index, nil
}

// UpdateIndexWithMeta is the atomic update helper.
// It creates the .kitcat directory, obtains a file lock, loads the index,
// invokes the callback to mutate it, then writes it back atomically.
func UpdateIndexWithMeta(fn func(index map[string]IndexEntry) error) error {
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return err
	}

	l, err := lock(indexPath)
	if err != nil {
		return err
	}
	defer unlock(l)

	index, err := LoadIndexWithMeta()
	if err != nil {
		return err
	}

	if err := fn(index); err != nil {
		return err
	}

	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	return SafeWriteFile(indexPath, data, 0644)
}

// UpdateIndex adapts legacy callers that expect map[string]string.
// It reconciles deletions and updates back into the richer IndexEntry form.
func UpdateIndex(fn func(index map[string]string) error) error {
	return UpdateIndexWithMeta(func(realIndex map[string]IndexEntry) error {
		// Build proxy simple map for legacy callback.
		proxy := make(map[string]string, len(realIndex))
		for path, entry := range realIndex {
			proxy[path] = entry.Hash
		}

		if err := fn(proxy); err != nil {
			return err
		}

		// Deletions: collect keys to delete to avoid modifying map while ranging.
		var toDelete []string
		for path := range realIndex {
			if _, exists := proxy[path]; !exists {
				toDelete = append(toDelete, path)
			}
		}
		for _, path := range toDelete {
			delete(realIndex, path)
		}

		// Updates / additions: unset metadata so the file will be re-validated later.
		for path, newHash := range proxy {
			existing, exists := realIndex[path]
			if !exists || existing.Hash != newHash {
				realIndex[path] = IndexEntry{
					Hash:    newHash,
					ModTime: 0,
					Size:    0,
				}
			}
		}
		return nil
	})
}

// WriteIndex writes a simple hash map to the index, discarding metadata.
// This is used by operations like 'reset' or 'checkout' that reconstruct the index from a tree.
// It sets ModTime/Size to 0, forcing 'add' to re-verify files later.
func WriteIndex(simpleIndex map[string]string) error {
	richIndex := make(map[string]IndexEntry, len(simpleIndex))
	for path, hash := range simpleIndex {
		richIndex[path] = IndexEntry{
			Hash:    hash,
			ModTime: 0,
			Size:    0,
		}
	}

	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return err
	}

	l, err := lock(indexPath)
	if err != nil {
		return err
	}
	defer unlock(l)

	data, err := json.MarshalIndent(richIndex, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	return SafeWriteFile(indexPath, data, 0644)
}
