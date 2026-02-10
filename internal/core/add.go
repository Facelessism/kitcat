package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/LeeFred3042U/kitcat/internal/storage"
)

// AddFile stages a file or directory.
// If inputPath is a directory, it recursively stages all files inside.
// Expects a repo-relative path (or a path that passes IsSafePath).
// Stores metadata (mtime, size) so future `AddAll` can avoid re-hashing unchanged files.
//
// Behaviour and invariants:
//   - Accepts an input path (file or directory). The function resolves absolute
//     paths and *stores only repo-relative paths* in the index. This prevents
//     split-brain (absolute vs relative) and ensures tree-hash determinism.
//   - The index update happens inside a single UpdateIndexWithMeta transaction
//     to avoid races and to keep metadata consistent.
//   - Uses size+modtime as a fast-path to avoid re-hashing unchanged files.
//   - Honors ignore rules and repository safety checks (IsSafePath).
func AddFile(inputPath string) error {
	// Step 1: Ensure we are inside a kitcat repository.
	if _, err := os.Stat(RepoDir); os.IsNotExist(err) {
		return errors.New("not a kitcat repository (run `kitcat init`)")
	}

	// Step 2: Resolve the absolute path of the input.
	absInputPath, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	// Step 3: Resolve the absolute path of the repo root.
	absRepoRoot, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("failed to resolve repo root: %w", err)
	}

	// Check if the file exists
	if _, err := os.Stat(absInputPath); os.IsNotExist(err) {
		return fmt.Errorf("path does not exist: %s", inputPath)
	}

	// Step 4: Open the Index Transaction ONCE.
	// We do the walking and hashing inside the lock to ensure consistency.
	return storage.UpdateIndexWithMeta(func(index map[string]storage.IndexEntry) error {
		ignorePatterns, err := LoadIgnorePatterns()
		if err != nil {
			return err
		}

		// Build a simple proxy for legacy ShouldIgnore behaviour.
		proxyIndex := make(map[string]string, len(index))
		for k, v := range index {
			proxyIndex[k] = v.Hash
		}

		// Step 5: Walk the target (File or Directory).
		// filepath.Walk works for both. If absInputPath is a file, the func runs once.
		return filepath.Walk(absInputPath, func(fullPath string, info os.FileInfo, err error) error {
			if err != nil {
				return err // Permission errors, etc.
			}

			// Step 6: Convert absolute file path → repo-relative path.
			// This is CRITICAL for portability and tree determinism.
			relPath, err := filepath.Rel(absRepoRoot, fullPath)
			if err != nil {
				return fmt.Errorf("file %s is outside repository", fullPath)
			}
			cleanPath := filepath.Clean(relPath)

			// Skip the repo root itself and .kitcat directory
			if cleanPath == "." {
				return nil
			}
			if strings.HasPrefix(cleanPath, RepoDir+string(os.PathSeparator)) || cleanPath == RepoDir {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			// We only care about files
			if info.IsDir() {
				return nil
			}

			// Step 7: Enforce repository safety rules.
			if !IsSafePath(cleanPath) {
				return nil // Skip unsafe paths during walk
			}

			// Check ignore rules
			if ShouldIgnore(cleanPath, ignorePatterns, proxyIndex) {
				return nil
			}

			// Step 8: Metadata Check (Optimization).
			// If size & mtime match index, skip hashing.
			if entry, exists := index[cleanPath]; exists {
				if entry.Size == info.Size() && entry.ModTime == info.ModTime().Unix() {
					return nil
				}
			}

			// Step 9: Hash and store the file content.
			// We use fullPath (absolute) to read, ensuring we find the file correctly.
			hash, err := storage.HashAndStoreFile(fullPath)
			if err != nil {
				return fmt.Errorf("failed to hash %s: %w", fullPath, err)
			}

			// Step 10: Update the index using ONLY the repo-relative path.
			index[cleanPath] = storage.IndexEntry{
				Hash:    hash,
				ModTime: info.ModTime().Unix(),
				Size:    info.Size(),
			}

			return nil
		})
	})
}

// AddAll scans the working tree and updates the index:
//   - skips files matching ignore patterns
//   - skips files whose (size, mtime) match index metadata (fast path)
//   - hashes and stores changed/new files
//   - deletes index entries for files no longer present in the walk root
//
// Behaviour and invariants:
//   - Walks the canonical repo root (absolute), computes repo-relative paths,
//     and updates the index using those repo-relative keys.
//   - Skips files matching ignore rules and paths failing IsSafePath.
//   - Uses (size, mtime) as a fast-path to avoid re-hashing unchanged files.
//   - Removes index entries for files that are not present under the walked root.
func AddAll() error {
	return storage.UpdateIndexWithMeta(func(index map[string]storage.IndexEntry) error {
		ignorePatterns, err := LoadIgnorePatterns()
		if err != nil {
			return err
		}

		seen := make(map[string]bool, len(index))

		// Build a simple proxy for legacy ShouldIgnore behaviour.
		proxyIndex := make(map[string]string, len(index))
		for k, v := range index {
			proxyIndex[k] = v.Hash
		}

		// Walk the canonical absolute root to avoid "works on my machine" path bugs.
		rootDir, err := filepath.Abs(".")
		if err != nil {
			return fmt.Errorf("failed to resolve absolute path: %w", err)
		}

		err = filepath.Walk(rootDir, func(fullPath string, info os.FileInfo, err error) error {
			if err != nil {
				return err // propagate I/O errors
			}

			// Convert to repo-relative, normalized path.
			relPath, err := filepath.Rel(rootDir, fullPath)
			if err != nil {
				return nil
			}
			cleanPath := filepath.Clean(relPath)
			if cleanPath == "." {
				return nil
			}

			// Safety: ensure path is safe and skip internal repo directory.
			if !IsSafePath(cleanPath) {
				return nil
			}
			if strings.HasPrefix(cleanPath, RepoDir+string(os.PathSeparator)) || cleanPath == RepoDir {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if info.IsDir() {
				return nil
			}

			// Check ignore rules (using proxy for legacy compatibility).
			if ShouldIgnore(cleanPath, ignorePatterns, proxyIndex) {
				return nil
			}

			// Mark as seen for later deletion-detection.
			seen[cleanPath] = true

			// Fast path: if size & mtime match, assume unchanged.
			if entry, exists := index[cleanPath]; exists {
				if entry.Size == info.Size() && entry.ModTime == info.ModTime().Unix() {
					return nil
				}
			}

			// Slow path: hash & store file.
			// Use fullPath (absolute) to ensure correct file reading.
			hash, err := storage.HashAndStoreFile(fullPath)
			if err != nil {
				fmt.Printf("warning: could not add file %s: %v\n", cleanPath, err)
				return nil
			}

			index[cleanPath] = storage.IndexEntry{
				Hash:    hash,
				ModTime: info.ModTime().Unix(),
				Size:    info.Size(),
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Delete index entries that were not seen during the walk.
		var toDelete []string
		for pathInIndex := range index {
			if !seen[pathInIndex] {
				toDelete = append(toDelete, pathInIndex)
			}
		}
		for _, path := range toDelete {
			delete(index, path)
		}

		return nil
	})
}
