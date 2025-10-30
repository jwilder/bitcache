package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jwilder/bitcache"

	"github.com/spf13/cobra"
)

var (
	dataDir     string
	segmentSize int64
	cache       bitcache.Cache[[]byte]
	db          *bitcache.DiskCache[[]byte]
)

var rootCmd = &cobra.Command{
	Use:   "bitcache",
	Short: "BitCache is a simple embedded key/value store",
	Long:  `BitCache is a simple embedded key/value store inspired by the Bitcask design.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip for commands that have their own initialization
		if cmd.Name() == "bench" || cmd.Name() == "set" || cmd.Name() == "compact" {
			return nil
		}
		// Initialize the database for all other commands
		var err error
		db, err = bitcache.NewDiskCache(dataDir, bitcache.ByteSliceMarshaler{})
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		cache, err = bitcache.NewMemCache(db, bitcache.MemCacheConfig{
			MaxMemoryBytes: 250 * 1024 * 1024,
		})
		return err
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		// Skip for commands that have their own cleanup
		if cmd.Name() == "bench" || cmd.Name() == "set" || cmd.Name() == "compact" {
			return nil
		}
		// Close the database after command execution
		if cache != nil {
			return cache.Close()
		}
		return nil
	},
}

var setCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "Set a key-value pair",
	Long:  `Store a key-value pair in the database.`,
	Args:  cobra.ExactArgs(2),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize database with custom segment size if specified
		var err error
		if segmentSize > 0 {
			db, err = bitcache.NewDiskCacheWithConfig(dataDir, bitcache.DiskCacheConfig{
				MaxSegmentSize: segmentSize,
			}, bitcache.ByteSliceMarshaler{})
		} else {
			db, err = bitcache.NewDiskCache(dataDir, bitcache.ByteSliceMarshaler{})
		}
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		cache, err = bitcache.NewMemCache(db, bitcache.MemCacheConfig{
			MaxMemoryBytes: 250 * 1024 * 1024,
		})
		return err
	},
	PostRunE: func(cmd *cobra.Command, args []string) error {
		// Close the database after command execution
		if cache != nil {
			return cache.Close()
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]
		value := args[1]

		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}

		fmt.Printf("Set %s = %s\n", key, value)
		return nil
	},
}

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get a value by key",
	Long:  `Retrieve the value associated with a key from the database.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		value, err := cache.Get([]byte(key))
		if err != nil {
			// Exit cleanly without printing anything if key not found
			if err == bitcache.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("failed to get key: %w", err)
		}

		fmt.Printf("%s\n", string(value))
		return nil
	},
}

var delCmd = &cobra.Command{
	Use:   "del [key]",
	Short: "Delete a key",
	Long:  `Remove a key and its associated value from the database.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		if err := cache.Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}

		fmt.Printf("Deleted %s\n", key)
		return nil
	},
}

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Compact the database",
	Long: `Compact the database by removing obsolete entries and reclaiming disk space.

Modes:
  Default: Automatic compaction (LSM or GC as needed)
  --list: List segments by level
  --gc: Perform garbage collection to reclaim dead data
  --auto: Automatic compaction (same as default)`,
	Args: cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Initialize database with custom segment size if specified
		var err error
		if segmentSize > 0 {
			db, err = bitcache.NewDiskCacheWithConfig(dataDir, bitcache.DiskCacheConfig{
				MaxSegmentSize: segmentSize,
			}, bitcache.ByteSliceMarshaler{})
		} else {
			db, err = bitcache.NewDiskCache(dataDir, bitcache.ByteSliceMarshaler{})
		}
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		cache, err = bitcache.NewMemCache(db, bitcache.MemCacheConfig{
			MaxMemoryBytes: 250 * 1024 * 1024,
		})
		return err
	},
	PostRunE: func(cmd *cobra.Command, args []string) error {
		// Close the database after command execution
		if cache != nil {
			return cache.Close()
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// List mode
		if compactList {
			return listSegments()
		}

		// Auto compaction mode
		if compactAuto {
			return performAutoCompaction()
		}

		// Garbage collection mode
		if compactGC {
			return performGarbageCollection()
		}

		// Default: Auto-compact mode (LSM or GC as needed)
		fmt.Println("Starting automatic compaction...")

		result, err := db.Compact()
		if err != nil {
			return fmt.Errorf("compaction failed: %w", err)
		}

		// Log compaction details
		if result != nil && result.Type != "none" {
			fmt.Println("\n=== Compaction Details ===")
			fmt.Printf("Type: %s\n", result.Type)
			if result.Level >= 0 {
				fmt.Printf("Level: %d\n", result.Level)
			}
			if len(result.InputSegments) > 0 {
				fmt.Printf("Input segments: %d\n", len(result.InputSegments))
			}
			if result.OutputSegment != "" {
				fmt.Printf("Output segment: %s\n", result.OutputSegment)
			}
			if result.LiveEntries > 0 {
				fmt.Printf("Live entries: %d\n", result.LiveEntries)
			}
			if result.BytesWritten > 0 {
				fmt.Printf("Bytes written: %d\n", result.BytesWritten)
			}
			if len(result.DeletedSegments) > 0 {
				fmt.Printf("Deleted segments: %d\n", len(result.DeletedSegments))
			}
		} else {
			fmt.Println("No compaction needed")
		}

		fmt.Println("\nCompaction completed successfully")
		return nil
	},
}

func listSegments() error {
	byLevel, err := db.GetSegmentsByLevel()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	fmt.Println("\nSegments by level:")
	for level := uint8(0); level <= 4; level++ {
		segments := byLevel[level]
		if len(segments) == 0 {
			continue
		}

		fmt.Printf("\nLevel %d (%d segments):\n", level, len(segments))
		for _, seg := range segments {
			fmt.Printf("  %s (%d bytes)\n", seg.ID.String(), seg.TotalBytes)
		}
	}
	return nil
}

func performGarbageCollection() error {
	fmt.Println("\n=== Garbage Collection ===")
	fmt.Println("Scanning segments for dead data...")

	// Get GC stats (which internally scans all segments)
	gcStats := db.GetGCStats()
	segmentStats := gcStats.SegmentStats

	if len(segmentStats) == 0 {
		fmt.Println("No segments to scan")
		return nil
	}

	fmt.Printf("\nFound %d segments:\n\n", len(segmentStats))

	// Display statistics for each segment
	var totalBytes, totalLive, totalDead int64
	var segmentsNeedingGC int

	for _, seg := range segmentStats {
		deadPct := seg.DeadRatio * 100
		fmt.Printf("Segment %d:\n", seg.FileID)
		fmt.Printf("  Total: %d bytes, Live: %d bytes, Dead: %d bytes\n",
			seg.TotalBytes, seg.LiveBytes, seg.DeadBytes)
		fmt.Printf("  Dead: %.1f%%", deadPct)

		if seg.DeadRatio >= 0.4 {
			fmt.Printf(" ⚠️  (needs GC)")
			segmentsNeedingGC++
		}
		fmt.Println()

		totalBytes += seg.TotalBytes
		totalLive += seg.LiveBytes
		totalDead += seg.DeadBytes
	}

	// Show summary
	fmt.Println("\nSummary:")
	fmt.Printf("  Total data: %d bytes\n", totalBytes)
	fmt.Printf("  Live data: %d bytes\n", totalLive)
	fmt.Printf("  Dead data: %d bytes", totalDead)
	if totalBytes > 0 {
		overallDeadPct := float64(totalDead) / float64(totalBytes) * 100
		fmt.Printf(" (%.1f%%)\n", overallDeadPct)
	} else {
		fmt.Println()
	}
	fmt.Printf("  Segments needing GC: %d\n", segmentsNeedingGC)

	if compactDryRun {
		fmt.Println("\n[DRY RUN] Would perform garbage collection")
		fmt.Println("Use without --dry-run to actually perform GC")
		return nil
	}

	if segmentsNeedingGC == 0 {
		fmt.Println("\n✓ No segments need GC at this time")
		return nil
	}

	// Perform integrated compaction (includes GC)
	beforeStats := db.Stats()
	fmt.Println("\nRunning compaction (includes GC)...")
	result, err := db.Compact()
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Show results
	afterStats := db.Stats()
	fmt.Printf("\n✓ Compaction completed\n\n")

	if result != nil && result.Type != "none" {
		fmt.Printf("Compaction type: %s\n", result.Type)
		if len(result.InputSegments) > 0 {
			fmt.Printf("Processed segments: %d\n", len(result.InputSegments))
		}
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Keys: %d\n", afterStats.Keys)
	fmt.Printf("  Data size: %d bytes", afterStats.DataSize)

	if beforeStats.DataSize > afterStats.DataSize {
		saved := beforeStats.DataSize - afterStats.DataSize
		pct := float64(saved) / float64(beforeStats.DataSize) * 100
		fmt.Printf(" (saved %d bytes, %.1f%%)\n", saved, pct)
	} else {
		fmt.Println()
	}

	// Show final segment layout
	return listSegments()
}

func performAutoCompaction() error {
	fmt.Println("\n=== Automatic Compaction ===")
	fmt.Println("Determining optimal compaction strategy...")

	// Show before state
	byLevel, err := db.GetSegmentsByLevel()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	fmt.Println("\nBefore compaction:")
	for level := uint8(0); level <= 4; level++ {
		if len(byLevel[level]) > 0 {
			fmt.Printf("  L%d: %d segments\n", level, len(byLevel[level]))
		}
	}

	// Perform automatic compaction
	fmt.Println("\nRunning Compact()...")
	beforeStats := db.Stats()

	result, err := db.Compact()
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Show after state
	byLevel, _ = db.GetSegmentsByLevel()
	afterStats := db.Stats()

	fmt.Println("\n✓ Compaction completed")

	if result != nil && result.Type != "none" {
		fmt.Printf("\nCompaction performed: %s at level %d\n", result.Type, result.Level)
		if len(result.InputSegments) > 0 {
			fmt.Printf("Processed %d segments\n", len(result.InputSegments))
		}
	} else {
		fmt.Println("\nNo compaction was needed")
	}

	fmt.Println("\nAfter compaction:")
	for level := uint8(0); level <= 4; level++ {
		if len(byLevel[level]) > 0 {
			fmt.Printf("  L%d: %d segments\n", level, len(byLevel[level]))
		}
	}

	// Show data size change
	if beforeStats.DataSize != afterStats.DataSize {
		if beforeStats.DataSize > afterStats.DataSize {
			saved := beforeStats.DataSize - afterStats.DataSize
			pct := float64(saved) / float64(beforeStats.DataSize) * 100
			fmt.Printf("\nSpace reclaimed: %d bytes (%.1f%%)\n", saved, pct)
		}
	} else {
		fmt.Println("\nNo compaction was needed")
	}

	// Show detailed segment info
	return listSegments()
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Display database statistics",
	Long:  `Display statistics about the database including number of keys, reads, writes, deletes, and data size.`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		stats := cache.Stats()

		fmt.Println("=== Database Statistics ===")
		fmt.Printf("Keys: %d\n", stats.Keys)
		fmt.Printf("Segments: %d\n", stats.Segments)
		fmt.Printf("Reads: %d\n", stats.Reads)
		fmt.Printf("Writes: %d\n", stats.Writes)
		fmt.Printf("Deletes: %d\n", stats.Deletes)
		fmt.Printf("Data size: %.2f MB (%d bytes)\n", float64(stats.DataSize)/(1024*1024), stats.DataSize)

		// Display memory cache statistics if available
		if mc, ok := cache.(*bitcache.MemCache[[]byte]); ok {
			memStats := mc.MemStats()
			fmt.Println("\n=== Memory Cache Statistics ===")
			fmt.Printf("Cached entries: %d\n", memStats.Entries)
			fmt.Printf("Shards: %d\n", memStats.Shards)
			fmt.Printf("Cache hits: %d\n", memStats.Hits)
			fmt.Printf("Cache misses: %d\n", memStats.Misses)
			fmt.Printf("Hit rate: %.2f%%\n", memStats.HitRate*100)
		}

		return nil
	},
}

var (
	scanPrefix    string
	compactList   bool
	compactDryRun bool
	compactGC     bool
	compactAuto   bool
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan and print keys with optional prefix filter",
	Long:  `Iterate through all keys in the database and print them. Optionally filter by prefix.`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		count := 0
		prefix := []byte(scanPrefix)

		if len(prefix) > 0 {
			fmt.Printf("Scanning keys with prefix: %q\n", scanPrefix)
		} else {
			fmt.Println("Scanning all keys...")
		}

		err := cache.Scan(prefix, func(key []byte) bool {
			fmt.Printf("%s\n", string(key))
			count++
			return false // continue iteration
		})

		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		fmt.Printf("\nTotal keys found: %d\n", count)
		return nil
	},
}

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify database integrity",
	Long:  `Verify the integrity of the database by checking if all keys can be read successfully.`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Verifying database integrity...")

		// For benchmark verification, check the deterministic keys
		keys := make([][]byte, benchKeys)
		for i := 0; i < benchKeys; i++ {
			keys[i] = generateDeterministicKey(benchKeySize, i)
		}

		fmt.Printf("Checking %d benchmark keys...\n", benchKeys)

		foundKeys := 0
		errorKeys := 0
		missingKeys := 0

		for i, key := range keys {
			value, err := cache.Get(key)
			if err == bitcache.ErrKeyNotFound {
				missingKeys++
			} else if err != nil {
				errorKeys++
				fmt.Printf("Error reading key %d: %v\n", i, err)
			} else {
				foundKeys++
				// Verify the value size is correct
				expectedSize := benchValueSize
				if len(value) != expectedSize {
					fmt.Printf("Key %d has incorrect value size: got %d, expected %d\n",
						i, len(value), expectedSize)
					errorKeys++
				}
			}

			// Show progress for large key sets
			if benchKeys >= 1000 && (i+1)%(benchKeys/10) == 0 {
				progress := float64(i+1) / float64(benchKeys) * 100
				fmt.Printf("  Progress: %.0f%% (%d/%d keys checked)\n", progress, i+1, benchKeys)
			}
		}

		fmt.Println("\n=== Verification Results ===")
		fmt.Printf("Total keys checked: %d\n", benchKeys)
		fmt.Printf("Found: %d\n", foundKeys)
		fmt.Printf("Missing: %d\n", missingKeys)
		fmt.Printf("Errors: %d\n", errorKeys)

		if errorKeys > 0 {
			return fmt.Errorf("database verification failed with %d errors", errorKeys)
		}

		if foundKeys > 0 {
			fmt.Println("✓ Database verification passed for existing keys")
		} else {
			fmt.Println("⚠ No keys found in database")
		}

		return nil
	},
}

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Start an interactive REPL session",
	Long:  `Start an interactive REPL (Read-Eval-Print Loop) session to interact with the database. Available commands: get, set, delete, scan, stats, help, exit/quit.`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("BitCache REPL - Interactive Session")
		fmt.Println("Type 'help' for available commands, 'exit' or 'quit' to exit")
		fmt.Println()

		cache.Scan(nil, func(key []byte) bool {
			cache.Get(key)
			return false
		})

		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("bitcache> ")
			if !scanner.Scan() {
				break
			}

			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			parts := strings.Fields(line)
			if len(parts) == 0 {
				continue
			}

			command := strings.ToLower(parts[0])

			switch command {
			case "exit", "quit":
				fmt.Println("Goodbye!")
				return nil

			case "help":
				fmt.Println("Available commands:")
				fmt.Println("  get <key>           - Get value for key")
				fmt.Println("  set <key> <value>   - Set key to value")
				fmt.Println("  delete <key>        - Delete key")
				fmt.Println("  scan [prefix]       - Scan all keys or keys with prefix")
				fmt.Println("  stats               - Show database statistics")
				fmt.Println("  help                - Show this help message")
				fmt.Println("  exit/quit           - Exit REPL")

			case "get":
				if len(parts) < 2 {
					fmt.Println("Error: get requires a key argument")
					fmt.Println("Usage: get <key>")
					continue
				}
				key := parts[1]
				value, err := cache.Get([]byte(key))
				if err != nil {
					if err == bitcache.ErrKeyNotFound {
						fmt.Println("(nil)")
					} else {
						fmt.Printf("Error: %v\n", err)
					}
					continue
				}
				fmt.Printf("%s\n", string(value))

			case "set":
				if len(parts) < 3 {
					fmt.Println("Error: set requires key and value arguments")
					fmt.Println("Usage: set <key> <value>")
					continue
				}
				key := parts[1]
				// Join remaining parts as value (allows spaces in values)
				value := strings.Join(parts[2:], " ")

				if err := cache.Set([]byte(key), []byte(value)); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				fmt.Println("OK")

			case "delete", "del":
				if len(parts) < 2 {
					fmt.Println("Error: delete requires a key argument")
					fmt.Println("Usage: delete <key>")
					continue
				}
				key := parts[1]
				if err := cache.Delete([]byte(key)); err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				fmt.Println("OK")

			case "scan":
				var prefix []byte
				if len(parts) >= 2 {
					prefix = []byte(parts[1])
					fmt.Printf("Scanning keys with prefix: %q\n", parts[1])
				}

				count := 0
				err := cache.Scan(prefix, func(key []byte) bool {
					fmt.Printf("%s\n", string(key))
					count++
					return false // continue iteration
				})

				if err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				fmt.Printf("(%d keys found)\n", count)

			case "stats":
				stats := cache.Stats()
				fmt.Println("Database Statistics:")
				fmt.Printf("  Keys: %d\n", stats.Keys)
				fmt.Printf("  Segments: %d\n", stats.Segments)
				fmt.Printf("  Reads: %d\n", stats.Reads)
				fmt.Printf("  Writes: %d\n", stats.Writes)
				fmt.Printf("  Deletes: %d\n", stats.Deletes)
				fmt.Printf("  Data size: %.2f MB (%d bytes)\n", float64(stats.DataSize)/(1024*1024), stats.DataSize)

				// Display memory cache statistics if available
				if mc, ok := cache.(*bitcache.MemCache[[]byte]); ok {
					memStats := mc.MemStats()
					fmt.Println("\nMemory Cache Statistics:")
					fmt.Printf("  Cached entries: %d\n", memStats.Entries)
					fmt.Printf("  Shards: %d\n", memStats.Shards)
					fmt.Printf("  Cache hits: %d\n", memStats.Hits)
					fmt.Printf("  Cache misses: %d\n", memStats.Misses)
					fmt.Printf("  Hit rate: %.2f%%\n", memStats.HitRate*100)
				}

			default:
				fmt.Printf("Unknown command: %s\n", command)
				fmt.Println("Type 'help' for available commands")
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}

		return nil
	},
}

var (
	benchKeys        int
	benchKeySize     int
	benchValueSize   int
	benchTotalWrites int
	benchOpType      string
	benchSegmentSize int64
	verifyKeys       int
)

type SystemInfo struct {
	OS       string
	CPUCores int
	Memory   string
	DiskType string
}

func getSystemInfo() SystemInfo {
	info := SystemInfo{
		OS:       runtime.GOOS + "/" + runtime.GOARCH,
		CPUCores: runtime.NumCPU(),
		Memory:   "unknown",
		DiskType: "unknown",
	}

	// Get memory information
	switch runtime.GOOS {
	case "darwin":
		// macOS
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			memBytes, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
			info.Memory = fmt.Sprintf("%.1f GB", float64(memBytes)/(1024*1024*1024))
		}
		// Try to detect disk type on macOS
		if out, err := exec.Command("diskutil", "info", "/").Output(); err == nil {
			output := string(out)
			if strings.Contains(output, "Solid State") || strings.Contains(output, "SSD") {
				info.DiskType = "SSD"
			} else if strings.Contains(output, "Rotational") || strings.Contains(output, "HDD") {
				info.DiskType = "HDD"
			}
		}
	case "linux":
		// Linux - read from /proc/meminfo
		if out, err := exec.Command("grep", "MemTotal", "/proc/meminfo").Output(); err == nil {
			fields := strings.Fields(string(out))
			if len(fields) >= 2 {
				memKB, _ := strconv.ParseInt(fields[1], 10, 64)
				info.Memory = fmt.Sprintf("%.1f GB", float64(memKB)/(1024*1024))
			}
		}
		// Try to detect disk type on Linux
		if out, err := exec.Command("sh", "-c", "lsblk -d -o name,rota | grep -v loop").Output(); err == nil {
			output := string(out)
			if strings.Contains(output, " 0") {
				info.DiskType = "SSD"
			} else if strings.Contains(output, " 1") {
				info.DiskType = "HDD"
			}
		}
	}

	return info
}

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run performance benchmarks",
	Long:  `Run performance benchmarks for write or read operations with configurable parameters.`,
	Args:  cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Create custom DiskCache with segment size if specified
		var err error
		if benchSegmentSize > 0 {
			db, err = bitcache.NewDiskCacheWithConfig(dataDir, bitcache.DiskCacheConfig{
				MaxSegmentSize: benchSegmentSize,
			}, bitcache.ByteSliceMarshaler{})
		} else {
			db, err = bitcache.NewDiskCache(dataDir, bitcache.ByteSliceMarshaler{})
		}
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		cache, err = bitcache.NewMemCache(db, bitcache.MemCacheConfig{
			MaxMemoryBytes: 50 * 1024 * 1024,
		})
		return err
	},
	PostRunE: func(cmd *cobra.Command, args []string) error {
		// Close the database after benchmark
		if cache != nil {
			return cache.Close()
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if benchOpType != "write" && benchOpType != "read" {
			return fmt.Errorf("operation type must be 'write' or 'read', got: %s", benchOpType)
		}

		// Get and display system information
		sysInfo := getSystemInfo()
		fmt.Println("=== System Information ===")
		fmt.Printf("OS: %s\n", sysInfo.OS)
		fmt.Printf("CPU Cores: %d\n", sysInfo.CPUCores)
		fmt.Printf("Memory: %s\n", sysInfo.Memory)
		fmt.Printf("Disk Type: %s\n", sysInfo.DiskType)
		fmt.Println()

		fmt.Printf("Running %s benchmark...\n", benchOpType)
		fmt.Printf("Configuration:\n")
		fmt.Printf("  Keys: %d\n", benchKeys)
		fmt.Printf("  Key size: %d bytes\n", benchKeySize)
		fmt.Printf("  Value size: %d bytes\n", benchValueSize)
		if benchSegmentSize > 0 {
			fmt.Printf("  Segment size: %.2f MB (%d bytes)\n", float64(benchSegmentSize)/(1024*1024), benchSegmentSize)
		}

		if benchOpType == "write" {
			fmt.Printf("  Total writes: %d\n", benchTotalWrites)
			return runWriteBench()
		} else {
			fmt.Printf("  Total reads: %d\n", benchTotalWrites)
			return runReadBench()
		}
	},
}

func runWriteBench() error {
	// Generate deterministic keys based on the number of unique keys
	// Using a fixed seed ensures repeated runs with same flags generate same data
	keys := make([][]byte, benchKeys)
	for i := 0; i < benchKeys; i++ {
		keys[i] = generateDeterministicKey(benchKeySize, i)
	}

	// Generate a deterministic value template
	valueTemplate := generateDeterministicData(benchValueSize, 0)

	fmt.Println("\nStarting write benchmark...")
	startTime := time.Now()

	// Perform writes
	for i := 0; i < benchTotalWrites; i++ {
		// Cycle through keys
		keyIdx := i % benchKeys
		key := keys[keyIdx]

		// Create a unique value for each write
		value := make([]byte, len(valueTemplate))
		copy(value, valueTemplate)
		// Add some variation to the value
		if len(value) >= 8 {
			// Embed the write counter in the value
			for j := 0; j < 8 && j < len(value); j++ {
				value[j] = byte(i >> (j * 8))
			}
		}

		if err := cache.Set(key, value); err != nil {
			return fmt.Errorf("failed to write key at iteration %d: %w", i, err)
		}

		// Print progress every 10% for large benchmarks
		if benchTotalWrites >= 1000 && (i+1)%(benchTotalWrites/10) == 0 {
			progress := float64(i+1) / float64(benchTotalWrites) * 100
			elapsed := time.Since(startTime)
			fmt.Printf("  Progress: %.0f%% (%d/%d writes, %v elapsed)\n",
				progress, i+1, benchTotalWrites, elapsed.Round(time.Millisecond))
		}
	}

	duration := time.Since(startTime)

	// Calculate statistics
	throughput := float64(benchTotalWrites) / duration.Seconds()
	avgLatency := duration / time.Duration(benchTotalWrites)
	totalDataWritten := int64(benchTotalWrites) * int64(benchKeySize+benchValueSize)
	dataThroughput := float64(totalDataWritten) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Println("\n=== Write Benchmark Results ===")
	fmt.Printf("Total writes: %d\n", benchTotalWrites)
	fmt.Printf("Unique keys: %d\n", benchKeys)
	fmt.Printf("Duration: %v\n", duration.Round(time.Millisecond))
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Average latency: %v\n", avgLatency)
	fmt.Printf("Data written: %.2f MB\n", float64(totalDataWritten)/(1024*1024))
	fmt.Printf("Data throughput: %.2f MB/sec\n", dataThroughput)

	return nil
}

func runReadBench() error {
	// Generate deterministic keys - same as write bench
	keys := make([][]byte, benchKeys)
	for i := 0; i < benchKeys; i++ {
		keys[i] = generateDeterministicKey(benchKeySize, i)
	}

	// Check if keys exist - do NOT populate, just verify
	existingKeys := 0
	for i := 0; i < benchKeys; i++ {
		if cache.Has(keys[i]) {
			existingKeys++
		}
	}

	if existingKeys > 0 {
		fmt.Printf("Found %d existing keys in database (%.1f%% of requested keys)\n",
			existingKeys, float64(existingKeys)/float64(benchKeys)*100)
	} else {
		fmt.Printf("No existing keys found in database (will result in key not found errors)\n")
	}

	fmt.Println("\nStarting read benchmark...")
	startTime := time.Now()

	// Perform reads
	successfulReads := 0
	for i := 0; i < benchTotalWrites; i++ {
		// Cycle through keys
		keyIdx := i % benchKeys
		key := keys[keyIdx]

		_, err := cache.Get(key)
		if err != nil {
			if err == bitcache.ErrKeyNotFound {
				continue // Key not found, skip
			}
			return fmt.Errorf("failed to read key at iteration %d: %w", i, err)
		}
		successfulReads++

		// Print progress every 10% for large benchmarks
		if benchTotalWrites >= 1000 && (i+1)%(benchTotalWrites/10) == 0 {
			progress := float64(i+1) / float64(benchTotalWrites) * 100
			elapsed := time.Since(startTime)
			fmt.Printf("  Progress: %.0f%% (%d/%d reads, %v elapsed)\n",
				progress, i+1, benchTotalWrites, elapsed.Round(time.Millisecond))
		}
	}

	duration := time.Since(startTime)

	// Calculate statistics
	throughput := float64(benchTotalWrites) / duration.Seconds()
	avgLatency := duration / time.Duration(benchTotalWrites)
	totalDataRead := int64(successfulReads) * int64(benchKeySize+benchValueSize)
	dataThroughput := float64(totalDataRead) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Println("\n=== Read Benchmark Results ===")
	fmt.Printf("Total reads: %d\n", benchTotalWrites)
	fmt.Printf("Successful reads: %d\n", successfulReads)
	fmt.Printf("Unique keys: %d\n", benchKeys)
	fmt.Printf("Duration: %v\n", duration.Round(time.Millisecond))
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Average latency: %v\n", avgLatency)
	fmt.Printf("Data read: %.2f MB\n", float64(totalDataRead)/(1024*1024))
	fmt.Printf("Data throughput: %.2f MB/sec\n", dataThroughput)

	return nil
}

func generateDeterministicKey(size int, index int) []byte {
	// Generate ASCII keys with format: "key:00000000001" padded to the requested size
	keyStr := fmt.Sprintf("key:%d", index)

	// If the generated key is already at or over the requested size, truncate it
	if len(keyStr) >= size {
		return []byte(keyStr[:size])
	}

	// Pad with zeros to reach the requested size
	padding := size - len(keyStr)
	paddedKey := fmt.Sprintf("key:%0*d", len(fmt.Sprintf("%d", index))+padding, index)

	// If still not exact size (due to formatting), adjust
	if len(paddedKey) > size {
		return []byte(paddedKey[:size])
	} else if len(paddedKey) < size {
		// Pad with spaces at the end if needed
		return []byte(fmt.Sprintf("%-*s", size, paddedKey))
	}

	return []byte(paddedKey)
}

func generateDeterministicData(size int, seed int) []byte {
	// Use a fixed base seed to ensure deterministic data generation
	rnd := rand.New(rand.NewSource(54321 + int64(seed)))
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(rnd.Intn(256))
	}
	return data
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func init() {
	// Add persistent flag for data directory (required)
	rootCmd.PersistentFlags().StringVarP(&dataDir, "dir", "d", "", "Data directory (required)")
	rootCmd.MarkPersistentFlagRequired("dir")

	// Add bench command flags
	benchCmd.Flags().IntVarP(&benchKeys, "keys", "k", 1000, "Number of unique keys")
	benchCmd.Flags().IntVarP(&benchKeySize, "key-size", "s", 16, "Size of each key in bytes")
	benchCmd.Flags().IntVarP(&benchValueSize, "value-size", "v", 128, "Size of each value in bytes")
	benchCmd.Flags().IntVarP(&benchTotalWrites, "total", "t", 10000, "Total number of operations to perform")
	benchCmd.Flags().StringVarP(&benchOpType, "op", "o", "write", "Operation type: 'write' or 'read'")
	benchCmd.Flags().Int64Var(&benchSegmentSize, "segment-size", 0, "Maximum segment size in bytes (0 = default 16MB)")

	// Add verify command flags
	verifyCmd.Flags().IntVarP(&benchKeys, "keys", "k", 1000, "Number of keys to verify (should match benchmark)")
	verifyCmd.Flags().IntVarP(&benchKeySize, "key-size", "s", 16, "Size of each key in bytes")
	verifyCmd.Flags().IntVarP(&benchValueSize, "value-size", "v", 128, "Size of each value in bytes")

	// Add scan command flags
	scanCmd.Flags().StringVarP(&scanPrefix, "prefix", "p", "", "Filter keys by prefix")

	// Add compact command flags
	compactCmd.Flags().BoolVar(&compactList, "list", false, "List segments by level")
	compactCmd.Flags().BoolVar(&compactDryRun, "dry-run", false, "Show what would be compacted without doing it")
	compactCmd.Flags().BoolVar(&compactGC, "gc", false, "Perform garbage collection to reclaim dead data")
	compactCmd.Flags().BoolVar(&compactAuto, "auto", false, "Automatic compaction (LSM or GC as needed)")
	compactCmd.Flags().Int64Var(&segmentSize, "segment-size", 0, "Maximum segment size in bytes (0 = default 16MB)")

	// Add set command flags
	setCmd.Flags().Int64Var(&segmentSize, "segment-size", 0, "Maximum segment size in bytes (0 = default 16MB)")

	// Add subcommands
	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(delCmd)
	rootCmd.AddCommand(compactCmd)
	rootCmd.AddCommand(statsCmd)
	rootCmd.AddCommand(scanCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(replCmd)
	rootCmd.AddCommand(benchCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
