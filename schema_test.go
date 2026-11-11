package riflrpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Seed the actual flat layout used before schema v2, without opening the new
// service. Keep multiple replies, a remembered error, and another client's data.
func seedLegacy(t *testing.T, path string, withAcks bool) {
	t.Helper()
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		state, err := tx.CreateBucket(stateBucket)
		if err != nil {
			return err
		}
		value := sequenceKey(17)
		if err := state.Put(valueKey, value[:]); err != nil {
			return err
		}
		results, err := tx.CreateBucket(legacyResultsBucket)
		if err != nil {
			return err
		}
		for _, record := range []struct {
			id     RequestID
			result diskCompletion
		}{
			{RequestID{"Alice", 2}, diskCompletion{Delta: 7, Reply: AddReply{7}}},
			{RequestID{"Alice", 10}, diskCompletion{Delta: 5, Reply: AddReply{12}}},
			{RequestID{"Bob", 1}, diskCompletion{Delta: 5, Reply: AddReply{17}}},
			{RequestID{"Alice", 11}, diskCompletion{Delta: math.MaxInt64, Error: "counter overflow"}},
		} {
			key, err := json.Marshal(record.id)
			if err != nil {
				return err
			}
			data, err := json.Marshal(record.result)
			if err != nil {
				return err
			}
			if err := results.Put(key, data); err != nil {
				return err
			}
		}
		if withAcks {
			clients, err := tx.CreateBucket(clientsBucket)
			if err != nil {
				return err
			}
			boundary := sequenceKey(2)
			return clients.Put([]byte("Alice"), boundary[:])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMigrationPreservesResultsAndBoundaries(t *testing.T) {
	for _, withAcks := range []bool{false, true} {
		name := "before-acks"
		if withAcks {
			name = "with-acks"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			seedLegacy(t, path, withAcks)
			// Opening repeatedly must not repeat migration or lose saved state.
			for reopen := 0; reopen < 2; reopen++ {
				c, err := OpenCounter(path)
				if err != nil {
					t.Fatal(err)
				}
				func() {
					defer c.Close()
					assertV2Schema(t, c.db)
					for _, tc := range []struct {
						req  AddRequest
						want int64
					}{
						{testRequest("Alice", 2, 7, 1), 7},
						{testRequest("Alice", 10, 5, 1), 12},
						{testRequest("Bob", 1, 5, 1), 17},
					} {
						var reply AddReply
						if err := c.Add(tc.req, &reply); err != nil || reply.Value != tc.want {
							t.Fatalf("migrated replay: %+v, %v; want %d", reply, err, tc.want)
						}
					}
					if err := c.Add(testRequest("Alice", 11, math.MaxInt64, 1), &AddReply{}); err == nil || err.Error() != "counter overflow" {
						t.Fatalf("lost remembered error: %v", err)
					}
					if err := c.Add(testRequest("Alice", 2, 8, 1), &AddReply{}); err == nil {
						t.Fatal("accepted conflicting arguments")
					}
					if withAcks {
						if err := c.Add(testRequest("Alice", 1, 7, 1), &AddReply{}); err == nil || err.Error() != staleRequestError {
							t.Fatalf("lost stale boundary: %v", err)
						}
					}
					var stats ServiceStats
					if err := c.Stats(struct{}{}, &stats); err != nil || stats.Value != 17 || stats.CompletionRecords != 4 {
						t.Fatalf("migration changed state/counts: %+v, %v", stats, err)
					}
				}()
			}
		})
	}
}

func assertV2Schema(t *testing.T, db *bolt.DB) {
	t.Helper()
	if err := db.View(func(tx *bolt.Tx) error {
		version := tx.Bucket(stateBucket).Get(schemaVersionKey)
		if len(version) != 8 || binary.BigEndian.Uint64(version) != 2 || tx.Bucket(legacyResultsBucket) != nil || tx.Bucket(resultsBucket) == nil {
			return errors.New("expected version 2 with only nested completions")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedMigrationRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	seedLegacy(t, path, true)
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(legacyResultsBucket).Put([]byte("~~invalid-json"), []byte("{}"))
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if c, err := OpenCounter(path); err == nil {
		c.Close()
		t.Fatal("malformed migration succeeded")
	}
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(resultsBucket) != nil || tx.Bucket(stateBucket).Get(schemaVersionKey) != nil || tx.Bucket(legacyResultsBucket).Stats().KeyN != 5 {
			return errors.New("failed migration left partial changes")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownSchemaIsRejectedWithoutChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	c, err := OpenCounter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.db.Update(func(tx *bolt.Tx) error {
		version := sequenceKey(99)
		return tx.Bucket(stateBucket).Put(schemaVersionKey, version[:])
	}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if c, err := OpenCounter(path); err == nil {
		c.Close()
		t.Fatal("unknown schema accepted")
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		if got := binary.BigEndian.Uint64(tx.Bucket(stateBucket).Get(schemaVersionKey)); got != 99 {
			t.Errorf("version changed to %d", got)
		}
		return nil
	})
}

// This helper exercises the same schema transaction as OpenCounter, but exits
// without closing the DB at the migration's before/after-commit boundaries.
func TestMigrationCrashHelper(t *testing.T) {
	point := os.Getenv("RIFL_MIGRATION_CRASH")
	if point == "" {
		return
	}
	db, err := bolt.Open(os.Getenv("RIFL_MIGRATION_DB"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initializeCounterSchema(tx); err != nil {
			return err
		}
		if point == "before" {
			os.Exit(79)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	os.Exit(79)
}

func TestMigrationCrashRecovery(t *testing.T) {
	for _, point := range []string{"before", "after"} {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			seedLegacy(t, path, true)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMigrationCrashHelper$")
			cmd.Env = append(os.Environ(), "RIFL_MIGRATION_CRASH="+point, "RIFL_MIGRATION_DB="+path)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 79 {
				t.Fatalf("wrong migration crash: %v, %s", err, output)
			}
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if point == "after" {
				assertV2Schema(t, db)
			} else if err := db.View(func(tx *bolt.Tx) error {
				if tx.Bucket(resultsBucket) != nil || tx.Bucket(legacyResultsBucket) == nil || tx.Bucket(stateBucket).Get(schemaVersionKey) != nil {
					return errors.New("uncommitted migration was partially visible")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.Close()
			c, err := OpenCounter(path)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var reply AddReply
			if err := c.Add(testRequest("Alice", 2, 7, 1), &reply); err != nil || reply.Value != 7 {
				t.Fatalf("retry after migration crash: %+v, %v", reply, err)
			}
			if err := c.Add(testRequest("Alice", 1, 7, 1), &reply); err == nil || err.Error() != staleRequestError {
				t.Fatalf("lost stale protection: %v", err)
			}
		})
	}
}
