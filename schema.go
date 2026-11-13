package riflrpc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	legacyResultsBucket = []byte("completions-v1") // unversioned: flat JSON RequestID keys
	nestedResultsBucket = []byte("completions-v2") // version 2: <client bucket>/<sequence>
	schemaVersionKey    = []byte("schema-version")
)

// Version 3 stores every completion in one flat bucket keyed by
// completionKey. Version 2's nested per-client buckets cost an extra dirty page
// per commit; the composite key keeps each client's records contiguous and
// numerically ordered, so acknowledgement GC still visits only that client.
const counterSchemaVersion uint64 = 3

// maxClientIDBytes keeps completion keys within bbolt's 32768-byte key limit.
const maxClientIDBytes = bolt.MaxKeySize - 2 - 8

// sequenceKey preserves numerical order in bbolt's lexicographic key order.
func sequenceKey(sequence uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sequence)
	return key
}

// completionPrefix is a 2-byte big-endian length followed by the raw client ID.
// The length makes the prefix unambiguous: no client's keys can fall inside
// another client's range, whatever bytes the IDs contain.
func completionPrefix(clientID string) []byte {
	prefix := make([]byte, 2+len(clientID), 2+len(clientID)+8)
	binary.BigEndian.PutUint16(prefix, uint16(len(clientID)))
	copy(prefix[2:], clientID)
	return prefix
}

func completionKey(clientID string, sequence uint64) []byte {
	encoded := sequenceKey(sequence)
	return append(completionPrefix(clientID), encoded[:]...)
}

// completionSequence reports the sequence of key if it belongs to prefix.
func completionSequence(key, prefix []byte) (uint64, bool) {
	if len(key) != len(prefix)+8 || !bytes.HasPrefix(key, prefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), true
}

// initializeCounterSchema runs in a single write transaction. Migration either
// commits the new layout, version and removal of the old layout together, or
// changes nothing.
func initializeCounterSchema(tx *bolt.Tx) error {
	state := tx.Bucket(stateBucket)
	legacy := tx.Bucket(legacyResultsBucket)
	nested := tx.Bucket(nestedResultsBucket)
	results := tx.Bucket(resultsBucket)
	clients := tx.Bucket(clientsBucket)
	if state == nil {
		if legacy != nil || nested != nil || results != nil || clients != nil {
			return errors.New("counter database is missing state")
		}
		var err error
		if state, err = tx.CreateBucket(stateBucket); err != nil {
			return err
		}
		if err := state.Put(valueKey, make([]byte, 8)); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(resultsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(clientsBucket); err != nil {
			return err
		}
		return setSchemaVersion(state)
	}
	if len(state.Get(valueKey)) != 8 {
		return errors.New("invalid counter state")
	}
	version := state.Get(schemaVersionKey)
	switch {
	case version == nil:
		// Both earliest releases used an unversioned flat completions-v1 bucket.
		// The very first release had no clients-v1 acknowledgement bucket.
		if legacy == nil || nested != nil || results != nil {
			return errors.New("invalid legacy counter database schema")
		}
		return migrateCompletions(tx, state, legacyResultsBucket, func(put func(string, uint64, []byte) error) error {
			return legacy.ForEach(func(key, data []byte) error {
				var id RequestID
				if data == nil {
					return errors.New("unexpected nested legacy completion bucket")
				}
				if err := json.Unmarshal(key, &id); err != nil {
					return fmt.Errorf("decode legacy request ID: %w", err)
				}
				return put(id.ClientID, id.Sequence, data)
			})
		})
	case len(version) != 8:
		return errors.New("unsupported counter database schema version")
	case binary.BigEndian.Uint64(version) == 2:
		if nested == nil || clients == nil || legacy != nil || results != nil {
			return errors.New("invalid v2 counter database schema")
		}
		return migrateCompletions(tx, state, nestedResultsBucket, func(put func(string, uint64, []byte) error) error {
			return nested.ForEach(func(client, data []byte) error {
				records := nested.Bucket(client)
				if data != nil || records == nil {
					return errors.New("unexpected value in v2 completion bucket")
				}
				return records.ForEach(func(key, data []byte) error {
					if len(key) != 8 || data == nil {
						return errors.New("invalid v2 completion record")
					}
					return put(string(client), binary.BigEndian.Uint64(key), data)
				})
			})
		})
	case binary.BigEndian.Uint64(version) == counterSchemaVersion:
		if results == nil || clients == nil || legacy != nil || nested != nil {
			return errors.New("invalid v3 counter database schema")
		}
		return nil
	default:
		return errors.New("unsupported counter database schema version")
	}
}

// migrateCompletions copies every old record into the flat v3 bucket, then
// removes the old bucket and records the new version in the same transaction.
func migrateCompletions(tx *bolt.Tx, state *bolt.Bucket, old []byte,
	each func(put func(clientID string, sequence uint64, data []byte) error) error) error {
	if _, err := tx.CreateBucketIfNotExists(clientsBucket); err != nil {
		return err
	}
	results, err := tx.CreateBucket(resultsBucket)
	if err != nil {
		return err
	}
	if err := each(func(clientID string, sequence uint64, data []byte) error {
		if clientID == "" || sequence == 0 {
			return errors.New("invalid migrated request ID")
		}
		if len(clientID) > maxClientIDBytes {
			return errors.New("migrated client ID is too long")
		}
		var result diskCompletion
		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("decode migrated completion: %w", err)
		}
		key := completionKey(clientID, sequence)
		if results.Get(key) != nil {
			return errors.New("duplicate logical ID in migrated completions")
		}
		// Copy the saved bytes exactly; never recompute replies from current state.
		return results.Put(key, append([]byte(nil), data...))
	}); err != nil {
		return err
	}
	if err := tx.DeleteBucket(old); err != nil {
		return err
	}
	return setSchemaVersion(state)
}

func setSchemaVersion(state *bolt.Bucket) error {
	encoded := sequenceKey(counterSchemaVersion)
	return state.Put(schemaVersionKey, encoded[:])
}
