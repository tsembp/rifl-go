package riflrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	legacyResultsBucket = []byte("completions-v1")
	schemaVersionKey    = []byte("schema-version")
)

const counterSchemaVersion uint64 = 2

// sequenceKey preserves numerical order in bbolt's lexicographic key order.
func sequenceKey(sequence uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], sequence)
	return key
}

// initializeCounterSchema runs in a single write transaction. Migration either
// commits the new layout, version and removal of v1 together, or changes nothing.
func initializeCounterSchema(tx *bolt.Tx) error {
	state := tx.Bucket(stateBucket)
	legacy := tx.Bucket(legacyResultsBucket)
	results := tx.Bucket(resultsBucket)
	clients := tx.Bucket(clientsBucket)
	if state == nil {
		if legacy != nil || results != nil || clients != nil {
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
		version := sequenceKey(counterSchemaVersion)
		return state.Put(schemaVersionKey, version[:])
	}
	if len(state.Get(valueKey)) != 8 {
		return errors.New("invalid counter state")
	}
	version := state.Get(schemaVersionKey)
	if version != nil {
		if len(version) != 8 || binary.BigEndian.Uint64(version) != counterSchemaVersion {
			return errors.New("unsupported counter database schema version")
		}
		if results == nil || clients == nil || legacy != nil {
			return errors.New("invalid v2 counter database schema")
		}
		return nil
	}
	// Both previous releases used an unversioned flat completions-v1 bucket.
	// The earliest release had no clients-v1 acknowledgement bucket.
	if legacy == nil || results != nil {
		return errors.New("invalid legacy counter database schema")
	}
	if _, err := tx.CreateBucketIfNotExists(clientsBucket); err != nil {
		return err
	}
	results, err := tx.CreateBucket(resultsBucket)
	if err != nil {
		return err
	}
	if err := legacy.ForEach(func(key, data []byte) error {
		var id RequestID
		if data == nil {
			return errors.New("unexpected nested legacy completion bucket")
		}
		if err := json.Unmarshal(key, &id); err != nil {
			return fmt.Errorf("decode legacy request ID: %w", err)
		}
		if id.ClientID == "" || id.Sequence == 0 {
			return errors.New("invalid legacy request ID")
		}
		var result diskCompletion
		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("decode legacy completion: %w", err)
		}
		client, err := results.CreateBucketIfNotExists([]byte(id.ClientID))
		if err != nil {
			return err
		}
		sequence := sequenceKey(id.Sequence)
		if client.Get(sequence[:]) != nil {
			return errors.New("duplicate logical ID in legacy completions")
		}
		// Copy the saved bytes exactly; never recompute replies from current state.
		return client.Put(sequence[:], data)
	}); err != nil {
		return err
	}
	if err := tx.DeleteBucket(legacyResultsBucket); err != nil {
		return err
	}
	encoded := sequenceKey(counterSchemaVersion)
	return state.Put(schemaVersionKey, encoded[:])
}
