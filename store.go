package riflrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

const storageErrorPrefix = "storage outcome unknown: "

var (
	stateBucket   = []byte("state-v1")
	resultsBucket = []byte("completions-v1")
	valueKey      = []byte("counter")
)

type diskCompletion struct {
	Delta int64
	Reply AddReply
	Error string
}

// PersistentCounter atomically commits the counter and its completion records.
// One process owns the database; bbolt serializes writers and syncs commits.
type PersistentCounter struct {
	db *bolt.DB
	// Test-only crash boundaries. Configure before serving; never change while in use.
	beforeCommit func()
	afterCommit  func()
}

// OpenCounter opens or creates a database. Never delete/reset this file to recover
// a failed server: losing its completion records makes old retries unsafe.
func OpenCounter(path string) (*PersistentCounter, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open counter database: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		state, results := tx.Bucket(stateBucket), tx.Bucket(resultsBucket)
		if state != nil || results != nil {
			if state == nil || results == nil || len(state.Get(valueKey)) != 8 {
				return errors.New("invalid counter database schema")
			}
			return nil
		}
		state, err := tx.CreateBucket(stateBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucket(resultsBucket); err != nil {
			return err
		}
		return state.Put(valueKey, make([]byte, 8))
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize counter database: %w", err)
	}
	return &PersistentCounter{db: db}, nil
}

func (c *PersistentCounter) Close() error { return c.db.Close() }

func (c *PersistentCounter) Add(req AddRequest, reply *AddReply) error {
	if req.ID.ClientID == "" || req.ID.Sequence == 0 {
		return errors.New("client ID and nonzero sequence are required")
	}
	// JSON encodes the pair unambiguously even if client IDs contain separators.
	key, err := json.Marshal(req.ID)
	if err != nil {
		return err
	}
	var result diskCompletion
	var applicationErr error
	committedNew := false
	err = c.db.Update(func(tx *bolt.Tx) error {
		state, results := tx.Bucket(stateBucket), tx.Bucket(resultsBucket)
		if data := results.Get(key); data != nil {
			if err := json.Unmarshal(data, &result); err != nil {
				return err
			}
			if result.Delta != req.Delta {
				applicationErr = errors.New("request ID reused with different arguments")
			}
			return nil
		}
		value := int64(binary.BigEndian.Uint64(state.Get(valueKey)))
		result.Delta = req.Delta
		result.Reply.Value, result.Error = addValue(value, req.Delta)
		if result.Error == "" {
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], uint64(result.Reply.Value))
			if err := state.Put(valueKey, encoded[:]); err != nil {
				return err
			}
		}
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err := results.Put(key, data); err != nil {
			return err
		}
		if c.beforeCommit != nil {
			c.beforeCommit()
		}
		committedNew = true
		return nil
	})
	if err != nil {
		// A storage failure is not a definitive application rejection. The client
		// must retain the ID and may retry; do not falsely report success.
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	if committedNew && c.afterCommit != nil {
		c.afterCommit()
	}
	if applicationErr != nil {
		return applicationErr
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	*reply = result.Reply
	return nil
}

func (c *PersistentCounter) Value(_ struct{}, reply *int64) error {
	var value int64
	err := c.db.View(func(tx *bolt.Tx) error {
		value = int64(binary.BigEndian.Uint64(tx.Bucket(stateBucket).Get(valueKey)))
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	*reply = value
	return nil
}
