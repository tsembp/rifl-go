package riflrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const storageErrorPrefix = "storage outcome unknown: "

var (
	stateBucket   = []byte("state-v1")
	resultsBucket = []byte("completions-v1")
	clientsBucket = []byte("clients-v1")
	valueKey      = []byte("counter")
)

type diskCompletion struct {
	Delta int64
	Reply AddReply
	Error string
}

// PersistentCounter atomically commits counter mutations, completion records,
// acknowledgement boundaries, and completion-record reclamation. One process
// owns the database; bbolt serializes writers and syncs commits.
type PersistentCounter struct {
	db   *bolt.DB
	path string
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
		if state == nil && results == nil {
			state, err = tx.CreateBucket(stateBucket)
			if err != nil {
				return err
			}
			if _, err := tx.CreateBucket(resultsBucket); err != nil {
				return err
			}
			if err := state.Put(valueKey, make([]byte, 8)); err != nil {
				return err
			}
		} else if state == nil || results == nil || len(state.Get(valueKey)) != 8 {
			return errors.New("invalid counter database schema")
		}
		_, err = tx.CreateBucketIfNotExists(clientsBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize counter database: %w", err)
	}
	return &PersistentCounter{db: db, path: path}, nil
}

func (c *PersistentCounter) Close() error { return c.db.Close() }

func (c *PersistentCounter) Add(req AddRequest, reply *AddReply) error {
	if err := validateAddRequest(req); err != nil {
		return err
	}
	// JSON encodes the pair unambiguously even if client IDs contain separators.
	key, err := json.Marshal(req.ID)
	if err != nil {
		return err
	}
	var result diskCompletion
	var applicationErr error
	committedWrite := false
	err = c.db.Update(func(tx *bolt.Tx) error {
		state, results := tx.Bucket(stateBucket), tx.Bucket(resultsBucket)
		firstIncomplete, advanced, err := processPersistentAck(tx, req.ID.ClientID, req.FirstIncomplete)
		if err != nil {
			return err
		}
		committedWrite = advanced
		if req.ID.Sequence < firstIncomplete {
			applicationErr = errors.New(staleRequestError)
			if committedWrite && c.beforeCommit != nil {
				c.beforeCommit()
			}
			return nil
		}
		if data := results.Get(key); data != nil {
			if err := json.Unmarshal(data, &result); err != nil {
				return err
			}
			if result.Delta != req.Delta {
				applicationErr = errors.New("request ID reused with different arguments")
			}
			if committedWrite && c.beforeCommit != nil {
				c.beforeCommit()
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
		committedWrite = true
		if c.beforeCommit != nil {
			c.beforeCommit()
		}
		return nil
	})
	if err != nil {
		// A storage failure is not a definitive application rejection. The client
		// must retain the ID and may retry; do not falsely report success.
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	if committedWrite && c.afterCommit != nil {
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

func processPersistentAck(tx *bolt.Tx, clientID string, incoming uint64) (uint64, bool, error) {
	clients := tx.Bucket(clientsBucket)
	stored, err := persistentFirstIncomplete(clients, clientID)
	if err != nil || incoming <= stored {
		return stored, false, err
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], incoming)
	if err := clients.Put([]byte(clientID), encoded[:]); err != nil {
		return 0, false, err
	}
	cursor := tx.Bucket(resultsBucket).Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		var id RequestID
		if err := json.Unmarshal(key, &id); err != nil {
			return 0, false, err
		}
		if id.ClientID == clientID && id.Sequence < incoming {
			if err := cursor.Delete(); err != nil {
				return 0, false, err
			}
		}
	}
	return incoming, true, nil
}

func persistentFirstIncomplete(clients *bolt.Bucket, clientID string) (uint64, error) {
	data := clients.Get([]byte(clientID))
	if data == nil {
		return 1, nil
	}
	if len(data) != 8 {
		return 0, errors.New("invalid client acknowledgement")
	}
	firstIncomplete := binary.BigEndian.Uint64(data)
	if firstIncomplete == 0 {
		return 0, errors.New("invalid client acknowledgement")
	}
	return firstIncomplete, nil
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

// Stats returns read-only state for experiments; it does not alter RPC semantics.
func (c *PersistentCounter) Stats(_ struct{}, reply *ServiceStats) error {
	reply.Protected = true
	err := c.db.View(func(tx *bolt.Tx) error {
		reply.Value = int64(binary.BigEndian.Uint64(tx.Bucket(stateBucket).Get(valueKey)))
		reply.CompletionRecords = tx.Bucket(resultsBucket).Stats().KeyN
		reply.ClientBoundaries = tx.Bucket(clientsBucket).Stats().KeyN
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	info, err := os.Stat(c.path)
	if err != nil {
		return fmt.Errorf("stat counter database: %w", err)
	}
	reply.DatabaseBytes = info.Size()
	return nil
}
