package riflrpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

var baselineBucket = []byte("baseline-state-v1")

// UnprotectedCounter is the durable retry-only baseline. It deliberately has no
// duplicate detection: every Add invocation applies its delta in a bbolt write
// transaction. It uses the same RPC request/reply types and bbolt sync defaults
// as PersistentCounter so benchmarks isolate the cost and benefit of protection.
type UnprotectedCounter struct {
	db   *bolt.DB
	path string
}

// OpenUnprotectedCounter opens or creates a durable retry-only counter.
func OpenUnprotectedCounter(path string) (*UnprotectedCounter, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open baseline database: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(baselineBucket)
		if err != nil {
			return err
		}
		value := bucket.Get(valueKey)
		if value == nil {
			return bucket.Put(valueKey, make([]byte, 8))
		}
		if len(value) != 8 {
			return errors.New("invalid baseline database schema")
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize baseline database: %w", err)
	}
	return &UnprotectedCounter{db: db, path: path}, nil
}

func (c *UnprotectedCounter) Close() error { return c.db.Close() }

// Add applies every invocation, including retries of the same logical ID.
func (c *UnprotectedCounter) Add(req AddRequest, reply *AddReply) error {
	if err := validateAddRequest(req); err != nil {
		return err
	}
	var applicationErr error
	err := c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(baselineBucket)
		value := int64(binary.BigEndian.Uint64(bucket.Get(valueKey)))
		updated, resultErr := addValue(value, req.Delta)
		if resultErr != "" {
			applicationErr = errors.New(resultErr)
			return nil
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(updated))
		if err := bucket.Put(valueKey, encoded[:]); err != nil {
			return err
		}
		reply.Value = updated
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	if applicationErr != nil {
		return applicationErr
	}
	return nil
}

func (c *UnprotectedCounter) Value(_ struct{}, reply *int64) error {
	err := c.db.View(func(tx *bolt.Tx) error {
		*reply = int64(binary.BigEndian.Uint64(tx.Bucket(baselineBucket).Get(valueKey)))
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	return nil
}

func (c *UnprotectedCounter) Stats(_ struct{}, reply *ServiceStats) error {
	err := c.db.View(func(tx *bolt.Tx) error {
		reply.Value = int64(binary.BigEndian.Uint64(tx.Bucket(baselineBucket).Get(valueKey)))
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s%w", storageErrorPrefix, err)
	}
	info, err := os.Stat(c.path)
	if err != nil {
		return fmt.Errorf("stat baseline database: %w", err)
	}
	reply.DatabaseBytes = info.Size()
	return nil
}
