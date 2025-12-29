// storing actual data here

package kv

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// WAL provides durability.
type WAL interface {
	Append(op Operation) error
	Replay() ([]Operation, error)
	Sync() error
	Close() error
}

// Store is the in-memory state.
type Store interface {
	Get(key string) (Record, bool)
	Put(key string, record Record)
	Delete(key string)
	Scan(fn func(key string, record Record) bool)
}

// Replicator ships operations to other nodes.
// this is async; coordination happens at higher layers.
type Replicator interface {
	Replicate(op Operation)
}

// Engine Config
type EngineConfig struct {
	NodeID        string
	SyncWrites    bool // fsync on every write
	EnableReplica bool
}

// Engine is the authoritative local state machine for a node. It includes engine config, store, wal, etc.
type Engine struct {
	mu sync.RWMutex

	cfg EngineConfig

	store Store
	wal   WAL
	repl  Replicator

	seq uint64 // monotonically increasing local sequence
}

func NewEngine(
	cfg EngineConfig,
	store Store,
	wal WAL,
	repl Replicator,
) (*Engine, error) {

	e := &Engine{
		cfg:   cfg,
		store: store,
		wal:   wal,
		repl:  repl,
	}

	// replay WAL to rebuild in-memory state
	ops, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	for _, op := range ops {
		e.applyNoWal(op)
	}

	return e, nil
}

func (e *Engine) Close() error {
	return e.wal.Close()
}

// API stuff
func (e *Engine) Get(ctx context.Context, key string) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rec, ok := e.store.Get(key)
	if !ok || rec.Tombstone {
		return nil, false
	}

	return rec.Value, true
}

func (e *Engine) Put(ctx context.Context, key string, value []byte) error {
	op := e.newOp(OpPut, key, value)

	if err := e.wal.Append(op); err != nil {
		return err
	}

	if e.cfg.SyncWrites {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}

	e.mu.Lock()
	e.applyNoWal(op) // actual change in memory state
	e.mu.Unlock()

	if e.cfg.EnableReplica && e.repl != nil {
		e.repl.Replicate(op)
	}

	return nil
}

func (e *Engine) Delete(ctx context.Context, key string) error {
	op := e.newOp(OpDelete, key, nil)

	if err := e.wal.Append(op); err != nil {
		return err
	}

	if e.cfg.SyncWrites {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}

	e.mu.Lock()
	e.applyNoWal(op)
	e.mu.Unlock()

	if e.cfg.EnableReplica && e.repl != nil {
		e.repl.Replicate(op)
	}

	return nil
}

// replication Apply
// ApplyReplica applies an incoming replicated operation
// must be idempotent
func (e *Engine) ApplyReplica(ctx context.Context, op Operation) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rec, ok := e.store.Get(op.Key)
	if ok && rec.Version >= op.Version {
		// stale or duplicate
		return nil
	}

	if err := e.wal.Append(op); err != nil {
		return err
	}

	e.applyNoWal(op)
	return nil
}

// migration and rebalancing
func (e *Engine) Export(fn func(key string, record Record) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	e.store.Scan(fn)
}

func (e *Engine) Import(op Operation) error {
	return e.ApplyReplica(context.Background(), op)
}

/* internal tools */
func (e *Engine) newOp(kind OpType, key string, value []byte) Operation {
	seq := atomic.AddUint64(&e.seq, 1)

	return Operation{
		Type:    kind,
		Key:     key,
		Value:   value,
		Version: Version{
			NodeID: e.cfg.NodeID,
			Seq:    seq,
		},
	}
}

func (e *Engine) applyNoWal(op Operation) {
	switch op.Type {
	case OpPut:
		e.store.Put(op.Key, Record{
			Value:     op.Value,
			Version:   op.Version,
			Tombstone: false,
		})
	case OpDelete:
		e.store.Put(op.Key, Record{
			Version:   op.Version,
			Tombstone: true,
		})
	default:
		panic("unknown op type")
	}
}

var ErrNotFound = errors.New("key not found")
