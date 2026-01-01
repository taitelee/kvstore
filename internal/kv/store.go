package kv

import "sync"

type MemStore struct {
	mu   sync.RWMutex
	data map[string]Record
}

func NewStore() Store {
	return &MemStore{
		data: make(map[string]Record),
	}
}

func (s *MemStore) Get(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.data[key]
	if !ok || rec.IsDeleted() {
		return Record{}, false
	}
	return rec, true
}

func (s *MemStore) Put(key string, record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[key]
	if ok && !record.Version.GreaterThan(existing.Version) {
		return
	}
	s.data[key] = record
}

func (s *MemStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.data[key]
	if !ok {
		return
	}

	existing.Tombstone = true
	s.data[key] = existing
}

func (s *MemStore) Scan(fn func(key string, record Record) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.data {
		if !fn(k, v) {
			return
		}
	}
}
