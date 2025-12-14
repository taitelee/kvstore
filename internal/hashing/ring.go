package hashing

import (
	"hash/fnv"
	"sort"
	"sync"
)

// NodeID identifies a node on the ring
type NodeID string 

type Ring interface {
	AddNode(id NodeID, weight int)
	RemoveNode(id NodeID)
	GetPrimary(key string) NodeID
	GetReplicas(key string, n int) []NodeID
	Nodes() []NodeID
}

// implements the Ring interface
type ring struct {
	mu sync.RWMutex
	entries []entry
	replicas int
}

// entries are essentially virtual nodes
type entry struct {
    hash uint32
    id   NodeID
}

func NewRing(replicationFactor int) Ring {
	return &ring{
		replicas: replicationFactor,
		entries: make([]entry, 0),
	}
}

// adding physical node to the ring with corresponding number of virtual nodes
func (r *ring) AddNode(id NodeID, weight int) {
    r.mu.Lock()
    defer r.mu.Unlock()
}

// remove all virtual nodes belonging to a physical node
func (r *ring) RemoveNode(id NodeID) {
    r.mu.Lock()
    defer r.mu.Unlock()

}

func (r *ring) GetPrimary(key string) NodeID {
    r.mu.Lock()
    defer r.mu.Unlock()
}

func (r *ring) GetReplicas(key string, n int) []NodeID {
    r.mu.Lock()
    defer r.mu.Unlock()
}

func (r *ring) Nodes() []NodeID {
    r.mu.Lock()
    defer r.mu.Unlock()
}

func hashID(s string) uint32 {
    r.mu.Lock()
    defer r.mu.Unlock()
}