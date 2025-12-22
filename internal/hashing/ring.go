package hashing

import (
	"hash/fnv"
	"sort"
	"sync"
	"fmt"
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

	for i := 0; i < weight; i++ {
		virtualID := fmt.Sprintf("%s#%d", id, i)
		
		h := hashID(virtualID)

		r.entries = append(r.entries, entry{
			hash:	h,
			id:		id,
		})
	}

	// keep entries sorted so we can binary search.
    sort.Slice(r.entries, func(i, j int) bool {
        return r.entries[i].hash < r.entries[j].hash
    })
}

// remove all virtual nodes belonging to a physical node
func (r *ring) RemoveNode(id NodeID) {
    r.mu.Lock()
    defer r.mu.Unlock()

    kept := r.entries[:0] // keep same capacity as r, but with length zero
	// NOTE: splices keep a reference to the original array, and the array exists at least one splice references it
    for _, e := range r.entries {
        if e.id != id {
            kept = append(kept, e)
        }
    }
    r.entries = kept
}

func (r *ring) GetPrimary(key string) NodeID {
    r.mu.RLock()
    defer r.mu.RUnlock()

    if len(r.entries) == 0 {
        return ""
    }

    keyHash := hashID(key)

	// perform binary search to get primary node
	idx := sort.Search(len(r.entries), func(i int) bool {
        return r.entries[i].hash >= keyHash
    })

    if idx == len(r.entries) {
        idx = 0
    }

    return r.entries[idx].id // return primary node
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
	h := fnv.New32a()
    _, _ = h.Write([]byte(s))
    return h.Sum32()	
}