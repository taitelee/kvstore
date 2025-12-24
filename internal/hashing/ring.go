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
    // predicate is r.entries[i].hash >= hash, and search until we find boundary between the predicate being false and true 
	idx := sort.Search(len(r.entries), func(i int) bool {
        return r.entries[i].hash >= keyHash
    })

    if idx == len(r.entries) {
        idx = 0
    }

    return r.entries[idx].id // return primary node
}


// replicas are not nodes, they are normal key-value pairs stored in other nodes other than the primary
func (r *ring) GetReplicas(key string, n int) []NodeID {
    r.mu.Lock()
    defer r.mu.Unlock()

    if len(r.entries) == 0 || n <= 0 {
        return nil
    }

    keyHash := hashID(key)

    // we could (with the way we've implemented locking) call GetPrimary here, but not best practice for deadlock safety and idiomatic Golang lol
    start := sort.Search(len(r.entries), func(i int) bool {
        return r.entries[i].hash >= keyHash
    })

    if start == len(r.entries) {
        start = 0
    }

    replicas := make([]NodeID, 0, n) // splice for replica set (holds physical nodes)
    seen := make(map[NodeID]struct{}) // deduplication of physical nodes

    for i := 0; len(replicas) < n && i < len(r.entries); i++ {
        idx := (start + i) % len(r.entries)
        nodeID := r.entries[idx].id

        // Deduplicate physical nodes
        if _, ok := seen[nodeID]; ok {
            continue
        }

        seen[nodeID] = struct{}{}
        replicas = append(replicas, nodeID)
    }

    return replicas
    
}

func (r *ring) Nodes() []NodeID {
    r.mu.Lock()
    defer r.mu.Unlock()

    nodes := make([]NodeID, 0)
    seen := make(map[NodeID]struct{})

    for _, e := range(r.entries) {
        if _, ok := seen[e.id]; ok {
            continue
        }
        seen[e.id] = struct{}{}
        nodes = append(nodes, e.id)
    }

    return nodes
}

func hashID(s string) uint32 {
	h := fnv.New32a()
    _, _ = h.Write([]byte(s))
    return h.Sum32()	
}