package kv

// OpType represents a state mutating operation (also reads are not operations)
type OpType int

const (
	// OpPut represents inserting or overwriting a value.
	OpPut OpType = iota

	// OpDelete represents a logical delete (tombstone).
	OpDelete
)

// Version defines a total ordering for writes.
// A version must allow deterministic comparison between writes from the same node and writes from different nodes

// Versions determine correctness — NOT mutex ordering.
type Version struct {
	NodeID string
	Seq    uint64
}

// GreaterThan returns true if v is strictly newer than other.
func (v Version) GreaterThan(other Version) bool {
	if v.Seq != other.Seq {
		return v.Seq > other.Seq
	}

	// break ties with NodeID ordernig
	return v.NodeID > other.NodeID
	
}

// Equal returns true if both versions represent the same write.
func (v Version) Equal(other Version) bool {
	return v.Seq == other.Seq && v.NodeID == other.NodeID
}

// LessThan returns true if v is strictly older than other.
func (v Version) LessThan(other Version) bool {
	if v.Seq != other.Seq {
		return v.Seq < other.Seq
	}

	return v.NodeID < other.NodeID
	// we could also do:
	// return !v.GreaterThan(other) && !v.Equal(other)
}


type Operation struct {
	Type    OpType
	Key     string
	Value   []byte // nil for deletes
	Version Version
}

// IsPut returns true if this operation is a PUT.
func (op Operation) IsPut() bool {
	// TODO: Return true when op.Type indicates a put.
	return false
}

// IsDelete returns true if this operation is a DELETE.
func (op Operation) IsDelete() bool {
	// TODO: Return true when op.Type indicates a delete.
	return false
}



// Record represents the latest known state for a key.
// Records represent materialized state, not history.
type Record struct {
	Value     []byte
	Version   Version
	Tombstone bool
}

// IsDeleted returns true if this record represents a deleted key.
func (r Record) IsDeleted() bool {
	// TODO: Decide what condition denotes a deletion.
	
	// Think about:
	// - Should this depend only on Tombstone?
	// - Should Value be ignored when deleted?
	return false
}

// Invariants??