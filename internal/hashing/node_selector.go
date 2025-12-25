package hashing

type NodeSelector interface {
	Primary(key string) NodeID
	Replicas(key string) []NodeID

	IsPrimary(key string) bool

	WriteTargets(key string) []NodeID
	ReadTargets(key string) []NodeID
}

// need to implement in cluster/healthcheck.go.
type HealthChecker interface {
	IsHealthy(node NodeID) bool
}

type ReadConsistency int

type WriteConsistency int

// here we have two consts ReadPrimary and ReadAnReplica of type ReadConsistency which will denote if we want to read from the primary node only or replica nodes
const (
	ReadPrimary ReadConsistency = iota
	ReadAnyReplica
)

const (
	WritePrimaryOnly WriteConsistency = iota
	WriteReplicate
)

// SelectorConfig defines routing policy.
type SelectorConfig struct {
	ReplicationFactor int
	ReadConsistency   ReadConsistency
	WriteConsistency  WriteConsistency
}

// selector is the concrete NodeSelector implementation.
type selector struct {
	ring   Ring
	self   NodeID
	health HealthChecker
	cfg    SelectorConfig
}

// NewNodeSelector constructs a NodeSelector.
func NewNodeSelector(
	ring Ring,
	self NodeID,
	health HealthChecker,
	cfg SelectorConfig,
) NodeSelector {
	return &selector{
		ring:   ring,
		self:   self,
		health: health,
		cfg:    cfg,
	}
}

func (s *selector) Primary(key string) NodeID {
	return s.ring.GetPrimary(key)
}

func (s *selector) Replicas(key string) []NodeID {
	return s.ring.GetReplicas(key, s.cfg.ReplicationFactor)
}

func (s *selector) IsPrimary(key string) bool {
	return s.Primary(key) == s.self
}

// WriteTargets returns the nodes that should receive a write.
func (s *selector) WriteTargets(key string) []NodeID {
	replicas := s.Replicas(key)
	if len(replicas) == 0 {
		return nil
	}

	primary := replicas[0]

	switch s.cfg.WriteConsistency {

	case WritePrimaryOnly:
		if s.health.IsHealthy(primary) {
			return []NodeID{primary}
		}
		// primary down means its not a safe write target
		return nil

	case WriteReplicate:
		targets := make([]NodeID, 0, len(replicas))
		for _, node := range replicas {
			if s.health.IsHealthy(node) {
				targets = append(targets, node)
			}
		}
		return targets

	default:
		return nil
	}
}

// ReadTargets returns nodes that may serve a read.
func (s *selector) ReadTargets(key string) []NodeID {
	replicas := s.Replicas(key)
	if len(replicas) == 0 {
		return nil
	}

	switch s.cfg.ReadConsistency {

	case ReadPrimary:
		primary := replicas[0]
		if s.health.IsHealthy(primary) {
			return []NodeID{primary}
		}
		return nil

	case ReadAnyReplica:
		for _, node := range replicas {
			if s.health.IsHealthy(node) {
				return []NodeID{node}
			}
		}
		return nil

	default:
		return nil
	}
}
