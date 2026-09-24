package master

import (
	"fmt"
	"math/rand"

	"project18/internal/common"
)

// ReplicaPlacementPolicy is the replaceable interface for deciding which
// chunkservers hold a chunk's replicas. GFSReplicaPlacementPolicy is the
// simplest correct baseline (uniform random among healthy candidates); a
// later experiment can swap in e.g. a load- or rack-aware policy without the
// rest of the master changing.
type ReplicaPlacementPolicy interface {
	// Choose selects `count` distinct servers from candidates, excluding any
	// in exclude. Returns an error if fewer than `count` are available.
	Choose(candidates []common.ServerID, exclude map[common.ServerID]bool, count int) ([]common.ServerID, error)
}

// GFSReplicaPlacementPolicy picks uniformly at random among healthy
// candidates, mirroring the baseline GFS master's placement behavior.
type GFSReplicaPlacementPolicy struct {
	rng *rand.Rand
}

func NewGFSReplicaPlacementPolicy(seed int64) *GFSReplicaPlacementPolicy {
	return &GFSReplicaPlacementPolicy{rng: rand.New(rand.NewSource(seed))}
}

func (p *GFSReplicaPlacementPolicy) Choose(candidates []common.ServerID, exclude map[common.ServerID]bool, count int) ([]common.ServerID, error) {
	pool := make([]common.ServerID, 0, len(candidates))
	for _, c := range candidates {
		if exclude == nil || !exclude[c] {
			pool = append(pool, c)
		}
	}
	if len(pool) < count {
		return nil, fmt.Errorf("placement: need %d servers, only %d available", count, len(pool))
	}
	p.rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	return pool[:count], nil
}
