package experiment

import (
	"fmt"
	"math"
	"sort"

	"project18/internal/common"
	"project18/internal/sim"
)

// DistributionSnapshot is a point-in-time read of how many chunk replicas
// each chunkserver is actually holding, per spec section 3 ("Distribution").
// In this simulator "chunks per node" and "replicas per node" are the same
// number — each stored chunk on a node is one replica — so we report one
// field, not two, rather than inventing a distinction the code doesn't
// have.
type DistributionSnapshot struct {
	PerServer map[common.ServerID]int
	Total     int // sum of PerServer; equals (live chunks * replication factor) in a healthy cluster
	Min       int
	Max       int
	Mean      float64
	// Imbalance is Max-Min (an absolute replica-count spread), not a
	// normalized/statistical measure — deliberately simple so it's exactly
	// as trustworthy as the counts it's built from. See
	// docs/experiments/M1-replica-placement.md for why we didn't compute a
	// fancier imbalance metric.
	Imbalance int
	StdDev    float64
}

// Snapshot reads the current replica distribution directly off every
// *online* chunkserver's local chunk map (StorageNode.ChunkHandles()) — the
// same data heartbeats report — not off the master's belief, so this
// reflects physical reality even mid-recovery.
//
// Offline nodes are deliberately excluded: a killed chunkserver's chunk
// data is never deleted (see TestFailedNodeDataIsNotGarbageCollected in
// experiment_test.go and docs/experiments/M1-replica-placement.md) — it
// just stops being tracked by master metadata. Including that orphaned
// data here would silently inflate every distribution/imbalance figure
// after any failure in the run's history, which is not what "current
// cluster distribution" should mean. Use RawSnapshot to see everything,
// orphaned data included.
func Snapshot(s *sim.Simulator) DistributionSnapshot {
	return snapshot(s, true)
}

// RawSnapshot is Snapshot without the online-only filter — includes
// whatever physically remains on offline nodes. Exists so the orphaned-data
// fact is independently checkable, not just asserted.
func RawSnapshot(s *sim.Simulator) DistributionSnapshot {
	return snapshot(s, false)
}

func snapshot(s *sim.Simulator, onlineOnly bool) DistributionSnapshot {
	ids := s.ServerIDs()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	per := make(map[common.ServerID]int, len(ids))
	total := 0
	min, max := -1, 0
	for _, id := range ids {
		node, ok := s.Node(id)
		if !ok {
			continue
		}
		if onlineOnly && !node.IsOnline() {
			continue
		}
		n := len(node.ChunkHandles())
		per[id] = n
		total += n
		if min == -1 || n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if min == -1 {
		min = 0
	}
	mean := 0.0
	if len(per) > 0 {
		mean = float64(total) / float64(len(per))
	}
	var variance float64
	for _, n := range per {
		d := float64(n) - mean
		variance += d * d
	}
	if len(per) > 0 {
		variance /= float64(len(per))
	}

	return DistributionSnapshot{
		PerServer: per,
		Total:     total,
		Min:       min,
		Max:       max,
		Mean:      mean,
		Imbalance: max - min,
		StdDev:    math.Sqrt(variance),
	}
}

func (d DistributionSnapshot) String() string {
	ids := make([]common.ServerID, 0, len(d.PerServer))
	for id := range d.PerServer {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	rows := ""
	for _, id := range ids {
		rows += fmt.Sprintf(" %s=%d", id, d.PerServer[id])
	}
	return fmt.Sprintf("total=%d min=%d max=%d mean=%.2f imbalance=%d stddev=%.2f |%s",
		d.Total, d.Min, d.Max, d.Mean, d.Imbalance, d.StdDev, rows)
}
