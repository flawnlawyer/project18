// Package metrics collects the measurements listed in the spec's "Metrics"
// section: read/write outcomes, replication health, recovery timing, and
// message volume. Kept deliberately dumb (counters + a couple of duration
// samples) since the point is to compare architectures later, not to build a
// metrics platform now.
package metrics

import (
	"fmt"
	"sync"
	"time"
)

type Metrics struct {
	mu sync.Mutex

	ReadSuccess  uint64
	ReadFailure  uint64
	WriteSuccess uint64
	WriteFailure uint64

	UnderReplicatedChunks uint64 // current gauge, set by the replication manager each scan
	StaleReplicasDetected uint64
	ReReplicationsStarted uint64
	ReReplicationsOK      uint64
	ReReplicationsFailed  uint64

	MasterMetadataOps uint64
	MessagesSent      uint64

	PlacementDecisions uint64 // M1: number of times ReplicaPlacementPolicy.Choose was invoked successfully (initial allocation + re-replication)

	// M2: repair-pipeline observability (spec section 12).
	RepairQueueDepth         uint64 // current gauge: chunks with an open repair job
	RepairAttemptsTotal      uint64 // every attempt within a repair job: bad source, failed store, failed verify, or successful copy
	VerificationFailures     uint64 // StoreChunk succeeded but the post-copy HasChunk check disagreed
	VerifiedRepairCommits    uint64 // AddReplicaLocation calls that followed a successful verification — see FalseSuccessRepairCommits
	UnexpectedChunksObserved uint64 // heartbeat reported a chunk the master has no record of for that file
	MetadataInconsistencies  uint64 // StaleReplicasDetected + UnexpectedChunksObserved, see Snapshot

	FailureDetections uint64
	detectionSamples  []time.Duration // heartbeat-timeout -> detection latency
	recoverySamples   []time.Duration // failure -> replication-restored latency
}

func New() *Metrics { return &Metrics{} }

func (m *Metrics) IncRead(ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok {
		m.ReadSuccess++
	} else {
		m.ReadFailure++
	}
}

func (m *Metrics) IncWrite(ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok {
		m.WriteSuccess++
	} else {
		m.WriteFailure++
	}
}

func (m *Metrics) SetUnderReplicated(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UnderReplicatedChunks = n
}

func (m *Metrics) SetRepairQueueDepth(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RepairQueueDepth = n
}

func (m *Metrics) IncStale() {
	m.mu.Lock()
	m.StaleReplicasDetected++
	m.MetadataInconsistencies++
	m.mu.Unlock()
}
func (m *Metrics) IncUnexpectedChunk() {
	m.mu.Lock()
	m.UnexpectedChunksObserved++
	m.MetadataInconsistencies++
	m.mu.Unlock()
}
func (m *Metrics) IncReReplicationStart()   { m.mu.Lock(); m.ReReplicationsStarted++; m.mu.Unlock() }
func (m *Metrics) IncReReplicationOK()      { m.mu.Lock(); m.ReReplicationsOK++; m.mu.Unlock() }
func (m *Metrics) IncReReplicationFailed()  { m.mu.Lock(); m.ReReplicationsFailed++; m.mu.Unlock() }
func (m *Metrics) IncMasterOp()             { m.mu.Lock(); m.MasterMetadataOps++; m.mu.Unlock() }
func (m *Metrics) IncMessage()              { m.mu.Lock(); m.MessagesSent++; m.mu.Unlock() }
func (m *Metrics) IncPlacementDecision()    { m.mu.Lock(); m.PlacementDecisions++; m.mu.Unlock() }
func (m *Metrics) IncRepairAttempt()        { m.mu.Lock(); m.RepairAttemptsTotal++; m.mu.Unlock() }
func (m *Metrics) IncVerificationFailure()  { m.mu.Lock(); m.VerificationFailures++; m.mu.Unlock() }
func (m *Metrics) IncVerifiedRepairCommit() { m.mu.Lock(); m.VerifiedRepairCommits++; m.mu.Unlock() }

func (m *Metrics) RecordDetection(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailureDetections++
	m.detectionSamples = append(m.detectionSamples, d)
}

func (m *Metrics) RecordRecovery(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recoverySamples = append(m.recoverySamples, d)
}

// Snapshot is a point-in-time copy safe to print or serialize.
type Snapshot struct {
	ReadSuccess, ReadFailure   uint64
	WriteSuccess, WriteFailure uint64
	UnderReplicatedChunks      uint64
	StaleReplicasDetected      uint64
	ReReplicationsStarted      uint64
	ReReplicationsOK           uint64
	ReReplicationsFailed       uint64
	MasterMetadataOps          uint64
	MessagesSent               uint64
	PlacementDecisions         uint64
	RepairQueueDepth           uint64
	RepairAttemptsTotal        uint64
	VerificationFailures       uint64
	VerifiedRepairCommits      uint64
	UnexpectedChunksObserved   uint64
	MetadataInconsistencies    uint64
	// FalseSuccessRepairCommits is always 0: GFSReplicationManager's code
	// path only ever calls AddReplicaLocation immediately after a
	// successful verification (see replication_manager.go), so there is no
	// branch that could produce a false-success commit to count. Reported
	// as a field anyway (per spec section 12's "target: 0" metric) so it's
	// an explicit, checkable claim rather than an implicit one — see
	// TestFalseSuccessRepairCommitsIsAlwaysZero.
	FalseSuccessRepairCommits uint64
	FailureDetections         uint64
	AvgDetectionLatency       time.Duration
	AvgRecoveryLatency        time.Duration
	RecoveryIncidents         uint64
}

func avg(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, s := range samples {
		total += s
	}
	return total / time.Duration(len(samples))
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{
		ReadSuccess: m.ReadSuccess, ReadFailure: m.ReadFailure,
		WriteSuccess: m.WriteSuccess, WriteFailure: m.WriteFailure,
		UnderReplicatedChunks:     m.UnderReplicatedChunks,
		StaleReplicasDetected:     m.StaleReplicasDetected,
		ReReplicationsStarted:     m.ReReplicationsStarted,
		ReReplicationsOK:          m.ReReplicationsOK,
		ReReplicationsFailed:      m.ReReplicationsFailed,
		MasterMetadataOps:         m.MasterMetadataOps,
		MessagesSent:              m.MessagesSent,
		PlacementDecisions:        m.PlacementDecisions,
		RepairQueueDepth:          m.RepairQueueDepth,
		RepairAttemptsTotal:       m.RepairAttemptsTotal,
		VerificationFailures:      m.VerificationFailures,
		VerifiedRepairCommits:     m.VerifiedRepairCommits,
		UnexpectedChunksObserved:  m.UnexpectedChunksObserved,
		MetadataInconsistencies:   m.MetadataInconsistencies,
		FalseSuccessRepairCommits: 0,
		FailureDetections:         m.FailureDetections,
		AvgDetectionLatency:       avg(m.detectionSamples),
		AvgRecoveryLatency:        avg(m.recoverySamples),
		RecoveryIncidents:         uint64(len(m.recoverySamples)),
	}
}

func (s Snapshot) String() string {
	return fmt.Sprintf(
		"reads=%d/%d writes=%d/%d under-replicated=%d stale=%d re-repl(start/ok/fail)=%d/%d/%d master-ops=%d messages=%d placement-decisions=%d "+
			"repair(queue-depth/attempts/verify-fail/verified-ok)=%d/%d/%d/%d unexpected-chunks=%d inconsistencies=%d false-success=%d "+
			"detections=%d avg-detect=%v avg-recover=%v recovery-incidents=%d",
		s.ReadSuccess, s.ReadFailure, s.WriteSuccess, s.WriteFailure,
		s.UnderReplicatedChunks, s.StaleReplicasDetected,
		s.ReReplicationsStarted, s.ReReplicationsOK, s.ReReplicationsFailed,
		s.MasterMetadataOps, s.MessagesSent, s.PlacementDecisions,
		s.RepairQueueDepth, s.RepairAttemptsTotal, s.VerificationFailures, s.VerifiedRepairCommits,
		s.UnexpectedChunksObserved, s.MetadataInconsistencies, s.FalseSuccessRepairCommits,
		s.FailureDetections, s.AvgDetectionLatency, s.AvgRecoveryLatency, s.RecoveryIncidents,
	)
}
