package master

import (
	"sync"

	"project18/internal/common"
)

// OpType enumerates the mutations the master persists so it can rebuild
// state after a restart without replaying arbitrary Go closures.
type OpType string

const (
	OpCreateFile    OpType = "CREATE_FILE"
	OpAppendChunk   OpType = "APPEND_CHUNK"
	OpInitChunk     OpType = "INIT_CHUNK"
	OpAddReplica    OpType = "ADD_REPLICA"
	OpRemoveReplica OpType = "REMOVE_REPLICA"
	OpBumpVersion   OpType = "BUMP_VERSION"
)

// Operation is one entry in the operation log.
type Operation struct {
	Seq       uint64
	Type      OpType
	Path      common.Path
	Handle    common.ChunkHandle
	Server    common.ServerID
	Version   common.ChunkVersion
	Locations []common.ServerID // only used by OpInitChunk
}

// OperationLog is a simple in-memory write-ahead log. In a real system this
// would be fsynced to disk per the spec's "operation -> metadata update ->
// operation log -> periodic checkpoint" pipeline; here it models the
// architecture (durability boundary + replay) without physical disk I/O,
// per "Do not over-engineer physical disk persistence yet."
type OperationLog struct {
	mu  sync.Mutex
	ops []Operation
	seq uint64
}

func NewOperationLog() *OperationLog {
	return &OperationLog{}
}

func (l *OperationLog) Append(op Operation) Operation {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	op.Seq = l.seq
	l.ops = append(l.ops, op)
	return op
}

func (l *OperationLog) Since(seq uint64) []Operation {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Operation
	for _, op := range l.ops {
		if op.Seq > seq {
			out = append(out, op)
		}
	}
	return out
}

func (l *OperationLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ops)
}

func (l *OperationLog) CurrentSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// CheckpointRecord pairs a metadata snapshot with the log position it
// reflects, so replay after restore only needs ops After that seq.
type CheckpointRecord struct {
	Snapshot Checkpoint
	AfterSeq uint64
}

// Apply replays a single operation against a MetadataStore. Used both for
// normal replay-from-checkpoint and (in principle) for shipping the log to a
// standby/shadow master.
func Apply(meta MetadataStore, op Operation) {
	switch op.Type {
	case OpCreateFile:
		_ = meta.CreateFile(op.Path)
	case OpAppendChunk:
		_ = meta.AppendChunkToFile(op.Path, op.Handle)
	case OpInitChunk:
		meta.InitChunk(op.Handle, op.Version, op.Locations)
	case OpAddReplica:
		meta.AddReplicaLocation(op.Handle, op.Server)
	case OpRemoveReplica:
		meta.RemoveReplicaLocation(op.Handle, op.Server)
	case OpBumpVersion:
		meta.BumpVersion(op.Handle)
	}
}
