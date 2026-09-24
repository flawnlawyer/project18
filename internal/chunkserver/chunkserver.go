// Package chunkserver implements the StorageNode replaceable interface: a
// node that holds chunk replicas, tracks their versions, and reports its
// health via heartbeat. Per the spec, chunk data is represented as a
// metadata/state object (a []byte payload of whatever size the client wrote)
// rather than physically consuming 64MB+ per chunk — this is a simulation.
package chunkserver

import (
	"fmt"
	"sync"

	"project18/internal/common"
	"project18/internal/events"
)

// StorageNode is the replaceable interface every chunkserver implementation
// must satisfy. Master and Client code should depend on this, never on
// *ChunkServer directly, so a future experiment can swap in a different
// storage node implementation.
type StorageNode interface {
	ID() common.ServerID
	IsOnline() bool

	StoreChunk(handle common.ChunkHandle, version common.ChunkVersion, data []byte) error
	ReadChunk(handle common.ChunkHandle) (data []byte, version common.ChunkVersion, err error)
	DeleteChunk(handle common.ChunkHandle)
	HasChunk(handle common.ChunkHandle) (version common.ChunkVersion, ok bool)
	ChunkHandles() []common.ChunkHandle

	Capacity() (total, used uint64)
}

// ChunkServer is the baseline, GFS-style StorageNode.
type ChunkServer struct {
	mu sync.RWMutex

	id     common.ServerID
	online bool

	chunks map[common.ChunkHandle]replica
	total  uint64
	used   uint64

	log *events.Log
}

type replica struct {
	version common.ChunkVersion
	data    []byte
}

func New(id common.ServerID, capacity uint64, log *events.Log) *ChunkServer {
	return &ChunkServer{
		id:     id,
		online: true,
		chunks: make(map[common.ChunkHandle]replica),
		total:  capacity,
		log:    log,
	}
}

func (cs *ChunkServer) ID() common.ServerID { return cs.id }

func (cs *ChunkServer) IsOnline() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.online
}

// SetOnline is used by the failure injector, not by normal operation code.
func (cs *ChunkServer) SetOnline(v bool) {
	cs.mu.Lock()
	cs.online = v
	cs.mu.Unlock()
}

func (cs *ChunkServer) StoreChunk(handle common.ChunkHandle, version common.ChunkVersion, data []byte) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.online {
		return fmt.Errorf("chunkserver %s is offline", cs.id)
	}
	existing, had := cs.chunks[handle]
	if had {
		cs.used -= uint64(len(existing.data))
	}
	if cs.used+uint64(len(data)) > cs.total {
		return fmt.Errorf("chunkserver %s: out of capacity", cs.id)
	}
	cs.chunks[handle] = replica{version: version, data: data}
	cs.used += uint64(len(data))
	if cs.log != nil {
		cs.log.Logf(string(cs.id), "stored %s at version %d (%d bytes)", handle, version, len(data))
	}
	return nil
}

func (cs *ChunkServer) ReadChunk(handle common.ChunkHandle) ([]byte, common.ChunkVersion, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if !cs.online {
		return nil, 0, fmt.Errorf("chunkserver %s is offline", cs.id)
	}
	r, ok := cs.chunks[handle]
	if !ok {
		return nil, 0, fmt.Errorf("chunkserver %s: no such chunk %s", cs.id, handle)
	}
	out := make([]byte, len(r.data))
	copy(out, r.data)
	return out, r.version, nil
}

func (cs *ChunkServer) DeleteChunk(handle common.ChunkHandle) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if r, ok := cs.chunks[handle]; ok {
		cs.used -= uint64(len(r.data))
		delete(cs.chunks, handle)
	}
}

func (cs *ChunkServer) HasChunk(handle common.ChunkHandle) (common.ChunkVersion, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	r, ok := cs.chunks[handle]
	return r.version, ok
}

func (cs *ChunkServer) ChunkHandles() []common.ChunkHandle {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	out := make([]common.ChunkHandle, 0, len(cs.chunks))
	for h := range cs.chunks {
		out = append(out, h)
	}
	return out
}

func (cs *ChunkServer) Capacity() (total, used uint64) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.total, cs.used
}
