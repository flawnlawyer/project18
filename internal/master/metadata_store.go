package master

import (
	"fmt"
	"sync"

	"project18/internal/common"
)

// ChunkMeta is everything the master knows about one chunk. Note this is
// metadata only — actual chunk bytes live on the chunkservers.
type ChunkMeta struct {
	Version   common.ChunkVersion
	Locations map[common.ServerID]bool // replicas the master believes hold this chunk
}

func (c ChunkMeta) locationList() []common.ServerID {
	out := make([]common.ServerID, 0, len(c.Locations))
	for s := range c.Locations {
		out = append(out, s)
	}
	return out
}

// MetadataStore is the replaceable interface for everything section 2 of the
// spec assigns to the master: namespace, file->chunk mapping, chunk IDs,
// locations, and versions. It deliberately knows nothing about leases,
// heartbeats, or placement — those are separate replaceable interfaces.
type MetadataStore interface {
	CreateFile(path common.Path) error
	FileExists(path common.Path) bool
	ChunkHandles(path common.Path) ([]common.ChunkHandle, error)
	AppendChunkToFile(path common.Path, handle common.ChunkHandle) error

	NewChunkHandle() common.ChunkHandle
	InitChunk(handle common.ChunkHandle, version common.ChunkVersion, locations []common.ServerID)
	GetChunkMeta(handle common.ChunkHandle) (ChunkMeta, bool)
	AddReplicaLocation(handle common.ChunkHandle, server common.ServerID)
	RemoveReplicaLocation(handle common.ChunkHandle, server common.ServerID)
	BumpVersion(handle common.ChunkHandle) common.ChunkVersion
	AllChunkHandles() []common.ChunkHandle

	Snapshot() Checkpoint
	Restore(c Checkpoint)
}

// InMemoryMetadataStore is the baseline GFS-style MetadataStore.
type InMemoryMetadataStore struct {
	mu sync.RWMutex

	files      map[common.Path][]common.ChunkHandle
	chunks     map[common.ChunkHandle]ChunkMeta
	nextHandle common.ChunkHandle
}

func NewInMemoryMetadataStore() *InMemoryMetadataStore {
	return &InMemoryMetadataStore{
		files:  make(map[common.Path][]common.ChunkHandle),
		chunks: make(map[common.ChunkHandle]ChunkMeta),
	}
}

func (s *InMemoryMetadataStore) CreateFile(path common.Path) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[path]; ok {
		return fmt.Errorf("file %s already exists", path)
	}
	s.files[path] = nil
	return nil
}

func (s *InMemoryMetadataStore) FileExists(path common.Path) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.files[path]
	return ok
}

func (s *InMemoryMetadataStore) ChunkHandles(path common.Path) ([]common.ChunkHandle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	handles, ok := s.files[path]
	if !ok {
		return nil, fmt.Errorf("file %s does not exist", path)
	}
	out := make([]common.ChunkHandle, len(handles))
	copy(out, handles)
	return out, nil
}

func (s *InMemoryMetadataStore) AppendChunkToFile(path common.Path, handle common.ChunkHandle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[path]; !ok {
		return fmt.Errorf("file %s does not exist", path)
	}
	s.files[path] = append(s.files[path], handle)
	return nil
}

func (s *InMemoryMetadataStore) NewChunkHandle() common.ChunkHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.nextHandle
	s.nextHandle++
	return h
}

func (s *InMemoryMetadataStore) InitChunk(handle common.ChunkHandle, version common.ChunkVersion, locations []common.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	locs := make(map[common.ServerID]bool, len(locations))
	for _, l := range locations {
		locs[l] = true
	}
	s.chunks[handle] = ChunkMeta{Version: version, Locations: locs}
}

func (s *InMemoryMetadataStore) GetChunkMeta(handle common.ChunkHandle) (ChunkMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.chunks[handle]
	if !ok {
		return ChunkMeta{}, false
	}
	// return a defensive copy of the location set
	cp := ChunkMeta{Version: m.Version, Locations: make(map[common.ServerID]bool, len(m.Locations))}
	for k, v := range m.Locations {
		cp.Locations[k] = v
	}
	return cp, true
}

func (s *InMemoryMetadataStore) AddReplicaLocation(handle common.ChunkHandle, server common.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.chunks[handle]
	if !ok {
		return
	}
	m.Locations[server] = true
}

func (s *InMemoryMetadataStore) RemoveReplicaLocation(handle common.ChunkHandle, server common.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.chunks[handle]
	if !ok {
		return
	}
	delete(m.Locations, server)
}

func (s *InMemoryMetadataStore) BumpVersion(handle common.ChunkHandle) common.ChunkVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.chunks[handle]
	if !ok {
		return 0
	}
	m.Version++
	s.chunks[handle] = m
	return m.Version
}

func (s *InMemoryMetadataStore) AllChunkHandles() []common.ChunkHandle {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]common.ChunkHandle, 0, len(s.chunks))
	for h := range s.chunks {
		out = append(out, h)
	}
	return out
}

// Checkpoint is a full snapshot of metadata state, used by the operation-log
// + checkpoint persistence scheme (spec section 11).
type Checkpoint struct {
	Files      map[common.Path][]common.ChunkHandle
	Chunks     map[common.ChunkHandle]ChunkMeta
	NextHandle common.ChunkHandle
}

func (s *InMemoryMetadataStore) Snapshot() Checkpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := Checkpoint{
		Files:      make(map[common.Path][]common.ChunkHandle, len(s.files)),
		Chunks:     make(map[common.ChunkHandle]ChunkMeta, len(s.chunks)),
		NextHandle: s.nextHandle,
	}
	for p, hs := range s.files {
		cpHandles := make([]common.ChunkHandle, len(hs))
		copy(cpHandles, hs)
		cp.Files[p] = cpHandles
	}
	for h, m := range s.chunks {
		locs := make(map[common.ServerID]bool, len(m.Locations))
		for k, v := range m.Locations {
			locs[k] = v
		}
		cp.Chunks[h] = ChunkMeta{Version: m.Version, Locations: locs}
	}
	return cp
}

func (s *InMemoryMetadataStore) Restore(c Checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = make(map[common.Path][]common.ChunkHandle, len(c.Files))
	for p, hs := range c.Files {
		cpHandles := make([]common.ChunkHandle, len(hs))
		copy(cpHandles, hs)
		s.files[p] = cpHandles
	}
	s.chunks = make(map[common.ChunkHandle]ChunkMeta, len(c.Chunks))
	for h, m := range c.Chunks {
		locs := make(map[common.ServerID]bool, len(m.Locations))
		for k, v := range m.Locations {
			locs[k] = v
		}
		s.chunks[h] = ChunkMeta{Version: m.Version, Locations: locs}
	}
	s.nextHandle = c.NextHandle
}

func (c ChunkMeta) String() string {
	return fmt.Sprintf("v%d locations=%v", c.Version, mapKeys(c.Locations))
}

func mapKeys(m map[common.ServerID]bool) []common.ServerID {
	out := make([]common.ServerID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
