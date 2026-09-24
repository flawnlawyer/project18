package master

import (
	"sync"
	"time"

	"project18/internal/common"
)

// LeaseInfo describes an active mutation lease on a chunk: one primary that
// orders mutations, plus secondaries that apply them in that order.
type LeaseInfo struct {
	Primary     common.ServerID
	Secondaries []common.ServerID
	Version     common.ChunkVersion
	Expiry      time.Time
}

func (l LeaseInfo) Replicas() []common.ServerID {
	return append([]common.ServerID{l.Primary}, l.Secondaries...)
}

// LeaseManager is the replaceable interface for spec section 9. The baseline
// grants leases on demand (no lease exists until a write needs one) rather
// than pre-assigning them, matching real GFS.
type LeaseManager interface {
	// Grant installs a new lease, replacing any existing one for handle.
	Grant(handle common.ChunkHandle, primary common.ServerID, secondaries []common.ServerID, version common.ChunkVersion, now time.Time, ttl time.Duration) LeaseInfo
	// Get returns the current lease if one is active (not expired) at `now`.
	Get(handle common.ChunkHandle, now time.Time) (LeaseInfo, bool)
	Revoke(handle common.ChunkHandle)
	// RevokeForServer drops any lease whose primary is `server` (e.g. because
	// the primary just failed) and returns the affected chunk handles.
	RevokeForServer(server common.ServerID) []common.ChunkHandle
}

type GFSLeaseManager struct {
	mu     sync.Mutex
	leases map[common.ChunkHandle]LeaseInfo
}

func NewGFSLeaseManager() *GFSLeaseManager {
	return &GFSLeaseManager{leases: make(map[common.ChunkHandle]LeaseInfo)}
}

func (m *GFSLeaseManager) Grant(handle common.ChunkHandle, primary common.ServerID, secondaries []common.ServerID, version common.ChunkVersion, now time.Time, ttl time.Duration) LeaseInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := LeaseInfo{Primary: primary, Secondaries: secondaries, Version: version, Expiry: now.Add(ttl)}
	m.leases[handle] = l
	return l
}

func (m *GFSLeaseManager) Get(handle common.ChunkHandle, now time.Time) (LeaseInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[handle]
	if !ok || now.After(l.Expiry) {
		return LeaseInfo{}, false
	}
	return l, true
}

func (m *GFSLeaseManager) Revoke(handle common.ChunkHandle) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, handle)
}

func (m *GFSLeaseManager) RevokeForServer(server common.ServerID) []common.ChunkHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	var affected []common.ChunkHandle
	for h, l := range m.leases {
		if l.Primary == server {
			affected = append(affected, h)
			delete(m.leases, h)
		}
	}
	return affected
}
