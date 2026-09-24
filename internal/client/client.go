// Package client implements the GFS client. The load-bearing architectural
// rule from spec section "Client" applies here: the client asks the master
// for metadata, then talks DIRECTLY to chunkservers for data. File bytes
// never round-trip through the master.
package client

import (
	"bytes"
	"fmt"

	"project18/internal/chunkserver"
	"project18/internal/common"
	"project18/internal/events"
	"project18/internal/master"
	"project18/internal/metrics"
	"project18/internal/network"
)

type Client struct {
	id        common.ServerID
	m         *master.Master
	nodes     func(common.ServerID) (chunkserver.StorageNode, bool)
	net       *network.InProcessNetwork
	log       *events.Log
	metrics   *metrics.Metrics
	chunkSize int
}

func New(id common.ServerID, m *master.Master, nodes func(common.ServerID) (chunkserver.StorageNode, bool), net *network.InProcessNetwork, log *events.Log, met *metrics.Metrics, chunkSize int) *Client {
	return &Client{id: id, m: m, nodes: nodes, net: net, log: log, metrics: met, chunkSize: chunkSize}
}

func (c *Client) CreateFile(path common.Path) error {
	if err := c.m.CreateFile(path); err != nil {
		c.log.Logf(string(c.id), "create %s failed: %v", path, err)
		return err
	}
	c.log.Logf(string(c.id), "created %s", path)
	return nil
}

// Write splits data into chunk-sized pieces, allocating a new chunk from the
// master for each piece and pushing the data to the chunk's primary, which
// (per the GFS mutation model) is responsible for propagating to secondaries
// before acknowledging.
func (c *Client) Write(path common.Path, data []byte) error {
	ok := true
	for offset := 0; offset < len(data) || len(data) == 0; offset += c.chunkSize {
		end := offset + c.chunkSize
		if end > len(data) {
			end = len(data)
		}
		piece := data[offset:end]
		if err := c.writeChunk(path, piece); err != nil {
			ok = false
			c.metrics.IncWrite(false)
			c.log.Logf(string(c.id), "write %s failed: %v", path, err)
			return err
		}
		if len(data) == 0 {
			break
		}
	}
	if ok {
		c.metrics.IncWrite(true)
	}
	return nil
}

func (c *Client) writeChunk(path common.Path, piece []byte) error {
	handle, _, err := c.m.AllocateChunk(path)
	if err != nil {
		return fmt.Errorf("allocate chunk: %w", err)
	}

	lease, err := c.m.RequestLease(handle)
	if err != nil {
		return fmt.Errorf("request lease: %w", err)
	}

	primaryNode, ok := c.nodes(lease.Primary)
	if !ok {
		return fmt.Errorf("primary %s not found", lease.Primary)
	}

	// Client -> primary: send the mutation.
	err = c.net.Call(c.id, lease.Primary, func() error {
		return primaryNode.StoreChunk(handle, lease.Version, piece)
	})
	if err != nil {
		return fmt.Errorf("write to primary %s: %w", lease.Primary, err)
	}
	c.log.Logf(string(c.id), "wrote %d bytes of %s to primary %s (%s)", len(piece), path, lease.Primary, handle)

	// Primary -> secondaries: the primary propagates the mutation in the
	// order it received it, per spec section 9.
	for _, secondary := range lease.Secondaries {
		node, ok := c.nodes(secondary)
		if !ok {
			c.log.Logf(string(lease.Primary), "secondary %s not found, skipping replication of %s", secondary, handle)
			continue
		}
		err := c.net.Call(lease.Primary, secondary, func() error {
			return node.StoreChunk(handle, lease.Version, piece)
		})
		if err != nil {
			c.log.Logf(string(lease.Primary), "replicate %s to secondary %s failed: %v", handle, secondary, err)
			continue
		}
		c.log.Logf(string(lease.Primary), "replicated %s to secondary %s", handle, secondary)
	}
	return nil
}

// Read reassembles a file by reading each of its chunks from any replica the
// master reports as alive.
func (c *Client) Read(path common.Path) ([]byte, error) {
	n, err := c.m.ChunkCount(path)
	if err != nil {
		c.metrics.IncRead(false)
		return nil, err
	}
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		handle, locations, err := c.m.GetChunkLocations(path, i)
		if err != nil {
			c.metrics.IncRead(false)
			return nil, err
		}
		if len(locations) == 0 {
			c.metrics.IncRead(false)
			return nil, fmt.Errorf("%s: no alive replicas for chunk %d (%s)", path, i, handle)
		}
		server := locations[0]
		node, ok := c.nodes(server)
		if !ok {
			c.metrics.IncRead(false)
			return nil, fmt.Errorf("replica %s not found", server)
		}
		var data []byte
		err = c.net.Call(c.id, server, func() error {
			d, _, rerr := node.ReadChunk(handle)
			data = d
			return rerr
		})
		if err != nil {
			c.metrics.IncRead(false)
			return nil, fmt.Errorf("read %s from %s: %w", handle, server, err)
		}
		buf.Write(data)
		c.log.Logf(string(c.id), "read %s (chunk %d, %s) from %s", path, i, handle, server)
	}
	c.metrics.IncRead(true)
	return buf.Bytes(), nil
}
