package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// HashRing implements consistent hashing.
type HashRing struct {
	mu           sync.RWMutex
	replicas     int               // Number of virtual nodes per physical node
	ring         []uint32          // Sorted hash values of all virtual nodes
	nodes        map[uint32]string // Maps hash value to physical node ID
	activeNodes  map[string]bool   // Tracks active nodes
}

// NewHashRing creates a new HashRing.
func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		replicas:    replicas,
		nodes:       make(map[uint32]string),
		activeNodes: make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring with its virtual nodes.
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.activeNodes[nodeID] = true
	for i := 0; i < hr.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%d", nodeID, i)))
		hr.ring = append(hr.ring, hash)
		hr.nodes[hash] = nodeID
	}
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// RemoveNode removes a physical node from the ring.
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	delete(hr.activeNodes, nodeID)
	var newRing []uint32
	for _, hash := range hr.ring {
		if hr.nodes[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(hr.nodes, hash)
		}
	}
	hr.ring = newRing
}

// GetNode finds the physical node responsible for a given key.
func (hr *HashRing) GetNode(key string) (string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return "", false
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	// sort.Search returns the smallest index i such that f(i) is true.
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= hash
	})

	// If idx == len(hr.ring), we've reached the end of the ring; wrap around to the first node.
	if idx == len(hr.ring) {
		idx = 0
	}

	return hr.nodes[hr.ring[idx]], true
}

// IsNodeActive checks if a node is currently marked as active.
func (hr *HashRing) IsNodeActive(nodeID string) bool {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.activeNodes[nodeID]
}
