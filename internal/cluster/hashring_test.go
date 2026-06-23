package cluster

import (
	"testing"
)

func TestHashRing(t *testing.T) {
	hr := NewHashRing(3) // 3 replicas per node

	hr.AddNode("node-1")
	hr.AddNode("node-2")

	nodeA, okA := hr.GetNode("document-1")
	nodeB, okB := hr.GetNode("document-2")

	if !okA || (nodeA != "node-1" && nodeA != "node-2") {
		t.Errorf("Unexpected node for document-1: %s", nodeA)
	}
	if !okB || (nodeB != "node-1" && nodeB != "node-2") {
		t.Errorf("Unexpected node for document-2: %s", nodeB)
	}

	// Test consistency: same key should yield same node
	nodeA2, _ := hr.GetNode("document-1")
	if nodeA != nodeA2 {
		t.Errorf("Inconsistent mapping for document-1: %s != %s", nodeA, nodeA2)
	}

	// Test removal
	hr.RemoveNode("node-1")
	nodeA3, _ := hr.GetNode("document-1")
	if nodeA3 != "node-2" {
		t.Errorf("document-1 should map to node-2 after node-1 removal, got %s", nodeA3)
	}
}
