package node

import (
	"fmt"
	"math/rand"

	"github.com/Filecoin-Titan/titan/api/types"
)

// GetResourceEdgeNodes retrieves all edge nodes that are available for resource utilization.
func (m *Manager) GetResourceEdgeNodes() []*Node {
	nodes := make([]*Node, 0)

	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeEdge {
			return true
		}

		if !node.IsResourceNode() {
			return true
		}

		nodes = append(nodes, node)

		return true
	})

	return nodes
}

// GetResourceCandidateNodes retrieves all valid candidate nodes that are available for resource utilization.
func (m *Manager) GetResourceCandidateNodes() ([]string, []*Node) {
	var ids []string
	var nodes []*Node
	m.onlineNodes.Range(func(key string, value interface{}) bool {
		nodeID := key
		node := value.(*Node)

		if node.Type != types.NodeCandidate {
			return true
		}

		if !node.IsResourceNode() {
			return true
		}

		ids = append(ids, nodeID)
		nodes = append(nodes, node)
		return true
	})

	return ids, nodes
}

// GetValidEdgeNode load all edge node
func (m *Manager) GetValidEdgeNode() []*Node {
	nodes := make([]*Node, 0)

	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeEdge {
			return true
		}

		if node.IsAbnormal() {
			return true
		}

		nodes = append(nodes, node)

		return true
	})

	return nodes
}

// GetValidL3Node load all edge node
func (m *Manager) GetValidL3Node() []*Node {
	nodes := make([]*Node, 0)

	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeL3 {
			return true
		}

		if node.IsAbnormal() {
			return true
		}

		nodes = append(nodes, node)

		return true
	})

	return nodes
}

// GetValidL5Node load all edge node
func (m *Manager) GetValidL5Node() []*Node {
	nodes := make([]*Node, 0)

	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeL5 {
			return true
		}

		if node.IsAbnormal() {
			return true
		}

		nodes = append(nodes, node)

		return true
	})

	return nodes
}

// GetValidCandidateNodes  Get all valid candidate nodes
func (m *Manager) GetValidCandidateNodes() ([]string, []*Node) {
	var ids []string
	var nodes []*Node
	m.onlineNodes.Range(func(key string, value interface{}) bool {
		nodeID := key
		node := value.(*Node)

		if node.Type != types.NodeCandidate {
			return true
		}

		if node.IsAbnormal() {
			return true
		}

		ids = append(ids, nodeID)
		nodes = append(nodes, node)
		return true
	})

	return ids, nodes
}

// GetAllCandidateNodes  Get all valid candidate nodes
func (m *Manager) GetAllCandidateNodes() []*Node {
	var nodes []*Node
	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeCandidate {
			return true
		}

		nodes = append(nodes, node)
		return true
	})

	return nodes
}

// GetCandidateNodes return n candidate node
func (m *Manager) GetCandidateNodes(num int) []*Node {
	var out []*Node
	m.onlineNodes.Range(func(key string, value interface{}) bool {
		node := value.(*Node)

		if node.Type != types.NodeCandidate {
			return true
		}

		out = append(out, node)
		return len(out) < num
	})

	return out
}

// GetNode retrieves a node with the given node ID
func (m *Manager) GetNode(nodeID string) *Node {
	nodeI, exist := m.onlineNodes.Load(nodeID)
	if exist && nodeI != nil {
		node := nodeI.(*Node)

		return node
	}

	return nil
}

// GetEdgeNode retrieves an edge node with the given node ID
func (m *Manager) GetEdgeNode(nodeID string) *Node {
	node := m.GetNode(nodeID)
	if node != nil && node.Type == types.NodeEdge {
		return node
	}

	return nil
}

// GetCandidateNode retrieves a candidate node with the given node ID
func (m *Manager) GetCandidateNode(nodeID string) *Node {
	node := m.GetNode(nodeID)
	if node != nil && node.Type == types.NodeCandidate {
		return node
	}

	return nil
}

// GetL5Node retrieves a l5 node with the given node ID
func (m *Manager) GetL5Node(nodeID string) *Node {
	node := m.GetNode(nodeID)
	if node != nil && node.Type == types.NodeL5 {
		return node
	}

	return nil
}

// GetL3Node retrieves a l3 node with the given node ID
func (m *Manager) GetL3Node(nodeID string) *Node {
	node := m.GetNode(nodeID)
	if node != nil && node.Type == types.NodeL3 {
		return node
	}

	return nil
}

// GetOnlineNodeCount returns online node count of the given type
func (m *Manager) GetOnlineNodeCount(nodeType types.NodeType) int {
	i := int64(0)
	if nodeType == types.NodeUnknown || nodeType == types.NodeCandidate {
		i += m.Candidates
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeEdge {
		i += m.Edges
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeL3 {
		i += m.L3Count
	}

	if nodeType == types.NodeUnknown || nodeType == types.NodeL5 {
		i += m.L5Count
	}

	return int(i)
}

// NodeOnline registers a node as online
func (m *Manager) NodeOnline(node *Node, info *types.NodeInfo) error {
	switch node.Type {
	case types.NodeEdge:
		m.storeEdgeNode(node)
	case types.NodeCandidate:
		m.storeCandidateNode(node)
	case types.NodeL5:
		m.storeL5Node(node)
	case types.NodeL3:
		m.storeL3Node(node)
	}

	// m.UpdateNodeDiskUsage(info.NodeID, info.DiskUsage)

	return nil
}

// GetRandomCandidates returns a random candidate node
func (m *Manager) GetRandomCandidates(count int) map[string]int {
	return m.weightMgr.getCandidateWeightRandom(count)
}

// GetRandomEdges returns a random edge node
func (m *Manager) GetRandomEdges(count int) map[string]int {
	return m.weightMgr.getEdgeWeightRandom(count)
}

// SetTunserverURL set node Tunserver URL
func (m *Manager) SetTunserverURL(edgeID, candidateID string) error {
	node := m.GetEdgeNode(edgeID)
	if node != nil {
		node.WSServerID = candidateID
	}

	return m.SaveWSServerID(edgeID, candidateID)
}

// UpdateTunserverURL update node Tunserver URL
func (m *Manager) UpdateTunserverURL(edgeID string) (*Node, error) {
	var vNode *Node
	// select candidate
	_, list := m.GetResourceCandidateNodes()
	if len(list) > 0 {
		index := rand.Intn(len(list))
		vNode = list[index]
	}

	if vNode == nil {
		return vNode, fmt.Errorf("node not found")
	}

	vID := vNode.NodeID

	node := m.GetEdgeNode(edgeID)
	if node != nil {
		node.WSServerID = vID
	}

	return vNode, m.SaveWSServerID(edgeID, vID)
}
