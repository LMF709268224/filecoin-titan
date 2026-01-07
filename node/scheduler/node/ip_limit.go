package node

import "sync"

// IPMgr node ip info manager
type IPMgr struct {
	ipLimit int
	nodeIPs map[string][]string
	mu      sync.Mutex
}

func newIPMgr(limit int) *IPMgr {
	return &IPMgr{
		ipLimit: limit,
		nodeIPs: make(map[string][]string),
	}
}

// StoreNodeIP store node
func (m *IPMgr) StoreNodeIP(nodeID, ip string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	list, ok := m.nodeIPs[ip]
	if ok {
		for _, nID := range list {
			if nID == nodeID {
				return true
			}
		}

		if len(list) < m.ipLimit {
			m.nodeIPs[ip] = append(list, nodeID)
			return true
		}

		return false
	}

	m.nodeIPs[ip] = []string{nodeID}
	return true
}

// RemoveNodeIP remove node
func (m *IPMgr) RemoveNodeIP(nodeID, ip string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	list, ok := m.nodeIPs[ip]
	if ok {
		nList := make([]string, 0, len(list))
		for _, nID := range list {
			if nID != nodeID {
				nList = append(nList, nID)
			}
		}

		if len(nList) == 0 {
			delete(m.nodeIPs, ip)
		} else {
			m.nodeIPs[ip] = nList
		}
	}
}

// GetNodeOfIP get node
func (m *IPMgr) GetNodeOfIP(ip string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	list, ok := m.nodeIPs[ip]
	if ok {
		// Return a copy to avoid race on the slice
		cp := make([]string, len(list))
		copy(cp, list)
		return cp
	}

	return []string{}
}

// CheckIPExist check node
func (m *IPMgr) CheckIPExist(ip string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.nodeIPs[ip]
	return ok
}
