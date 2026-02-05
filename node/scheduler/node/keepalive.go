package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

type BatchUpdate struct {
	Nodes      *[]types.NodeDynamicInfo
	Details    *[]*types.ProfitDetails
	OnlineData map[string]int
	SaveDate   time.Time
}

func (m *Manager) processNode(node *Node, t time.Time, minute int, isSave bool) (bool, *types.NodeDynamicInfo, *types.ProfitDetails) {
	isOnline := m.checkNodeStatus(node, t)
	if !isOnline {
		return false, nil, nil
	}

	node.AddOnlineDuration(minute)
	node.AddTodayOnlineTimeWindow((minute * 60) / 5)

	if !isSave {
		return true, nil, nil
	}

	var dInfo *types.ProfitDetails
	profitMinute := node.TodayOnlineTimeWindow * 5 / 60

	switch node.Type {
	case types.NodeEdge, types.NodeL3:
		incr, details := m.GetEdgeBaseProfitDetails(node, profitMinute)
		node.IncomeIncr = float32(incr)
		dInfo = details
	case types.NodeCandidate:
		if !node.IsAbnormal() && qualifiedNAT(node.NATType) {
			dInfo = m.GetCandidateBaseProfitDetails(node, profitMinute)
		}
	}

	dyInfo := node.GetDynamicInfoWithIncrement()
	return true, &dyInfo, dInfo
}

func (m *Manager) startNodeKeepaliveTimer() {
	<-time.After(10 * time.Minute)
	m.nodesKeepalive(10, true)

	minute := 2

	ticker := time.NewTicker(time.Duration(minute) * time.Minute)
	defer ticker.Stop()

	saveCounter := 0
	for range ticker.C {
		saveCounter++
		if saveCounter >= 5 { // 5 * 2 minutes = 10 minutes (matches saveInfoInterval)
			m.nodesKeepalive(minute, true)
			saveCounter = 0
		} else {
			m.nodesKeepalive(minute, false)
		}
	}
}

func (m *Manager) nodesKeepalive(minute int, isSave bool) {
	now := time.Now()

	// --- Scheduler Health Guard (Anti-Kill) Logic ---
	// Expected interval is 'minute' minutes (2 or 10).
	// If the actual gap is much larger (e.g., > 5 mins over expected),
	// the scheduler was likely frozen.
	gracePeriod := time.Duration(0)
	if !m.lastKeepaliveTime.IsZero() {
		gap := now.Sub(m.lastKeepaliveTime)
		expected := time.Duration(minute) * time.Minute
		if gap > expected+5*time.Minute {
			gracePeriod = gap - expected
			log.Warnf("Scheduler lag detected! Gap: %v, Expected: %v. Applying grace extension: %v", gap, expected, gracePeriod)
		}
	}
	// Update last run time before processing
	m.lastKeepaliveTime = now

	// Standard threshold (10 minutes) + Grace period from scheduler lag
	t := now.Add(-(keepaliveTime + gracePeriod))

	timeWindow := (minute * 60) / 5
	m.mu.Lock()
	m.serverTodayOnlineTimeWindow += timeWindow
	m.mu.Unlock()

	nodesCount := int(m.Edges + m.Candidates + m.L5Count + m.L3Count)
	if nodesCount < 1000 {
		nodesCount = 1000
	}

	batch := BatchUpdate{
		SaveDate:   time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()),
		OnlineData: make(map[string]int, nodesCount),
		Nodes:      GetDynamicInfoSlice(),
		Details:    GetProfitDetailsSlice(),
	}
	defer func() {
		PutDynamicInfoSlice(batch.Nodes)
		PutProfitDetailsSlice(batch.Details)
	}()

	m.onlineNodes.Range(func(nodeID string, value interface{}) bool {
		node := value.(*Node)
		isOnline, dyInfo, dInfo := m.processNode(node, t, minute, isSave)
		if isOnline && isSave {
			if dyInfo != nil {
				*batch.Nodes = append(*batch.Nodes, *dyInfo)
			}
			if dInfo != nil {
				*batch.Details = append(*batch.Details, dInfo)
			}
			batch.OnlineData[node.NodeID] = node.TodayOnlineTimeWindow
			node.ResetOnlineDurationIncrement()
			node.ResetTodayOnlineTimeWindow()
		}
		return true
	})

	if isSave {
		m.mu.Lock()
		batch.OnlineData[string(m.ServerID)] = m.serverTodayOnlineTimeWindow
		m.serverTodayOnlineTimeWindow = 0
		m.mu.Unlock()

		// Use fast update for large batches (500+ nodes)
		var err error
		if len(*batch.Nodes) >= 500 {
			err = m.UpdateNodeDynamicInfoFast(*batch.Nodes)
		} else {
			err = m.UpdateNodeDynamicInfo(*batch.Nodes)
		}
		if err != nil {
			log.Errorf("updateNodeData UpdateNodeDynamicInfo err:%s", err.Error())
		}

		err = m.AddNodeProfitDetails(*batch.Details)
		if err != nil {
			log.Errorf("updateNodeData AddNodeProfits err:%s", err.Error())
		}

		err = m.UpdateNodeOnlineCount(batch.OnlineData, batch.SaveDate)
		if err != nil {
			log.Errorf("updateNodeData UpdateNodeOnlineCount err:%s", err.Error())
		}
	}
}

// SetNodeOffline removes the node's IP and geo information from the manager.
func (m *Manager) SetNodeOffline(node *Node) {
	m.IPMgr.RemoveNodeIP(node.NodeID, node.ExternalIP)
	m.GeoMgr.RemoveNodeGeo(node.NodeID, node.Type, node.AreaID)
	node.Close()

	switch node.Type {
	case types.NodeCandidate:
		m.deleteCandidateNode(node)
	case types.NodeEdge:
		m.deleteEdgeNode(node)
	case types.NodeL5:
		m.deleteL5Node(node)
	case types.NodeL3:
		m.deleteL3Node(node)
	}

	// log.Debugf("node offline %s, %s", node.NodeID, node.ExternalIP)
}

// checkNodeStatus checks if a node has sent a keepalive recently and updates node status accordingly
func (m *Manager) checkNodeStatus(node *Node, t time.Time) bool {
	lastTime := node.LastRequestTime()

	if t.After(lastTime.Add(keepaliveTime)) {
		m.SetNodeOffline(node)

		return false
	}

	return true
}
