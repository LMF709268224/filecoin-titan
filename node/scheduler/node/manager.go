package node

import (
	"context"
	"crypto/rsa"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/docker/go-units"
	"github.com/filecoin-project/pubsub"

	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("node")

	levelSelectWeight = []int{1, 2, 3, 4, 5}
	nodeScoreLevel    = []int{20, 50, 70, 90, 100}
)

const (
	loadCandidateInfoTime = 3 * time.Minute // seconds
	// keepaliveTime is the interval between keepalive requests
	keepaliveTime = 10 * time.Minute

	// saveInfoInterval is the interval at which node information is saved during keepalive requests
	saveInfoInterval = 10 * time.Minute // keepalive saves information
	penaltyInterval  = 60 * time.Second

	oneDay = 24 * time.Hour

	penaltyFreeTime = 10 * time.Minute

	calcScoreTime = 5 * time.Minute

	qualityCheckTime = 15 * time.Minute

	bandwidthEventTime = time.Hour
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	edgeNodes      *shardedNodeMap
	candidateNodes *shardedNodeMap
	l5Nodes        *shardedNodeMap
	l3Nodes        *shardedNodeMap

	Edges      int64 // online edge node count
	Candidates int64 // online candidate node count
	L5Count    int64 // online l5 node count
	L3Count    int64 // online l3 node count
	weightMgr  *weightManager
	config     dtypes.GetSchedulerConfigFunc
	notify     *pubsub.PubSub
	etcdcli    *etcdcli.Client
	*db.SQLDB
	*rsa.PrivateKey // scheduler privateKey
	dtypes.ServerID // scheduler server id

	// TotalNetworkEdges int // Number of edge nodes in the entire network (including those on other schedulers)

	GeoMgr *GeoMgr
	IPMgr  *IPMgr

	mu                 sync.RWMutex
	serverOnlineCounts map[time.Time]int

	serverTodayOnlineTimeWindow int
	candidateOfflineTime        map[string]int // offline minute
}

// NewManager creates a new instance of the node manager
func NewManager(sdb *db.SQLDB, serverID dtypes.ServerID, pk *rsa.PrivateKey, pb *pubsub.PubSub, config dtypes.GetSchedulerConfigFunc, ec *etcdcli.Client) *Manager {
	ipLimit := 5
	cfg, err := config()
	if err == nil {
		ipLimit = cfg.IPLimit
	}

	nodeManager := &Manager{
		SQLDB:                sdb,
		ServerID:             serverID,
		PrivateKey:           pk,
		notify:               pb,
		config:               config,
		etcdcli:              ec,
		weightMgr:            newWeightManager(config),
		GeoMgr:               newGeoMgr(),
		IPMgr:                newIPMgr(ipLimit),
		candidateOfflineTime: map[string]int{},
		// Initialize ShardedMaps with 32 shards for better concurrency
		edgeNodes:      newShardedNodeMap(32),
		candidateNodes: newShardedNodeMap(32),
		l5Nodes:        newShardedNodeMap(32),
		l3Nodes:        newShardedNodeMap(32),
	}

	nodeManager.updateServerOnlineCounts()

	go nodeManager.startNodeKeepaliveTimer()
	go nodeManager.startNodePenaltyTimer()
	go nodeManager.startCheckNodeInfoTimer()
	go nodeManager.startUpdateNodeMetricsTimer()
	// go nodeManager.startCalcScoreTimer()
	// go nodeManager.qualityCheckTimer()
	go nodeManager.startBandwidthEventTimer()

	return nodeManager
}

func (m *Manager) startCheckNodeInfoTimer() {
	now := time.Now()

	nextTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	if now.After(nextTime) {
		nextTime = nextTime.Add(oneDay)
	}

	duration := nextTime.Sub(now)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		<-timer.C

		m.redistributeNodeSelectWeights()
		m.checkNodeDeactivate()
		m.CleanData()

		timer.Reset(oneDay)
	}
}

func (m *Manager) startUpdateNodeMetricsTimer() {
	ticker := time.NewTicker(loadCandidateInfoTime)
	defer ticker.Stop()

	for {
		<-ticker.C

		_, cList := m.GetValidCandidateNodes()

		// Use global worker pool instead of creating unlimited goroutines
		pool := GetGlobalWorkerPool()

		for _, n := range cList {
			node := n // Capture loop variable
			pool.Submit(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				info, err := node.API.GetNodeInfo(ctx)
				if err != nil {
					log.Errorf("GetNodeInfo %s err:%s", node.NodeID, err.Error())
					return
				}

				node.UpdateMetrics(&info)
			})
		}

		// Wait for all tasks to complete
		pool.Wait()
	}
}

// storeEdgeNode adds an edge node to the manager's list of edge nodes
func (m *Manager) storeEdgeNode(node *Node) {
	if node == nil {
		return
	}
	nodeID := node.NodeID
	_, loaded := m.edgeNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	atomic.AddInt64(&m.Edges, 1)

	m.DistributeNodeWeight(node)

	m.notify.Pub(node, types.EventNodeOnline.String())
}

// adds a candidate node to the manager's list of candidate nodes
func (m *Manager) storeCandidateNode(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	atomic.AddInt64(&m.Candidates, 1)

	m.DistributeNodeWeight(node)

	m.notify.Pub(node, types.EventNodeOnline.String())
}

func (m *Manager) storeL5Node(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeID
	_, loaded := m.l5Nodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	atomic.AddInt64(&m.L5Count, 1)
}

func (m *Manager) storeL3Node(node *Node) {
	if node == nil {
		return
	}

	nodeID := node.NodeID
	_, loaded := m.l3Nodes.LoadOrStore(nodeID, node)
	if loaded {
		return
	}
	atomic.AddInt64(&m.L3Count, 1)
}

// deleteEdgeNode removes an edge node from the manager's list of edge nodes
func (m *Manager) deleteEdgeNode(node *Node) {
	m.RepayNodeWeight(node)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeID
	_, loaded := m.edgeNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	atomic.AddInt64(&m.Edges, -1)
}

// deleteCandidateNode removes a candidate node from the manager's list of candidate nodes
func (m *Manager) deleteCandidateNode(node *Node) {
	m.RepayNodeWeight(node)
	m.notify.Pub(node, types.EventNodeOffline.String())

	nodeID := node.NodeID
	_, loaded := m.candidateNodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	atomic.AddInt64(&m.Candidates, -1)
}

// deleteL5Node removes a l5 node from the manager's list of l5 nodes
func (m *Manager) deleteL5Node(node *Node) {
	nodeID := node.NodeID
	_, loaded := m.l5Nodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	atomic.AddInt64(&m.L5Count, -1)
}

func (m *Manager) deleteL3Node(node *Node) {
	nodeID := node.NodeID
	_, loaded := m.l3Nodes.LoadAndDelete(nodeID)
	if !loaded {
		return
	}
	atomic.AddInt64(&m.L3Count, -1)
}

// DistributeNodeWeight Distribute Node Weight
func (m *Manager) DistributeNodeWeight(node *Node) {
	node.Level = m.getNodeScoreLevel(node)
	if !node.IsResourceNode() {
		return
	}

	wNum := m.weightMgr.getWeightNum(node.Level)
	if node.Type == types.NodeCandidate {
		node.selectWeights = m.weightMgr.distributeCandidateWeight(node.NodeID, wNum)
	} else if node.Type == types.NodeEdge {
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)
	}
}

// RepayNodeWeight Repay Node Weight
func (m *Manager) RepayNodeWeight(node *Node) {
	if node.Type == types.NodeCandidate {
		m.weightMgr.repayCandidateWeight(node.selectWeights)
		node.selectWeights = nil
	} else if node.Type == types.NodeEdge {
		m.weightMgr.repayEdgeWeight(node.selectWeights)
		node.selectWeights = nil
	}
}

func roundDivision(a, b int) int {
	result := float64(a) / float64(b)
	return int(math.Round(result))
}

func qualifiedNAT(natType string) bool {
	if natType == types.NatTypeNo.String() || natType == types.NatTypeFullCone.String() || natType == types.NatTypeUnknown.String() {
		return true
	}

	return false
}

func (m *Manager) updateServerOnlineCounts() {
	now := time.Now()

	var dates []time.Time
	for i := 1; i < 8; i++ {
		date := now.AddDate(0, 0, -i)
		date = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
		dates = append(dates, date)
	}

	counts, err := m.GetOnlineCountsByNode(string(m.ServerID), dates)
	if err != nil {
		log.Errorf("GetOnlineCountsByNode %s err: %s", string(m.ServerID), err.Error())
	}

	m.mu.Lock()
	m.serverOnlineCounts = counts
	m.mu.Unlock()

	// // today
	// today := time.Now()
	// todayDate := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())
	// todayCount, _ := m.GetOnlineCount(string(m.ServerID), todayDate)

	// m.serverTodayOnlineTimeWindow = todayCount
}

func (m *Manager) redistributeNodeSelectWeights() {
	// log.Debugln("start redistributeNodeSelectWeights ...")
	// repay all weights
	m.weightMgr.cleanWeights()

	m.updateServerOnlineCounts()

	// redistribute weights
	_, cList := m.GetValidCandidateNodes()
	if len(cList) > 0 {
		// Use slice pool to reduce allocations
		nodeIDsPtr := GetStringSlice()
		defer PutStringSlice(nodeIDsPtr)
		nodeIDs := *nodeIDsPtr

		for _, node := range cList {
			nodeIDs = append(nodeIDs, node.NodeID)
		}

		infMap, _ := m.LoadNodeInfos(nodeIDs)

		m.mu.RLock()
		allDates := make([]time.Time, 0, len(m.serverOnlineCounts))
		for date := range m.serverOnlineCounts {
			allDates = append(allDates, date)
		}
		m.mu.RUnlock()

		onlineCountsMatrix, _ := m.GetNodesOnlineCounts(nodeIDs, allDates)

		for _, node := range cList {
			info, ok := infMap[node.NodeID]
			if !ok {
				continue
			}

			// Calculate OnlineRate manually from the matrix to avoid N more DB queries
			var nodeC, serverC int
			m.mu.RLock()
			for date, serverCount := range m.serverOnlineCounts {
				if info.FirstTime.Before(date) {
					if nodeDateMap, ok := onlineCountsMatrix[node.NodeID]; ok {
						nodeC += nodeDateMap[date]
					}
					serverC += serverCount
				}
			}
			m.mu.RUnlock()

			if serverC > 0 {
				if nodeC >= serverC {
					node.OnlineRate = 1.0
				} else {
					node.OnlineRate = float64(nodeC) / float64(serverC)
				}
			} else {
				node.OnlineRate = 1.0
			}

			node.Level = m.getNodeScoreLevel(node)

			if !node.IsResourceNode() {
				continue
			}
			wNum := m.weightMgr.getWeightNum(node.Level)
			node.selectWeights = m.weightMgr.distributeCandidateWeight(node.NodeID, wNum)
		}
	}

	eList := m.GetValidEdgeNode()
	for _, node := range eList {
		// info, err := m.LoadNodeInfo(node.NodeID)
		// if err != nil {
		// 	continue
		// }

		// node.OnlineRate, _ = m.ComputeNodeOnlineRate(node.NodeID, info.FirstTime)
		// node.TodayOnlineTimeWindow = 0
		node.Level = m.getNodeScoreLevel(node)

		if !node.IsResourceNode() {
			continue
		}
		wNum := m.weightMgr.getWeightNum(node.Level)
		node.selectWeights = m.weightMgr.distributeEdgeWeight(node.NodeID, wNum)
	}

	// log.Debugln("end redistributeNodeSelectWeights ...")
}

// ComputeNodeOnlineRate Compute node online rate
func (m *Manager) ComputeNodeOnlineRate(nodeID string, firstTime time.Time) float64 {
	nodeC := 0
	serverC := 0

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect dates that are after firstTime
	var dates []time.Time
	for date := range m.serverOnlineCounts {
		if firstTime.Before(date) {
			dates = append(dates, date)
		}
	}

	// Batch fetch online counts for all relevant dates
	nodeCountMap, _ := m.GetOnlineCountsByNode(nodeID, dates)

	for _, date := range dates {
		serverCount := m.serverOnlineCounts[date]
		nodeC += nodeCountMap[date]
		serverC += serverCount
	}

	// today := time.Now()
	// todayDate := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())
	// todayCount, err := m.GetOnlineCount(nodeID, todayDate)

	log.Infof("%s serverOnlineCounts [%v] %d/%d [%d][%v]", nodeID, m.serverOnlineCounts, nodeC, serverC)

	if serverC == 0 {
		return 1.0
	}

	if nodeC >= serverC {
		return 1.0
	}

	return float64(nodeC) / float64(serverC)
}

// UpdateNodeBandwidths update node bandwidthDown and bandwidthUp
func (m *Manager) UpdateNodeBandwidths(nodeID string, bandwidthDown, bandwidthUp int64) {
	node := m.GetNode(nodeID)
	if node == nil {
		return
	}

	node.UpdateBandwidths(bandwidthDown, bandwidthUp)
}

func (m *Manager) checkNodeDeactivate() {
	nodes, err := m.LoadDeactivateNodes(time.Now().Unix())
	if err != nil {
		log.Errorf("LoadDeactivateNodes err:%s", err.Error())
		return
	}

	for _, nodeID := range nodes {
		err = m.DeleteAssetRecordsOfNode(nodeID)
		if err != nil {
			log.Errorf("DeleteAssetOfNode err:%s", err.Error())
		}
	}
}

// UpdateNodeDiskUsage update node disk usage
func (m *Manager) UpdateNodeDiskUsage(nodeID string, diskUsage float64) {
	node := m.GetNode(nodeID)
	if node == nil {
		return
	}

	size, err := m.LoadReplicaSizeByNodeID(nodeID)
	if err != nil {
		log.Errorf("LoadReplicaSizeByNodeID %s err:%s", nodeID, err.Error())
		return
	}

	titanDiskUsage := float64(size)
	if node.ClientType == types.NodeAndroid || node.ClientType == types.NodeIOS {
		if size > 5*units.GiB {
			titanDiskUsage = 5 * units.GiB
		}
	}

	node.UpdateDiskUsage(diskUsage, titanDiskUsage)
	log.Infof("LoadReplicaSizeByNodeID %s update:%v", nodeID, titanDiskUsage)
}
