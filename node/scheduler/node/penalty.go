package node

import (
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

func (m *Manager) startNodePenaltyTimer() {
	time.Sleep(penaltyFreeTime)

	ticker := time.NewTicker(penaltyInterval)
	defer ticker.Stop()

	lastResetDay := time.Now().Day()

	for {
		<-ticker.C

		now := time.Now()
		if now.Day() != lastResetDay {
			m.mu.Lock()
			m.candidateOfflineTime = make(map[string]int)
			m.mu.Unlock()
			lastResetDay = now.Day()
		}

		m.penaltyNode()
	}
}

func (m *Manager) penaltyNode() {
	list, err := m.LoadCandidateInfos()
	if err != nil {
		log.Errorf("LoadCandidateInfos err:%s", err.Error())
		return
	}

	offlineNodes := make(map[string]float64)
	detailsList := make([]*types.ProfitDetails, 0)

	// Prepare updates outside the lock to minimize contention
	type updateInfo struct {
		nodeID          string
		count           int
		profit          float64
		offlineDuration int
		freeDeduction   int
		onlineDuration  int
	}
	toUpdate := make([]updateInfo, 0)

	for _, info := range list {
		// If node is online, skip
		if _, online := m.onlineNodes.Load(info.NodeID); online {
			continue
		}

		if info.DeactivateTime > 0 {
			continue
		}

		if info.Profit <= 0.0001 {
			continue
		}

		m.mu.RLock()
		count := m.candidateOfflineTime[info.NodeID]
		m.mu.RUnlock()

		toUpdate = append(toUpdate, updateInfo{
			nodeID:          info.NodeID,
			count:           count,
			profit:          info.Profit,
			offlineDuration: info.OfflineDuration,
			freeDeduction:   info.FreeDeductionTime,
			onlineDuration:  info.OnlineDuration,
		})
	}

	// Now apply updates
	m.mu.Lock()
	for _, item := range toUpdate {
		if item.count > 30 {
			dInfo := m.CalculatePenalty(item.nodeID, item.profit, (max(item.offlineDuration+1-item.freeDeduction, 0)), item.onlineDuration)
			if dInfo != nil {
				detailsList = append(detailsList, dInfo)
			}
		}
		offlineNodes[item.nodeID] = 0
		m.candidateOfflineTime[item.nodeID]++
	}
	m.mu.Unlock()

	if len(offlineNodes) > 0 {
		err := m.UpdateNodePenalty(offlineNodes)
		if err != nil {
			log.Errorf("UpdateNodePenalty err:%s", err.Error())
		}
	}

	if len(detailsList) > 0 {
		err = m.AddNodeProfitDetails(detailsList)
		if err != nil {
			log.Errorf("AddNodeProfit err:%s", err.Error())
		}
	}
}
