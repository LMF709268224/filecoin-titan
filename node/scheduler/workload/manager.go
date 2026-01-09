package workload

import (
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/cidutil"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("workload")

// Manager node workload
type Manager struct {
	config        dtypes.GetSchedulerConfigFunc
	leadershipMgr *leadership.Manager
	nodeMgr       *node.Manager
	*db.SQLDB

	resultQueue chan *Result
}

// NewManager return new node manager instance
func NewManager(sdb *db.SQLDB, configFunc dtypes.GetSchedulerConfigFunc, lmgr *leadership.Manager, nmgr *node.Manager) *Manager {
	manager := &Manager{
		config:        configFunc,
		leadershipMgr: lmgr,
		SQLDB:         sdb,
		nodeMgr:       nmgr,
		resultQueue:   make(chan *Result, 1000), // buffered channel
	}

	go manager.handleResults()

	return manager
}

// Result represents the result of a workload operation.
type Result struct {
	data   *types.WorkloadRecordReq
	nodeID string
}

func (m *Manager) addWorkloadResult(r *Result) {
	select {
	case m.resultQueue <- r:
	default:
		log.Warn("workload result queue is full, dropping result")
	}
}

// PushResult sends the result of a workload to the specified node.
func (m *Manager) PushResult(data *types.WorkloadRecordReq, nodeID string) error {
	// log.Infof("workload PushResult nodeID:[%s] , %s [%s]\n", nodeID, data.WorkloadID, data.AssetCID)

	m.addWorkloadResult(&Result{data: data, nodeID: nodeID})

	return nil
}

func (m *Manager) handleResults() {
	for {
		select {
		case result := <-m.resultQueue:
			if result != nil {
				m.handleClientWorkload(result.data, result.nodeID)
			}
		}
	}
}

func (m *Manager) collectRetrieveEventFromWorkload(dw types.Workload, hash, downloadNode string, events []*types.RetrieveEvent, statsMap map[string]*types.NodeStatisticsInfo) ([]*types.RetrieveEvent, map[string]*types.NodeStatisticsInfo) {
	status := types.EventStatusFailed
	speed := int64(0)
	succeededCount := 0
	failedCount := 0

	if dw.Status == types.WorkloadReqStatusSucceeded {
		sd := int64((float64(dw.DownloadSize) / float64(dw.CostTime)) * 1000)
		if sd > 0 {
			m.nodeMgr.UpdateNodeBandwidths(downloadNode, speed, 0)
			m.nodeMgr.UpdateNodeBandwidths(dw.SourceID, 0, speed)
			speed = sd
		}
		status = types.EventStatusSucceed
		succeededCount = 1
	} else {
		failedCount = 1
	}

	event := &types.RetrieveEvent{
		Hash:     hash,
		NodeID:   dw.SourceID,
		ClientID: downloadNode,
		Size:     dw.DownloadSize,
		Status:   status,
		Speed:    speed,
	}
	events = append(events, event)

	stats, ok := statsMap[dw.SourceID]
	if !ok {
		stats = &types.NodeStatisticsInfo{}
		statsMap[dw.SourceID] = stats
	}
	stats.RetrieveSucceededCount += int64(succeededCount)
	stats.RetrieveFailedCount += int64(failedCount)

	return events, statsMap
}

// handleClientWorkload handle node workload
func (m *Manager) handleClientWorkload(data *types.WorkloadRecordReq, downloadNode string) error {
	events := make([]*types.RetrieveEvent, 0)
	statsMap := make(map[string]*types.NodeStatisticsInfo)

	defer func() {
		if len(events) > 0 {
			if err := m.SaveRetrieveEventInfos(events, statsMap); err != nil {
				log.Errorf("handleClientWorkload SaveRetrieveEventInfos error %s", err.Error())
			}
		}
	}()

	if data.WorkloadID == "" {
		hash, err := cidutil.CIDToHash(data.AssetCID)
		if err != nil {
			return err
		}

		// L1 download
		for _, dw := range data.Workloads {
			events, statsMap = m.collectRetrieveEventFromWorkload(dw, hash, downloadNode, events, statsMap)
		}

		return nil
	}

	record, err := m.LoadWorkloadRecordOfID(data.WorkloadID)
	if err != nil {
		log.Errorf("handleClientWorkload LoadWorkloadRecordOfID %s error: %s", data.WorkloadID, err.Error())
		return err
	}

	if record.Status != types.WorkloadStatusCreate {
		return nil
	}

	// update status
	record.Status = types.WorkloadStatusSucceeded
	err = m.UpdateWorkloadRecord(record, types.WorkloadStatusCreate)
	if err != nil {
		log.Errorf("handleClientWorkload UpdateWorkloadRecord %s error: %s", data.WorkloadID, err.Error())
		return err
	}

	hash, err := cidutil.CIDToHash(record.AssetCID)
	if err != nil {
		return err
	}

	detailsList := make([]*types.ProfitDetails, 0)

	limit := int64(float64(record.AssetSize))

	for _, dw := range data.Workloads {
		events, statsMap = m.collectRetrieveEventFromWorkload(dw, hash, record.ClientID, events, statsMap)

		if dw.Status == types.WorkloadReqStatusSucceeded {
			// Only edge can get this reward
			node := m.nodeMgr.GetNode(dw.SourceID)
			if node == nil {
				continue
			}
			node.UploadTraffic += dw.DownloadSize

			if downloadNode != "" && node.Type == types.NodeEdge {
				if dw.DownloadSize > limit {
					dw.DownloadSize = limit
				}

				dInfo := m.nodeMgr.GetNodeBePullProfitDetails(node, float64(dw.DownloadSize), "")
				if dInfo != nil {
					dInfo.CID = record.AssetCID
					dInfo.Note = fmt.Sprintf("%s,%s", dInfo.Note, record.WorkloadID)

					detailsList = append(detailsList, dInfo)
				}
			}
		}

	}

	err = m.nodeMgr.AddNodeProfitDetails(detailsList)
	if err != nil {
		log.Errorf("AddNodeProfit err:%s", err.Error())
	}

	return nil
}
