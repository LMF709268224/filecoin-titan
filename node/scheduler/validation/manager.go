package validation

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lotuscli"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/leadership"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/filecoin-project/pubsub"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("validation")

const (
	filecoinEpochDuration  = 30
	gameChainEpochLookback = 10

	validationWorkers = 50
	oneDay            = 24 * time.Hour
)

// Manager validation manager
type Manager struct {
	nodeMgr *node.Manager
	notify  *pubsub.PubSub

	seed       int64
	curRoundID string
	close      chan struct{}
	config     dtypes.GetSchedulerConfigFunc

	updateCh chan struct{}

	// validation result worker
	resultQueue chan *api.ValidationResult

	leadershipMgr *leadership.Manager

	// Use sync.Map for lock-free reads and easy maintenance
	validators sync.Map

	lotusRPCAddress  string
	enableValidation bool

	cache atomic.Value // Stores *chainCache

	profitBatch     []*types.ProfitDetails
	resultInfoBatch []*types.ValidationResultInfo
	batchMu         sync.Mutex
}

type chainCache struct {
	epoch     uint64
	timestamp time.Time
}

func (m *Manager) addValidator(nodeID string) {
	m.validators.Store(nodeID, struct{}{})
}

// IsValidator checks if the given nodeID is a validator.
func (m *Manager) IsValidator(nodeID string) bool {
	_, ok := m.validators.Load(nodeID)
	return ok
}

// GetValidators returns a list of validators.
func (m *Manager) GetValidators() []string {
	var list []string
	m.validators.Range(func(key, value interface{}) bool {
		list = append(list, key.(string))
		return true
	})
	return list
}

func (m *Manager) cleanValidator() {
	// Replacing the Map with a new one is the cleanest way to clear it
	m.validators = sync.Map{}
}

// NewManager return new node manager instance
func NewManager(nodeMgr *node.Manager, configFunc dtypes.GetSchedulerConfigFunc, p *pubsub.PubSub, lmgr *leadership.Manager) *Manager {
	manager := &Manager{
		nodeMgr:         nodeMgr,
		config:          configFunc,
		close:           make(chan struct{}),
		updateCh:        make(chan struct{}, 1),
		notify:          p,
		resultQueue:     make(chan *api.ValidationResult, 1000), // buffered channel
		leadershipMgr:   lmgr,
		profitBatch:     make([]*types.ProfitDetails, 0),
		resultInfoBatch: make([]*types.ValidationResultInfo, 0),
	}

	manager.initCfg()

	return manager
}

func (m *Manager) initCfg() {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("get schedulerConfig err:%s", err.Error())
		return
	}

	m.lotusRPCAddress = cfg.LotusRPCAddress
	m.enableValidation = cfg.EnableValidation
}

// Start start validate and elect task
func (m *Manager) Start(ctx context.Context) {
	go m.startValidationTicker()
	go m.startBatchFlushTicker()

	m.handleResults()
}

// Stop stop
func (m *Manager) Stop(ctx context.Context) error {
	return m.stopValidation()
}

func (m *Manager) getGameEpoch() (uint64, error) {
	c, _ := m.cache.Load().(*chainCache)
	if c != nil && time.Since(c.timestamp) < 15*time.Second && c.epoch > 0 {
		return c.epoch, nil
	}

	// Directly call RPC without holding any lock
	h, err := lotuscli.ChainHead(m.lotusRPCAddress)
	if err != nil {
		log.Errorf("getGameEpoch ChainHead err:%s", err.Error())
		if c != nil {
			return c.epoch, nil // Return stale data if RPC fails to prevent system freeze
		}
		return 0, err
	}

	m.cache.Store(&chainCache{
		epoch:     h,
		timestamp: time.Now(),
	})

	return h, nil
}

func (m *Manager) getSeedFromFilecoin() (int64, error) {
	seed := time.Now().UnixNano()

	height, err := m.getGameEpoch()
	if err != nil {
		return seed, xerrors.Errorf("getGameEpoch failed: %w", err)
	}

	if height <= gameChainEpochLookback {
		return seed, xerrors.Errorf("getGameEpoch return invalid height: %d", height)
	}

	lookback := height - gameChainEpochLookback
	tps, err := m.getTipsetByHeight(lookback)
	if err != nil {
		return seed, xerrors.Errorf("getTipsetByHeight failed: %w", err)
	}

	rs := tps.MinTicket().VRFProof
	if len(rs) >= 3 {
		s := binary.BigEndian.Uint32(rs)
		// log.Debugf("lotus Randomness:%d \n", s)
		return int64(s), nil
	}

	return seed, xerrors.Errorf("VRFProof size %d < 3", len(rs))
}

func (m *Manager) getTipsetByHeight(height uint64) (*lotuscli.TipSet, error) {
	iheight := int64(height)
	for i := 0; i < gameChainEpochLookback && iheight > 0; i++ {
		tps, err := lotuscli.ChainGetTipSetByHeight(m.lotusRPCAddress, iheight)
		if err != nil {
			return nil, err
		}

		if len(tps.Blocks()) > 0 {
			return tps, nil
		}

		iheight--
	}

	return nil, xerrors.Errorf("getTipsetByHeight can't found a non-empty tipset from height: %d", height)
}

func (m *Manager) addProfitToBatch(profit *types.ProfitDetails) {
	m.batchMu.Lock()
	defer m.batchMu.Unlock()
	m.profitBatch = append(m.profitBatch, profit)
}

func (m *Manager) addResultInfoToBatch(resultInfo *types.ValidationResultInfo) {
	m.batchMu.Lock()
	defer m.batchMu.Unlock()
	m.resultInfoBatch = append(m.resultInfoBatch, resultInfo)
}

func (m *Manager) startBatchFlushTicker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.flushBatch()
		case <-m.close:
			m.flushBatch()
			return
		}
	}
}

func (m *Manager) flushBatch() {
	m.batchMu.Lock()
	profits := m.profitBatch
	results := m.resultInfoBatch
	m.profitBatch = make([]*types.ProfitDetails, 0)
	m.resultInfoBatch = make([]*types.ValidationResultInfo, 0)
	m.batchMu.Unlock()

	if len(profits) > 0 {
		if err := m.nodeMgr.AddNodeProfitDetails(profits); err != nil {
			log.Errorf("flushBatch AddNodeProfitDetails err: %v", err)
		}
	}

	if len(results) > 0 {
		for _, result := range results {
			if err := m.nodeMgr.UpdateValidationResultInfo(result); err != nil {
				log.Errorf("flushBatch UpdateValidationResultInfo err: %v", err)
			}
		}
	}
}
