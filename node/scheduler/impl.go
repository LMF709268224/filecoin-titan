package scheduler

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/scheduler/nat"
	"github.com/Filecoin-Titan/titan/node/scheduler/projects"
	"github.com/Filecoin-Titan/titan/node/scheduler/validation"
	"github.com/Filecoin-Titan/titan/node/scheduler/workload"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/quic-go/quic-go"

	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"

	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	sSync "github.com/Filecoin-Titan/titan/node/scheduler/sync"
	"golang.org/x/xerrors"
)

var log = logging.Logger("scheduler")

const (
	cpuLimit           = 140
	memoryLimit        = 2250 * units.GiB
	diskSpaceLimit     = 500 * units.TiB
	bandwidthLimit     = 100 * units.GiB
	availableDiskLimit = 2 * units.TiB

	l1CpuLimit       = 8
	l1MemoryLimit    = 16 * units.GB
	l1DiskSpaceLimit = 4 * units.TB
)

// Scheduler represents a scheduler node in a distributed system.
type Scheduler struct {
	fx.In

	region.Region
	*common.CommonAPI
	*EdgeUpdateManager
	dtypes.ServerID

	NodeManager            *node.Manager
	ValidationMgr          *validation.Manager
	AssetManager           *assets.Manager
	NatManager             *nat.Manager
	DataSync               *sSync.DataSync
	SchedulerCfg           *config.SchedulerCfg
	SetSchedulerConfigFunc dtypes.SetSchedulerConfigFunc
	GetSchedulerConfigFunc dtypes.GetSchedulerConfigFunc
	WorkloadManager        *workload.Manager
	ProjectManager         *projects.Manager

	PrivateKey *rsa.PrivateKey
	Transport  *quic.Transport
}

var _ api.Scheduler = &Scheduler{}

func (s *Scheduler) getAreaInfo() (string, bool) {
	aID := s.SchedulerCfg.AreaID

	parts := strings.Split(aID, "-")
	continent := strings.ToLower(strings.Replace(parts[0], " ", "", -1))
	country := strings.ToLower(strings.Replace(parts[1], " ", "", -1))

	areaID := fmt.Sprintf("%s-%s", continent, country)

	communityArea := aID == "Asia-HongKong"

	return areaID, communityArea
}

type NodeConnectContext struct {
	nodeID     string
	remoteAddr string
	nodeType   types.NodeType
	opts       *types.ConnectOptions
	cNode      *node.Node
	externalIP string
	nodeInfo   *types.NodeInfo

	alreadyConnect bool
}

func (s *Scheduler) nodeConnect(ctx context.Context, opts *types.ConnectOptions, nodeType types.NodeType) error {
	context := &NodeConnectContext{
		nodeID:     handler.GetNodeID(ctx),
		remoteAddr: handler.GetRemoteAddr(ctx),
		nodeType:   nodeType,
		opts:       opts,
	}

	err := s.validateNodeExistence(context)
	if err != nil {
		return xerrors.Errorf("nodeConnect err node: %s, type: %d, error: %s", context.nodeID, nodeType, err.Error())
	}

	err = s.storeNodeIP(context)
	if err != nil {
		return err
	}

	s.initNode(context)

	defer s.deferCleanup(context, err)

	err = s.processNodeConnection(context)
	return err
}

func (s *Scheduler) validateNodeExistence(ctx *NodeConnectContext) error {
	if cNode := s.NodeManager.GetNode(ctx.nodeID); cNode != nil {
		ctx.cNode = cNode
		ctx.alreadyConnect = true
		if cNode.ExternalIP != "" {
			s.NodeManager.IPMgr.RemoveNodeIP(ctx.nodeID, cNode.ExternalIP)
		}
		return nil
	}
	return s.NodeManager.NodeExistsFromType(ctx.nodeID, ctx.nodeType)
}

func (s *Scheduler) storeNodeIP(ctx *NodeConnectContext) error {
	ip, _, err := net.SplitHostPort(ctx.remoteAddr)
	if err != nil {
		return xerrors.Errorf("SplitHostPort error: %w", err)
	}
	ctx.externalIP = ip

	if !s.NodeManager.IPMgr.StoreNodeIP(ctx.nodeID, ip) {
		return xerrors.Errorf("nodeConnect err %s The number of IPs exceeds the limit %s ", ctx.nodeID, ctx.externalIP)
	}
	return nil
}

func (s *Scheduler) initNode(ctx *NodeConnectContext) {
	if ctx.cNode == nil {
		ctx.cNode = node.New()
	}

	ctx.cNode.Token = ctx.opts.Token
	ctx.cNode.ExternalURL = ctx.opts.ExternalURL
	ctx.cNode.TCPPort = ctx.opts.TcpServerPort
	ctx.cNode.IsPrivateMinioOnly = ctx.opts.IsPrivateMinioOnly
	ctx.cNode.ResourcesStatistics = ctx.opts.ResourcesStatistics
}

func (s *Scheduler) processNodeConnection(ctx *NodeConnectContext) error {
	if err := s.connectRPC(ctx); err != nil {
		return err
	}

	// get node info from client
	nInfo, err := ctx.cNode.API.GetNodeInfo(context.Background())
	if err != nil {
		return xerrors.Errorf("%s nodeConnect err NodeInfo err:%s", ctx.nodeID, err.Error())
	}

	if ctx.nodeID != nInfo.NodeID {
		return xerrors.Errorf("nodeConnect err nodeID mismatch %s, %s", ctx.nodeID, nInfo.NodeID)
	}

	ctx.nodeInfo, err = s.checkNodeParameters(nInfo, ctx.nodeType)
	if err != nil {
		return xerrors.Errorf("nodeConnect err Node %s does not meet the standard %s", ctx.nodeID, err.Error())
	}

	ver := api.NewVerFromString(ctx.nodeInfo.SystemVersion)
	ctx.nodeInfo.Version = int64(ver)

	ctx.nodeInfo.NodeID = ctx.nodeID
	ctx.nodeInfo.RemoteAddr = ctx.remoteAddr
	ctx.nodeInfo.SchedulerID = s.ServerID
	ctx.nodeInfo.ExternalIP = ctx.externalIP
	ctx.nodeInfo.BandwidthUp = units.KiB
	ctx.nodeInfo.NATType = types.NatTypeUnknown.String()

	if ctx.opts.GeoInfo != nil {
		ctx.nodeInfo.AreaID = ctx.opts.GeoInfo.Geo
	} else {
		geoInfo, err := s.GetGeoInfo(ctx.externalIP)
		if err != nil {
			log.Warnf("%s getAreaID error %s", ctx.nodeID, err.Error())
		}
		ctx.nodeInfo.AreaID = geoInfo.Geo
	}

	dbInfo, err := s.loadNodeData(ctx)
	if err != nil {
		return err
	}

	return s.handleNodeTypeLogic(ctx, dbInfo)
}

func (s *Scheduler) handleNodeTypeLogic(ctx *NodeConnectContext, dbInfo *types.NodeInfo) error {
	ctx.cNode.InitInfo(ctx.nodeInfo)

	if !ctx.alreadyConnect || dbInfo == nil || dbInfo.SystemVersion != ctx.nodeInfo.SystemVersion {
		err := s.saveNodeInfo(ctx.nodeInfo)
		if err != nil {
			return err
		}
	}

	if !ctx.alreadyConnect {
		if ctx.nodeType == types.NodeEdge {
			incr, _ := s.NodeManager.GetEdgeBaseProfitDetails(ctx.cNode, 0)
			ctx.cNode.IncomeIncr = incr
		}

		if ctx.cNode.IsResourceNode() {
			s.NodeManager.GeoMgr.AddNodeGeo(ctx.nodeInfo, ctx.cNode.AreaID)
		}

		pStr, err := s.NodeManager.LoadNodePublicKey(ctx.nodeID)
		if err != nil && err != sql.ErrNoRows {
			return xerrors.Errorf("nodeConnect err load node port %s err : %s", ctx.nodeID, err.Error())
		}

		publicKey, err := titanrsa.Pem2PublicKey([]byte(pStr))
		if err != nil {
			return xerrors.Errorf("nodeConnect err load node port %s err : %s", ctx.nodeID, err.Error())
		}
		ctx.cNode.PublicKey = publicKey
		// init LastValidateTime
		ctx.cNode.LastValidateTime = s.getNodeLastValidateTime(ctx.nodeID)

		ctx.cNode.OnlineRate = s.NodeManager.ComputeNodeOnlineRate(ctx.nodeID, ctx.nodeInfo.FirstTime)

		err = s.NodeManager.NodeOnline(ctx.cNode, ctx.nodeInfo)
		if err != nil {
			log.Errorf("nodeConnect err:%s,nodeID:%s", err.Error(), ctx.nodeID)
			return err
		}

		s.ProjectManager.CheckProjectReplicasFromNode(ctx.nodeID)

		s.DataSync.AddNodeToList(ctx.nodeID)

		if ctx.nodeType == types.NodeEdge {
			s.NatManager.DetermineEdgeNATType(context.Background(), ctx.nodeID)
		} else if ctx.nodeType == types.NodeCandidate {
			// err := checkDomain(cNode.ExternalURL)
			// log.Infof("%s checkDomain [%s] %v", nodeID, cNode.ExternalURL, err)
			// cNode.IsStorageNode = err == nil

			s.NatManager.DetermineCandidateNATType(context.Background(), ctx.nodeID)
		}
	}

	return nil
}

func (s *Scheduler) connectRPC(ctx *NodeConnectContext) error {
	if err := ctx.cNode.ConnectRPC(s.Transport, ctx.remoteAddr, ctx.nodeType); err != nil {
		return xerrors.Errorf("%s nodeConnect err ConnectRPC err:%s", ctx.nodeID, err.Error())
	}
	return nil
}

func (s *Scheduler) loadNodeData(ctx *NodeConnectContext) (*types.NodeInfo, error) {
	dbInfo, err := s.NodeManager.LoadNodeInfo(ctx.nodeID)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("nodeConnect err load node online duration %s err : %s", ctx.nodeID, err.Error())
	}

	ctx.nodeInfo.FirstTime = time.Now()
	ctx.nodeInfo.LastSeen = time.Now()
	if dbInfo != nil {
		// init node info
		// ctx.nodeInfo.PortMapping = dbInfo.PortMapping
		ctx.nodeInfo.OnlineDuration = dbInfo.OnlineDuration
		ctx.nodeInfo.OfflineDuration = dbInfo.OfflineDuration
		ctx.nodeInfo.BandwidthDown = dbInfo.BandwidthDown
		ctx.nodeInfo.BandwidthUp = dbInfo.BandwidthUp
		ctx.nodeInfo.DeactivateTime = dbInfo.DeactivateTime
		ctx.nodeInfo.DownloadTraffic = dbInfo.DownloadTraffic
		ctx.nodeInfo.UploadTraffic = dbInfo.UploadTraffic
		ctx.nodeInfo.WSServerID = dbInfo.WSServerID
		ctx.nodeInfo.Profit = dbInfo.Profit
		ctx.nodeInfo.FirstTime = dbInfo.FirstTime
		ctx.nodeInfo.NATType = dbInfo.NATType

		if dbInfo.DeactivateTime > 0 && dbInfo.DeactivateTime < time.Now().Unix() {
			return nil, xerrors.Errorf("nodeConnect err The node %s has been deactivate and cannot be logged in", ctx.nodeID)
		}

		if dbInfo.ForceOffline {
			return nil, xerrors.Errorf("nodeConnect err The node %s has been forced offline", ctx.nodeID)
		}
	}

	return dbInfo, nil
}

func (s *Scheduler) deferCleanup(ctx *NodeConnectContext, err error) {
	defer func() {
		if err != nil {
			s.NodeManager.IPMgr.RemoveNodeIP(ctx.nodeID, ctx.externalIP)
			s.NodeManager.GeoMgr.RemoveNodeGeo(ctx.nodeID, ctx.nodeType, ctx.cNode.AreaID)
		}
	}()
}

// SaveInfo Save node information when it comes online
func (s *Scheduler) saveNodeInfo(n *types.NodeInfo) error {
	return s.db.SaveNodeInfo(n)
}

func (s *Scheduler) getNodeLastValidateTime(nodeID string) int64 {
	rsp, err := s.NodeManager.LoadValidationResultInfos(nodeID, 1, 0)
	if err != nil || len(rsp.ValidationResultInfos) == 0 {
		return 0
	}

	info := rsp.ValidationResultInfos[0]
	return info.StartTime.Unix()
}

func checkNodeClientType(systemVersion, androidSymbol, iosSymbol, windowsSymbol, macosSymbol string) types.NodeClientType {
	if strings.Contains(systemVersion, androidSymbol) {
		return types.NodeAndroid
	}

	if strings.Contains(systemVersion, iosSymbol) {
		return types.NodeIOS
	}

	if strings.Contains(systemVersion, windowsSymbol) {
		return types.NodeWindows
	}

	if strings.Contains(systemVersion, macosSymbol) {
		return types.NodeMacos
	}

	return types.NodeOther
}

func roundUpToNextGB(bytes int64) int64 {
	const GB = 1 << 30
	if bytes%GB == 0 {
		return bytes
	}
	return ((bytes / GB) + 1) * GB
}

func (s *Scheduler) nodeParametersApplyLimits(nodeInfo types.NodeInfo) *types.NodeInfo {
	nodeInfo.Memory = max(0, min(nodeInfo.Memory, memoryLimit))
	nodeInfo.CPUCores = max(0, min(nodeInfo.CPUCores, cpuLimit))
	nodeInfo.DiskSpace = max(0, min(nodeInfo.DiskSpace, diskSpaceLimit))
	nodeInfo.BandwidthDown = max(0, min(nodeInfo.BandwidthDown, bandwidthLimit))
	nodeInfo.BandwidthUp = max(0, min(nodeInfo.BandwidthUp, bandwidthLimit))

	return &nodeInfo
}

func (s *Scheduler) checkNodeParameters(nodeInfo types.NodeInfo, nodeType types.NodeType) (*types.NodeInfo, error) {
	nInfo := s.nodeParametersApplyLimits(nodeInfo)

	nodeInfo.AvailableDiskSpace = max(2*units.GiB, min(nodeInfo.AvailableDiskSpace, nInfo.DiskSpace))

	nInfo.Type = nodeType

	useSize, err := s.db.LoadReplicaSizeByNodeID(nInfo.NodeID)
	if err != nil {
		return nil, xerrors.Errorf("LoadReplicaSizeByNodeID %s err:%s", nInfo.NodeID, err.Error())
	}

	if nodeType == types.NodeEdge {
		nodeInfo.AvailableDiskSpace = min(nodeInfo.AvailableDiskSpace, availableDiskLimit)

		nInfo.ClientType = checkNodeClientType(nInfo.SystemVersion, s.SchedulerCfg.AndroidSymbol, s.SchedulerCfg.IOSSymbol, s.SchedulerCfg.WindowsSymbol, s.SchedulerCfg.MacosSymbol)
		// limit node availableDiskSpace to 5 GiB when using phone
		if nInfo.ClientType == types.NodeAndroid || nInfo.ClientType == types.NodeIOS {
			nInfo.AvailableDiskSpace = min(nInfo.AvailableDiskSpace, 5*units.GiB)
			useSize = min(useSize, 5*units.GiB)
		}

	} else if nodeType == types.NodeCandidate {
		info, err := s.db.GetCandidateCodeInfoForNodeID(nInfo.NodeID)
		if err != nil {
			return nil, xerrors.Errorf("nodeID GetCandidateCodeInfoForNodeID %s, %s", nInfo.NodeID, err.Error())
		}
		nInfo.IsTestNode = info.IsTest

		if !nInfo.IsTestNode {
			if nInfo.Memory < l1MemoryLimit {
				return nil, xerrors.Errorf("Memory [%s]<[%s]", units.BytesSize(nInfo.Memory), units.BytesSize(l1MemoryLimit))
			}

			if nInfo.CPUCores < l1CpuLimit {
				return nil, xerrors.Errorf("CPUCores [%d]<[%d]", nInfo.CPUCores, l1CpuLimit)
			}

			if nInfo.DiskSpace < l1DiskSpaceLimit {
				return nil, xerrors.Errorf("DiskSpace [%s]<[%s]", units.BytesSize(nInfo.DiskSpace), units.BytesSize(l1DiskSpaceLimit))
			}
		}

		nInfo.AvailableDiskSpace = nInfo.DiskSpace * 0.9
	}

	nInfo.TitanDiskUsage = float64(useSize)

	return nInfo, nil
}

// NodeValidationResult processes the validation result for a node
func (s *Scheduler) NodeValidationResult(ctx context.Context, r io.Reader, sign string) error {
	validator := handler.GetNodeID(ctx)
	node := s.NodeManager.GetNode(validator)
	if node == nil {
		return xerrors.Errorf("node %s not online", validator)
	}

	signBuf, err := hex.DecodeString(sign)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(node.PublicKey, signBuf, data)
	if err != nil {
		return err
	}

	result := &api.ValidationResult{}
	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(result)
	if err != nil {
		return err
	}

	result.Validator = validator
	s.ValidationMgr.PushResult(result)

	return nil
}

// GetValidationResults retrieves a list of validation results.
func (s *Scheduler) GetValidationResults(ctx context.Context, nodeID string, limit, offset int) (*types.ListValidationResultRsp, error) {
	svm, err := s.NodeManager.LoadValidationResultInfos(nodeID, limit, offset)
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// GetSchedulerPublicKey get server publicKey
func (s *Scheduler) GetSchedulerPublicKey(ctx context.Context) (string, error) {
	if s.PrivateKey == nil {
		return "", xerrors.Errorf("scheduler private key not exist")
	}

	publicKey := s.PrivateKey.PublicKey
	pem := titanrsa.PublicKey2Pem(&publicKey)
	return string(pem), nil
}

// GetNodePublicKey get node publicKey
func (s *Scheduler) GetNodePublicKey(ctx context.Context, nodeID string) (string, error) {
	pem, err := s.NodeManager.LoadNodePublicKey(nodeID)
	if err != nil {
		return "", xerrors.Errorf("%s load node public key failed: %w", nodeID, err)
	}

	return string(pem), nil
}

// SubmitProjectReport submits a project report for the given request.
func (s *Scheduler) SubmitProjectReport(ctx context.Context, req *types.ProjectRecordReq) error {
	candidateID := handler.GetNodeID(ctx)
	if len(candidateID) == 0 {
		return xerrors.New("SubmitProjectReport invalid request")
	}

	if req.NodeID == "" {
		return xerrors.New("SubmitProjectReport node id is nil")
	}

	if req.ProjectID == "" {
		return xerrors.New("SubmitProjectReport project id is nil")
	}

	rInfo, err := s.db.LoadProjectReplicaInfo(req.ProjectID, req.NodeID)
	if err != nil {
		return xerrors.Errorf("SubmitProjectReport LoadProjectReplicaInfo err:%s", err.Error())
	}

	if rInfo.Status != types.ProjectReplicaStatusStarted {
		return xerrors.Errorf("SubmitProjectReport project status is %s", rInfo.Status.String())
	}

	wID, err := s.db.LoadWSServerID(req.NodeID)
	if err != nil {
		return xerrors.Errorf("SubmitProjectReport LoadWSServerID err:%s", err.Error())
	}

	if wID != candidateID {
		return xerrors.Errorf("SubmitProjectReport candidate id %s != %s", candidateID, wID)
	}

	node := s.NodeManager.GetEdgeNode(req.NodeID)
	if node == nil {
		return xerrors.Errorf("SubmitProjectReport node %s offline", req.NodeID)
	}

	if req.BandwidthDownSize > 0 {
		pInfo := s.NodeManager.GetDownloadProfitDetails(node, req.BandwidthDownSize, req.ProjectID)
		if pInfo != nil {
			// pInfo.Profit = 0 // TODO test
			err := s.db.AddNodeProfitDetails([]*types.ProfitDetails{pInfo})
			if err != nil {
				log.Errorf("SubmitProjectReport AddNodeProfit %s,%d, %.4f err:%s", pInfo.NodeID, pInfo.PType, pInfo.Profit, err.Error())
			}
		}
	}

	if req.BandwidthUpSize > 0 {
		pInfo := s.NodeManager.GetUploadProfitDetails(node, req.BandwidthUpSize, req.ProjectID)
		if pInfo != nil {
			// pInfo.Profit = 0 // TODO test
			err := s.db.AddNodeProfitDetails([]*types.ProfitDetails{pInfo})
			if err != nil {
				log.Errorf("SubmitProjectReport AddNodeProfit %s,%d, %.4f err:%s", pInfo.NodeID, pInfo.PType, pInfo.Profit, err.Error())
			}
		}
	}

	// update replica info
	if rInfo.MaxDelay < req.MaxDelay {
		rInfo.MaxDelay = req.MaxDelay
	}

	if rInfo.MinDelay > req.MinDelay {
		rInfo.MinDelay = req.MinDelay
	}
	rInfo.AvgDelay = req.AvgDelay

	rInfo.UploadTraffic += int64(req.BandwidthUpSize)
	rInfo.DownTraffic += int64(req.BandwidthDownSize)

	duration := req.StartTime.Sub(req.EndTime)
	seconds := duration.Seconds()
	rInfo.Time += int64(seconds)

	return s.db.UpdateProjectReplicasInfo(rInfo)
}

// SubmitWorkloadReportV2 submits a workload report to the scheduler.
func (s *Scheduler) SubmitWorkloadReportV2(ctx context.Context, workload *types.WorkloadRecordReq) error {
	// from sdk or web or client
	// return s.WorkloadManager.PushResult(workload, "")
	return nil
}

// SubmitWorkloadReport submits a workload report to the scheduler.
func (s *Scheduler) SubmitWorkloadReport(ctx context.Context, workload *types.WorkloadRecordReq) error {
	// from node
	nodeID := handler.GetNodeID(ctx)

	node := s.NodeManager.GetNode(nodeID)
	if node == nil {
		return xerrors.Errorf("node %s not exists", nodeID)
	}

	return s.WorkloadManager.PushResult(workload, nodeID)
}

// GetWorkloadRecords retrieves a list of workload results.
func (s *Scheduler) GetWorkloadRecords(ctx context.Context, nodeID string, limit, offset int) (*types.ListWorkloadRecordRsp, error) {
	return s.db.LoadWorkloadRecords(nodeID, limit, offset)
}

// GetWorkloadRecord retrieves a list of workload results.
func (s *Scheduler) GetWorkloadRecord(ctx context.Context, id string) (*types.WorkloadRecord, error) {
	return s.db.LoadWorkloadRecordOfID(id)
}

// ReDetermineNodeNATType re-determines the NAT type for the specified node.
func (s *Scheduler) ReDetermineNodeNATType(ctx context.Context, nodeID string) error {
	node := s.NodeManager.GetCandidateNode(nodeID)
	if node != nil {
		s.NatManager.DetermineCandidateNATType(ctx, nodeID)
		return nil
	}

	node = s.NodeManager.GetEdgeNode(nodeID)
	if node != nil {
		s.NatManager.DetermineEdgeNATType(ctx, nodeID)
		return nil
	}

	return nil
}

// GenerateCandidateCodes generates a specified number of candidate codes for a given node type.
func (s *Scheduler) GenerateCandidateCodes(ctx context.Context, count int, nodeType types.NodeType, isTest bool) ([]string, error) {
	infos := make([]*types.CandidateCodeInfo, 0)
	out := make([]string, 0)
	for i := 0; i < count; i++ {
		code := uuid.NewString()
		code = strings.Replace(code, "-", "", -1)

		infos = append(infos, &types.CandidateCodeInfo{
			Code:       code,
			NodeType:   nodeType,
			Expiration: time.Now().Add(time.Hour * 24),
			IsTest:     isTest,
		})
		out = append(out, code)
	}

	return out, s.db.SaveCandidateCodeInfo(infos)
}

// GetCandidateCodeInfos retrieves candidate code information for a given node ID and code.
func (s *Scheduler) GetCandidateCodeInfos(ctx context.Context, nodeID, code string) ([]*types.CandidateCodeInfo, error) {
	if nodeID != "" {
		info, err := s.db.GetCandidateCodeInfoForNodeID(nodeID)
		if err != nil {
			return nil, err
		}

		return []*types.CandidateCodeInfo{info}, nil
	}

	if code != "" {
		info, err := s.db.GetCandidateCodeInfo(code)
		if err != nil {
			return nil, err
		}

		return []*types.CandidateCodeInfo{info}, nil
	}

	return s.db.GetCandidateCodeInfos()
}

// ResetCandidateCode resets the candidate code for the specified node.
func (s *Scheduler) ResetCandidateCode(ctx context.Context, nodeID, code string) error {
	return s.db.ResetCandidateCodeInfo(code, nodeID)
}

// RemoveCandidateCode removes the candidate code information from the database.
func (s *Scheduler) RemoveCandidateCode(ctx context.Context, code string) error {
	return s.db.DeleteCandidateCodeInfo(code)
}

// GetValidators returns a list of validator addresses.
func (s *Scheduler) GetValidators(ctx context.Context) ([]string, error) {
	return s.ValidationMgr.GetValidators(), nil
}
