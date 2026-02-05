package node

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
)

// Node represents an Edge or Candidate node
type Node struct {
	mu                        sync.RWMutex
	resetCountOfIPChangesTime time.Time
	*API
	jsonrpc.ClientCloser
	PublicKey           *rsa.PublicKey
	ResourcesStatistics *types.ResourcesStatistics
	bandwidthTracker    *BandwidthTracker
	selectWeights       []int
	countOfIPChanges    int64
	LastValidateTime    int64
	BackProjectTime     int64
	NetFlowUp           int64
	NetFlowDown         int64
	DeactivateTime      int64
	BandwidthFreeUp     int64
	BandwidthFreeDown   int64
	BandwidthUpScore    int64
	BandwidthDownScore  int64

	Token       string
	ExternalIP  string
	RemoteAddr  string // ExternalIP:UDPPort
	ExternalURL string
	AreaID      string
	WSServerID  string

	OnlineRate              float32
	CPUUsage                float32
	MemoryUsage             float32
	DiskSpace               float32
	IncomeIncr              float32
	TCPPort                 int32
	Level                   int32
	OnlineDurationIncrement int32

	Type               types.NodeType
	ClientType         types.NodeClientType
	IsPrivateMinioOnly bool
	IsStorageNode      bool
	IsTestNode         bool
	ForceOffline       bool

	types.NodeDynamicInfo
}

// FillInfo fills the node information to the types.NodeInfo
func (n *Node) FillInfo(info *types.NodeInfo) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	info.NATType = n.NATType
	info.Type = n.Type
	info.CPUUsage = float64(n.CPUUsage)
	info.MemoryUsage = float64(n.MemoryUsage)
	info.DiskUsage = n.DiskUsage
	info.DiskSpace = float64(n.DiskSpace)
	info.ExternalIP = n.ExternalIP
	info.IncomeIncr = float64(n.IncomeIncr)
	info.IsTestNode = n.IsTestNode
	info.AreaID = n.AreaID
	info.RemoteAddr = n.RemoteAddr
	info.OnlineDuration = n.OnlineDuration
	info.Mx = RateOfL2Mx(n.OnlineDuration)
}

// FillDynamicInfo fills specific dynamic information to the types.NodeInfo
func (n *Node) FillDynamicInfo(ni *types.NodeInfo) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	ni.NATType = n.NATType
	ni.Type = n.Type
	ni.CPUUsage = float64(n.CPUUsage)
	ni.MemoryUsage = float64(n.MemoryUsage)
	ni.DiskUsage = n.DiskUsage
	ni.DiskSpace = float64(n.DiskSpace)
	ni.ExternalIP = n.ExternalIP
	ni.IncomeIncr = float64(n.IncomeIncr)
	ni.IsTestNode = n.IsTestNode
	ni.AreaID = n.AreaID
	ni.RemoteAddr = n.RemoteAddr
	ni.Mx = RateOfL2Mx(n.OnlineDuration)
}

// API represents the node API
type API struct {
	// common api
	api.Common
	api.Device
	api.Validation
	api.DataSync
	api.Asset
	api.Workerd
	// api.ProviderAPI
	WaitQuiet func(ctx context.Context) error
	// edge api
	// ExternalServiceAddress func(ctx context.Context, candidateURL string) (string, error)
	UserNATPunch func(ctx context.Context, sourceURL string, req *types.NatPunchReq) error
	CreateTunnel func(ctx context.Context, req *types.CreateTunnelReq) error

	// candidate api
	GetBlocksOfAsset        func(ctx context.Context, assetCID string, randomSeed int64, randomCount int) ([]string, error)
	CheckNetworkConnectable func(ctx context.Context, network, targetURL string) (bool, error)
	GetMinioConfig          func(ctx context.Context) (*types.MinioConfig, error)
}

// New creates a new node
func New() *Node {
	node := &Node{
		IsStorageNode:             true,
		resetCountOfIPChangesTime: time.Now(),
	}

	return node
}

// APIFromEdge creates a new API from an Edge API
func APIFromEdge(api api.Edge) *API {
	a := &API{
		Common:       api,
		Device:       api,
		Validation:   api,
		DataSync:     api,
		Asset:        api,
		WaitQuiet:    api.WaitQuiet,
		UserNATPunch: api.UserNATPunch,
		Workerd:      api,
		CreateTunnel: api.CreateTunnel,
	}
	return a
}

// APIFromCandidate creates a new API from a Candidate API
func APIFromCandidate(api api.Candidate) *API {
	a := &API{
		Common:     api,
		Device:     api,
		Validation: api,
		DataSync:   api,
		Asset:      api,
		// ProviderAPI:             api,
		WaitQuiet:               api.WaitQuiet,
		GetBlocksOfAsset:        api.GetBlocksWithAssetCID,
		CheckNetworkConnectable: api.CheckNetworkConnectable,
		GetMinioConfig:          api.GetMinioConfig,
	}
	return a
}

// InitInfo initializes the node information.
func (n *Node) InitInfo(nodeInfo *types.NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Type == types.NodeCandidate {
		n.bandwidthTracker = NewBandwidthTracker(20)
	} else {
		n.bandwidthTracker = NewBandwidthTracker(5)
	}

	oldValue := n.TodayOnlineTimeWindow
	oldIncr := n.OnlineDurationIncrement

	n.NodeDynamicInfo = nodeInfo.NodeDynamicInfo

	n.TodayOnlineTimeWindow = oldValue
	n.OnlineDurationIncrement = oldIncr

	n.NetFlowUp = nodeInfo.NetFlowUp
	n.NetFlowDown = nodeInfo.NetFlowDown
	n.DiskSpace = float32(nodeInfo.DiskSpace)
	n.WSServerID = nodeInfo.WSServerID
	// n.FirstTime = nodeInfo.FirstTime
	// n.PortMapping = nodeInfo.PortMapping
	n.DeactivateTime = nodeInfo.DeactivateTime
	// n.Memory = nodeInfo.Memory
	// n.CPUCores = nodeInfo.CPUCores

	n.IsTestNode = nodeInfo.IsTestNode
	n.Type = nodeInfo.Type
	n.ExternalIP = nodeInfo.ExternalIP
	// n.InternalIP = nodeInfo.InternalIP
	n.MemoryUsage = float32(nodeInfo.MemoryUsage)
	n.CPUUsage = float32(nodeInfo.CPUUsage)
	n.AreaID = nodeInfo.AreaID
	n.NATType = nodeInfo.NATType
	n.ClientType = nodeInfo.ClientType
	n.BackProjectTime = nodeInfo.BackProjectTime
	n.RemoteAddr = nodeInfo.RemoteAddr
	n.Level = int32(nodeInfo.Level)
	n.IncomeIncr = float32(nodeInfo.IncomeIncr)
	n.LastSeen = nodeInfo.LastSeen

	n.bandwidthTracker.PutBandwidthDown(nodeInfo.BandwidthDown)
	n.bandwidthTracker.PutBandwidthUp(nodeInfo.BandwidthUp)

	n.BandwidthDownScore = 30
	n.BandwidthUpScore = 30
}

// SetCountOfIPChanges node change ip count
func (n *Node) SetCountOfIPChanges(count int64) {
	n.countOfIPChanges = count

	if count == 0 {
		n.resetCountOfIPChangesTime = time.Now()
	}
}

// GetNumberOfIPChanges node change ip count
func (n *Node) GetNumberOfIPChanges() (int64, time.Time) {
	return n.countOfIPChanges, n.resetCountOfIPChangesTime
}

// Close closes the node RPC connection
func (n *Node) Close() error {
	if n.ClientCloser != nil {
		n.ClientCloser()
	}
	return nil
}

// ConnectRPC connects to the node RPC
func (n *Node) ConnectRPC(transport *quic.Transport, addr string, nodeType types.NodeType) error {
	if n.ClientCloser != nil {
		n.ClientCloser()
	}

	httpClient, err := client.NewHTTP3ClientWithPacketConn(transport)
	if err != nil {
		return err
	}

	rpcURL := fmt.Sprintf("https://%s/rpc/v0", addr)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+n.Token)

	if nodeType == types.NodeEdge || nodeType == types.NodeL3 {
		// Connect to node
		edgeAPI, closer, err := client.NewEdge(context.Background(), rpcURL, headers, jsonrpc.WithHTTPClient(httpClient))
		if err != nil {
			return xerrors.Errorf("NewEdge err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromEdge(edgeAPI)
		n.ClientCloser = closer
		return nil
	}

	if nodeType == types.NodeCandidate {
		// Connect to node
		candidateAPI, closer, err := client.NewCandidate(context.Background(), rpcURL, headers, jsonrpc.WithHTTPClient(httpClient))
		if err != nil {
			return xerrors.Errorf("NewCandidate err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = APIFromCandidate(candidateAPI)
		n.ClientCloser = closer
		return nil
	}

	if nodeType == types.NodeL5 {
		// Connect to node
		l5API, closer, err := client.NewL5(context.Background(), rpcURL, headers, jsonrpc.WithHTTPClient(httpClient))
		if err != nil {
			return xerrors.Errorf("NewCandidate err:%s,url:%s", err.Error(), rpcURL)
		}

		n.API = &API{Common: l5API, WaitQuiet: l5API.WaitQuiet}
		n.ClientCloser = closer
		return nil
	}

	return xerrors.Errorf("node %s type %d not wrongful", n.NodeID, n.Type)
}

// IsResourceNode checks if the node is available for resource utilization.
func (n *Node) IsResourceNode() bool {
	// Node is considered unavailable if it has been deactivated.
	if n.IsAbnormal() {
		return false
	}

	// ...
	return true
}

// IsAbnormal is node abnormal
func (n *Node) IsAbnormal() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Node is considered unavailable if it has been deactivated.
	if n.DeactivateTime > 0 {
		return true
	}

	// Node is considered unavailable if it is dedicated as a private Minio node.
	if n.IsPrivateMinioOnly {
		return true
	}

	return false
}

// SelectWeights get node select weights
func (n *Node) SelectWeights() []int {
	return n.selectWeights
}

// TCPAddr returns the tcp address of the node
func (n *Node) TCPAddr() string {
	index := strings.Index(n.RemoteAddr, ":")
	ip := n.RemoteAddr[:index+1]
	return fmt.Sprintf("%s%d", ip, n.TCPPort)
}

// RPCURL returns the rpc url of the node
func (n *Node) RPCURL() string {
	return fmt.Sprintf("https://%s/rpc/v0", n.RemoteAddr)
}

// WsURL returns the ws url of the node
func (n *Node) WsURL() string {
	wsURL, err := transformURL(n.ExternalURL)
	if err != nil {
		wsURL = fmt.Sprintf("ws://%s", n.RemoteAddr)
	}

	return wsURL
}

func transformURL(inputURL string) (string, error) {
	// Parse the URL from the string
	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		return "", err
	}

	switch parsedURL.Scheme {
	case "https":
		parsedURL.Scheme = "wss"
	case "http":
		parsedURL.Scheme = "ws"
	default:
		return "", xerrors.New("Scheme not http or https")
	}

	// Remove the path to clear '/rpc/v0'
	parsedURL.Path = ""

	// Return the modified URL as a string
	return parsedURL.String(), nil
}

// DownloadAddr returns the download address of the node
func (n *Node) DownloadAddr() string {
	if len(n.ExternalURL) > 0 {
		u, err := url.Parse(n.ExternalURL)
		if err == nil {
			return u.Host
		}
	}

	return n.TCPAddr()
}

// LastRequestTime returns the last request time of the node
func (n *Node) LastRequestTime() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LastSeen
}

// SetLastRequestTime sets the last request time of the node
func (n *Node) SetLastRequestTime(t time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastSeen = t
}

// EncryptToken returns the token of the node
func (n *Node) EncryptToken(cid, clientID string, titanRsa *titanrsa.Rsa, privateKey *rsa.PrivateKey) (*types.Token, error) {
	if n == nil {
		return nil, xerrors.Errorf("node is nil")
	}

	tkPayload := &types.TokenPayload{
		ID:          uuid.NewString(),
		NodeID:      n.NodeID,
		AssetCID:    cid,
		ClientID:    clientID, // TODO auth client and allocate id
		CreatedTime: time.Now(),
		Expiration:  time.Now().Add(10 * time.Hour),
	}

	b, err := n.encryptTokenPayload(tkPayload, n.PublicKey, titanRsa)
	if err != nil {
		return nil, xerrors.Errorf("%s encryptTokenPayload err:%s", n.NodeID, err.Error())
	}

	sign, err := titanRsa.Sign(privateKey, b)
	if err != nil {
		return nil, xerrors.Errorf("%s Sign err:%s", n.NodeID, err.Error())
	}

	return &types.Token{ID: tkPayload.ID, CipherText: hex.EncodeToString(b), Sign: hex.EncodeToString(sign)}, nil
}

// encryptTokenPayload encrypts a token payload object using the given public key and RSA instance.
func (n *Node) encryptTokenPayload(tkPayload *types.TokenPayload, publicKey *rsa.PublicKey, rsa *titanrsa.Rsa) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(tkPayload)
	if err != nil {
		return nil, err
	}

	return rsa.Encrypt(buffer.Bytes(), publicKey)
}

// HasEnoughDiskSpace Is there enough storage on the compute node
func (n *Node) HasEnoughDiskSpace(size float64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	residual := ((100 - n.DiskUsage) / 100) * float64(n.DiskSpace)
	if residual <= size {
		return false
	}

	residual = n.AvailableDiskSpace - n.TitanDiskUsage
	if residual <= size {
		return false
	}

	return true
}

// NetFlowUpExcess  Whether the upstream traffic exceeds the limit
func (n *Node) NetFlowUpExcess(size float64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.NetFlowUp <= 0 {
		return false
	}

	if n.NetFlowUp >= n.UploadTraffic+int64(size) {
		return false
	}

	return true
}

// NetFlowDownExcess  Whether the downstream traffic exceeds the limit
func (n *Node) NetFlowDownExcess(size float64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.NetFlowDown <= 0 {
		return false
	}

	if n.NetFlowDown >= n.DownloadTraffic+int64(size) {
		return false
	}

	return true
}

func (n *Node) SetBandwidths(free, peak types.FlowUnit) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.BandwidthDown = peak.D
	n.BandwidthUp = peak.U

	n.BandwidthFreeDown = free.D
	n.BandwidthFreeUp = free.U

	// free
	n.BandwidthDownScore = int64(math.Min(1, float64(n.BandwidthFreeDown)/float64(50*units.MiB)) * 90)
	n.BandwidthUpScore = int64(math.Min(1, float64(n.BandwidthFreeUp)/float64(20*units.MiB)) * 90)
}

// AddOnlineDuration increases the online duration of the node.
func (n *Node) AddOnlineDuration(minute int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.OnlineDuration += minute
	n.OnlineDurationIncrement += int32(minute)
}

// AddTodayOnlineTimeWindow increases the today online time window of the node.
func (n *Node) AddTodayOnlineTimeWindow(window int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.TodayOnlineTimeWindow += window
}

// ResetTodayOnlineTimeWindow resets the today online time window to zero.
func (n *Node) ResetTodayOnlineTimeWindow() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.TodayOnlineTimeWindow = 0
}

// GetDynamicInfoWithIncrement returns a copy of NodeDynamicInfo with the current increment.
func (n *Node) GetDynamicInfoWithIncrement() types.NodeDynamicInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	info := n.NodeDynamicInfo
	info.OnlineDuration = int(n.OnlineDurationIncrement)
	return info
}

// ResetOnlineDurationIncrement resets the online duration increment to zero.
func (n *Node) ResetOnlineDurationIncrement() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.OnlineDurationIncrement = 0
}

// UpdateMetrics updates the resource metrics of the node.
func (n *Node) UpdateMetrics(info *types.NodeInfo) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CPUUsage = float32(info.CPUUsage)
	n.MemoryUsage = float32(info.MemoryUsage)
	n.DiskSpace = float32(info.DiskSpace)
	n.DiskUsage = info.DiskUsage
}

// UpdateDiskUsage updates the disk usage metrics of the node.
func (n *Node) UpdateDiskUsage(diskUsage, titanDiskUsage float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.DiskUsage = diskUsage
	n.TitanDiskUsage = titanDiskUsage
}

// UpdateBandwidths updates the bandwidth metrics of the node.
func (n *Node) UpdateBandwidths(bandwidthDown, bandwidthUp int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if bandwidthDown > 0 {
		n.BandwidthDown = n.bandwidthTracker.PutBandwidthDown(bandwidthDown)
	}

	if bandwidthUp > 0 {
		n.BandwidthUp = n.bandwidthTracker.PutBandwidthUp(bandwidthUp)
	}
}

// func (n *Node) AddServiceEvent(event *types.ServiceEvent) {
// 	count := int64(0)
// 	if event.Status == types.ServiceTypeSucceed {
// 		count = 1
// 	}

// 	n.TaskSuccess += count
// 	n.TaskTotal++
// }

func (n *Node) CalcQualityScore(sCount, tCount int64) int64 {
	// fds := math.Min(1, float64(n.BandwidthDown)/float64(500*units.MiB)) * 90
	// fus := math.Min(1, float64(n.BandwidthUp)/float64(200*units.MiB)) * 90

	fds := float64(n.BandwidthFreeDown)
	fus := float64(n.BandwidthFreeUp)
	// fs := calculateMean([]float64{fds, fus})
	// success rate
	srs := 10.0
	if tCount > 0 {
		srs = (math.Min(1, float64(sCount)/float64(tCount))) * 10
	}

	upScore := fds + srs
	downScore := fus + srs

	fs := calculateMean([]float64{upScore, downScore})

	return int64(fs)
}
