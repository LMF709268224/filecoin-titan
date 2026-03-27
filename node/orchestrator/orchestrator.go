package orchestrator

import (
	"context"
	"crypto"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/gbrlsnchs/jwt/v3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	logging "github.com/ipfs/go-log/v2"
	"github.com/jmoiron/sqlx"
	"golang.org/x/time/rate"
)

var log = logging.Logger("orchestrator")

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Strict Origin check: Only allow same host
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		// Basic check: origin must contain the host
		return strings.Contains(origin, r.Host)
	},
}

//go:embed schema.sql
var schemaSQL string

// Storage defines the interface for persisting topology data
type Storage interface {
	GetTopology() ([]byte, error)
	UpdateTopology(data []byte) error
	// Status Reporting
	ReportStatus(ctx context.Context, report *Report) error
	GetSupervisors(ctx context.Context) ([]SupervisorInfo, error)
	// Node Authentication
	RegisterNode(ctx context.Context, nodeID, publicKey string) error
	GetNodePublicKey(ctx context.Context, nodeID string) (string, error)
	// Per-node admin controls (tags)
	GetNodeTags(ctx context.Context, nodeID string) ([]string, error)
	IsNodeTagsConfigured(ctx context.Context, nodeID string) (bool, error)
	SetNodeTags(ctx context.Context, nodeID string, tags []string, override bool) error
	GetRecentlyUpdatedNodes(ctx context.Context, since time.Time) ([]string, error)
	UpdateSupervisorResources(ctx context.Context, nodeID string, req *NodeResourceReport) error
}

// ProductionStorage implements Storage using MySQL for metadata and Redis for speeds/configurations
type ProductionStorage struct {
	db  *sqlx.DB
	ctx context.Context
}

func NewProductionStorage(mysqlURL string, unusedRedisURL string, unusedKey string) (*ProductionStorage, error) {
	// 1. Setup MySQL
	db, err := sqlx.Connect("mysql", mysqlURL)
	if err != nil {
		return nil, fmt.Errorf("mysql connection failed: %v", err)
	}

	// Optimize connection pool to avoid "bad idle connection" warnings
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)

	res := &ProductionStorage{
		db:  db,
		ctx: context.Background(),
	}

	if err := res.InitDatabase(); err != nil {
		return nil, fmt.Errorf("failed to init database: %v", err)
	}

	return res, nil
}

func (s *ProductionStorage) InitDatabase() error {
	// Split schema into individual commands (primitive split by ;)
	commands := strings.Split(schemaSQL, ";")
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}
		if _, err := s.db.ExecContext(s.ctx, cmd); err != nil {
			// Ignore Duplicate Column errors to allow idempotent schema migrations
			if strings.Contains(err.Error(), "Duplicate column name") {
				continue
			}
			return fmt.Errorf("failed to execute schema command [%s]: %v", cmd, err)
		}
	}
	return nil
}

func (s *ProductionStorage) GetTopology() ([]byte, error) {
	var rows []struct {
		Name    string `db:"name"`
		Content []byte `db:"content"`
	}
	err := s.db.SelectContext(s.ctx, &rows, "SELECT name, content FROM topology")
	if err != nil || len(rows) == 0 {
		return []byte(`{"instances": {}}`), nil
	}

	instances := make(map[string]interface{})
	for _, row := range rows {
		var inst interface{}
		if err := json.Unmarshal(row.Content, &inst); err != nil {
			log.Errorf("failed to unmarshal topology row %s: %v", row.Name, err)
			continue
		}
		instances[row.Name] = inst
	}

	res := map[string]interface{}{
		"instances": instances,
	}
	return json.Marshal(res)
}

func (s *ProductionStorage) UpdateTopology(data []byte) error {
	var topo struct {
		Instances map[string]interface{} `json:"instances"`
	}
	if err := json.Unmarshal(data, &topo); err != nil {
		return fmt.Errorf("invalid topology format: %v", err)
	}

	tx, err := s.db.BeginTxx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Clear old state (or we could do a diff, but wipe/rewrite is safer for a "save" operation)
	if _, err := tx.ExecContext(s.ctx, "DELETE FROM topology"); err != nil {
		return err
	}

	for name, content := range topo.Instances {
		cBytes, _ := json.Marshal(content)
		if _, err := tx.ExecContext(s.ctx, "INSERT INTO topology (name, content) VALUES (?, ?)", name, cBytes); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *ProductionStorage) ReportStatus(ctx context.Context, report *Report) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tags := strings.Join(report.Tags, ",")
	_, err = tx.ExecContext(ctx,
		"INSERT INTO supervisors (id, ip, version, last_heartbeat, tags) VALUES (?, ?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE ip=?, version=?, last_heartbeat=?, tags=?",
		report.ID, report.IP, report.Version, time.Now(), tags,
		report.IP, report.Version, time.Now(), tags)
	if err != nil {
		return err
	}

	for name, inst := range report.Instances {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO supervisor_instances (supervisor_id, instance_name, hash, status, crash_count, last_error) "+
				"VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE hash=?, status=?, crash_count=?, last_error=?",
			report.ID, name, inst.Hash, inst.Status, inst.CrashCount, inst.LastError,
			inst.Hash, inst.Status, inst.CrashCount, inst.LastError)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *ProductionStorage) GetSupervisors(ctx context.Context) ([]SupervisorInfo, error) {
	var supervisors []SupervisorInfo
	query := `
		SELECT 
			s.id, s.ip, s.version, s.last_heartbeat, s.tags,
			s.cpu_cores, s.total_memory_bytes, s.total_disk_bytes,
			s.cpu_percent, s.used_memory_bytes, s.used_disk_bytes,
			s.net_rx_bytes_per_sec, s.net_tx_bytes_per_sec,
			IFNULL(nc.tags_configured, 0) as tags_override
		FROM supervisors s
		LEFT JOIN node_config nc ON s.id = nc.node_id
		ORDER BY s.last_heartbeat DESC
	`
	err := s.db.SelectContext(ctx, &supervisors, query)
	if err != nil {
		return nil, err
	}

	if len(supervisors) == 0 {
		return supervisors, nil
	}

	// 1. Fetch all per-node tags
	var allTags []struct {
		NodeID string `db:"node_id"`
		Tag    string `db:"tag"`
	}
	if err := s.db.SelectContext(ctx, &allTags, "SELECT node_id, tag FROM node_tags"); err == nil {
		tagMap := make(map[string][]string)
		for _, t := range allTags {
			tagMap[t.NodeID] = append(tagMap[t.NodeID], t.Tag)
		}
		for i, sup := range supervisors {
			if tags, ok := tagMap[sup.ID]; ok {
				supervisors[i].ServerTags = tags
			} else {
				supervisors[i].ServerTags = []string{}
			}
		}
	}

	// 2. Fetch all instances for these supervisors
	var allInstances []struct {
		InstanceReport
		SupervisorID string `db:"supervisor_id"`
	}
	if err := s.db.SelectContext(ctx, &allInstances, "SELECT supervisor_id, instance_name, hash, status, crash_count, last_error FROM supervisor_instances"); err == nil {
		instanceMap := make(map[string][]InstanceReport)
		for _, inst := range allInstances {
			instanceMap[inst.SupervisorID] = append(instanceMap[inst.SupervisorID], inst.InstanceReport)
		}
		for i, sup := range supervisors {
			if insts, ok := instanceMap[sup.ID]; ok {
				supervisors[i].Instances = insts
			} else {
				supervisors[i].Instances = []InstanceReport{}
			}
		}
	}

	return supervisors, nil
}

func (s *ProductionStorage) UpdateSupervisorResources(ctx context.Context, nodeID string, res *NodeResourceReport) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE supervisors SET 
			cpu_cores=?, total_memory_bytes=?, total_disk_bytes=?, 
			cpu_percent=?, used_memory_bytes=?, used_disk_bytes=?, 
			net_rx_bytes_per_sec=?, net_tx_bytes_per_sec=?,
			last_heartbeat=NOW()
		WHERE id=?`,
		res.CPUCores, res.TotalMemory, res.TotalDisk,
		res.CPUPercent, res.UsedMemory, res.UsedDisk,
		res.NetRxBytes, res.NetTxBytes, nodeID)
	return err
}

func (s *ProductionStorage) RegisterNode(ctx context.Context, nodeID, publicKey string) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO nodes (node_id, public_key) VALUES (?, ?)", nodeID, publicKey)
	return err
}

func (s *ProductionStorage) GetNodePublicKey(ctx context.Context, nodeID string) (string, error) {
	var pk string
	err := s.db.GetContext(ctx, &pk, "SELECT public_key FROM nodes WHERE node_id = ?", nodeID)
	return pk, err
}

func (s *ProductionStorage) GetNodeTags(ctx context.Context, nodeID string) ([]string, error) {
	var tags []string
	err := s.db.SelectContext(ctx, &tags, "SELECT tag FROM node_tags WHERE node_id = ? ORDER BY tag", nodeID)
	if err != nil {
		return nil, err
	}
	return tags, nil
}

func (s *ProductionStorage) SetNodeTags(ctx context.Context, nodeID string, tags []string, override bool) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	overrideVal := 0
	if override {
		overrideVal = 1
	}

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO node_config (node_id, tags_configured) VALUES (?, ?) ON DUPLICATE KEY UPDATE tags_configured=?",
		nodeID, overrideVal, overrideVal); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM node_tags WHERE node_id = ?", nodeID); err != nil {
		return err
	}
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO node_tags (node_id, tag) VALUES (?, ?)", nodeID, tag); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *ProductionStorage) IsNodeTagsConfigured(ctx context.Context, nodeID string) (bool, error) {
	var configured bool
	err := s.db.GetContext(ctx, &configured, "SELECT tags_configured FROM node_config WHERE node_id = ?", nodeID)
	if err != nil {
		// Row not found means not configured
		return false, nil
	}
	return configured, nil
}

func (s *ProductionStorage) GetRecentlyUpdatedNodes(ctx context.Context, since time.Time) ([]string, error) {
	var nodes []string
	err := s.db.SelectContext(ctx, &nodes, "SELECT node_id FROM node_config WHERE updated_at > ?", since)
	return nodes, err
}

type WSMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type SupervisorInfo struct {
	ID            string           `json:"id" db:"id"`
	IP            string           `json:"ip" db:"ip"`
	Version       string           `json:"version" db:"version"`
	LastHeartbeat time.Time        `json:"last_heartbeat" db:"last_heartbeat"`
	Tags          string           `json:"tags" db:"tags"`
	TagsOverride  bool             `json:"tags_override" db:"tags_override"`
	CPUCores      int              `json:"cpu_cores" db:"cpu_cores"`
	TotalMemory   uint64           `json:"total_memory_bytes" db:"total_memory_bytes"`
	TotalDisk     uint64           `json:"total_disk_bytes" db:"total_disk_bytes"`
	CPUPercent    float64          `json:"cpu_percent" db:"cpu_percent"`
	UsedMemory    uint64           `json:"used_memory_bytes" db:"used_memory_bytes"`
	UsedDisk      uint64           `json:"used_disk_bytes" db:"used_disk_bytes"`
	NetRxBytes    uint64           `json:"net_rx_bytes_per_sec" db:"net_rx_bytes_per_sec"`
	NetTxBytes    uint64           `json:"net_tx_bytes_per_sec" db:"net_tx_bytes_per_sec"`
	ServerTags    []string         `json:"_serverTags" db:"-"`
	Instances     []InstanceReport `json:"instances" db:"-"`
}

type NodeResourceReport struct {
	CPUCores    int     `json:"cpu_cores"`
	TotalMemory uint64  `json:"total_memory_bytes"`
	TotalDisk   uint64  `json:"total_disk_bytes"`
	CPUPercent  float64 `json:"cpu_percent"`
	UsedMemory  uint64  `json:"used_memory_bytes"`
	UsedDisk    uint64  `json:"used_disk_bytes"`
	NetRxBytes  uint64  `json:"net_rx_bytes_per_sec"`
	NetTxBytes  uint64  `json:"net_tx_bytes_per_sec"`
}

type InstanceReport struct {
	Name       string `json:"name" db:"instance_name"`
	Hash       string `json:"hash" db:"hash"`
	Status     string `json:"status" db:"status"`
	CrashCount int    `json:"crash_count" db:"crash_count"`
	LastError  string `json:"last_error" db:"last_error"`
}

type Report struct {
	ID        string                    `json:"id"`
	IP        string                    `json:"ip"`
	Version   string                    `json:"version"`
	Tags      []string                  `json:"tags"`
	Instances map[string]InstanceReport `json:"instances"`
}

type Orchestrator struct {
	storage Storage
	cache   struct {
		sync.RWMutex
		data []byte
	}
	AdminUser    string
	AdminPass    string
	JWTSecret    []byte
	loginLimiter *rate.Limiter

	// Node-side rate limiters
	regLimiter  *rate.Limiter
	nodeLimiter *rate.Limiter

	// WebSocket Node Pool
	nodesMu sync.RWMutex
	nodes   map[string]*websocket.Conn

	// Distributed Sync State
	lastTopoHash string
	lastTagSync  time.Time
}

type AdminPayload struct {
	jwt.Payload
	User string `json:"user"`
}

type NodePayload struct {
	jwt.Payload
	NodeID string `json:"node_id"`
}

func NewOrchestrator(storage Storage) *Orchestrator {
	o := &Orchestrator{
		storage:     storage,
		nodes:       make(map[string]*websocket.Conn),
		lastTagSync: time.Now(),
	}
	o.loginLimiter = rate.NewLimiter(rate.Every(1*time.Second), 5)
	o.regLimiter = rate.NewLimiter(rate.Every(10*time.Second), 1)         // 1 registration per 10s
	o.nodeLimiter = rate.NewLimiter(rate.Every(200*time.Millisecond), 10) // 5 logins per sec burst
	// Initial cache fill
	o.RefreshCache()
	return o
}

func (o *Orchestrator) JwtAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.AdminUser == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Allow OPTIONS for preflight
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		// Allow Login API
		if strings.HasSuffix(r.URL.Path, "/login") {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		var payload AdminPayload
		if _, err := jwt.Verify([]byte(tokenStr), jwt.NewHS256(o.JWTSecret), &payload); err != nil {
			log.Warnf("JWT verification failed: %v", err)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		if payload.User != o.AdminUser {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (o *Orchestrator) RefreshCache() bool {
	data, err := o.storage.GetTopology()
	if err != nil {
		log.Errorf("Failed to refresh cache: %v", err)
		return false
	}

	h := crypto.SHA1.New()
	h.Write(data)
	hash := fmt.Sprintf("%x", h.Sum(nil))
	changed := false
	if o.lastTopoHash != "" && o.lastTopoHash != hash {
		changed = true
	}
	o.lastTopoHash = hash

	o.cache.Lock()
	o.cache.data = data
	o.cache.Unlock()
	return changed
}

// StartCacheRefresher periodically updates the local cache from storage
func (o *Orchestrator) StartCacheRefresher() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		// 1. Global Sync
		if o.RefreshCache() {
			log.Infof("Topology changed (detected via DB poll), broadcasting reload...")
			o.BroadcastReload()
		}

		// 2. Incremental Node Sync (with 10s overlap)
		since := o.lastTagSync.Add(-10 * time.Second)
		nodes, err := o.storage.GetRecentlyUpdatedNodes(context.Background(), since)
		if err == nil && len(nodes) > 0 {
			log.Infof("Detected %d nodes with updated tags via DB poll, triggering reloads...", len(nodes))
			for _, nodeID := range nodes {
				o.ReloadNode(nodeID)
			}
		}
		o.lastTagSync = time.Now()
	}
}

// GetNodeTopologyData returns the parsed topology map with per-node tags injected
func (o *Orchestrator) GetNodeTopologyData(ctx context.Context, nodeID string) map[string]interface{} {
	o.cache.RLock()
	data := o.cache.data
	o.cache.RUnlock()

	var topo map[string]interface{}
	if data == nil {
		topo = make(map[string]interface{})
		topo["instances"] = make(map[string]interface{})
	} else if err := json.Unmarshal(data, &topo); err != nil {
		topo = make(map[string]interface{})
		topo["instances"] = make(map[string]interface{})
	}

	// Inject server-assigned node_tags for this node.
	override, err := o.storage.IsNodeTagsConfigured(ctx, nodeID)
	// Always send override status and tags
	nodeTags, tagErr := o.storage.GetNodeTags(ctx, nodeID)
	if tagErr == nil {
		if nodeTags == nil {
			nodeTags = []string{}
		}
		topo["node_tags"] = nodeTags
		if err == nil {
			topo["tags_override"] = override
		} else {
			topo["tags_override"] = false
		}
	}
	return topo
}

// HandleGetTopology returns the topology JSON for a Supervisor node with per-node node_tags injected.
func (o *Orchestrator) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	authorized := false
	var nodeID string

	if strings.HasPrefix(auth, "Bearer ") {
		token := auth[len("Bearer "):]
		var pl NodePayload
		if _, err := jwt.Verify([]byte(token), jwt.NewHS256(o.JWTSecret), &pl); err == nil {
			authorized = true
			nodeID = pl.NodeID
		}
	}

	if !authorized {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	topo := o.GetNodeTopologyData(r.Context(), nodeID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topo)
}

// HandleReportStatus receives real-time state from Supervisors
func (o *Orchestrator) HandleReportStatus(w http.ResponseWriter, r *http.Request) {
	// Security: Limit request body size to 1MB to prevent OOM
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Security: Only allow authorized supervisors to report
	auth := r.Header.Get("Authorization")
	authorized := false

	// Try Node JWT (New System)
	if strings.HasPrefix(auth, "Bearer ") {
		token := auth[len("Bearer "):]
		var pl NodePayload
		if _, err := jwt.Verify([]byte(token), jwt.NewHS256(o.JWTSecret), &pl); err == nil {
			authorized = true
		}
	}

	if !authorized {
		log.Warnf("Unauthorized report attempt from %s", r.RemoteAddr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var pl NodePayload
	token := auth[len("Bearer "):]
	jwt.Verify([]byte(token), jwt.NewHS256(o.JWTSecret), &pl) // We know it's valid if authorized is true

	var report Report
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// SECURITY: Verify that the reporting node is only reporting for ITSELF
	if report.ID != pl.NodeID {
		log.Errorf("🔥 SECURITY ALERT: Node %s attempted to report status for Node %s! Request blocked.", pl.NodeID, report.ID)
		http.Error(w, "Forbidden: ID mismatch", http.StatusForbidden)
		return
	}

	// Simple IP detection if not provided
	if report.IP == "" {
		report.IP = getClientIP(r)
	}

	if err := o.storage.ReportStatus(r.Context(), &report); err != nil {
		log.Errorf("Failed to save report from %s: %v", report.ID, err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getClientIP(r *http.Request) string {
	// 1. Check X-Forwarded-For
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	// 2. Check X-Real-IP
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return strings.TrimSpace(xri)
	}

	// 3. Fallback to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

/*
// HandleDownload serves files from the ./downloads directory ONLY
func (o *Orchestrator) HandleDownload(w http.ResponseWriter, r *http.Request) {
	// 1. Get just the filename, preventing path traversal
	baseName := filepath.Base(r.URL.Path)
	if baseName == "" || baseName == "." || baseName == "/" {
		http.Error(w, "Filename required", http.StatusBadRequest)
		return
	}

	// SECURITY: Block all hidden files (starting with '.')
	if strings.HasPrefix(baseName, ".") {
		log.Warnf("Access denied: Attempt to download hidden file '%s' from %s", baseName, r.RemoteAddr)
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// 2. Force download from a dedicated "downloads" directory
	safePath := filepath.Join("downloads", baseName)

	// 3. Verify file exists in that restricted location
	if _, err := os.Stat(safePath); err != nil {
		log.Warnf("Download failed: file '%s' not found in downloads/ (Request: %s)", baseName, r.URL.Path)
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	log.Infof("Serving file from downloads/: %s", baseName)
	http.ServeFile(w, r, safePath)
}
*/

// HandleAdminAPI handles management requests from the Web UI
func (o *Orchestrator) HandleAdminAPI(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	endpoint := parts[len(parts)-1]

	// URL patterns:
	//   /api/node-tags/{nodeID}        GET/POST
	//   /api/node-force-update/{nodeID}/{instanceName}  POST
	// We detect these by checking the second-to-last path segment.
	if len(parts) >= 2 {
		secondLast := parts[len(parts)-2]
		if secondLast == "node-tags" {
			o.HandleNodeTags(w, r, endpoint) // endpoint == nodeID here
			return
		}
	}

	switch endpoint {
	case "login":
		o.HandleLogin(w, r)
	case "config":
		if r.Method == http.MethodGet {
			o.cache.RLock()
			data := o.cache.data
			o.cache.RUnlock()

			var topo map[string]interface{}
			json.Unmarshal(data, &topo)
			if topo == nil {
				topo = make(map[string]interface{})
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(topo)
		} else if r.Method == http.MethodPost {
			o.HandleUpdateTopology(w, r)
		}
	case "supervisors":
		o.HandleGetSupervisors(w, r)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

// HandleNodeTags manages server-assigned tags for a specific node.
//
//	GET  /api/node-tags/{nodeID}  -> returns current tags as JSON array
//	POST /api/node-tags/{nodeID}  -> body: ["edge","candidate"], replaces all tags
func (o *Orchestrator) HandleNodeTags(w http.ResponseWriter, r *http.Request, nodeID string) {
	if nodeID == "" {
		http.Error(w, "nodeID required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		tags, err := o.storage.GetNodeTags(r.Context(), nodeID)
		if err != nil {
			log.Errorf("GetNodeTags failed: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if tags == nil {
			tags = []string{}
		}
		override, _ := o.storage.IsNodeTagsConfigured(r.Context(), nodeID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"tags":     tags,
			"override": override,
		})
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
		var req struct {
			Tags     []string `json:"tags"`
			Override bool     `json:"override"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if err := o.storage.SetNodeTags(r.Context(), nodeID, req.Tags, req.Override); err != nil {
			log.Errorf("SetNodeTags failed: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		log.Infof("Admin set node_tags for %s: %v (override=%v)", nodeID, req.Tags, req.Override)
		o.ReloadNode(nodeID)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (o *Orchestrator) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if !o.loginLimiter.Allow() {
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Security: Limit admin login body size
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

	var creds struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if o.AdminUser == "" || creds.User != o.AdminUser || creds.Pass != o.AdminPass {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	now := time.Now()
	pl := AdminPayload{
		Payload: jwt.Payload{
			Issuer:         "titan-orchestrator",
			Subject:        "admin",
			ExpirationTime: jwt.NumericDate(now.Add(24 * time.Hour)),
			IssuedAt:       jwt.NumericDate(now),
		},
		User: o.AdminUser,
	}

	token, err := jwt.Sign(pl, jwt.NewHS256(o.JWTSecret))
	if err != nil {
		log.Errorf("Token generation error: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token": string(token),
	})
}

func (o *Orchestrator) HandleGetSupervisors(w http.ResponseWriter, r *http.Request) {
	list, err := o.storage.GetSupervisors(r.Context())
	if err != nil {
		log.Errorf("Failed to get supervisors: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if list == nil {
		list = make([]SupervisorInfo, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(list)
}

// HandleUpdateTopology saves the new configuration to disk and refreshes cache
func (o *Orchestrator) HandleUpdateTopology(w http.ResponseWriter, r *http.Request) {
	// Security: Limit request body size to 2MB to prevent OOM
	r.Body = http.MaxBytesReader(w, r.Body, 2*1024*1024)

	var newTopo interface{}
	if err := json.NewDecoder(r.Body).Decode(&newTopo); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	data, err := json.MarshalIndent(newTopo, "", "    ")
	if err != nil {
		http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
		return
	}

	if err := o.storage.UpdateTopology(data); err != nil {
		http.Error(w, "Failed to save data: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Topology updated by admin, refreshing local cache")
	o.RefreshCache()
	o.BroadcastReload()
	w.WriteHeader(http.StatusOK)
}

func (o *Orchestrator) HandleNodeRegister(w http.ResponseWriter, r *http.Request) {
	if !o.regLimiter.Allow() {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Security: Limit body size
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NodeID    string `json:"node_id"`
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.PublicKey == "" {
		http.Error(w, "Missing fields", http.StatusBadRequest)
		return
	}

	if err := o.storage.RegisterNode(r.Context(), req.NodeID, req.PublicKey); err != nil {
		log.Warnf("Registration failed for %s: %v (likely already registered)", req.NodeID, err)
		http.Error(w, "Registration failed (ID might be taken)", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (o *Orchestrator) HandleNodeLogin(w http.ResponseWriter, r *http.Request) {
	if !o.nodeLimiter.Allow() {
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		NodeID    string `json:"node_id"`
		Signature string `json:"signature"`
		Timestamp string `json:"timestamp"`  // ISO8601 or Unix
		PublicKey string `json:"public_key"` // Included for auto-registration
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	pkStr, err := o.storage.GetNodePublicKey(r.Context(), req.NodeID)
	if err != nil {
		if req.PublicKey != "" {
			// Auto-register the node on the fly
			log.Infof("Auto-registering new node %s on first login", req.NodeID)
			if err := o.storage.RegisterNode(r.Context(), req.NodeID, req.PublicKey); err != nil {
				log.Errorf("Auto-registration failed for %s: %v", req.NodeID, err)
				http.Error(w, "Auto-registration failed", http.StatusInternalServerError)
				return
			}
			pkStr = req.PublicKey
		} else {
			http.Error(w, "Node not found and no public_key provided", http.StatusUnauthorized)
			return
		}
	}

	pubKey, err := titanrsa.Pem2PublicKey([]byte(pkStr))
	if err != nil {
		http.Error(w, "Invalid node key", http.StatusInternalServerError)
		return
	}

	sig, _ := hex.DecodeString(req.Signature)

	// Security: Limit body size
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

	// Replay Protection: Check if timestamp is fresh (within 5 minutes)
	parsedTS, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		http.Error(w, "Invalid timestamp format", http.StatusBadRequest)
		return
	}

	if time.Since(parsedTS).Abs() > 5*time.Minute {
		log.Warnf("Replay attack suspected or clock drift for node %s: %s", req.NodeID, req.Timestamp)
		http.Error(w, "Timestamp too old or clock drift", http.StatusUnauthorized)
		return
	}

	rsaVerify := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	if err := rsaVerify.VerifySign(pubKey, sig, []byte(req.NodeID+req.Timestamp)); err != nil {
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
	}

	now := time.Now()
	token, err := jwt.Sign(NodePayload{
		Payload: jwt.Payload{
			Issuer:         "titan-orchestrator",
			Subject:        "node",
			ExpirationTime: jwt.NumericDate(now.Add(24 * time.Hour)),
			IssuedAt:       jwt.NumericDate(now),
		},
		NodeID: req.NodeID,
	}, jwt.NewHS256(o.JWTSecret))
	if err != nil {
		http.Error(w, "Token error", http.StatusInternalServerError)
		return
	}

	topo := o.GetNodeTopologyData(r.Context(), req.NodeID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"token":    string(token),
		"topology": topo,
	})
}

func (o *Orchestrator) HandleWS(w http.ResponseWriter, r *http.Request) {
	// Security: Limit request headers/handshake size
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("WS Upgrade failed: %v", err)
		return
	}

	// Security: Limit incoming message size (RELOAD signals are very small)
	conn.SetReadLimit(4096)

	// Verify Token from query or header
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		tokenStr = r.Header.Get("Sec-WebSocket-Protocol") // Alternative
	}

	var pl NodePayload
	if _, err := jwt.Verify([]byte(tokenStr), jwt.NewHS256(o.JWTSecret), &pl); err != nil {
		log.Warnf("WS Auth failed for %s: %v", r.RemoteAddr, err)
		conn.Close()
		return
	}

	nodeID := pl.NodeID
	o.nodesMu.Lock()
	if old, ok := o.nodes[nodeID]; ok {
		old.Close()
	}
	o.nodes[nodeID] = conn
	o.nodesMu.Unlock()

	log.Infof("Node %s connected via WebSocket", nodeID)

	defer func() {
		o.nodesMu.Lock()
		delete(o.nodes, nodeID)
		o.nodesMu.Unlock()
		conn.Close()
		log.Infof("Node %s disconnected from WebSocket", nodeID)
	}()

	// Keep-alive: Server strictly expects a Ping or Message every 70 seconds max (Client sends every ~54s)
	conn.SetReadDeadline(time.Now().Add(70 * time.Second))
	conn.SetPingHandler(func(appData string) error {
		// Reset the read deadline every time a Ping is received
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		// Manually reply with the expected Pong (Gorilla default behavior)
		conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		return nil
	})

	for {
		msgType, rawMsgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}
		// Any message (like TextMessage or Binary) proves the node is alive, reset deadline
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))

		if msgType == websocket.TextMessage {
			var wsMsg WSMessage
			if err := json.Unmarshal(rawMsgBytes, &wsMsg); err == nil {
				switch wsMsg.Event {
				case "resource_report":
					var report NodeResourceReport
					if err := json.Unmarshal(wsMsg.Data, &report); err == nil {
						o.storage.UpdateSupervisorResources(r.Context(), nodeID, &report)
					}
				case "report_status":
					var report Report
					if err := json.Unmarshal(wsMsg.Data, &report); err == nil {
						report.ID = nodeID // Force override to prevent spoofing
						if report.IP == "" {
							report.IP = getClientIP(r)
						}
						o.storage.ReportStatus(r.Context(), &report)
					}
				}
			}
		}
	}
}

func (o *Orchestrator) sendTopologyUpdate(nodeID string, c *websocket.Conn) {
	topo := o.GetNodeTopologyData(context.Background(), nodeID)
	msg := map[string]interface{}{
		"event": "topology",
		"data":  topo,
	}
	msgBytes, _ := json.Marshal(msg)

	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	c.WriteMessage(websocket.TextMessage, msgBytes)
	c.SetWriteDeadline(time.Time{})
}

// BroadcastReload notifies ALL connected supervisors to reload their topology.
func (o *Orchestrator) BroadcastReload() {
	o.nodesMu.RLock()
	defer o.nodesMu.RUnlock()
	for id, conn := range o.nodes {
		go o.sendTopologyUpdate(id, conn)
	}
}

// ReloadNode notifies a SPECIFIC connected supervisor to reload its topology.
func (o *Orchestrator) ReloadNode(nodeID string) {
	o.nodesMu.RLock()
	conn, ok := o.nodes[nodeID]
	o.nodesMu.RUnlock()
	if ok {
		go o.sendTopologyUpdate(nodeID, conn)
	}
}
