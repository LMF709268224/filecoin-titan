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
		// Strict Origin check: Allow same-host or explicit list
		// In production, you'd check r.Host against a whitelist
		return true // Still allowing for now but providing hook for restriction
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
	err := s.db.SelectContext(ctx, &supervisors, "SELECT id, ip, version, last_heartbeat, tags FROM supervisors ORDER BY last_heartbeat DESC")
	return supervisors, err
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

type SupervisorInfo struct {
	ID            string    `json:"id" db:"id"`
	IP            string    `json:"ip" db:"ip"`
	Version       string    `json:"version" db:"version"`
	LastHeartbeat time.Time `json:"last_heartbeat" db:"last_heartbeat"`
	Tags          string    `json:"tags" db:"tags"`
}

type InstanceReport struct {
	Hash       string `json:"hash"`
	Status     string `json:"status"`
	CrashCount int    `json:"crash_count"`
	LastError  string `json:"last_error"`
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
		storage: storage,
		nodes:   make(map[string]*websocket.Conn),
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

func (o *Orchestrator) RefreshCache() {
	data, err := o.storage.GetTopology()
	if err != nil {
		log.Errorf("Failed to refresh cache: %v", err)
		return
	}
	o.cache.Lock()
	o.cache.data = data
	o.cache.Unlock()
}

// StartCacheRefresher periodically updates the local cache from storage
func (o *Orchestrator) StartCacheRefresher() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		o.RefreshCache()
	}
}

// HandleGetTopology returns the cached JSON for Supervisor nodes
func (o *Orchestrator) HandleGetTopology(w http.ResponseWriter, r *http.Request) {
	// Security: Authenticate the node
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
		// log.Warnf("Unauthorized topology request from %s", r.RemoteAddr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	o.cache.RLock()
	data := o.cache.data
	o.cache.RUnlock()

	if data == nil {
		http.Error(w, "Service initializing", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
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

	var report Report
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
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
		Timestamp string `json:"timestamp"` // ISO8601 or Unix
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	pkStr, err := o.storage.GetNodePublicKey(r.Context(), req.NodeID)
	if err != nil {
		http.Error(w, "Node not found", http.StatusUnauthorized)
		return
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"token": string(token)})
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

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (o *Orchestrator) BroadcastReload() {
	o.nodesMu.RLock()
	defer o.nodesMu.RUnlock()
	for id, conn := range o.nodes {
		err := conn.WriteMessage(websocket.TextMessage, []byte("RELOAD"))
		if err != nil {
			log.Errorf("Failed to push RELOAD to node %s: %v", id, err)
		}
	}
}
