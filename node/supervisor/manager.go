package supervisor

import (
	"context"
	"crypto"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Filecoin-Titan/titan/build"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/types"
	"github.com/gorilla/websocket"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"gopkg.in/yaml.v3"
)

// exeSuffix is ".exe" on Windows and "" on Linux/macOS.
var exeSuffix = func() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}()

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
)

// Topology represents the server's desired state for ALL nodes.
// NodeTags are tags the administrator has assigned to this specific node
// (in addition to the locally configured --tags flag).
type Topology struct {
	Instances map[string]InstanceTarget `json:"instances"`
	NodeTags  []string                  `json:"node_tags"` // Server-assigned tags for this node (overridable per-node by admin)
}

type PlatformConfig struct {
	Hash       string `json:"hash"`
	URL        string `json:"url"`
	ConfigHash string `json:"config_hash"`
	ConfigURL  string `json:"config_url"`
}

type InstanceTarget struct {
	Hash           string                    `json:"hash"`
	URL            string                    `json:"url"` // Download URL for this binary version
	Platforms      map[string]PlatformConfig `json:"platforms"`
	ConfigURL      string                    `json:"config_url"` // Optional: Download URL for configuration asset
	ConfigHash     string                    `json:"config_hash"`
	Args           []string                  `json:"args"`         // Command-line args template
	InstanceDir    string                    `json:"instance_dir"` // Optional: custom dir for run_config.yaml and user_config.yaml
	Env            map[string]string         `json:"env"`          // Optional: extra environment variables
	Params         map[string]interface{}    `json:"params"`
	Tags           []string                  `json:"tags"`             // Optional: tags for selective running
	Enabled        *bool                     `json:"enabled"`          // Optional: global ON/OFF switch
	PreStopCommand []string                  `json:"pre_stop_command"` // Optional: command(s) to run before first start
}

type InstanceState struct {
	TargetHash        string
	CurrentHash       string
	CurrentConfigHash string
	RunConfigHash     string
	Cmd               *exec.Cmd
	CrashCount        int

	FailedHash   string
	LastGoodHash string
	AdoptedPID   int
	LastError    string // Captured terminal/exit errors
} // PID of a process adopted from a previous Supervisor run

type Manager struct {
	repoPath     string
	nodeID       string
	binPath      string
	instancePath string
	serverUrl    string
	allowedTags  []string // Tags configured locally via --tags flag
	serverTags   []string // Tags dynamically assigned by the administrator from the Orchestrator
	// serverTagsConfigured is true when the server has explicitly set tags for this node
	// (even to empty = run nothing). When false, local allowedTags are used as fallback.
	serverTagsConfigured bool

	mu        sync.Mutex
	instances map[string]*InstanceState

	// Log Rotation Settings
	logMaxAge       time.Duration
	logRotationTime time.Duration
	logRotationSize int64

	platformOverride string

	keystore types.KeyStore
	token    string
}

// effectiveTags returns the tags used for instance filtering.
//
// Priority rules (server is authoritative when configured):
//   - If server has explicitly configured tags for this node → use server tags ONLY
//     (even if empty, which means "run nothing")
//   - If server has not configured any tags → fall back to local --tags flag
func (m *Manager) effectiveTags() []string {
	if m.serverTagsConfigured {
		// Server is authoritative: use its tags exclusively, ignoring --tags
		return m.serverTags
	}
	// Not yet configured on server: respect the user's local --tags
	return m.allowedTags
}

func NewManager(repoPath string, nodeID string, serverUrl string, allowedTags []string, logMaxAge, logRotationTime time.Duration, logRotationSize int64, platformOverride string, keystore types.KeyStore) *Manager {
	return &Manager{
		repoPath:         repoPath,
		nodeID:           nodeID,
		binPath:          filepath.Join(repoPath, "workers", "bin"),
		instancePath:     filepath.Join(repoPath, "workers", "instances"),
		serverUrl:        serverUrl,
		allowedTags:      allowedTags,
		instances:        make(map[string]*InstanceState),
		logMaxAge:        logMaxAge,
		logRotationTime:  logRotationTime,
		logRotationSize:  logRotationSize,
		platformOverride: platformOverride,
		keystore:         keystore,
	}
}

func (m *Manager) Login(ctx context.Context) error {
	m.mu.Lock()
	if m.token != "" {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	keyName := "private-key"
	kInfo, err := m.keystore.Get(keyName)
	if err != nil {
		return fmt.Errorf("failed to get identity key: %w", err)
	}

	priv, err := titanrsa.Pem2PrivateKey(kInfo.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	ts := time.Now().Format(time.RFC3339)
	rsaSigner := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sig, err := rsaSigner.Sign(priv, []byte(m.nodeID+ts))
	if err != nil {
		return fmt.Errorf("failed to sign login request: %w", err)
	}

	reqBody := map[string]string{
		"node_id":    m.nodeID,
		"signature":  hex.EncodeToString(sig),
		"timestamp":  ts,
		"public_key": string(titanrsa.PublicKey2Pem(&priv.PublicKey)),
	}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(strings.TrimSuffix(m.serverUrl, "/")+"/api/v1/node/login", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("login failed with status %d", resp.StatusCode)
	}

	var res struct {
		Token    string   `json:"token"`
		Topology Topology `json:"topology"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}

	m.mu.Lock()
	m.token = res.Token
	m.mu.Unlock()

	// Step 4: Apply initial topology received in the login response immediately.
	// This enables "Zero-delay startup" without waiting for a WebSocket push.
	m.applyTopology(res.Topology)

	log.Infof("Successfully logged in, received JWT and applied initial topology for node %s", m.nodeID)
	return nil
}

func (m *Manager) WatchConfig(ctx context.Context) {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := m.Login(ctx); err != nil {
			log.Errorf("WS Watcher: Login failed: %v, retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Convert HTTP URL to WS URL (e.g., http://... -> ws://...) and append the JWT token
		wsUrl := strings.TrimSuffix(strings.Replace(m.serverUrl, "http", "ws", 1), "/") + "/api/v1/node/ws?token=" + m.token
		log.Infof("Attempting WebSocket connection to %s", wsUrl)

		// Establish the WebSocket connection tightly coupled with the Orchestrator
		conn, _, err := websocket.DefaultDialer.Dial(wsUrl, nil)
		if err != nil {
			log.Errorf("WebSocket dial failed: %v, retrying in %v", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = 1 * time.Second // Reset backoff on success
		log.Info("WebSocket connected to Orchestrator")

		conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})

		heartbeatDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(pingPeriod)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					// 1. Send a standard WebSocket Ping control frame to keep the connection alive
					if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait)); err != nil {
						log.Errorf("WebSocket ping failed: %v", err)
						return
					}

					// 2. Piggyback Resource Report: Send CPU/Mem/Disk/Network usage as a JSON event
					report := CollectResourceReport()
					reportMsg := map[string]interface{}{
						"event": "resource_report",
						"data":  report,
					}
					if data, err := json.Marshal(reportMsg); err == nil {
						conn.SetWriteDeadline(time.Now().Add(writeWait))
						if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
							log.Errorf("WebSocket resource report push failed: %v", err)
							return
						}
					}

					// 3. Piggyback Status Report: Send worker instance statuses (running, crashed, etc.)
					statusData := m.buildStatusReport()
					statusMsg := map[string]interface{}{
						"event": "report_status",
						"data":  statusData,
					}
					if data, err := json.Marshal(statusMsg); err == nil {
						conn.SetWriteDeadline(time.Now().Add(writeWait))
						if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
							log.Errorf("WebSocket status report push failed: %v", err)
							return
						}
					}
				case <-heartbeatDone:
					return
				}
			}
		}()

		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				log.Warnf("WebSocket connection lost: %v", err)
				close(heartbeatDone)
				conn.Close()
				break
			}

			if msgType == websocket.TextMessage {
				// 4. Listen for incoming JSON events pushed proactively by the Orchestrator
				var wsMsg struct {
					Event string          `json:"event"`
					Data  json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(msg, &wsMsg); err == nil {
					if wsMsg.Event == "topology" {
						// Instantly apply the new topology (zero polling delay)
						log.Info("📬 Received updated topology via WebSocket! Applying immediately.")
						var topo Topology
						if err := json.Unmarshal(wsMsg.Data, &topo); err == nil {
							m.applyTopology(topo)
						} else {
							log.Errorf("Failed to parse topology from WS: %v", err)
						}
					} else if wsMsg.Event == "RELOAD" {
						// Fallback legacy reload signal, ignore or handle if needed
						log.Warn("Received legacy RELOAD string, ignoring since we use JSON now.")
					}
				}
			}
		}
	}
}

func (m *Manager) InitDirs() error {
	if err := os.MkdirAll(m.binPath, 0755); err != nil {
		return err
	}
	return os.MkdirAll(m.instancePath, 0755)
}

func (m *Manager) buildStatusReport() map[string]interface{} {
	instances := make(map[string]interface{})
	m.mu.Lock()
	for name, state := range m.instances {
		instances[name] = map[string]interface{}{
			"hash":        state.CurrentHash,
			"status":      m.deriveStatus(state),
			"crash_count": state.CrashCount,
			"last_error":  state.LastError, // Update this if you add an error field to InstanceState
		}
	}
	m.mu.Unlock()

	return map[string]interface{}{
		"version":   build.UserVersion(),
		"tags":      m.allowedTags,
		"instances": instances,
	}
}

func (m *Manager) deriveStatus(state *InstanceState) string {
	if state.Cmd != nil {
		return "running"
	}
	if state.AdoptedPID > 0 {
		return "running"
	}
	if state.FailedHash != "" {
		return "blacklisted"
	}
	if state.CrashCount > 0 {
		return "crashed"
	}
	return "stopped"
}

func (m *Manager) applyTopology(topo Topology) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update server-assigned tags from the topology response.
	// node_tags present in JSON (even as []) means server has explicitly configured this node.
	// node_tags absent from JSON (nil) means server has not configured this node → fall back to local --tags.
	if topo.NodeTags != nil {
		// Server is authoritative: compare and update
		if !m.serverTagsConfigured || !stringSlicesEqual(m.serverTags, topo.NodeTags) {
			log.Infof("Server tags updated for this node: %v → %v (server authoritative)", m.serverTags, topo.NodeTags)
			m.serverTags = topo.NodeTags
		}
		m.serverTagsConfigured = true
	} else {
		// Server has not configured tags for this node → clear server override
		if m.serverTagsConfigured {
			log.Infof("Server tags removed for this node. Falling back to local --tags: %v", m.allowedTags)
		}
		m.serverTagsConfigured = false
		m.serverTags = nil
	}

	activeTags := m.effectiveTags()

	// 1. Start or Update Instances
	for name, target := range topo.Instances {
		// SECURITY: Validate instance name to prevent path traversal
		if strings.Contains(name, "..") || strings.Contains(name, "/") || strings.Contains(name, "\\") {
			log.Errorf("🔥 SECURITY ALERT: Malicious instance name '%s' detected! Skipping.", name)
			continue
		}
		compatible := m.resolveTargetPlatform(&target)
		if !compatible {
			log.Debugf("Instance %s skipped: no compatible platform found in %v for current environment", name, target.Platforms)
			state, exists := m.instances[name]
			if exists {
				m.stopInstance(name, state)
			}
			continue
		}

		state, exists := m.instances[name]

		// Tag filtering:
		// 1. If instance has no tags, it's "Global" and always runs.
		// 2. If instance has tags, it ONLY runs if the effective tags (local + server) has a match.
		if len(target.Tags) > 0 {
			match := false
			for _, t := range target.Tags {
				if isTagAllowed(t, activeTags) {
					match = true
					break
				}
			}
			if !match {
				log.Debugf("Instance %s skipped: tags %v not matched by effective tags %v", name, target.Tags, activeTags)
				if exists {
					m.stopInstance(name, state)
				}
				continue
			}
		}

		// 3. Global Enable/Disable switch
		if target.Enabled != nil && !*target.Enabled {
			if exists {
				m.stopInstance(name, state)
				// Clear blacklist state when manually disabled
				if state.FailedHash != "" || state.CrashCount > 0 {
					log.Infof("Instance %s is manually disabled. Clearing its blacklist state.", name)
					state.FailedHash = ""
					state.CrashCount = 0
				}
			}
			continue
		}

		if !exists {
			state = &InstanceState{}
			// Load persisted stable version from disk (survives Supervisor restarts)
			svPath := filepath.Join(m.instancePath, name, "stable_version.txt")
			if data, err := os.ReadFile(svPath); err == nil {
				state.LastGoodHash = strings.TrimSpace(string(data))
				if state.LastGoodHash != "" {
					log.Infof("Loaded stable version for %s from disk: %s", name, state.LastGoodHash)
				}
			}
			// Attempt to adopt an orphan process from a previous Supervisor run
			instanceDir := filepath.Join(m.instancePath, name)
			if pid, hash, err := readPIDFile(instanceDir); err == nil {
				if isProcessRunning(pid) {
					log.Infof("🐣 Adopted orphan instance %s (PID %d, version %s) from previous run.", name, pid, hash)
					state.AdoptedPID = pid
					state.CurrentHash = hash
				} else {
					log.Infof("Stale PID file for %s (PID %d) - process not running. Will restart.", name, pid)
					cleanPIDFile(instanceDir)
				}
			}
			// Run pre_stop_command to gracefully stop a legacy service before we take over
			if len(target.PreStopCommand) > 0 {
				log.Infof("Running pre_stop_command for %s: %v", name, target.PreStopCommand)
				var cmd *exec.Cmd

				processedCmds := make([]string, len(target.PreStopCommand))
				for i, c := range target.PreStopCommand {
					processedCmds[i] = m.replacePlaceholders(c, name, instanceDir, "", "")
				}

				// Directly execute, no shell wrapping to avoid shell injection risk
				cmd = exec.Command(processedCmds[0], processedCmds[1:]...)

				if out, err := cmd.CombinedOutput(); err != nil {
					log.Warnf("pre_stop_command for %s failed (continuing anyway): %v\n%s", name, err, string(out))
				} else {
					log.Infof("pre_stop_command for %s succeeded: %s", name, string(out))
				}
			}
			m.instances[name] = state
		}

		// 1. Resolve remote configuration asset if provided
		remoteConfigPath := ""
		if target.ConfigURL != "" {
			mgmtDir := filepath.Join(m.instancePath, name)
			configExt := filepath.Ext(target.ConfigURL)
			if configExt == "" {
				configExt = ".conf" // default
			}
			remoteConfigPath = filepath.Join(mgmtDir, "remote_config"+configExt)

			// Ensure instance directory exists before downloading config
			os.MkdirAll(mgmtDir, 0755)

			// "Only pull if missing" - preserve user modifications
			// Hash-based config verification (Fixed Sticky Bug)
			needDownload := false
			if _, err := os.Stat(remoteConfigPath); os.IsNotExist(err) {
				needDownload = true
			} else if !verifySHA256(remoteConfigPath, target.ConfigHash) {
				log.Infof("Config for %s hash mismatch. Redownloading version %s", name, target.ConfigHash)
				needDownload = true
			}

			if needDownload {
				log.Infof("Downloading config %s to %s", target.ConfigURL, remoteConfigPath)
				if err := downloadFile(remoteConfigPath, target.ConfigURL); err == nil {
					if verifySHA256(remoteConfigPath, target.ConfigHash) {
						state.CurrentConfigHash = target.ConfigHash
					} else {
						log.Warnf("Downloaded config for %s hash mismatch! Found %s, expected %s.", name, calculateSHA256(remoteConfigPath), target.ConfigHash)
					}
				} else {
					log.Errorf("Failed to download remote config for %s: %v", name, err)
				}
			} else {
				log.Debugf("Config for %s is up-to-date. Skipping download.", name)
			}
		}

		// Combine Server params and user config
		newConfigHash, finalArgs, finalEnv, effectiveInstanceDir := m.provisionInstance(name, target.Args, target.InstanceDir, target.Params, target.Env, remoteConfigPath)

		desiredHash := target.Hash
		isRollback := false

		// If config changed, give it another chance by clearing the blacklist
		if state.FailedHash != "" && state.RunConfigHash != "" && state.RunConfigHash != newConfigHash {
			log.Infof("Config for %s has changed. Clearing blacklist to retry version %s.", name, state.FailedHash)
			state.FailedHash = ""
			state.CrashCount = 0
		}

		// Permanent blacklist: once a hash fails 3 times, never try it again until server changes the hash
		if state.FailedHash == desiredHash {
			if state.LastGoodHash != "" && state.LastGoodHash != desiredHash {
				if state.CurrentHash != state.LastGoodHash {
					log.Warnf("🛡️ Instance %s target version (%s) is BLACKLISTED! Rolling back to known stable version: %s", name, desiredHash, state.LastGoodHash)
				}
				desiredHash = state.LastGoodHash
				isRollback = true
			} else {
				if state.CurrentHash != "" {
					log.Warnf("Instance %s target version is blacklisted and no rollback exists. Paused.", name)
				}
				continue
			}
		}

		// A process counts as running if we have a Cmd handle OR we adopted it from a PID file
		isRunning := (state.Cmd != nil) || (state.AdoptedPID > 0)
		if state.CurrentHash != desiredHash || (!isRollback && state.RunConfigHash != newConfigHash) || !isRunning {
			log.Infof("Instance %s requires update (Current: %s, Desired: %s)", name, state.CurrentHash, desiredHash)

			// Download binary if we don't have it or hash mismatch
			binFile := filepath.Join(m.binPath, fmt.Sprintf("%s-%s", name, desiredHash))
			if !verifySHA256(binFile, desiredHash) {
				if isRollback {
					log.Errorf("Rollback binary %s not found in cache! Cannot rollback.", binFile)
					continue
				}

				log.Infof("Hash mismatch or file missing. Downloading %s to %s", target.URL, binFile)
				if err := downloadFile(binFile, target.URL); err != nil {
					log.Errorf("Failed to download binary: %v", err)
					continue
				}

				// Security Verification: Ensure the downloaded file matches the expected Hash!
				actualCalcHash := calculateSHA256(binFile)
				if !strings.EqualFold(actualCalcHash, target.Hash) {
					log.Errorf("🔥 SECURITY ALERT: Downloaded file %s hash mismatch! Expected: %s, Actual: %s. Update aborted. Retrying in 10 minutes.", binFile, target.Hash, actualCalcHash)
					os.Remove(binFile) // Delete the tainted file

					// Register failure: blacklisted permanently until server changes the hash
					state.FailedHash = target.Hash
					continue // Wait for the next cycle
				}
			}

			// Reset failure state upon successful verification and fresh target
			if !isRollback {
				state.FailedHash = ""
			}

			os.Chmod(binFile, 0755)

			// Stop existing (handle both live Cmd and adopted orphan)
			if state.Cmd != nil && state.Cmd.Process != nil {
				log.Infof("Stopping old instance %s (cmd)...", name)
				state.Cmd.Process.Kill()
				state.Cmd.Wait()
				state.Cmd = nil
			}
			if state.AdoptedPID > 0 {
				log.Infof("Stopping adopted instance %s (PID %d)...", name, state.AdoptedPID)
				if proc, err := os.FindProcess(state.AdoptedPID); err == nil {
					proc.Kill()
				}
				state.AdoptedPID = 0
				cleanPIDFile(filepath.Join(m.instancePath, name))
			}

			// Copy binary into instance directory so the executable name remains constant
			instanceDir := filepath.Join(m.instancePath, name)
			os.MkdirAll(instanceDir, 0755)

			instanceBin := filepath.Join(instanceDir, name+exeSuffix)
			if err := copyLocalFile(binFile, instanceBin); err != nil {
				log.Errorf("Failed to copy binary to instance dir: %v", err)
				continue
			}
			os.Chmod(instanceBin, 0755)

			// Start new
			log.Infof("Starting new instance %s with version %s", name, desiredHash)

			cmd := exec.Command(instanceBin, finalArgs...)
			cmd.Dir = effectiveInstanceDir
			log.Infof("  Command: %s %v (WD: %s)", instanceBin, finalArgs, cmd.Dir)

			// Build subprocess environment: inherit parent env + per-instance overrides
			cmd.Env = buildEnv(finalEnv)

			// Redirect stdout/stderr to per-instance log file (with rotation)
			logWriter, logErr := m.openInstanceLog(instanceDir, name)
			if logErr != nil {
				log.Warnf("Failed to open log file for %s: %v, falling back to stdout", name, logErr)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
			} else {
				cmd.Stdout = logWriter
				cmd.Stderr = logWriter
			}

			if err := cmd.Start(); err != nil {
				log.Errorf("Failed to start instance %s: %v", name, err)
				// Count a start failure the same as a rapid crash
				state.CrashCount++
				if state.CrashCount >= 3 {
					log.Errorf("🚨🚨🚨 Instance %s failed to start %d times (version %s)! Blacklisting version.", name, state.CrashCount, desiredHash)
					state.FailedHash = desiredHash
				} else {
					log.Warnf("Instance %s failed to start. Retry %d/3...", name, state.CrashCount)
				}
				continue
			}

			state.Cmd = cmd
			state.AdoptedPID = 0 // Clear any adoption since we now own the process via Cmd
			state.CurrentHash = desiredHash
			if state.TargetHash != target.Hash {
				state.CrashCount = 0
			}
			state.TargetHash = target.Hash
			if !isRollback {
				state.RunConfigHash = newConfigHash
			}

			// Write PID file so we can adopt this process if Supervisor restarts
			writePIDFile(filepath.Join(m.instancePath, name), cmd.Process.Pid, desiredHash)

			// Background monitor
			go m.monitorWorker(name, cmd, desiredHash)

			// Validation timer: mark as stable after 15 seconds, and persist to disk
			go func(n string, c *exec.Cmd, h string) {
				time.Sleep(15 * time.Second)
				m.mu.Lock()
				defer m.mu.Unlock()
				s, ok := m.instances[n]
				if ok && s.Cmd == c && s.CurrentHash == h {
					log.Infof("✅ Instance %s (version %s) ran smoothly for 15s. Marked as stable.", n, h)
					s.LastGoodHash = h
					s.CrashCount = 0
					// Persist to disk so rollback works after Supervisor restarts
					svPath := filepath.Join(m.instancePath, n, "stable_version.txt")
					if err := os.WriteFile(svPath, []byte(h), 0644); err != nil {
						log.Warnf("Failed to persist stable version for %s: %v", n, err)
					} else {
						log.Infof("💾 Stable version %s persisted to disk for %s", h, n)
					}
				}
			}(name, cmd, desiredHash)
		}
	}

	// 2. Cleanup: Stop and remove instances that are no longer in the topology
	for name, state := range m.instances {
		if _, stillExists := topo.Instances[name]; !stillExists {
			log.Infof("Instance %s removed from topology. Stopping and cleaning up.", name)
			m.stopInstance(name, state)
			delete(m.instances, name)
		}
	}
}

// provisionInstance merges server Params/Env and local user_config.yaml, writes run_config.yaml,
// resolves args, and returns the config hash + final args + final env vars + effective instance directory.
// serverInstanceDir (if not empty) overrides the default config directory.
func (m *Manager) provisionInstance(name string, serverArgs []string, serverInstanceDir string, serverParams map[string]interface{}, serverEnv map[string]string, remoteConfigPath string) (string, []string, map[string]string, string) {
	// Management dir: always under supervisor's path (for pid, stable_version, binary)
	mgmtDir := filepath.Join(m.instancePath, name)
	os.MkdirAll(mgmtDir, 0755)

	// Config dir: use server's instance_dir if provided, else default to mgmtDir
	userConfigPath := filepath.Join(mgmtDir, "user_config.yaml")

	// Read user_config.yaml first (it may contain overrides for args, env, params, or instance_dir)
	userConfig := make(map[string]interface{})
	if b, err := os.ReadFile(userConfigPath); err == nil {
		yaml.Unmarshal(b, &userConfig)
	} else if os.IsNotExist(err) {
		// create a blank template for user convenience
		os.WriteFile(userConfigPath, []byte(
			"# Titan Supervisor User Config\n"+
				"# Use this file to override server-side settings locally.\n\n"+
				"# 1. Override the instance data directory (e.g., for reusing identity)\n"+
				"# instance_dir: \"/root/.titanedge\"\n\n"+
				"# 2. Local Environment Overrides (e.g., for specialized logging or secrets)\n"+
				"# These will override server-side environment variables.\n"+
				"env:\n"+
				"  # GOLOG_LOG_LEVEL: \"DEBUG\"\n"+
				"  # MY_LOCAL_VAR: \"something\"\n\n"+
				"# 3. Local Argument Overrides (Replace entire argument list)\n"+
				"# args: [\"daemon\", \"start\", \"--custom-flag\"]\n\n"+
				"# 4. Local Parameter Overrides (Substituted into args via {params.KEY})\n"+
				"params:\n"+
				"  # CODE: \"new-registration-code\"\n\n"+
				"# 5. Configuration Protection\n"+
				"# Set to true to prevent remote configuration assets from overwriting local changes.\n"+
				"# protect_config: true\n",
		), 0644)
	}

	// Resolve effective instance_dir: user_config.yaml > topology.json > default (mgmtDir)
	effectiveInstanceDir := mgmtDir // default
	if serverInstanceDir != "" {
		// Resolve placeholders in serverInstanceDir too
		effectiveInstanceDir = m.replacePlaceholders(serverInstanceDir, name, mgmtDir, "", "")
	}
	if userInstanceDirRaw, ok := userConfig["instance_dir"]; ok {
		if userInstanceDir, ok := userInstanceDirRaw.(string); ok && userInstanceDir != "" {
			// Resolve placeholders in user-provided instance_dir too
			effectiveInstanceDir = m.replacePlaceholders(userInstanceDir, name, mgmtDir, "", "")
		}
	}
	os.MkdirAll(effectiveInstanceDir, 0755)

	// 1. Resolve effective params: server defaults → user overrides
	finalParams := make(map[string]interface{})
	for k, v := range serverParams {
		finalParams[k] = v
	}
	if up, ok := userConfig["params"].(map[string]interface{}); ok {
		for k, v := range up {
			finalParams[k] = v
		}
	}

	// 2. Generate run_config.yaml (final merged configuration for the worker)
	runConfigPath := filepath.Join(effectiveInstanceDir, "run_config.yaml")
	if configBytes, err := yaml.Marshal(finalParams); err == nil {
		os.WriteFile(runConfigPath, configBytes, 0644)
	}

	// 2. Resolve args with parameter substitution
	effectiveArgs := serverArgs
	if userArgsRaw, ok := userConfig["args"]; ok {
		if userArgsList, ok := userArgsRaw.([]interface{}); ok {
			effectiveArgs = make([]string, len(userArgsList))
			for i, a := range userArgsList {
				effectiveArgs[i] = fmt.Sprintf("%v", a)
			}
		}
	}

	// 3. Resolve environment variables with parameter substitution
	finalEnv := make(map[string]string)
	// Server defaults
	for k, v := range serverEnv {
		finalEnv[k] = v
	}
	// User overrides
	if ue, ok := userConfig["env"].(map[string]interface{}); ok {
		for k, v := range ue {
			finalEnv[k] = fmt.Sprintf("%v", v)
		}
	}

	// 4. Apply placeholders ({params.KEY}, {instance_dir}, {config}, {remote_config}) to BOTH args and env values
	finalArgs := make([]string, len(effectiveArgs))
	copy(finalArgs, effectiveArgs)
	for i, arg := range finalArgs {
		// First handle parameters
		arg = m.substituteParams(arg, finalParams)
		finalArgs[i] = m.replacePlaceholders(arg, name, effectiveInstanceDir, runConfigPath, remoteConfigPath)
	}
	for k, v := range finalEnv {
		v = m.substituteParams(v, finalParams)
		finalEnv[k] = m.replacePlaceholders(v, name, effectiveInstanceDir, runConfigPath, remoteConfigPath)
	}

	// 4. Calculate configuration hash (for change detection)
	configJSON, _ := json.Marshal(finalParams)
	argsJSON, _ := json.Marshal(finalArgs)
	envJSON, _ := json.Marshal(finalEnv)
	combined := append(configJSON, argsJSON...)
	combined = append(combined, envJSON...)
	h := sha256.Sum256(combined)

	return hex.EncodeToString(h[:]), finalArgs, finalEnv, effectiveInstanceDir
}

func (m *Manager) substituteParams(input string, params map[string]interface{}) string {
	output := input
	for k, v := range params {
		placeholder := fmt.Sprintf("{params.%s}", k)
		output = strings.ReplaceAll(output, placeholder, fmt.Sprintf("%v", v))
	}
	return output
}

func (m *Manager) replacePlaceholders(input string, name string, instanceDir string, runConfigPath string, remoteConfigPath string) string {
	res := input
	res = strings.ReplaceAll(res, "{name}", name)
	res = strings.ReplaceAll(res, "{repo}", m.repoPath)
	res = strings.ReplaceAll(res, "{instance_dir}", instanceDir)

	// Special placeholder for Home directory
	home, _ := os.UserHomeDir()
	if home != "" {
		res = strings.ReplaceAll(res, "{home}", home)
	}

	// Replace {config} with the path of the downloaded configuration file
	res = strings.ReplaceAll(res, "{config}", runConfigPath)
	res = strings.ReplaceAll(res, "{remote_config}", remoteConfigPath)

	// Replace platform-specific separators and handle /root/ alias
	if runtime.GOOS == "windows" {
		lower := strings.ToLower(res)
		if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
			// It's a URL. We must NOT use FromSlash.
			// Just ensure it uses forward slashes throughout.
			return strings.ReplaceAll(res, "\\", "/")
		}

		if strings.HasPrefix(res, "/root/") && home != "" {
			res = filepath.Join(home, strings.TrimPrefix(res, "/root/"))
		} else if strings.HasPrefix(res, "/") && !strings.Contains(res, ":") {
			// Best effort for other / paths: make relative to home
			res = filepath.Join(home, res)
		}
		res = filepath.FromSlash(res)
	} else {
		res = filepath.FromSlash(res)
	}

	return res
}

func (m *Manager) monitorWorker(name string, cmd *exec.Cmd, runningHash string) {
	startTime := time.Now()
	err := cmd.Wait()
	runDuration := time.Since(startTime)

	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.instances[name]
	if !exists {
		return
	}

	if state.Cmd == cmd {
		// Only log as warning if the process exited with an error
		if err != nil {
			log.Warnf("Instance %s exited with error: %v (ran for %v)", name, err, runDuration)
		} else {
			log.Infof("Instance %s exited cleanly (ran for %v)", name, runDuration)
		}
		state.Cmd = nil
		state.CurrentHash = "" // Force restart on next poll if it's supposed to be running
		cleanPIDFile(filepath.Join(m.instancePath, name))

		if runDuration < 15*time.Second {
			state.CrashCount++
			if state.CrashCount >= 3 {
				log.Errorf("🚨🚨🚨 Instance %s crashed %d times rapidly (version %s)! Blacklisting version.", name, state.CrashCount, runningHash)
				state.FailedHash = runningHash
			} else {
				log.Warnf("Instance %s crashed. Retry %d/3...", name, state.CrashCount)
			}
		} else {
			// Ran long enough: not a crash loop
			state.CrashCount = 0
		}
	}
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, state := range m.instances {
		m.stopInstance(name, state)
	}
}

func (m *Manager) stopInstance(name string, state *InstanceState) {
	instanceDir := filepath.Join(m.instancePath, name)
	if state.Cmd != nil && state.Cmd.Process != nil {
		log.Infof("Killing instance %s", name)
		state.Cmd.Process.Kill()
		state.Cmd.Wait() // Reap the process to avoid zombies
		cleanPIDFile(instanceDir)
	}
	if state.AdoptedPID > 0 {
		log.Infof("Killing adopted instance %s (PID %d)", name, state.AdoptedPID)
		if proc, err := os.FindProcess(state.AdoptedPID); err == nil {
			proc.Kill()
		}
		cleanPIDFile(instanceDir)
	}
}

// Helper to download a file
func downloadFile(filepath string, url string) error {
	client := &http.Client{
		Timeout: 10 * time.Minute,
	}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func calculateSHA256(filepath string) string {
	f, err := os.Open(filepath)
	if err != nil {
		return ""
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}

	return hex.EncodeToString(h.Sum(nil))
}

func verifySHA256(filepath string, expectedHash string) bool {
	return calculateSHA256(filepath) == expectedHash
}

func copyLocalFile(src, dst string) error {
	var err error
	for i := 0; i < 3; i++ {
		err = func() error {
			in, err := os.Open(src)
			if err != nil {
				return err
			}
			defer in.Close()

			out, err := os.Create(dst)
			if err != nil {
				return err
			}
			defer out.Close()

			_, err = io.Copy(out, in)
			return err
		}()

		if err == nil {
			return nil
		}

		// On Windows, process termination can leave the file locked for a split second.
		log.Warnf("Failed to copy file to %s (attempt %d/3): %v. Retrying in 500ms...", dst, i+1, err)
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

// ---- PID File Helpers ----

// writePIDFile writes "pid\nhash\n" to {instanceDir}/pid
func writePIDFile(instanceDir string, pid int, hash string) {
	pidPath := filepath.Join(instanceDir, "pid")
	content := fmt.Sprintf("%d\n%s\n", pid, hash)
	if err := os.WriteFile(pidPath, []byte(content), 0644); err != nil {
		log.Warnf("Failed to write PID file %s: %v", pidPath, err)
	}
}

// readPIDFile reads {instanceDir}/pid and returns (pid, hash, error)
// resolveTargetPlatform selects the correct binary and config version for the current platform.
// It returns true if a compatible platform was found and applied, or false if it should be skipped.
func (m *Manager) resolveTargetPlatform(target *InstanceTarget) bool {
	if len(target.Platforms) == 0 {
		// If no platforms defined and no global fallback, we consider it incompatible
		// if the user's requirement is to remove defaults.
		return target.Hash != "" && target.URL != ""
	}

	goos := runtime.GOOS
	goarch := runtime.GOARCH

	// Priority 1: Manual platform override from command line
	if m.platformOverride != "" {
		pKey := m.platformOverride
		if !strings.Contains(pKey, "/") {
			pKey = m.platformOverride + "/" + goarch
		}
		if p, ok := target.Platforms[pKey]; ok {
			m.applyPlatformConfig(target, p)
			return true
		}
		if p, ok := target.Platforms[m.platformOverride]; ok {
			m.applyPlatformConfig(target, p)
			return true
		}
		log.Warnf("Platform override '%s' specified but no matching config found in topology for %s", m.platformOverride, target.URL)
		return false
	}

	// Priority 2: Auto-detect Full Match (os/arch)
	if p, ok := target.Platforms[goos+"/"+goarch]; ok {
		m.applyPlatformConfig(target, p)
		return true
	}

	// Priority 3: OS Match (e.g. "windows", "linux")
	if p, ok := target.Platforms[goos]; ok {
		m.applyPlatformConfig(target, p)
		return true
	}

	return false
}

func (m *Manager) applyPlatformConfig(target *InstanceTarget, p PlatformConfig) {
	target.Hash = p.Hash
	target.URL = p.URL
	if p.ConfigHash != "" {
		target.ConfigHash = p.ConfigHash
	}
	if p.ConfigURL != "" {
		target.ConfigURL = p.ConfigURL
	}
}

// readPIDFile reads {instanceDir}/pid and returns (pid, hash, error)
func readPIDFile(instanceDir string) (int, string, error) {
	pidPath := filepath.Join(instanceDir, "pid")
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, "", err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 2 {
		return 0, "", fmt.Errorf("invalid PID file format")
	}
	var pid int
	if _, err := fmt.Sscanf(lines[0], "%d", &pid); err != nil {
		return 0, "", fmt.Errorf("invalid PID in file: %v", err)
	}
	return pid, strings.TrimSpace(lines[1]), nil
}

// cleanPIDFile removes {instanceDir}/pid
func cleanPIDFile(instanceDir string) {
	os.Remove(filepath.Join(instanceDir, "pid"))
}

// isProcessRunning checks if a process with the given PID is alive (cross-platform).
func isProcessRunning(pid int) bool {
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH", "/FO", "CSV").Output()
		if err != nil {
			return false
		}
		return strings.Contains(string(out), strconv.Itoa(pid))
	}

	// Unix-like (Linux, Darwin)
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

func (m *Manager) openInstanceLog(instanceDir, name string) (io.Writer, error) {
	logPath := filepath.Join(instanceDir, "instance.log")

	// Use rotatelogs for automatic management
	rl, err := rotatelogs.New(
		logPath+"-%Y%m%d-%H%M%S.log",
		rotatelogs.WithLinkName(logPath),
		rotatelogs.WithRotationTime(m.logRotationTime),
		rotatelogs.WithMaxAge(m.logMaxAge),
		rotatelogs.WithRotationSize(m.logRotationSize),
	)
	if err != nil {
		return nil, err
	}

	log.Infof("Instance %s logging initialized: path=%s, maxAge=%v, rotationTime=%v, maxSize=%v MiB",
		name, logPath, m.logMaxAge, m.logRotationTime, m.logRotationSize/(1024*1024))
	return rl, nil
}

// buildEnv returns os.Environ() merged with the provided extra key=value pairs.
// Extra values override inherited ones if the key collides.
func buildEnv(extra map[string]string) []string {
	env := os.Environ()

	if runtime.GOOS == "windows" {
		pathIdx := -1
		var pathVal string
		for i, e := range env {
			if strings.HasPrefix(strings.ToUpper(e), "PATH=") {
				pathIdx = i
				pathVal = strings.TrimPrefix(e[len("PATH="):], "=")
				break
			}
		}

		// Simple sanitization:
		// 1. Convert /c/ to C:\
		// 2. Prepend System32 to be 100% sure
		winPaths := []string{
			`C:\Windows\System32`,
			`C:\Windows`,
			`C:\Windows\System32\Wbem`,
		}

		var finalParts []string
		if pathIdx != -1 {
			// Try to handle both ; and : (Git Bash uses ; in os.Environ() usually, but be safe)
			sep := ";"
			if !strings.Contains(pathVal, ";") && strings.Contains(pathVal, ":") && strings.Count(pathVal, ":") > 1 {
				sep = ":"
			}
			parts := strings.Split(pathVal, sep)
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				// Minimal conversion
				if len(p) >= 3 && p[0] == '/' && p[2] == '/' {
					p = string(p[1]) + ":" + p[2:]
				}
				finalParts = append(finalParts, filepath.FromSlash(p))
			}
		}

		// Prepend winPaths (avoid duplicates)
		var resultParts []string
		for _, wp := range winPaths {
			resultParts = append(resultParts, wp)
		}
		for _, p := range finalParts {
			isWinPath := false
			for _, wp := range winPaths {
				if strings.EqualFold(p, wp) {
					isWinPath = true
					break
				}
			}
			if !isWinPath {
				resultParts = append(resultParts, p)
			}
		}

		newPath := "PATH=" + strings.Join(resultParts, ";")
		if pathIdx != -1 {
			env[pathIdx] = newPath
		} else {
			env = append(env, newPath)
		}

		// Ensure SystemRoot, TEMP, TMP exist (some binaries fail without them)
		essentialVars := []string{"SystemRoot", "TEMP", "TMP", "APPDATA", "LOCALAPPDATA"}
		for _, v := range essentialVars {
			found := false
			for _, e := range env {
				if strings.HasPrefix(strings.ToUpper(e), strings.ToUpper(v)+"=") {
					found = true
					break
				}
			}
			if !found {
				if val := os.Getenv(v); val != "" {
					env = append(env, v+"="+val)
				}
			}
		}
	}

	if len(extra) == 0 {
		return env
	}
	for k, v := range extra {
		found := false
		newEnvVar := fmt.Sprintf("%s=%s", k, v)
		for i, e := range env {
			if strings.HasPrefix(strings.ToUpper(e), strings.ToUpper(k)+"=") {
				env[i] = newEnvVar
				found = true
				break
			}
		}
		if !found {
			env = append(env, newEnvVar)
		}
	}
	return env
}
func isTagAllowed(tag string, allowed []string) bool {
	for _, a := range allowed {
		if a == tag {
			return true
		}
	}
	return false
}

// stringSlicesEqual returns true if two string slices have the same elements in the same order.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
