package supervisor

import (
	"crypto/md5"
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
	"time"

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

// Topology represents the server's desired state
type Topology struct {
	Instances map[string]InstanceTarget `json:"instances"`
}

type InstanceTarget struct {
	Hash        string                 `json:"hash"`
	URL         string                 `json:"url"`          // Download URL for this binary version
	ConfigURL   string                 `json:"config_url"`   // Optional: Download URL for configuration asset
	ConfigHash  string                 `json:"config_hash"`  // Optional: Hash for configuration asset
	Args        []string               `json:"args"`         // Command-line args template
	InstanceDir string                 `json:"instance_dir"` // Optional: custom dir for run_config.yaml and user_config.yaml
	Env         map[string]string      `json:"env"`          // Optional: extra environment variables
	Params      map[string]interface{} `json:"params"`
	Tags        []string               `json:"tags"`    // Optional: tags for selective running
	Enabled     *bool                  `json:"enabled"` // Optional: global ON/OFF switch
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
	AdoptedPID   int // PID of a process adopted from a previous Supervisor run
}

type Manager struct {
	repoPath     string
	binPath      string
	instancePath string
	serverUrl    string
	allowedTags  []string // Tags this node is allowed to run

	mu        sync.Mutex
	instances map[string]*InstanceState

	// Log Rotation Settings
	logMaxAge       time.Duration
	logRotationTime time.Duration
	logRotationSize int64
}

func NewManager(repoPath string, serverUrl string, allowedTags []string, logMaxAge, logRotationTime time.Duration, logRotationSize int64) *Manager {
	return &Manager{
		repoPath:        repoPath,
		binPath:         filepath.Join(repoPath, "workers", "bin"),
		instancePath:    filepath.Join(repoPath, "workers", "instances"),
		serverUrl:       serverUrl,
		allowedTags:     allowedTags,
		instances:       make(map[string]*InstanceState),
		logMaxAge:       logMaxAge,
		logRotationTime: logRotationTime,
		logRotationSize: logRotationSize,
	}
}

func (m *Manager) InitDirs() error {
	if err := os.MkdirAll(m.binPath, 0755); err != nil {
		return err
	}
	return os.MkdirAll(m.instancePath, 0755)
}

func (m *Manager) PollServer() {
	// Use a client with timeout to prevent hanging indefinitely
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(m.serverUrl)
	if err != nil {
		log.Warnf("Failed to poll server: %v", err)
		return
	}
	defer resp.Body.Close()

	var topo Topology
	if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
		log.Warnf("Failed to parse topology: %v", err)
		return
	}

	m.applyTopology(topo)
}

func (m *Manager) applyTopology(topo Topology) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Start or Update Instances
	for name, target := range topo.Instances {
		state, exists := m.instances[name]

		// Tag filtering:
		// 1. If instance has no tags, it's "Global" and always runs.
		// 2. If instance has tags, it ONLY runs if the Supervisor has a matching tag.
		if len(target.Tags) > 0 {
			match := false
			for _, t := range target.Tags {
				if isTagAllowed(t, m.allowedTags) {
					match = true
					break
				}
			}
			if !match {
				log.Debugf("Instance %s skipped: tags %v not matched by node tags %v", name, target.Tags, m.allowedTags)
				if exists {
					m.stopInstance(name, state)
				}
				continue
			}
		}

		// 3. Global Enable/Disable switch
		if target.Enabled != nil && !*target.Enabled {
			log.Infof("Instance %s is DISABLED in topology. Stopping if running.", name)
			if exists {
				m.stopInstance(name, state)
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
			if _, err := os.Stat(remoteConfigPath); os.IsNotExist(err) {
				log.Infof("Config missing for %s. Downloading %s", name, target.ConfigURL)
				if err := downloadFile(remoteConfigPath, target.ConfigURL); err == nil {
					if verifyMD5(remoteConfigPath, target.ConfigHash) {
						state.CurrentConfigHash = target.ConfigHash
					} else {
						log.Warnf("Downloaded config for %s hash mismatch! Using it anyway but marking as unstable.", name)
					}
				} else {
					log.Errorf("Failed to download remote config for %s: %v", name, err)
				}
			} else {
				log.Debugf("Config for %s already exists locally. Skipping download to preserve local changes.", name)
			}
		}

		// Combine Server params and user config
		newConfigHash, finalArgs, finalEnv, effectiveInstanceDir := m.provisionInstance(name, target.Args, target.InstanceDir, target.Params, target.Env, remoteConfigPath)

		desiredHash := target.Hash
		isRollback := false

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

		// If version changed OR config changed, we need to restart
		// A process counts as running if we have a Cmd handle OR we adopted it from a PID file
		isRunning := (state.Cmd != nil) || (state.AdoptedPID > 0)
		if state.CurrentHash != desiredHash || (!isRollback && state.RunConfigHash != newConfigHash) || !isRunning {
			log.Infof("Instance %s requires update (Current: %s, Desired: %s)", name, state.CurrentHash, desiredHash)

			// Download binary if we don't have it or hash mismatch
			binFile := filepath.Join(m.binPath, fmt.Sprintf("%s-%s", name, desiredHash))
			if !verifyMD5(binFile, desiredHash) {
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
				if !verifyMD5(binFile, target.Hash) {
					log.Errorf("🔥 SECURITY ALERT: Downloaded file %s does not match expected hash %s! Update aborted. Retrying in 10 minutes.", binFile, target.Hash)
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
		effectiveInstanceDir = serverInstanceDir
	}
	if userInstanceDirRaw, ok := userConfig["instance_dir"]; ok {
		if userInstanceDir, ok := userInstanceDirRaw.(string); ok && userInstanceDir != "" {
			effectiveInstanceDir = userInstanceDir
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
		finalArgs[i] = m.substitute(arg, finalParams, effectiveInstanceDir, runConfigPath, remoteConfigPath)
	}
	for k, v := range finalEnv {
		finalEnv[k] = m.substitute(v, finalParams, effectiveInstanceDir, runConfigPath, remoteConfigPath)
	}

	// 4. Calculate configuration hash (for change detection)
	configJSON, _ := json.Marshal(finalParams)
	argsJSON, _ := json.Marshal(finalArgs)
	envJSON, _ := json.Marshal(finalEnv)
	combined := append(configJSON, argsJSON...)
	combined = append(combined, envJSON...)
	h := md5.Sum(combined)

	return hex.EncodeToString(h[:]), finalArgs, finalEnv, effectiveInstanceDir
}

func (m *Manager) substitute(input string, params map[string]interface{}, instanceDir string, configPath string, remoteConfigPath string) string {
	output := input
	// 1. Replace all {params.KEY} placeholders.
	for k, v := range params {
		placeholder := fmt.Sprintf("{params.%s}", k)
		output = strings.ReplaceAll(output, placeholder, fmt.Sprintf("%v", v))
	}
	// 2. Resolve {instance_dir}
	output = strings.ReplaceAll(output, "{instance_dir}", instanceDir)
	// 3. Resolve {config} (path to run_config.yaml)
	output = strings.ReplaceAll(output, "{config}", configPath)
	// 4. Resolve {remote_config} (path to downloaded configuration asset)
	output = strings.ReplaceAll(output, "{remote_config}", remoteConfigPath)
	return output
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

func downloadFile(filepath string, url string) error {
	resp, err := http.Get(url)
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

func verifyMD5(filepath string, expectedHash string) bool {
	f, err := os.Open(filepath)
	if err != nil {
		return false
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return false
	}

	calculatedHash := hex.EncodeToString(h.Sum(nil))
	return calculatedHash == expectedHash
}

func copyLocalFile(src, dst string) error {
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
	// Linux/macOS: check /proc/{pid} existence
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
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
	if len(extra) == 0 {
		return env
	}
	// Build override set
	for k, v := range extra {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
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
