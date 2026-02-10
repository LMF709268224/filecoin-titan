package scheduler

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	logging "github.com/ipfs/go-log/v2"
)

var monitorLog = logging.Logger("scheduler")

// GoroutineMonitor monitors goroutine count and distribution
type GoroutineMonitor struct {
	nodeManager   *node.Manager
	lastCount     int
	checkInterval time.Duration
}

// NewGoroutineMonitor creates a new goroutine monitor
func NewGoroutineMonitor(nm *node.Manager) *GoroutineMonitor {
	return &GoroutineMonitor{
		nodeManager:   nm,
		lastCount:     0,
		checkInterval: 5 * time.Minute,
	}
}

// Start begins monitoring goroutines
func (m *GoroutineMonitor) Start(ctx context.Context) {
	monitorLog.Info("Starting goroutine monitor")

	// Initial report
	m.reportGoroutines()

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			monitorLog.Info("Stopping goroutine monitor")
			return
		case <-ticker.C:
			m.reportGoroutines()
		}
	}
}

// reportGoroutines reports current goroutine statistics
func (m *GoroutineMonitor) reportGoroutines() {
	numGoroutines := runtime.NumGoroutine()
	nodeCount := m.nodeManager.GetOnlineNodeCount(types.NodeUnknown)

	if nodeCount == 0 {
		nodeCount = 1 // Avoid division by zero
	}

	avgPerNode := float64(numGoroutines) / float64(nodeCount)
	growth := numGoroutines - m.lastCount

	monitorLog.Infof("[GoroutineMonitor] === Goroutine Statistics ===")
	monitorLog.Infof("[GoroutineMonitor] Total goroutines: %d (change: %+d)", numGoroutines, growth)
	monitorLog.Infof("[GoroutineMonitor] Online nodes: %d", nodeCount)
	monitorLog.Infof("[GoroutineMonitor] Avg goroutines/node: %.2f", avgPerNode)

	// Check for abnormal conditions
	if numGoroutines > 15000 {
		monitorLog.Warnf("[GoroutineMonitor] High goroutine count detected: %d", numGoroutines)
		m.analyzeGoroutines()
	}

	// Critical threshold - possible leak
	if numGoroutines > 25000 {
		monitorLog.Errorf("CRITICAL: Possible goroutine leak! Count: %d", numGoroutines)
		m.dumpGoroutineProfile()
	}

	// Check for rapid growth
	if m.lastCount > 0 && growth > 1000 {
		monitorLog.Warnf("Rapid goroutine growth detected: +%d in %v", growth, m.checkInterval)
		m.analyzeGoroutines()
	}

	m.lastCount = numGoroutines
}

// analyzeGoroutines analyzes goroutine distribution
func (m *GoroutineMonitor) analyzeGoroutines() {
	monitorLog.Info("Analyzing goroutine distribution...")

	// Get stack dump
	buf := make([]byte, 16<<20) // 16MB buffer
	n := runtime.Stack(buf, true)
	stackDump := string(buf[:n])

	// Parse and count goroutine sources
	stats := make(map[string]int)
	lines := strings.Split(stackDump, "\n")

	for i := 0; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "goroutine ") {
			// Look for the first function call in the stack
			for j := i + 1; j < len(lines) && j < i+10; j++ {
				line := strings.TrimSpace(lines[j])
				if line == "" {
					break
				}
				// Extract function name
				if strings.Contains(line, "(") {
					funcName := extractFunctionName(line)
					if funcName != "" {
						stats[funcName]++
						break
					}
				}
			}
		}
	}

	// Sort and display top sources
	type kv struct {
		Key   string
		Value int
	}
	var sorted []kv
	for k, v := range stats {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Value > sorted[j].Value
	})

	monitorLog.Infof("Top 15 goroutine sources:")
	for i := 0; i < 15 && i < len(sorted); i++ {
		percentage := float64(sorted[i].Value) / float64(m.lastCount) * 100
		monitorLog.Infof("  #%d: %s (%d goroutines, %.1f%%)",
			i+1, sorted[i].Key, sorted[i].Value, percentage)
	}
}

// extractFunctionName extracts function name from stack trace line
func extractFunctionName(line string) string {
	// Example: "github.com/Filecoin-Titan/titan/node/scheduler.(*Scheduler).Start(0x...)"
	// We want: "scheduler.(*Scheduler).Start"

	line = strings.TrimSpace(line)

	// Find the function part (before the address)
	if idx := strings.Index(line, "(0x"); idx > 0 {
		funcPart := line[:idx]

		// Simplify the package path
		if lastSlash := strings.LastIndex(funcPart, "/"); lastSlash > 0 {
			funcPart = funcPart[lastSlash+1:]
		}

		return funcPart
	}

	return ""
}

// dumpGoroutineProfile saves goroutine profile to file
func (m *GoroutineMonitor) dumpGoroutineProfile() {
	filename := fmt.Sprintf("goroutine_dump_%d.txt", time.Now().Unix())

	f, err := os.Create(filename)
	if err != nil {
		monitorLog.Errorf("Failed to create goroutine dump file: %v", err)
		return
	}
	defer f.Close()

	// Write header
	fmt.Fprintf(f, "Goroutine Dump - %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "Total goroutines: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(f, "Online nodes: %d\n\n", m.nodeManager.GetOnlineNodeCount(types.NodeUnknown))
	fmt.Fprintf(f, "===========================================\n\n")

	// Write full stack dump
	buf := make([]byte, 16<<20) // 16MB
	n := runtime.Stack(buf, true)
	f.Write(buf[:n])

	monitorLog.Infof("Goroutine profile dumped to: %s", filename)

	// Also write pprof format
	pprofFile := fmt.Sprintf("goroutine_pprof_%d.prof", time.Now().Unix())
	pf, err := os.Create(pprofFile)
	if err == nil {
		defer pf.Close()
		if profile := pprof.Lookup("goroutine"); profile != nil {
			profile.WriteTo(pf, 0)
			monitorLog.Infof("Goroutine pprof saved to: %s", pprofFile)
		}
	}
}

// GetCurrentStats returns current goroutine statistics
func (m *GoroutineMonitor) GetCurrentStats() map[string]interface{} {
	numGoroutines := runtime.NumGoroutine()
	nodeCount := m.nodeManager.GetOnlineNodeCount(types.NodeUnknown)

	if nodeCount == 0 {
		nodeCount = 1
	}

	return map[string]interface{}{
		"total_goroutines": numGoroutines,
		"online_nodes":     nodeCount,
		"avg_per_node":     float64(numGoroutines) / float64(nodeCount),
		"last_count":       m.lastCount,
		"growth":           numGoroutines - m.lastCount,
	}
}
