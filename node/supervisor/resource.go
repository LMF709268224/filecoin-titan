package supervisor

import (
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"
)

// NodeResourceReport holds both static and dynamic resource metrics.
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

var prevNet struct {
	rxBytes   uint64
	txBytes   uint64
	sampledAt time.Time
}

// CollectResourceReport gathers current system resource metrics.
func CollectResourceReport() *NodeResourceReport {
	_ = runtime.GOOS // Just to ensure runtime import is used

	cores, _ := cpu.Counts(true)

	// Memory
	totalMem, usedMem := uint64(0), uint64(0)
	if vm, err := mem.VirtualMemory(); err == nil {
		totalMem = vm.Total
		usedMem = vm.Used
	}

	// Disk (root partition)
	totalDisk, usedDisk := uint64(0), uint64(0)
	if du, err := disk.Usage("/"); err == nil {
		totalDisk = du.Total
		usedDisk = du.Used
	}

	// CPU: measure over 500ms for accuracy
	percents, err := cpu.Percent(500*time.Millisecond, false)
	cpuPct := 0.0
	if err == nil && len(percents) > 0 {
		cpuPct = percents[0]
	}

	// Bandwidth: delta since last sample
	rxPerSec, txPerSec := uint64(0), uint64(0)
	if counters, err := psnet.IOCounters(false); err == nil && len(counters) > 0 {
		now := time.Now()
		rx := counters[0].BytesRecv
		tx := counters[0].BytesSent
		if !prevNet.sampledAt.IsZero() {
			elapsed := now.Sub(prevNet.sampledAt).Seconds()
			if elapsed > 0 && rx >= prevNet.rxBytes && tx >= prevNet.txBytes {
				rxPerSec = uint64(float64(rx-prevNet.rxBytes) / elapsed)
				txPerSec = uint64(float64(tx-prevNet.txBytes) / elapsed)
			}
		}
		prevNet.rxBytes = rx
		prevNet.txBytes = tx
		prevNet.sampledAt = now
	}

	return &NodeResourceReport{
		CPUCores:    cores,
		TotalMemory: totalMem,
		TotalDisk:   totalDisk,
		CPUPercent:  cpuPct,
		UsedMemory:  usedMem,
		UsedDisk:    usedDisk,
		NetRxBytes:  rxPerSec,
		NetTxBytes:  txPerSec,
	}
}
