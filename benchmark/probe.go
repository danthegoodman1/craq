package benchmark

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type ProbeConfig struct {
	OutputPath string        `json:"output_path"`
	Interval   time.Duration `json:"interval"`
	Duration   time.Duration `json:"duration"`
}

type ProbeSample struct {
	Timestamp time.Time             `json:"timestamp"`
	CPU       CPUCounters           `json:"cpu"`
	Memory    MemorySample          `json:"memory"`
	Network   map[string]NetSample  `json:"network"`
	Disks     map[string]DiskSample `json:"disks"`
}

type CPUCounters struct {
	User    uint64 `json:"user"`
	Nice    uint64 `json:"nice"`
	System  uint64 `json:"system"`
	Idle    uint64 `json:"idle"`
	IOWait  uint64 `json:"iowait"`
	IRQ     uint64 `json:"irq"`
	SoftIRQ uint64 `json:"softirq"`
	Steal   uint64 `json:"steal"`
}

type MemorySample struct {
	TotalBytes     uint64 `json:"total_bytes"`
	FreeBytes      uint64 `json:"free_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
	BuffersBytes   uint64 `json:"buffers_bytes"`
	CachedBytes    uint64 `json:"cached_bytes"`
}

type NetSample struct {
	RXBytes uint64 `json:"rx_bytes"`
	TXBytes uint64 `json:"tx_bytes"`
}

type DiskSample struct {
	ReadsCompleted  uint64 `json:"reads_completed"`
	WritesCompleted uint64 `json:"writes_completed"`
	ReadSectors     uint64 `json:"read_sectors"`
	WriteSectors    uint64 `json:"write_sectors"`
}

func RunProbe(ctx context.Context, cfg ProbeConfig) error {
	if cfg.OutputPath == "" {
		return fmt.Errorf("probe output path must not be empty")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	file, err := os.Create(cfg.OutputPath)
	if err != nil {
		return fmt.Errorf("create probe output: %w", err)
	}
	defer file.Close()

	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	if err := writeProbeSample(file); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := writeProbeSample(file); err != nil {
				return err
			}
		}
	}
}

func writeProbeSample(file *os.File) error {
	sample, err := CollectProbeSample()
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(sample)
	if err != nil {
		return fmt.Errorf("marshal probe sample: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write probe sample: %w", err)
	}
	return nil
}

func CollectProbeSample() (ProbeSample, error) {
	cpu, err := readCPUStat()
	if err != nil {
		return ProbeSample{}, err
	}
	mem, err := readMemInfo()
	if err != nil {
		return ProbeSample{}, err
	}
	network, err := readNetDev()
	if err != nil {
		return ProbeSample{}, err
	}
	disks, err := readDiskStats()
	if err != nil {
		return ProbeSample{}, err
	}
	return ProbeSample{
		Timestamp: time.Now().UTC(),
		CPU:       cpu,
		Memory:    mem,
		Network:   network,
		Disks:     disks,
	}, nil
}

func readCPUStat() (CPUCounters, error) {
	line, err := firstLine("/proc/stat")
	if err != nil {
		return CPUCounters{}, fmt.Errorf("read /proc/stat: %w", err)
	}
	fields := strings.Fields(line)
	if len(fields) < 8 || fields[0] != "cpu" {
		return CPUCounters{}, fmt.Errorf("unexpected /proc/stat format")
	}
	values := make([]uint64, 8)
	for i := range values {
		values[i], err = strconv.ParseUint(fields[i+1], 10, 64)
		if err != nil {
			return CPUCounters{}, err
		}
	}
	return CPUCounters{
		User:    values[0],
		Nice:    values[1],
		System:  values[2],
		Idle:    values[3],
		IOWait:  values[4],
		IRQ:     values[5],
		SoftIRQ: values[6],
		Steal:   values[7],
	}, nil
}

func readMemInfo() (MemorySample, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return MemorySample{}, fmt.Errorf("open /proc/meminfo: %w", err)
	}
	defer file.Close()
	out := MemorySample{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			out.TotalBytes = value * 1024
		case "MemFree:":
			out.FreeBytes = value * 1024
		case "MemAvailable:":
			out.AvailableBytes = value * 1024
		case "Buffers:":
			out.BuffersBytes = value * 1024
		case "Cached:":
			out.CachedBytes = value * 1024
		}
	}
	if err := scanner.Err(); err != nil {
		return MemorySample{}, fmt.Errorf("scan /proc/meminfo: %w", err)
	}
	return out, nil
}

func readNetDev() (map[string]NetSample, error) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return nil, fmt.Errorf("open /proc/net/dev: %w", err)
	}
	defer file.Close()
	out := map[string]NetSample{}
	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if lineNum <= 2 {
			continue
		}
		parts := strings.Split(scanner.Text(), ":")
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		rx, _ := strconv.ParseUint(fields[0], 10, 64)
		tx, _ := strconv.ParseUint(fields[8], 10, 64)
		out[iface] = NetSample{RXBytes: rx, TXBytes: tx}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan /proc/net/dev: %w", err)
	}
	return out, nil
}

func readDiskStats() (map[string]DiskSample, error) {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil, fmt.Errorf("open /proc/diskstats: %w", err)
	}
	defer file.Close()
	out := map[string]DiskSample{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 14 {
			continue
		}
		name := fields[2]
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "ram") {
			continue
		}
		reads, _ := strconv.ParseUint(fields[3], 10, 64)
		readSectors, _ := strconv.ParseUint(fields[5], 10, 64)
		writes, _ := strconv.ParseUint(fields[7], 10, 64)
		writeSectors, _ := strconv.ParseUint(fields[9], 10, 64)
		out[name] = DiskSample{
			ReadsCompleted:  reads,
			WritesCompleted: writes,
			ReadSectors:     readSectors,
			WriteSectors:    writeSectors,
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan /proc/diskstats: %w", err)
	}
	return out, nil
}

func firstLine(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("empty file")
	}
	return scanner.Text(), nil
}

func PrimaryIPv4() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip := ipnet.IP.To4()
		if ip == nil {
			continue
		}
		return ip.String(), nil
	}
	return "", fmt.Errorf("no non-loopback IPv4 address found")
}
