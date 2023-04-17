package stats

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/prometheus/procfs/blockdevice"
	"github.com/vkcom/statshouse/internal/format"
	"golang.org/x/sys/unix"
)

type DiskStats struct {
	fs blockdevice.FS

	writer                     MetricWriter
	old                        map[string]blockdevice.Diskstats
	excludedMountPointsPattern *regexp.Regexp
	excludedFSTypesPattern     *regexp.Regexp
	logErr                     *log.Logger
}

type mount struct {
	device, mountPoint, fsType, options string
}

const (
	defMountPointsExcluded = "^/(dev|proc|run/credentials/.+|sys|var/lib/docker/.+|var/lib/containers/storage/.+)($|/)"
	defFSTypesExcluded     = "^(autofs|binfmt_misc|bpf|cgroup2?|configfs|debugfs|devpts|devtmpfs|fusectl|hugetlbfs|iso9660|mqueue|nsfs|overlay|proc|procfs|pstore|rpc_pipefs|securityfs|selinuxfs|squashfs|sysfs|tracefs)$"
)

func (*DiskStats) Name() string {
	return "disk_stats"
}

func NewDiskStats(writer MetricWriter, logErr *log.Logger) (*DiskStats, error) {
	fs, err := blockdevice.NewFS(procPath, sysPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &DiskStats{
		fs:                         fs,
		writer:                     writer,
		excludedMountPointsPattern: regexp.MustCompile(defMountPointsExcluded),
		excludedFSTypesPattern:     regexp.MustCompile(defFSTypesExcluded),
		logErr:                     logErr,
	}, nil
}

func (c *DiskStats) WriteMetrics() error {
	stats, err := c.fs.ProcDiskstats()
	if err != nil {
		return fmt.Errorf("failed to get disk stats: %w", err)
	}
	for _, stat := range stats {
		device := stat.DeviceName
		oldStat, ok := c.old[device]
		c.old[device] = stat
		if !ok {
			continue
		}
		readIO := stat.ReadIOs - oldStat.ReadIOs
		writeIO := stat.WriteIOs - oldStat.WriteIOs
		discardIO := stat.DiscardIOs - oldStat.DiscardIOs

		c.writer.WriteSystemMetricCount(format.BuiltinMetricNameBlockIOTime, float64(readIO), format.RawIDTagRead)
		c.writer.WriteSystemMetricCount(format.BuiltinMetricNameBlockIOTime, float64(writeIO), format.RawIDTagWrite)
		c.writer.WriteSystemMetricCount(format.BuiltinMetricNameBlockIOTime, float64(discardIO), format.RawIDTagDiscard)

		readIOTicks := float64(stat.ReadTicks-oldStat.ReadTicks) / 1000
		writeIOTicks := float64(stat.WriteTicks-oldStat.WriteTicks) / 1000
		discardIOTicks := float64(stat.DiscardTicks-oldStat.DiscardTicks) / 1000

		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameBlockIOTime, readIOTicks, format.RawIDTagRead)
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameBlockIOTime, writeIOTicks, format.RawIDTagWrite)
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameBlockIOTime, discardIOTicks, format.RawIDTagDiscard)
	}
	err = c.writeFSStats()
	return err
}

func (c *DiskStats) writeFSStats() error {
	stats, err := parseMounts()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, stat := range stats {
		if c.excludedMountPointsPattern.MatchString(stat.mountPoint) {
			continue
		}
		if c.excludedFSTypesPattern.MatchString(stat.fsType) {
			continue
		}
		if seen[stat.device] {
			continue
		}
		seen[stat.device] = true
		s := unix.Statfs_t{}
		err := unix.Statfs(stat.mountPoint, &s)
		if err != nil {
			c.logErr.Printf("failed to statfs of %s: %s", stat.mountPoint, err.Error())
			continue
		}
		free := float64(s.Bfree) * float64(s.Bsize)
		used := float64(s.Blocks)*float64(s.Bsize) - free
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameDiskUsage, free, format.RawIDTagFree)
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameDiskUsage, used, format.RawIDTagUsed)

		inodeFree := float64(s.Ffree)
		inodeUsed := float64(s.Files) - inodeFree
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameINodeUsage, inodeFree, format.RawIDTagFree)
		c.writer.WriteSystemMetricValue(format.BuiltinMetricNameINodeUsage, inodeUsed, format.RawIDTagUsed)
	}
	return nil
}

func parseMounts() ([]mount, error) {
	file, err := os.Open("/proc/1/mounts")
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Open("/proc/mounts")
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var mounts []mount

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())

		if len(parts) < 4 {
			return nil, fmt.Errorf("malformed mount point information: %q", scanner.Text())
		}

		// Ensure we handle the translation of \040 and \011
		// as per fstab(5).
		parts[1] = strings.Replace(parts[1], "\\040", " ", -1)
		parts[1] = strings.Replace(parts[1], "\\011", "\t", -1)

		mounts = append(mounts, mount{
			device:     parts[0],
			mountPoint: parts[1],
			fsType:     parts[2],
			options:    parts[3],
		})
	}

	return mounts, scanner.Err()
}
