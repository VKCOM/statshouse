package stats

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/procfs/blockdevice"
	"golang.org/x/sys/unix"

	"github.com/vkcom/statshouse/internal/format"
)

type (
	DiskStats struct {
		fs blockdevice.FS

		writer                     MetricWriter
		old                        map[string]blockdevice.Diskstats
		types                      map[string]deviceType
		excludedMountPointsPattern *regexp.Regexp
		excludedFSTypesPattern     *regexp.Regexp
		excludedDevicePattern      *regexp.Regexp
		logErr                     *log.Logger
	}

	mount struct {
		device, mountPoint, fsType, options string
	}

	deviceType int
)

const (
	unknown   deviceType = iota
	physical  deviceType = iota
	partition deviceType = iota
	virtual   deviceType = iota
)

const (
	defMountPointsExcluded = "^/(dev|proc|run/credentials/.+|sys|var/lib/docker/.+|var/lib/containers/storage/.+)($|/)"
	defFSTypesExcluded     = "^(autofs|binfmt_misc|bpf|cgroup2?|configfs|debugfs|devpts|devtmpfs|fusectl|hugetlbfs|iso9660|mqueue|nsfs|overlay|proc|procfs|pstore|rpc_pipefs|securityfs|selinuxfs|squashfs|sysfs|tracefs)$"
	deviceExcluded         = "(loop\\d+)"
	sectorSize             = 512
)

func (c *DiskStats) Skip() bool {
	return false
}

func (*DiskStats) Name() string {
	return "disk_stats"
}
func (c *DiskStats) PushDuration(now int64, d time.Duration) {
	c.writer.WriteSystemMetricValueWithoutHost(now, format.BuiltinMetricNameSystemMetricScrapeDuration, d.Seconds(), format.TagValueIDSystemMetricDisk)
}

func NewDiskStats(writer MetricWriter, logErr *log.Logger) (*DiskStats, error) {
	fs, err := blockdevice.NewFS(procPath, sysPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize procfs: %w", err)
	}
	return &DiskStats{
		fs:                         fs,
		old:                        map[string]blockdevice.Diskstats{},
		types:                      map[string]deviceType{},
		writer:                     writer,
		excludedMountPointsPattern: regexp.MustCompile(defMountPointsExcluded),
		excludedFSTypesPattern:     regexp.MustCompile(defFSTypesExcluded),
		excludedDevicePattern:      regexp.MustCompile(deviceExcluded),
		logErr:                     logErr,
	}, nil
}

func (c *DiskStats) WriteMetrics(nowUnix int64) error {
	stats, err := c.fs.ProcDiskstats()
	if err != nil {
		return fmt.Errorf("failed to get disk stats: %w", err)
	}
	for _, stat := range stats {
		if c.excludedDevicePattern.MatchString(stat.DeviceName) {
			continue
		}
		device := stat.DeviceName
		oldStat, ok := c.old[device]
		c.old[device] = stat
		if !ok {
			continue
		}
		deviceType, ok := c.types[device]
		if !ok {
			deviceType, err = getDeviceType(stat)
			if err != nil {
				c.logErr.Println("failed to get device type", err)
				continue
			}
			c.types[device] = deviceType
		}
		if deviceType != physical {
			continue
		}
		readIOI := int(stat.ReadIOs) - int(oldStat.ReadIOs)
		writeIOI := int(stat.WriteIOs) - int(oldStat.WriteIOs)
		discardIOI := int(stat.DiscardIOs) - int(oldStat.DiscardIOs)
		flushIOI := int(stat.FlushRequestsCompleted) - int(oldStat.FlushRequestsCompleted)

		readIOSeconds := (float64(stat.ReadTicks) - float64(oldStat.ReadTicks)) / 1000
		writeIOSeconds := (float64(stat.WriteTicks) - float64(oldStat.WriteTicks)) / 1000
		discardIOSeconds := (float64(stat.DiscardTicks) - float64(oldStat.DiscardTicks)) / 1000
		flushIOSeconds := (float64(stat.TimeSpentFlushing) - float64(oldStat.TimeSpentFlushing)) / 1000

		readIOSize := (float64(stat.ReadSectors) - float64(oldStat.ReadSectors)) * sectorSize
		writeIOSize := (float64(stat.WriteSectors) - float64(oldStat.WriteSectors)) * sectorSize
		discardIOSize := (float64(stat.DiscardSectors) - float64(oldStat.DiscardSectors)) * sectorSize

		if readIOI > 0 {
			readIO := float64(readIOI)
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOTime, readIO, readIOSeconds/readIO, Tag{Str: device}, Tag{Raw: format.RawIDTagRead})
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOSize, readIO, readIOSize/readIO, Tag{Str: device}, Tag{Raw: format.RawIDTagRead})
		}
		if writeIOI > 0 {
			writeIO := float64(writeIOI)
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOTime, writeIO, writeIOSeconds/writeIO, Tag{Str: device}, Tag{Raw: format.RawIDTagWrite})
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOSize, writeIO, writeIOSize/writeIO, Tag{Str: device}, Tag{Raw: format.RawIDTagWrite})
		}
		if discardIOI > 0 {
			discardIO := float64(discardIOI)
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOTime, discardIO, discardIOSeconds/discardIO, Tag{Str: device}, Tag{Raw: format.RawIDTagDiscard})
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOSize, discardIO, discardIOSize/discardIO, Tag{Str: device}, Tag{Raw: format.RawIDTagDiscard})
		}
		if flushIOI > 0 {
			flushIO := float64(discardIOI)
			c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameBlockIOTime, flushIO, flushIOSeconds/flushIO, Tag{Str: device}, Tag{Raw: format.RawIDTagFlush})
		}

	}
	err = c.writeFSStats(nowUnix)
	return err
}

func (c *DiskStats) writeFSStats(nowUnix int64) error {
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
			continue
		}
		blocksTotal := s.Blocks
		blocksAvailable := s.Bavail
		blocksAvailableRoot := s.Bfree
		blocksReservedRoot := blocksAvailableRoot - blocksAvailable
		var blocksUsed uint64 = 0
		// https://github.com/netdata/netdata/blob/db63ab82265f0606e33600a350e4ee6cc2dda687/src/collectors/diskspace.plugin/plugin_diskspace.c#L488
		if blocksTotal >= blocksAvailableRoot {
			blocksUsed = blocksTotal - blocksAvailableRoot
		} else {
			blocksUsed = blocksAvailableRoot - blocksTotal
		}
		free := float64(blocksAvailable) * float64(s.Bsize)
		used := float64(blocksUsed) * float64(s.Bsize)
		reservedForRoot := float64(blocksReservedRoot) * float64(s.Bsize)
		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameDiskUsage, 1, free, Tag{Raw: format.RawIDTagFree}, Tag{Str: stat.device})
		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameDiskUsage, 1, used, Tag{Raw: format.RawIDTagUsed}, Tag{Str: stat.device})
		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameDiskUsage, 1, reservedForRoot, Tag{Raw: format.RawIDTagReservedForRoot}, Tag{Str: stat.device})

		inodeFree := float64(s.Ffree)
		inodeUsed := float64(s.Files) - inodeFree
		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameINodeUsage, 1, inodeFree, Tag{Raw: format.RawIDTagFree}, Tag{Str: stat.device})
		c.writer.WriteSystemMetricCountValueExtendedTag(nowUnix, format.BuiltinMetricNameINodeUsage, 1, inodeUsed, Tag{Raw: format.RawIDTagUsed}, Tag{Str: stat.device})

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

func pathIsExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getDeviceType(device blockdevice.Diskstats) (deviceType, error) {
	res := unknown
	isExists, err := pathIsExists("/sys/block/" + device.DeviceName)
	if err != nil {
		return res, err
	}
	if isExists {
		res = physical
	}

	isExists, err = pathIsExists(fmt.Sprintf("/sys/dev/block/%d:%d/partition", device.MajorNumber, device.MinorNumber))
	if err != nil {
		return res, err
	}
	if isExists {
		res = partition
	} else {
		isExists, err = pathIsExists("/sys/devices/virtual/" + device.DeviceName)
		if err != nil {
			return res, err
		}
		if isExists {
			res = virtual
		} else {
			dirs, err := os.ReadDir(fmt.Sprintf("/sys/dev/block/%d:%d/slaves", device.MajorNumber, device.MinorNumber))
			if err != nil && !os.IsNotExist(err) {
				return res, err
			}
			if len(dirs) > 0 {
				res = virtual
			}
		}
	}

	return res, nil
}
