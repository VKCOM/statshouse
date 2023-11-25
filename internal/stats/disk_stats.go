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
		readIO := float64(stat.ReadIOs) - float64(oldStat.ReadIOs)
		writeIO := float64(stat.WriteIOs) - float64(oldStat.WriteIOs)
		discardIO := float64(stat.DiscardIOs) - float64(oldStat.DiscardIOs)

		readIOTicks := (float64(stat.ReadTicks) - float64(oldStat.ReadTicks)) / 1000 / readIO
		writeIOTicks := (float64(stat.WriteTicks) - float64(oldStat.WriteTicks)) / 1000 / writeIO
		discardIOTicks := (float64(stat.DiscardTicks) - float64(oldStat.DiscardTicks)) / 1000 / discardIO

		readIOSize := (float64(stat.ReadSectors) - float64(oldStat.ReadSectors)) * sectorSize / readIO
		writeIOSize := (float64(stat.WriteSectors) - float64(oldStat.WriteSectors)) * sectorSize / writeIO
		discardIOSize := (float64(stat.DiscardSectors) - float64(oldStat.DiscardSectors)) * sectorSize / discardIO

		if readIO > 0 {
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOTime, readIO, readIOTicks, 0, format.RawIDTagRead)
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOSize, readIO, readIOSize, 0, format.RawIDTagRead)
		}
		if writeIO > 0 {
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOTime, writeIO, writeIOTicks, 0, format.RawIDTagWrite)
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOSize, writeIO, writeIOSize, 0, format.RawIDTagWrite)
		}
		if discardIO > 0 {
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOTime, discardIO, discardIOTicks, 0, format.RawIDTagDiscard)
			c.writer.WriteSystemMetricCountValue(nowUnix, format.BuiltinMetricNameBlockIOSize, discardIO, discardIOSize, 0, format.RawIDTagDiscard)
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
			// c.logErr.Printf("failed to statfs of %s: %s", stat.mountPoint, err.Error())
			continue
		}
		free := float64(s.Bfree) * float64(s.Bsize)
		used := float64(s.Blocks)*float64(s.Bsize) - free
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameDiskUsage, free, format.RawIDTagFree)
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameDiskUsage, used, format.RawIDTagUsed)

		inodeFree := float64(s.Ffree)
		inodeUsed := float64(s.Files) - inodeFree
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameINodeUsage, inodeFree, format.RawIDTagFree)
		c.writer.WriteSystemMetricValue(nowUnix, format.BuiltinMetricNameINodeUsage, inodeUsed, format.RawIDTagUsed)
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
