package stats

import (
	"testing"

	"github.com/VKCOM/statshouse/internal/format"
	"github.com/stretchr/testify/require"
)

func TestDmesgStats_handleMsgsCC(t *testing.T) {
	c := &DMesgStats{lastTS: timestamp{-1, 0}}
	body := []byte(`<6>[    0.000000] BIOS-e820: [mem 0x000000000008e400-0x000000000009ffff] reserved
<4>[   32.369778] Unsafe core_pattern used with fs.suid_dumpable=2.
                  Pipe handler or fully qualified core dump path required.
                  Set kernel.core_pattern before fs.suid_dumpable.
<4>[33.445681] Killed process 3029 (Web Content) total-vm:10206696kB, anon-rss:6584572kB, file-rss:0kB, shmem-rss:8732kB
`)
	var data []klogMsg
	cb := func(nowUnix int64, klog klogMsg) {
		data = append(data, klog)
	}
	err := c.handleMsgs(0, body, true, cb)
	require.NoError(t, err)
	require.Len(t, data, 3)
	a := data[0]
	require.Equal(t, int32(format.RawIDTag_Info), a.level)
	require.Equal(t, int32(format.RawIDTag_kern), a.facility)
	require.Equal(t, timestamp{sec: 0, usec: 0}, a.ts)
	a = data[1]
	require.Equal(t, int32(format.RawIDTag_Warn), a.level)
	require.Equal(t, int32(format.RawIDTag_kern), a.facility)
	require.Equal(t, timestamp{sec: 32, usec: 369778}, a.ts)
	a = data[2]
	require.Equal(t, int32(format.RawIDTag_Warn), a.level)
	require.Equal(t, int32(format.RawIDTag_kern), a.facility)
	require.Equal(t, timestamp{sec: 33, usec: 445681}, a.ts)
	require.Equal(t, "Web Content", a.oom.oomProcessName)

}

func TestMesgStats_handleMsgs(t *testing.T) {
	c := &DMesgStats{}

	body := []byte(
		`<4>[12555048.809330] audit: audit_lost=50409498 audit_rate_limit=0 audit_backlog_limit=8192
<4>[12555058.445681] Killed process 3029 (Web Content) total-vm:10206696kB, anon-rss:6584572kB, file-rss:0kB, shmem-rss:8732kB
<4>[12555059.240818] audit_log_start: 2 callbacks suppressed
`)
	var data []klogMsg
	cb := func(nowUnix int64, klog klogMsg) {
		data = append(data, klog)
	}
	err := c.handleMsgs(0, body, true, cb)
	require.NoError(t, err)
	require.Len(t, data, 3)
	a := data[0]
	require.Equal(t, int32(format.RawIDTag_Warn), a.level)
	require.Equal(t, int32(format.RawIDTag_kern), a.facility)
	require.Equal(t, timestamp{sec: 12555048, usec: 809330}, a.ts)
	a = data[1]
	require.Equal(t, int32(format.RawIDTag_Warn), a.level)
	require.Equal(t, int32(format.RawIDTag_kern), a.facility)
	require.Equal(t, timestamp{sec: 12555058, usec: 445681}, a.ts)
	require.Equal(t, "Web Content", a.oom.oomProcessName)
	a = data[2]
	require.Equal(t, a.level, int32(format.RawIDTag_Warn))
	require.Equal(t, a.facility, int32(format.RawIDTag_kern))
	require.Equal(t, a.ts, timestamp{sec: 12555059, usec: 240818})

	body = append(body, "<4>[12555059.240819] audit_log_start: 2 callbacks suppressed\n"...)
	body = append(body, "<4>[12555060.240817] audit_log_start: 2 callbacks suppressed\n"...)
	err = c.handleMsgs(0, body, true, cb)
	require.NoError(t, err)
	require.Len(t, data, 5)
	a = data[3]
	require.Equal(t, a.level, int32(format.RawIDTag_Warn))
	require.Equal(t, a.facility, int32(format.RawIDTag_kern))
	require.Equal(t, a.ts, timestamp{sec: 12555059, usec: 240819})
	a = data[4]
	require.Equal(t, a.level, int32(format.RawIDTag_Warn))
	require.Equal(t, a.facility, int32(format.RawIDTag_kern))
	require.Equal(t, a.ts, timestamp{sec: 12555060, usec: 240817})

	body = append(body, "<4>[12555070.955796] Memory cgroup out of memory: Killed process 21075 (check_binlog_re) total-vm:7756kB, anon-rss:876kB, file-rss:2984kB, shmem-rss:0kB, UID:0 pgtables:56kB oom_score_adj:0\n"...)
	err = c.handleMsgs(0, body, true, cb)
	require.NoError(t, err)
	a = data[5]
	require.Equal(t, a.level, int32(format.RawIDTag_Warn))
	require.Equal(t, a.facility, int32(format.RawIDTag_kern))
	require.Equal(t, a.ts, timestamp{sec: 12555070, usec: 955796})
}
