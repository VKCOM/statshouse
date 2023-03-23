// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/tlpprof"
)

const (
	tlTrueTag   = uint32(0x3fedd339) // copy of vktl.MagicTlTrue
	tlStringTag = uint32(0xb5286e24)
	tlNetPIDTag = uint32(0x46409ccf) // copy of vktl.MagicTlNetPid
	tlStatTag   = uint32(0x9d56e6b2) // copy of vktl.MagicTlStat
)

func (s *Server) collectStats(localAddr net.Addr) map[string]string {
	cmdline := readCommandLine()
	now := time.Now().Unix()
	uptime := now - int64(s.startTime)
	pid := prepareHandshakePIDServer(localAddr, s.startTime)

	rps := s.statRPS.Load()
	reqTotal := s.statRequestsTotal.Load()
	reqCurrent := s.statRequestsCurrent.Load()
	connTotal := s.statConnectionsTotal.Load()
	connCurrent := s.statConnectionsCurrent.Load()
	requestMem := s.statRequestMemory.Load()
	responseMem := s.statResponseMemory.Load()

	gc := srvfunc.GetGCStats()
	gcPausesMs, _ := json.Marshal(gc.LastPausesMs)
	gcPausesMcs, _ := json.Marshal(gc.LastPausesMcs)

	maxCPUUsage := 100 * runtime.GOMAXPROCS(-1)
	avgUsage, curUsage := statCPUInfo.GetSelfCpuUsage()
	avgIdle := 100 * (maxCPUUsage - avgUsage) / maxCPUUsage
	curIdle := 100 * (maxCPUUsage - curUsage) / maxCPUUsage
	if avgIdle < 0 {
		avgIdle = 0
	}
	if curIdle < 0 {
		curIdle = 0
	}

	m := map[string]string{}
	s.statsHandler(m)

	m["version"] = s.version
	m["hostname"] = s.statHostname
	m["command_line"] = cmdline
	m["current_time"] = strconv.Itoa(int(now))
	m["start_time"] = strconv.Itoa(int(s.startTime))
	m["uptime"] = strconv.Itoa(int(uptime))
	m["pid"] = strconv.Itoa(int(pid.PID))
	m["PID"] = pid.asTextStat()

	m["qps"] = "0" // memcached protocol
	m["rpc_qps"] = strconv.FormatInt(rps, 10)
	m["queries_total"] = strconv.FormatInt(reqTotal, 10)
	m["queries_active"] = strconv.FormatInt(reqCurrent, 10)
	m["clients_total"] = strconv.FormatInt(connTotal, 10)
	m["clients_active"] = strconv.FormatInt(connCurrent, 10)
	m["request_memory"] = strconv.FormatInt(requestMem, 10)
	m["response_memory"] = strconv.FormatInt(responseMem, 10)

	m["gc_ms"] = strconv.FormatUint(gc.PauseTotalMs, 10)
	m["gc_mcs"] = strconv.FormatUint(gc.PauseTotalMcs, 10)
	m["gc_cpu_usage"] = strconv.FormatFloat(gc.GCCPUFraction, 'f', 5, 32)
	m["gc_pauses"] = string(gcPausesMs)
	m["gc_pauses_mcs"] = string(gcPausesMcs)

	m["average_idle_percent"] = strconv.Itoa(avgIdle)
	m["recent_idle_percent"] = strconv.Itoa(curIdle)

	mem, _ := srvfunc.GetMemStat(0)
	if mem != nil {
		m["vmsize_bytes"] = strconv.FormatUint(mem.Size, 10)
		m["vmrss_bytes"] = strconv.FormatUint(mem.Res, 10)
		m["vmdata_bytes"] = strconv.FormatUint(mem.Data, 10)
	}

	return m
}

func readCommandLine() string {
	buf, _ := os.ReadFile("/proc/self/cmdline")
	return strings.ReplaceAll(string(buf), "\x00", " ")
}

func (s *Server) handleEnginePID(hctx *HandlerContext) error {
	pid := prepareHandshakePIDServer(hctx.localAddr, s.startTime)
	hctx.Response = basictl.NatWrite(hctx.Response, tlNetPIDTag)
	hctx.Response = pid.write(hctx.Response)
	return nil
}

func (s *Server) handleEngineStat(hctx *HandlerContext) error {
	stats := s.collectStats(hctx.localAddr)
	keys := sortedStatKeys(stats)

	hctx.Response = basictl.NatWrite(hctx.Response, tlStatTag)
	hctx.Response = basictl.NatWrite(hctx.Response, uint32(len(keys)))
	for _, k := range keys {
		hctx.Response = basictl.StringWriteTruncated(hctx.Response, k)
		hctx.Response = basictl.StringWriteTruncated(hctx.Response, stats[k])
	}

	return nil
}

func (s *Server) handleEngineVersion(hctx *HandlerContext) error {
	hctx.Response = basictl.NatWrite(hctx.Response, tlStringTag)
	hctx.Response = basictl.StringWriteTruncated(hctx.Response, s.version)
	return nil
}

func (s *Server) handleEngineSetVerbosity(hctx *HandlerContext) (err error) {
	if s.verbosityHandler == nil {
		return nil
	}

	if hctx.Request, err = basictl.NatReadExactTag(hctx.Request, engineSetVerbosityTag); err != nil {
		return err
	}
	var verbosity int32
	if hctx.Request, err = basictl.IntRead(hctx.Request, &verbosity); err != nil {
		return fmt.Errorf("failed to read verbosity level: %w", err)
	}
	if err = s.verbosityHandler(int(verbosity)); err != nil {
		return err
	}
	hctx.Response = basictl.NatWrite(hctx.Response, tlTrueTag)

	return nil
}

func (s *Server) respondWithMemcachedVersion(conn *PacketConn) {
	resp := fmt.Sprintf("VERSION %v\r\n", s.version)

	err := s.sendMemcachedResponse(conn, "version", []byte(resp))
	if err != nil {
		s.logf(err.Error())
	}
}

func (s *Server) respondWithMemcachedStats(conn *PacketConn) {
	stats := s.collectStats(conn.conn.LocalAddr())
	resp := marshalMemcachedStats(stats)

	err := s.sendMemcachedResponse(conn, "stats", resp)
	if err != nil {
		s.logf(err.Error())
	}
}

func (s *Server) sendMemcachedResponse(c *PacketConn, name string, resp []byte) error {
	err := c.conn.SetWriteDeadline(time.Now().Add(maxPacketRWTime))
	if err != nil {
		return fmt.Errorf("rpc: failed to set write deadline for memcached %v for %v: %w", name, c.remoteAddr, err)
	}

	_, err = c.conn.Write(resp)
	if err != nil {
		return fmt.Errorf("rpc: failed to write memcached %v for %v: %w", name, c.remoteAddr, err)
	}

	return nil
}

func sortedStatKeys(stats map[string]string) []string {
	var keys []string
	for k, v := range stats {
		if v != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func marshalMemcachedStats(stats map[string]string) []byte {
	keys := sortedStatKeys(stats)

	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(stats[k])
		buf.WriteString("\r\n")
	}
	buf.WriteString("END\r\n")

	return buf.Bytes()
}

func (s *Server) handleGoPProf(hctx *HandlerContext) (err error) {
	hctx.Request, err = basictl.NatReadExactTag(hctx.Request, goPProfTag)
	if err != nil {
		return err
	}
	var q string
	hctx.Request, err = basictl.StringRead(hctx.Request, &q)
	if err != nil {
		return fmt.Errorf("failed to read pprof URL: %w", err)
	}
	u, err := url.Parse(q)
	if err != nil {
		return fmt.Errorf("failed to parse pprof URL: %w", err)
	}

	var buf bytes.Buffer
	switch u.Path {
	case "/debug/pprof/cmdline":
		tlpprof.Cmdline(&buf)
	case "/debug/pprof/profile":
		err = tlpprof.Profile(&buf, u)
	case "/debug/pprof/trace":
		err = tlpprof.Trace(&buf, u)
	case "/debug/pprof/allocs",
		"/debug/pprof/block",
		"/debug/pprof/goroutine",
		"/debug/pprof/heap",
		"/debug/pprof/mutex",
		"/debug/pprof/threadcreate":
		name := strings.TrimPrefix(u.Path, "/debug/pprof/")
		err = tlpprof.Index(&buf, name, u)
	default:
		err = fmt.Errorf("unsupported pprof URL %q", q)
	}
	if err != nil {
		return err
	}
	hctx.Response = basictl.NatWrite(hctx.Response, tlStringTag)
	hctx.Response = basictl.StringWriteBytesTruncated(hctx.Response, buf.Bytes())
	return nil
}
