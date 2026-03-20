// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build unix

package srvfunc

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

const (
	pagesize = 4096 // замена C.sysconf(C._SC_PAGESIZE)
)

// GetMemStat возвращает статистику по использованию памяти
//
// Максимальное значение PID в Linux может доходить до 4 миллионов (см. исходники Linux),
// поэтому у параметра pid стоит тип int, а не uint16
//
// @see man proc по /proc/*/statm
// @see https://elixir.bootlin.com/linux/latest/source/include/linux/threads.h#L34
func GetMemStat(pid int) (*MemStats, error) {
	var fname string
	if pid > 0 {
		fname = fmt.Sprintf(`/proc/%d/statm`, pid)
	} else {
		fname = `/proc/self/statm`
	}

	fd, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	var m MemStats

	if _, err := fmt.Fscanf(fd, `%d %d %d %d %d %d %d`, &m.Size, &m.Res, &m.Share, &m.Text, &m.Lib, &m.Data, &m.Dt); err != nil {
		return nil, err
	}

	m.Size *= pagesize
	m.Res *= pagesize
	m.Share *= pagesize
	m.Text *= pagesize
	m.Lib *= pagesize
	m.Data *= pagesize
	m.Dt *= pagesize

	return &m, nil
}

// GetGCStats возвращает статистику по работе GC
func GetGCStats() (stat GCStats) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	const NsecInMsec = uint64(time.Millisecond / time.Nanosecond)
	const NsecInMcsec = uint64(time.Microsecond / time.Nanosecond)

	stat.PauseTotalMs = memStats.PauseTotalNs / NsecInMsec
	stat.PauseTotalMcs = memStats.PauseTotalNs / NsecInMcsec

	stat.GCCPUFraction = 100 * memStats.GCCPUFraction

	mod := uint64(len(memStats.PauseNs))
	numGC := uint64(memStats.NumGC) % mod

	for {
		pauseMs := memStats.PauseNs[stat.prevNumGC] / NsecInMsec
		pauseMcs := memStats.PauseNs[stat.prevNumGC] / NsecInMcsec
		stat.LastPausesMs = append(stat.LastPausesMs, pauseMs)
		stat.LastPausesMcs = append(stat.LastPausesMcs, pauseMcs)

		if len(stat.LastPausesMcs) >= 30 {
			break
		}

		if stat.prevNumGC = (stat.prevNumGC + 1) % mod; stat.prevNumGC == numGC {
			break
		}
	}

	return stat
}

// SetMaxRLimitNoFile пробует выставить текущие nofile лимиты (ulimit -n) в максимально разрешенные
// Вернет в случае успеха кортеж (cur, max) значений лимита
func SetMaxRLimitNoFile() ([]uint64, error) {
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	if rLimit.Cur < rLimit.Max {
		rLimit.Cur = rLimit.Max
		_ = unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
	}

	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	return []uint64{rLimit.Cur, rLimit.Max}, nil
}

// SetHardMaxRLimitNoFile выставляет максимальный лимит для RLIMIT_NOFILE
// требует привилегированного доступа.
// возвращает (cur, max) значений лимита в случае успеха.
func SetHardRLimitNoFile(maxCount uint64) ([]uint64, error) {
	rLimit := unix.Rlimit{}
	rLimit.Cur = maxCount
	rLimit.Max = maxCount

	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	return []uint64{rLimit.Cur, rLimit.Max}, nil
}

// GetNumOpenedFile вычисляет количество используемых файловых дескрипторов приложением
// @see man proc по /proc/*/fd/
func GetNumOpenedFile(pid int) (int, error) {
	var path string
	if pid < 1 {
		path = `/proc/self/fd`
	} else {
		path = fmt.Sprintf(`/proc/%d/fd`, pid)
	}

	list, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// LogRotate переоткрывает указанный файл и подменяем stdout/stderr вывод на этот файл
func LogRotate(prevLogFd *os.File, fname string) (newLogFd *os.File, err error) {
	if prevLogFd != nil {
		prevLogFd.Close()
		prevLogFd = nil
	}

	flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
	newLogFd, err = os.OpenFile(fname, flag, os.FileMode(0644))
	if err != nil {
		return nil, err
	}

	_ = unix.Dup2(int(newLogFd.Fd()), unix.Stdout)
	_ = unix.Dup2(int(newLogFd.Fd()), unix.Stderr)

	return newLogFd, nil
}
