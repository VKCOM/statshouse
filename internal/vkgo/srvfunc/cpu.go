// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	// CPUInfo считает статистику по использованию CPU глобально по ОС
	CPUInfo struct {
		lock sync.RWMutex

		procUsage map[string]float32

		clockTicks int // Clock ticks/second

		// нагрузка по CPU: средняя за время время (avg) и средняя за последнее время (cur)
		avgCpuUsagePerc int
		curCpuUsagePerc int

		cpuNum int // кешируем ответ GetCPUNum()
	}
)

// MakeCPUInfo инициализирует сбор статистики
func MakeCPUInfo() *CPUInfo {
	ci := &CPUInfo{}
	ci.initWithoutLock()

	return ci
}

// GetThisProcUsage возвращает текущую статистику использования CPU в целом по системе: us, ni, sy, id, io
// Если от инициализации (MakeCPUInfo) прошло менее секунды, то вернет пустой словарь
func (ci *CPUInfo) GetThisProcUsage() map[string]float32 {
	m := make(map[string]float32)

	ci.lock.RLock()
	for k, v := range ci.procUsage {
		m[k] = v
	}
	ci.lock.RUnlock()

	return m
}

// GetSelfCpuUsage возвращает статистику по использованию CPU текущим процессом:
//
//	среднее и последнее использование в % (100 - полностью занято 1 ядро, 800 - полностью заняты 8 ядер и т.п.)
func (ci *CPUInfo) GetSelfCpuUsage() (avgPerc int, curPerc int) {
	ci.lock.RLock()
	defer ci.lock.RUnlock()

	return ci.avgCpuUsagePerc, ci.curCpuUsagePerc
}

// GetCPUNum возвращает число ядер (виртуальных) CPU
func (ci *CPUInfo) GetCPUNum() (int, error) {
	ci.lock.RLock()
	cpuNum := ci.cpuNum
	ci.lock.RUnlock()

	if cpuNum > 0 {
		return cpuNum, nil
	}

	ci.lock.Lock()
	defer ci.lock.Unlock()

	if ci.cpuNum == 0 {
		if buf, err := exec.Command(`nproc`).Output(); err != nil {
			return 0, err
		} else if n, err := strconv.Atoi(strings.TrimSpace(string(buf))); err != nil {
			return 0, err
		} else {
			ci.cpuNum = n
		}
	}

	return ci.cpuNum, nil
}

func (ci *CPUInfo) initWithoutLock() {
	ci.procUsage = make(map[string]float32)

	ci.clockTicks = 0
	if out, err := exec.Command(`getconf`, `CLK_TCK`).Output(); err != nil {
	} else if n, err := strconv.ParseUint(string(bytes.TrimSpace(out)), 10, 32); err != nil {
	} else if n > 0 {
		ci.clockTicks = int(n)
	}
	if ci.clockTicks == 0 {
		ci.clockTicks = 100 // стандартный вариант
	}

	go ci.allSystemCpuUsageLoop()
	go ci.thisProcessCpuUsageLoop()
}

func (ci *CPUInfo) allSystemCpuUsageLoop() {
	bytesNl := []byte("\n")
	bytesSpace := []byte(` `)

	titles := []string{`us`, `ni`, `sy`, `id`, `io`}
	titlesPref := 2 // сколько ведущих колонок из выдачи /proc/stat пропускаем

	cntsCur := make([]uint64, len(titles))
	cntsPrev := make([]uint64, len(titles))
	var prevTotal uint64

	for range time.Tick(1 * time.Second) {
		if buf, err := os.ReadFile(`/proc/stat`); err != nil {
		} else if lines := bytes.SplitN(buf, bytesNl, 2); len(lines) < 2 {
		} else if cols := bytes.Split(lines[0], bytesSpace); len(cols) < (len(titles) + titlesPref) { //nolint:gocritic
		} else if string(cols[0]) != `cpu` {
		} else {
			cols = cols[titlesPref:]

			total := uint64(0)
			for i := len(titles) - 1; i >= 0; i-- {
				if n, err := strconv.Atoi(string(cols[i])); err == nil {
					cntsCur[i] = uint64(n)
					total += uint64(n)
				}
			}

			if prevTotal > 0 {
				ci.lock.Lock()
				for k := range ci.procUsage {
					ci.procUsage[k] = 0
				}

				diffTotal := total - prevTotal
				for i := range cntsCur {
					if diff := cntsCur[i] - cntsPrev[i]; diff > 0 {
						ci.procUsage[titles[i]] = 100.0 * float32(diff) / float32(diffTotal)
					}
				}
				ci.lock.Unlock()
			}

			prevTotal = total
			copy(cntsPrev, cntsCur)
		}
	}
}

func (ci *CPUInfo) readProcSelfStat() (totalTime float64, err error) {
	raw, err := os.ReadFile(`/proc/self/stat`)
	if err != nil {
		return 0, err
	}

	chunks := bytes.Split(raw, []byte(` `))
	if len(chunks) <= 16 {
		return 0, fmt.Errorf("only %d chunks in /proc/self/stat", len(chunks))
	}

	var utime, stime, cutime, cstime int

	if utime, err = strconv.Atoi(string(chunks[13])); err != nil {
		return 0, err
	} else if stime, err = strconv.Atoi(string(chunks[14])); err != nil {
		return 0, err
	} else if cutime, err = strconv.Atoi(string(chunks[15])); err != nil {
		return 0, err
	} else if cstime, err = strconv.Atoi(string(chunks[16])); err != nil {
		return 0, err
	}

	ci.lock.RLock()
	clockTicks := ci.clockTicks
	ci.lock.RUnlock()

	totalTime = float64(utime+stime+cutime+cstime) / float64(clockTicks)
	return totalTime, nil
}

func (ci *CPUInfo) thisProcessCpuUsageLoop() {
	var (
		prevTotalTime    float64
		prevTime         int64
		cpuUsagesPerc    [60]float64
		lastCpuUsagesIdx = -1
	)

	for range time.Tick(1 * time.Second) {
		totalTime, err := ci.readProcSelfStat()
		if err != nil {
			continue
		}

		if prevTotalTime > 0 {
			// расчет текущей нагрузки за прошлую секунду
			now := time.Now().UnixNano()
			diff := now - prevTime
			prevTime = now

			curUsagePerc := 100 * (totalTime - prevTotalTime) / (float64(diff) / float64(time.Second))
			if curUsagePerc < 0.001 {
				curUsagePerc = 0
			}

			// расчет средней текущей нагрузки через циклический буфер точечных замеров
			lastCpuUsagesIdx++
			lastCpuUsagesIdx %= len(cpuUsagesPerc)
			cpuUsagesPerc[lastCpuUsagesIdx] = curUsagePerc

			avgUsagePerc := 0.0
			avg, cnt := 0.0, 0
			for _, v := range cpuUsagesPerc {
				if v > 0 {
					avg += v
					cnt++
				}
			}
			if cnt > 0 {
				avgUsagePerc = avg / float64(cnt)
			}

			ci.lock.Lock()
			ci.curCpuUsagePerc = int(curUsagePerc)
			ci.avgCpuUsagePerc = int(avgUsagePerc)
			ci.lock.Unlock()
		}

		prevTotalTime = totalTime
	}
}
