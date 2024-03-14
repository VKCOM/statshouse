// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package srvfunc

import (
	"log"
	"sync"
	"syscall"
	"time"
)

type (
	// CPUInfo считает статистику по использованию CPU глобально по ОС
	CPUInfo struct {
		lock   sync.RWMutex
		init   sync.Once
		rUsage syscall.Rusage

		// нагрузка по CPU: средняя за время время (avg) и средняя за последнее время (cur)
		avgCpuUsagePerc int
		curCpuUsagePerc int
	}
)

// MakeCPUInfo инициализирует сбор статистики
func MakeCPUInfo() *CPUInfo {
	ci := &CPUInfo{}
	return ci
}

// GetSelfCpuUsage возвращает статистику по использованию CPU текущим процессом:
//
//	среднее и последнее использование в % (100 - полностью занято 1 ядро, 800 - полностью заняты 8 ядер и т.п.)
func (ci *CPUInfo) GetSelfCpuUsage() (avgPerc int, curPerc int) {
	ci.init.Do(func() {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("GetSelfCpuUsage panic", r)
				}
			}()
			ci.thisProcessCpuUsageLoop()
		}()
	})
	ci.lock.RLock()
	defer ci.lock.RUnlock()

	return ci.avgCpuUsagePerc, ci.curCpuUsagePerc
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
