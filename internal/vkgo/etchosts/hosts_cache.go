// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package etchosts

import (
	"bytes"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type callback func(hostnameToIP map[string]string)

type HostsCache struct {
	hostsFile string // never changes

	sync.RWMutex
	m        map[string]string // hostname => ip, if nil, first invocation
	lastTime time.Time         // stats of hostsFile just before we read it
	lastSize int64

	callbackMu sync.RWMutex
	callbacks  []callback
}

func NewHostsCache(hostsFile string, cachePeriod time.Duration) *HostsCache {
	hc := &HostsCache{hostsFile: hostsFile}
	hc.parseIfChanged(true)
	go hc.hostsUpdater(cachePeriod)
	return hc
}

func (hc *HostsCache) hostsUpdater(cachePeriod time.Duration) {
	for {
		time.Sleep(cachePeriod) // At start, because hosts was just read by the caller of go hostsUpdater

		hc.parseIfChanged(false)
	}
}

func (hc *HostsCache) Resolve(hostname string) string {
	hc.RLock()
	ip := hc.m[hostname]
	hc.RUnlock()

	return ip
}

func (hc *HostsCache) OnParse(f callback) {
	hc.callbackMu.Lock()
	hc.callbacks = append(hc.callbacks, f)
	hc.callbackMu.Unlock()

	// f could be called twice if background update happens here, but that is good enough
	hc.RLock()
	defer hc.RUnlock()
	f(hc.m)
}

// For tests only
func (hc *HostsCache) ParseForce() {
	hc.parseIfChanged(true)
}

func (hc *HostsCache) parseIfChanged(force bool) {
	lastTime, lastSize := getStamps(hc.hostsFile)

	hc.RLock()
	eq := hc.lastTime == lastTime && hc.lastSize == lastSize
	hc.RUnlock()

	if eq && !force {
		return
	}
	m := readHosts(hc.hostsFile)

	hc.Lock()
	hc.lastTime, hc.lastSize = lastTime, lastSize
	hc.m = m
	hc.Unlock()

	hc.callbackMu.RLock()
	for _, cb := range hc.callbacks {
		cb(m)
	}
	hc.callbackMu.RUnlock()
}

func getStamps(path string) (time.Time, int64) {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}, 0
	}
	return fi.ModTime(), fi.Size()
}

func readHosts(hostsFile string) map[string]string {
	newMap := make(map[string]string)
	body, err := os.ReadFile(hostsFile) // We presume we have enough memory to load hosts file
	if err != nil {
		log.Printf("etchosts.readHosts could not open '%s': %v", hostsFile, err)
		return newMap
	}
	for len(body) != 0 {
		line := body
		nextLineIndex := bytes.IndexByte(body, '\n')
		if nextLineIndex >= 0 {
			line = body[:nextLineIndex]
			body = body[nextLineIndex+1:]
		} else {
			body = nil
		}

		commentIndex := bytes.IndexByte(line, '#')
		if commentIndex >= 0 {
			line = line[:commentIndex]
		}

		// 127.0.0.1       localhost loclahost loclhsot lolcahost
		fields := bytes.Fields(line)

		if len(fields) <= 1 {
			continue
		}
		ips := string(fields[0])

		// ensure that it is IPv4 address because we do not support dual stack in this resolver anyway
		if ip := net.ParseIP(ips); ip == nil || ip.To4() == nil {
			continue
		}

		for _, f := range fields[1:] {
			hostname := string(f)
			newMap[hostname] = ips

		}
	}

	return newMap
}
