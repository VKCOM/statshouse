// Copyright 2022 V Kontakte LLC
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

const (
	cachePeriod = time.Second
)

var (
	hostsFile           = "/etc/hosts" // ok it is not a constant, but it is for tests
	runHostsUpdaterOnce sync.Once
)

var (
	callbacks  []callback
	callbackMu sync.RWMutex
)

var hostsCache = struct {
	m        map[string]string // hostname => ip, if nil, first invocation
	lastStat os.FileInfo       // stats of hostsFile just before we read it
	sync.RWMutex
}{}

func initCacheIfNeed() {
	if !isAlreadyInitialized() {
		hostsCache.Lock()
		defer hostsCache.Unlock()

		if hostsCache.m != nil {
			return
		}

		hostsCache.lastStat, _ = os.Stat(hostsFile)
		hostsCache.m = readHosts()
		runHostsUpdaterOnce.Do(func() { go hostsUpdater() })
	}
}

func isAlreadyInitialized() bool {
	hostsCache.RLock()
	defer hostsCache.RUnlock()
	return hostsCache.m != nil
}

func readHosts() map[string]string {
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

	callbackMu.RLock()
	for _, cb := range callbacks {
		cb(newMap)
	}
	callbackMu.RUnlock()

	return newMap
}

func hostsUpdater() {
	for {
		time.Sleep(cachePeriod) // At start, because hosts was just read by the caller of go hostsUpdater
		stat, _ := os.Stat(hostsFile)
		hostsCache.RLock()
		if (stat == nil && hostsCache.lastStat == nil) || (stat != nil && hostsCache.lastStat != nil && hostsCache.lastStat.ModTime() == stat.ModTime() && hostsCache.lastStat.Size() == stat.Size()) {
			hostsCache.RUnlock()
			continue
		}
		hostsCache.RUnlock()
		newMap := readHosts() // TODO: need to lock?
		hostsCache.Lock()
		hostsCache.lastStat = stat
		hostsCache.m = newMap
		hostsCache.Unlock()
	}
}

func Resolve(hostname string) string {
	initCacheIfNeed()

	hostsCache.RLock()
	ip := hostsCache.m[hostname]
	hostsCache.RUnlock()

	return ip
}

func Parse() {
	initCacheIfNeed()
}

// SetFile sets the path to the hosts file to parse. Non thread safe.
func SetFile(path string) {
	hostsFile = path
}

func OnParse(f callback) {
	callbackMu.Lock()
	defer callbackMu.Unlock()

	callbacks = append(callbacks, f)
}

func ParseForce() {
	hostsCache.Lock()
	defer hostsCache.Unlock()

	hostsCache.lastStat, _ = os.Stat(hostsFile)
	hostsCache.m = readHosts()
}
