// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package commonmetrics

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"sort"
	"strconv"
	"sync"
)

type configHash struct {
	value string
	mu    sync.Mutex
}

var configHashValue configHash

func SetConfigHash(toBeHashing ...hashingAim) {
	hasher := configHashHolder{b: bytes.NewBuffer(nil)}
	for _, h := range toBeHashing {
		hasher = h(hasher)
	}
	newHash := hasher.Hash()
	SetPreparedConfigHash(newHash)
}

// SetPreparedConfigHash sets "config hash" tag in common_uptime metric. Caller should prepare hash in proper format, or use SetConfigHash
func SetPreparedConfigHash(hash string) {
	configHashValue.mu.Lock()
	defer configHashValue.mu.Unlock()
	configHashValue.value = hash
}

func getConfigHash() string {
	configHashValue.mu.Lock()
	defer configHashValue.mu.Unlock()
	return configHashValue.value
}

type hashingAim func(configHashHolder) configHashHolder

// WithFlags conflict with WithFlagsSet
func WithFlags(flags map[string]string) hashingAim {
	return func(cfg configHashHolder) configHashHolder {
		cfg.flags = flags
		return cfg
	}
}

// WithFlagsSet conflict with WithFlags
func WithFlagsSet(set *flag.FlagSet) hashingAim {
	return func(cfg configHashHolder) configHashHolder {
		cfg.flags = map[string]string{}
		set.Visit(func(f *flag.Flag) {
			cfg.flags[f.Name] = f.Value.String()
		})
		return cfg
	}
}

func WithEnvs(envs map[string]string) hashingAim {
	return func(cfg configHashHolder) configHashHolder {
		cfg.envs = envs
		return cfg
	}
}

func WithConfigString(config ...string) hashingAim {
	return func(cfg configHashHolder) configHashHolder {
		cfg.configStrings = config
		return cfg
	}
}

func WithConfigBytes(config ...[]byte) hashingAim {
	return func(cfg configHashHolder) configHashHolder {
		cfg.configBytes = config
		return cfg
	}
}

type configHashHolder struct {
	flags         map[string]string
	envs          map[string]string
	configStrings []string
	configBytes   [][]byte
	b             *bytes.Buffer
}

func (c configHashHolder) writeMap(m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		c.b.WriteString(k)
		c.b.WriteString(m[k])
	}
}

func (c configHashHolder) Hash() string {
	c.b.Reset()

	c.writeMap(c.flags)
	c.writeMap(c.envs)

	growTo := 0
	for _, s := range c.configStrings {
		growTo += len(s)
	}
	for _, b := range c.configBytes {
		growTo += len(b)
	}

	c.b.Grow(growTo)

	for _, s := range c.configStrings {
		c.b.WriteString(s)
	}
	for _, b := range c.configBytes {
		c.b.Write(b)
	}

	sum := sha256.Sum256(c.b.Bytes())
	prefix, _ := strconv.ParseInt(hex.EncodeToString(sum[:])[:8], 16, 64)
	return strconv.FormatUint(uint64(prefix), 10)
}
