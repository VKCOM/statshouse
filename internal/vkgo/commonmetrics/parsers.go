// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package commonmetrics

import "sync"

type parserType int

const (
	Environment parserType = iota
	Service
	Cluster
	DataCenter
	Commit
	CommitTime
	Branch
	BuildID
	BuildName
	BuildTime
)

var (
	parsers    = &parsersRegister{p: make(map[parserType]Parser)}
	noopParser = func() string { return "" }
)

type Parser func() string

type parsersRegister struct {
	mu sync.RWMutex
	p  map[parserType]Parser
}

func (p *parsersRegister) get(name parserType) Parser {
	p.mu.RLock()
	parser, ok := p.p[name]
	p.mu.RUnlock()

	if !ok {
		return noopParser
	}
	return parser
}

func (p *parsersRegister) set(name parserType, parser Parser) {
	p.mu.Lock()
	p.p[name] = parser
	p.mu.Unlock()
}

func RegisterParser(name parserType, parser Parser) {
	parsers.set(name, parser)
}
