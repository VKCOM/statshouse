// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statlogs

import (
	"encoding/binary"
	"log"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/basictl"
)

const (
	DefaultStatsHouseAddr = "127.0.0.1:13337"
	defaultSendPeriod     = 1 * time.Second
	maxPayloadSize        = 1232 // IPv6 mandated minimum MTU size of 1280 (minus 40 byte IPv6 header and 8 byte UDP header)
	tlInt32Size           = 4
	tlInt64Size           = 8
	tlFloat64Size         = 8
	metricsBatchTag       = 0x56580239
	counterFieldsMask     = uint32(1 << 0)
	valueFieldsMask       = uint32(1 << 1)
	uniqueFieldsMask      = uint32(1 << 2)
	tsFieldsMask          = uint32(1 << 4)
	newSemanticFieldsMask = uint32(1 << 31) // this lib uses no sampling, so can claim support of new semantic
	batcnHeaderLen        = 3 * tlInt32Size // tag, fields_mask, # of batches
	maxTags               = 16
)

var (
	globalRegistry = NewRegistry(log.Printf, DefaultStatsHouseAddr, "")
)

type LoggerFunc func(format string, args ...interface{})

// Configure is expected to be called once on app startup.
// Order of Configure/AccessMetricRaw is not important, so AccessMetricRaw can be assigned to global var, then Configure called from main().
// Pass empty address to configure Registry to silently discard all data (like /dev/null).
// If Env is empty (not specified) in call to AccessMetricRaw, env will be used instead.
func Configure(logf LoggerFunc, statsHouseAddr string, env string) {
	globalRegistry.configure(logf, statsHouseAddr, env)
}

// Close registry on app exit, otherwise you risk losing some statistics, often related to reason of app exit
// after Close() no more data will be sent
func Close() error {
	return globalRegistry.Close()
}

// AccessMetricRaw is recommended to be used via helper functions, not directly. Instead of
//
//	statlogs.AccessMetricRaw("packet_size", statlogs.RawTags{Tag1: "ok"}).Value(float64(len(pkg)))
//
// it is better to create a helper function that encapsulates raw access for your metric:
//
//	func RecordPacketSize(ok bool, size int) {
//	    status := "fail"
//	    if ok {
//	        status = "ok"
//	    }
//	    statlogs.AccessMetricRaw("packet_size", statlogs.RawTags{Tag1: status}).Value(float64(size))
//	}
//
//	RecordPacketSize(true, len(pkg))
//
// AccessMetricRaw locks mutex for very short time (map access). Metric functions also lock mutex for very short time.
// so writing statistics cannot block your goroutines.
//
// result of AccessMetricRaw can be saved if map access is too slow for your code, and even can be
// assigned to global variable
//
//	var countPacketOK = statlogs.AccessMetricRaw("foo", statlogs.RawTags{Tag1: "ok"})
//
// so that when packet arrives
//
//	countPacketOK.Count(1)
//
// will be extremely fast
func AccessMetricRaw(metric string, tags RawTags) *Metric {
	return globalRegistry.AccessMetricRaw(metric, tags)
}

// AccessMetric we recommend to use AccessMetricRaw, but if you need to send custom named tags, use this
// usage example:
//
//	statlogs.AccessMetric("packet_size", statlogs.Tags{{"status", "ok"}, {"code", "2"}}).Value(10e3)
func AccessMetric(metric string, tags Tags) *Metric {
	return globalRegistry.AccessMetric(metric, tags)
}

// StartRegularMeasurement will call f once per collection interval with no gaps or drift,
// until StopRegularMeasurement is called with the same ID.
func StartRegularMeasurement(f func(*Registry)) (id int) {
	return globalRegistry.StartRegularMeasurement(f)
}

// StopRegularMeasurement cancels StartRegularMeasurement with specified id
func StopRegularMeasurement(id int) {
	globalRegistry.StopRegularMeasurement(id)
}

// Use only if you are sending metrics to 2 or more statshouses. Otherwise, simply use statlogs.Configure
// Do not forget to Close registry on app exit, otherwise you risk losing some data, often related to reason of app exit
// Pass empty address to configure Registry to silently discard all data (like /dev/null).
func NewRegistry(logf LoggerFunc, statsHouseAddr string, env string) *Registry {
	r := &Registry{
		logf:         logf,
		addr:         statsHouseAddr,
		packetBuf:    make([]byte, batcnHeaderLen, maxPayloadSize), // can grow larger than maxPayloadSize if writing huge header
		close:        make(chan chan struct{}),
		cur:          &Metric{},
		w:            map[metricKey]*Metric{},
		wn:           map[metricKeyNamed]*Metric{},
		env:          env,
		regularFuncs: map[int]func(*Registry){},
	}
	go r.run()
	return r
}

type metricKey struct {
	name string
	tags RawTags
}

type internalTags [16][2]string

type metricKeyNamed struct {
	name string
	tags internalTags
}

type Tags [][2]string

// RawTags should not be used directly; see AccessMetricRaw docs for an example of proper usage.
type RawTags struct {
	Env, Tag1, Tag2, Tag3, Tag4, Tag5, Tag6, Tag7, Tag8, Tag9, Tag10, Tag11, Tag12, Tag13, Tag14, Tag15 string
}

type metricKeyValue struct {
	k metricKey
	v *Metric
}

type metricKeyValueNamed struct {
	k metricKeyNamed
	v *Metric
}

type Registry struct {
	confMu sync.Mutex
	logf   LoggerFunc
	addr   string
	conn   *net.UDPConn

	closeOnce  sync.Once
	closeErr   error
	close      chan chan struct{}
	packetBuf  []byte
	batchCount int
	cur        *Metric

	mu             sync.RWMutex
	w              map[metricKey]*Metric
	r              []metricKeyValue
	wn             map[metricKeyNamed]*Metric
	rn             []metricKeyValueNamed
	env            string // if set, will be put into key0/env
	regularFuncsMu sync.Mutex
	regularFuncs   map[int]func(*Registry)
	nextRegularID  int
}

func (r *Registry) SetEnv(env string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.env = env
}

// see statlogs.Close for documentation
func (r *Registry) Close() error {
	r.closeOnce.Do(func() {
		ch := make(chan struct{})
		r.close <- ch
		<-ch

		r.confMu.Lock()
		defer r.confMu.Unlock()

		if r.conn != nil {
			r.closeErr = r.conn.Close()
		}
	})
	return r.closeErr
}

// see statlogs.StartRegularMeasurement for documentation
func (r *Registry) StartRegularMeasurement(f func(*Registry)) (id int) {
	r.regularFuncsMu.Lock()
	defer r.regularFuncsMu.Unlock()
	r.nextRegularID++
	r.regularFuncs[r.nextRegularID] = f
	return r.nextRegularID
}

// see statlogs.StopRegularMeasurement for documentation
func (r *Registry) StopRegularMeasurement(id int) {
	r.regularFuncsMu.Lock()
	defer r.regularFuncsMu.Unlock()
	delete(r.regularFuncs, id)
}

func (r *Registry) callRegularFuncs(regularCache []func(*Registry)) []func(*Registry) {
	r.regularFuncsMu.Lock()
	for _, f := range r.regularFuncs { // TODO - call in order of registration. Use RB-tree when available
		regularCache = append(regularCache, f)
	}
	r.regularFuncsMu.Unlock()
	defer func() {
		if p := recover(); p != nil {
			r.getLog()("[statlogs] panic inside regular measurement function, ignoring: %v", p)
		}
	}()
	for _, f := range regularCache { // called without locking to prevent deadlock
		f(r)
	}
	return regularCache
}

func (r *Registry) configure(logf LoggerFunc, statsHouseAddr string, env string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.env = env

	r.confMu.Lock()
	defer r.confMu.Unlock()

	r.logf = logf
	r.addr = statsHouseAddr
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			logf("[statlogs] failed to close connection: %v", err)
		}
		r.conn = nil
	}
	if r.addr == "" {
		r.logf("[statlogs] configured with empty address, all statistics will be silently dropped")
	}
}

func (r *Registry) getLog() LoggerFunc {
	r.confMu.Lock()
	defer r.confMu.Unlock()
	return r.logf // return func instead of calling it here to not alter the callstack information in the log
}

func tillNextHalfPeriod(now time.Time) time.Duration {
	return now.Truncate(defaultSendPeriod).Add(defaultSendPeriod * 3 / 2).Sub(now)
}

func (r *Registry) run() {
	var regularCache []func(*Registry)
	tick := time.After(tillNextHalfPeriod(time.Now()))
	for {
		select {
		case now := <-tick:
			regularCache = r.callRegularFuncs(regularCache[:0])
			r.send()
			tick = time.After(tillNextHalfPeriod(now))
		case ch := <-r.close:
			r.send() // last send: we will lose all metrics produced "after"
			close(ch)
			return
		}
	}
}

func (r *Registry) load() ([]metricKeyValue, []metricKeyValueNamed, string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.r, r.rn, r.env
}

func (r *Registry) swapToCur(s *Metric) {
	n := atomicLoadFloat64(&s.atomicCount)
	for !atomicCASFloat64(&s.atomicCount, n, 0) {
		n = atomicLoadFloat64(&s.atomicCount)
	}
	atomicStoreFloat64(&r.cur.atomicCount, n)

	s.mu.Lock()
	defer s.mu.Unlock()

	r.cur.value = append(r.cur.value[:0], s.value...)
	r.cur.unique = append(r.cur.unique[:0], s.unique...)
	r.cur.stop = append(r.cur.stop[:0], s.stop...)
	s.value = s.value[:0]
	s.unique = s.unique[:0]
	s.stop = s.stop[:0]
}

type metricKeyTransport struct {
	name   string
	tags   internalTags
	numSet int
	hasEnv bool
}

func fillTag(k *metricKeyTransport, tagName string, tagValue string) {
	if tagValue == "" || k.numSet >= maxTags { // both checks are not strictly required
		return
	}
	k.tags[k.numSet] = [2]string{tagName, tagValue}
	k.numSet++
	k.hasEnv = k.hasEnv || tagName == "0" || tagName == "env" || tagName == "key0" // TODO - keep only "0", rest are legacy
}

func (r *Registry) send() {
	ss, ssn, env := r.load()
	for _, s := range ss {
		k := metricKeyTransport{name: s.k.name}
		fillTag(&k, "0", s.k.tags.Env)
		fillTag(&k, "1", s.k.tags.Tag1)
		fillTag(&k, "2", s.k.tags.Tag2)
		fillTag(&k, "3", s.k.tags.Tag3)
		fillTag(&k, "4", s.k.tags.Tag4)
		fillTag(&k, "5", s.k.tags.Tag5)
		fillTag(&k, "6", s.k.tags.Tag6)
		fillTag(&k, "7", s.k.tags.Tag7)
		fillTag(&k, "8", s.k.tags.Tag8)
		fillTag(&k, "9", s.k.tags.Tag9)
		fillTag(&k, "10", s.k.tags.Tag10)
		fillTag(&k, "11", s.k.tags.Tag11)
		fillTag(&k, "12", s.k.tags.Tag12)
		fillTag(&k, "13", s.k.tags.Tag13)
		fillTag(&k, "14", s.k.tags.Tag14)
		fillTag(&k, "15", s.k.tags.Tag15)
		if !k.hasEnv {
			fillTag(&k, "0", env)
		}

		r.swapToCur(s.v)
		if n := atomicLoadFloat64(&r.cur.atomicCount); n > 0 {
			r.sendCounter(&k, "", n, 0)
		}
		r.sendValues(&k, "", 0, 0, r.cur.value)
		r.sendUniques(&k, "", 0, 0, r.cur.unique)
		for _, skey := range r.cur.stop {
			r.sendCounter(&k, skey, 1, 0)
		}
	}
	for _, s := range ssn {
		k := metricKeyTransport{name: s.k.name}
		for _, v := range s.k.tags {
			fillTag(&k, v[0], v[1])
		}
		if !k.hasEnv {
			fillTag(&k, "0", env)
		}

		r.swapToCur(s.v)
		if n := atomicLoadFloat64(&r.cur.atomicCount); n > 0 {
			r.sendCounter(&k, "", n, 0)
		}
		r.sendValues(&k, "", 0, 0, r.cur.value)
		r.sendUniques(&k, "", 0, 0, r.cur.unique)
		for _, skey := range r.cur.stop {
			r.sendCounter(&k, skey, 1, 0)
		}
	}

	r.flush()
}

func (r *Registry) sendCounter(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32) {
	_ = r.writeHeader(k, skey, counter, tsUnixSec, counterFieldsMask|newSemanticFieldsMask, 0)
}

func (r *Registry) sendUniques(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []int64) {
	fieldsMask := uniqueFieldsMask | newSemanticFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := r.writeHeader(k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlInt64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlInt64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		r.packetBuf = basictl.NatWrite(r.packetBuf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			r.packetBuf = basictl.LongWrite(r.packetBuf, values[i])
		}
		values = values[writeCount:]
	}
}

func (r *Registry) sendValues(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, values []float64) {
	fieldsMask := valueFieldsMask | newSemanticFieldsMask
	if counter != 0 && counter != float64(len(values)) {
		fieldsMask |= counterFieldsMask
	}
	for len(values) > 0 {
		left := r.writeHeader(k, skey, counter, tsUnixSec, fieldsMask, tlInt32Size+tlFloat64Size)
		if left < 0 {
			return // header did not fit into empty buffer
		}
		writeCount := 1 + left/tlFloat64Size
		if writeCount > len(values) {
			writeCount = len(values)
		}
		r.packetBuf = basictl.NatWrite(r.packetBuf, uint32(writeCount))
		for i := 0; i < writeCount; i++ {
			r.packetBuf = basictl.DoubleWrite(r.packetBuf, values[i])
		}
		values = values[writeCount:]
	}
}

func (r *Registry) flush() {
	if r.batchCount <= 0 {
		return
	}
	binary.LittleEndian.PutUint32(r.packetBuf, metricsBatchTag)
	binary.LittleEndian.PutUint32(r.packetBuf[tlInt32Size:], 0) // fields_mask
	binary.LittleEndian.PutUint32(r.packetBuf[2*tlInt32Size:], uint32(r.batchCount))
	data := r.packetBuf
	r.packetBuf = r.packetBuf[:batcnHeaderLen]
	r.batchCount = 0

	r.confMu.Lock()
	defer r.confMu.Unlock()

	if r.conn == nil && r.addr != "" {
		conn, err := net.Dial("udp", r.addr)
		if err != nil {
			r.logf("[statlogs] failed to dial statshouse: %v", err) // not using getLog() because confMu is already locked
			return
		}
		r.conn = conn.(*net.UDPConn)
	}
	if r.conn != nil && r.addr != "" {
		_, err := r.conn.Write(data)
		if err != nil {
			r.logf("[statlogs] failed to send data to statshouse: %v", err) // not using getLog() because confMu is already locked
		}
	}
}

func (r *Registry) writeHeaderImpl(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32) {
	if tsUnixSec != 0 {
		fieldsMask |= tsFieldsMask
	}
	r.packetBuf = basictl.NatWrite(r.packetBuf, fieldsMask)
	r.packetBuf = basictl.StringWriteTruncated(r.packetBuf, k.name)
	// can write more than maxTags pairs, but this is allowed by statshouse
	numSet := k.numSet
	if skey != "" {
		numSet++
	}
	r.packetBuf = basictl.NatWrite(r.packetBuf, uint32(numSet))
	if skey != "" {
		r.writeTag("_s", skey)
	}
	for i := 0; i < k.numSet; i++ {
		r.writeTag(k.tags[i][0], k.tags[i][1])
	}
	if fieldsMask&counterFieldsMask != 0 {
		r.packetBuf = basictl.DoubleWrite(r.packetBuf, counter)
	}
	if fieldsMask&tsFieldsMask != 0 {
		r.packetBuf = basictl.NatWrite(r.packetBuf, tsUnixSec)
	}
}

// returns space reserve or <0 if did not fit
func (r *Registry) writeHeader(k *metricKeyTransport, skey string, counter float64, tsUnixSec uint32, fieldsMask uint32, reserveSpace int) int {
	wasLen := len(r.packetBuf)
	r.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
	left := maxPayloadSize - len(r.packetBuf) - reserveSpace
	if left >= 0 {
		r.batchCount++
		return left
	}
	if wasLen != batcnHeaderLen {
		r.packetBuf = r.packetBuf[:wasLen]
		r.flush()
		r.writeHeaderImpl(k, skey, counter, tsUnixSec, fieldsMask)
		left = maxPayloadSize - len(r.packetBuf) - reserveSpace
		if left >= 0 {
			r.batchCount++
			return left
		}
	}
	r.packetBuf = r.packetBuf[:wasLen]
	r.getLog()("[statlogs] metric %q payload too big to fit into packet, discarding", k.name)
	return -1
}

func (r *Registry) writeTag(tagName string, tagValue string) {
	r.packetBuf = basictl.StringWriteTruncated(r.packetBuf, tagName)
	r.packetBuf = basictl.StringWriteTruncated(r.packetBuf, tagValue)
}

// see statlogs.AccessMetricRaw for documentation
func (r *Registry) AccessMetricRaw(metric string, tags RawTags) *Metric {
	// We must do absolute minimum of work here
	k := metricKey{name: metric, tags: tags}
	r.mu.RLock()
	e, ok := r.w[k]
	r.mu.RUnlock()
	if ok {
		return e
	}

	r.mu.Lock()
	e, ok = r.w[k]
	if !ok {
		e = &Metric{}
		r.w[k] = e
		r.r = append(r.r, metricKeyValue{k: k, v: e})
	}
	r.mu.Unlock()
	return e
}

func (r *Registry) AccessMetric(metric string, tagsList Tags) *Metric {
	// We must do absolute minimum of work here
	k := metricKeyNamed{name: metric}
	copy(k.tags[:], tagsList)

	r.mu.RLock()
	e, ok := r.wn[k]
	r.mu.RUnlock()
	if ok {
		return e
	}

	r.mu.Lock()
	e, ok = r.wn[k]
	if !ok {
		e = &Metric{}
		r.wn[k] = e
		r.rn = append(r.rn, metricKeyValueNamed{k: k, v: e})
	}
	r.mu.Unlock()
	return e
}

// see AccessMetricRaw docs for documentation
type Metric struct {
	// Place atomics first to ensure proper alignment, see https://pkg.go.dev/sync/atomic#pkg-note-BUG
	atomicCount uint64

	mu     sync.Mutex
	value  []float64
	unique []int64
	stop   []string
}

func (m *Metric) Count(n float64) {
	c := atomicLoadFloat64(&m.atomicCount)
	for !atomicCASFloat64(&m.atomicCount, c, c+n) {
		c = atomicLoadFloat64(&m.atomicCount)
	}
}

func (m *Metric) Value(value float64) {
	m.mu.Lock()
	m.value = append(m.value, value)
	m.mu.Unlock()
}

func (m *Metric) Values(values []float64) {
	m.mu.Lock()
	m.value = append(m.value, values...)
	m.mu.Unlock()
}

func (m *Metric) Unique(value int64) {
	m.mu.Lock()
	m.unique = append(m.unique, value)
	m.mu.Unlock()
}

func (m *Metric) Uniques(values []int64) {
	m.mu.Lock()
	m.unique = append(m.unique, values...)
	m.mu.Unlock()
}

func (m *Metric) StringTop(value string) {
	m.mu.Lock()
	m.stop = append(m.stop, value)
	m.mu.Unlock()
}

func (m *Metric) StringsTop(values []string) {
	m.mu.Lock()
	m.stop = append(m.stop, values...)
	m.mu.Unlock()
}

func atomicLoadFloat64(addr *uint64) float64 {
	return math.Float64frombits(atomic.LoadUint64(addr))
}

func atomicStoreFloat64(addr *uint64, val float64) {
	atomic.StoreUint64(addr, math.Float64bits(val))
}

func atomicCASFloat64(addr *uint64, old float64, new float64) (swapped bool) {
	return atomic.CompareAndSwapUint64(addr, math.Float64bits(old), math.Float64bits(new))
}
