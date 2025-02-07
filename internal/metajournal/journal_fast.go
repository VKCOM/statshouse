// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/basictl"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/zeebo/xxh3"
)

type journalEvent struct {
	tlmetadata.Event
	hash xxh3.Uint128
}

type journalOrder struct {
	key     journalEventID
	version int64
}

type JournalFast struct {
	modifyMu sync.Mutex // allows consistency through modifier functions without taking mu.Lock

	mu         sync.RWMutex
	metaLoader MetricsStorageLoader
	applyEvent []ApplyEvent

	metricsDead         bool      // together with this bool
	lastUpdateTime      time.Time // we no more use this information for logic
	stateHash           xxh3.Uint128
	stateHashStr        string
	currentVersion      int64
	journalRequestDelay time.Duration // to avoid overusing of CPU by handling journal updates

	journal map[journalEventID]journalEvent
	order   *btree.BTreeG[journalOrder]

	getJournalResponse     tlmetadata.GetJournalResponsenew // this slice is heavily reused
	clientsMu              sync.Mutex                       // Always taken after mu
	metricsVersionClients3 map[*rpc.HandlerContext]tlstatshouse.GetMetrics3

	sh2       *agent.Agent
	MetricsMu sync.Mutex

	compact bool

	BuiltinLongPollImmediateOK    data_model.ItemValue
	BuiltinLongPollImmediateError data_model.ItemValue
	BuiltinLongPollEnqueue        data_model.ItemValue
	BuiltinLongPollDelayedOK      data_model.ItemValue
	BuiltinLongPollDelayedError   data_model.ItemValue

	BuiltinJournalUpdateOK    data_model.ItemValue
	BuiltinJournalUpdateError data_model.ItemValue

	// custom FS
	writeAt  func(offset int64, data []byte) error
	truncate func(offset int64) error
}

func journalOrderLess(a, b journalOrder) bool {
	return a.version < b.version
}

func MakeJournalFast(journalRequestDelay time.Duration, compact bool, applyEvent []ApplyEvent) *JournalFast {
	return &JournalFast{
		journalRequestDelay:    journalRequestDelay,
		journal:                map[journalEventID]journalEvent{},
		order:                  btree.NewG[journalOrder](32, journalOrderLess), // degree selected by running benchmarks
		applyEvent:             applyEvent,
		metricsVersionClients3: map[*rpc.HandlerContext]tlstatshouse.GetMetrics3{},
		lastUpdateTime:         time.Now(),
		compact:                compact,
		writeAt:                func(offset int64, data []byte) error { return nil },
		truncate:               func(offset int64) error { return nil },
	}
}

// fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666) - recommended flags
// if fp nil, then cache works in memory-only mode
func LoadJournalFastFile(fp *os.File, journalRequestDelay time.Duration, compact bool, applyEvent []ApplyEvent) (*JournalFast, error) {
	c := MakeJournalFast(journalRequestDelay, compact, applyEvent)
	w, t, r, fs := data_model.ChunkedStorageFile(fp)
	c.writeAt = w
	c.truncate = t
	err := c.load(fs, r)
	return c, err
}

func (ms *JournalFast) load(fileSize int64, readAt func(b []byte, offset int64) error) error {
	var explicitNewVersion int64
	src, err := ms.loadImpl(fileSize, readAt, &explicitNewVersion) // in case of error, returns events to apply
	ms.finishUpdateLocked(explicitNewVersion)
	for _, f := range ms.applyEvent {
		f(src)
	}
	return err
}

func (ms *JournalFast) loadImpl(fileSize int64, readAt func(b []byte, offset int64) error, explicitNewVersion *int64) ([]tlmetadata.Event, error) {
	loader := data_model.ChunkedStorageLoader{ReadAt: readAt}
	loader.StartRead(fileSize, data_model.ChunkedMagicJournal)
	var scratch []byte
	var src []tlmetadata.Event
	for {
		chunk, first, err := loader.ReadNext()
		if err != nil {
			return src, err
		}
		if len(chunk) == 0 {
			break
		}
		if first {
			if chunk, err = basictl.LongRead(chunk, explicitNewVersion); err != nil {
				return src, fmt.Errorf("error parsing journal version: %v", err)
			}
		}
		var entry tlmetadata.Event
		for len(chunk) != 0 {
			if chunk, err = entry.ReadBoxed(chunk); err != nil {
				return src, fmt.Errorf("error parsing journal entry: %v", err)
			}
			src = append(src, entry)
			scratch = ms.addEventLocked(scratch, entry)
		}
		ms.finishUpdateLocked(ms.currentVersion)
		for _, f := range ms.applyEvent {
			f(src)
		}
		src = src[:0]
	}
	return src, nil
}

func (ms *JournalFast) Save() error {
	// We exclude writers so that they do not block on Lock() while code below runs in RLock().
	// If we allow this, all new readers (GetValue) block on RLock(), effectively waiting for Save to finish.
	ms.modifyMu.Lock()
	defer ms.modifyMu.Unlock()

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	saver := data_model.ChunkedStorageSaver{
		WriteAt:  ms.writeAt,
		Truncate: ms.truncate,
	}

	chunk := saver.StartWrite(data_model.ChunkedMagicJournal)
	chunk = basictl.LongWrite(chunk, ms.currentVersion) // we need explicit version for compact journal
	// we do not FinishItem after version because version is small
	// we must write in order, because we must deliver them in order during load
	var iteratorError error
	ms.order.Ascend(func(order journalOrder) bool {
		event, ok := ms.journal[order.key]
		if !ok {
			panic("journal order violation - entry not found")
		}
		chunk = event.WriteBoxed(chunk)
		chunk, iteratorError = saver.FinishItem(chunk)
		return iteratorError == nil // ok, save only elements that fit
	})
	if iteratorError != nil {
		return iteratorError
	}
	return saver.FinishWrite(chunk)
}

func (ms *JournalFast) Start(sh2 *agent.Agent, aggLog AggLog, metaLoader MetricsStorageLoader) {
	ms.metaLoader = metaLoader
	ms.sh2 = sh2
	ms.parseDiscCache()
	go ms.goUpdateMetrics(aggLog)
}

func (ms *JournalFast) VersionHash() (int64, string) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.currentVersion, ms.stateHashStr
}

func (ms *JournalFast) versionLocked() int64 {
	return ms.currentVersion
}

func (ms *JournalFast) builtinAddValue(m *data_model.ItemValue, value float64) {
	ms.MetricsMu.Lock()
	defer ms.MetricsMu.Unlock()
	m.AddValue(value)
}

func (ms *JournalFast) parseDiscCache() {
}

func equalWithoutVersionJournalEvent(a, b tlmetadata.Event) bool {
	a.Version = 0
	b.Version = 0
	return a == b
}

func hashJournalEvent(scratch []byte, entry tlmetadata.Event) ([]byte, xxh3.Uint128) {
	scratch = entry.Write(scratch[:0])
	return scratch, xxh3.Hash128(scratch)
}

func (ms *JournalFast) addEventLocked(scratch []byte, entry tlmetadata.Event) []byte {
	if entry.Version <= ms.currentVersion {
		panic("journal order invariant violated - adding old element")
	}
	var hash xxh3.Uint128
	scratch, hash = hashJournalEvent(scratch, entry)
	key := journalEventID{typ: entry.EventType, id: entry.Id}
	old, ok := ms.journal[key] // hash is 0 if not there
	if ok {
		if _, ok = ms.order.Delete(journalOrder{version: old.Version}); !ok {
			panic("journal order invariant violation - not found element being removed")
		}
	}
	ms.stateHash.Hi ^= old.hash.Hi
	ms.stateHash.Lo ^= old.hash.Lo
	ms.stateHash.Hi ^= hash.Hi
	ms.stateHash.Lo ^= hash.Lo
	ms.journal[key] = journalEvent{Event: entry, hash: hash}
	if _, ok = ms.order.ReplaceOrInsert(journalOrder{key: key, version: entry.Version}); ok {
		panic("journal order invariant violation - found element being inserted")
	}
	ms.currentVersion = entry.Version
	return scratch
}

func (ms *JournalFast) finishUpdateLocked(explicitNewVersion int64) {
	if explicitNewVersion > ms.currentVersion {
		ms.currentVersion = explicitNewVersion // we could panic instead, but decided to simplify
	}
	hb := ms.stateHash.Bytes()
	stateHashStr := hex.EncodeToString(hb[:])
	ms.stateHashStr = stateHashStr
	ms.lastUpdateTime = time.Now()
	ms.metricsDead = false
}

func (ms *JournalFast) updateJournal(aggLog AggLog) error {
	_, err := ms.updateJournalIsFinished(aggLog)
	return err
}

// for tests to stop when all events are played back
func (ms *JournalFast) updateJournalIsFinished(aggLog AggLog) (bool, error) {
	ms.mu.RLock()
	isDead := ms.metricsDead
	oldVersion := ms.currentVersion
	ms.mu.RUnlock()

	src, _, err := ms.metaLoader(context.Background(), oldVersion, isDead)
	if err != nil {
		if !isDead {
			func() {
				ms.modifyMu.Lock()
				defer ms.modifyMu.Unlock()
				ms.mu.Lock()
				defer ms.mu.Unlock()
				ms.metricsDead = true
			}()
			ms.broadcastJournal()
		}
		ms.builtinAddValue(&ms.BuiltinJournalUpdateError, 0)
		log.Printf("error updating journal: %v", err)
		if aggLog != nil {
			aggLog("journal_update", "", "error", "", "", "", "", err.Error())
		}
		return false, err
	}
	ms.applyUpdate(src, aggLog)
	return len(src) == 0, nil
}

func compactJournalEvent(event *tlmetadata.Event) bool {
	if event.EventType == format.MetricsGroupEvent || event.EventType == format.NamespaceEvent {
		return true // keep as is, they are tiny
	}
	if event.EventType != format.MetricEvent {
		return false // discard
	}
	value := &format.MetricMetaValue{}
	err := json.Unmarshal([]byte(event.Data), value)
	if err != nil {
		return false // broken, discard
	}
	value.NamespaceID = int32(event.NamespaceId)
	value.Version = event.Version
	value.Name = event.Name
	value.MetricID = int32(event.Id) // TODO - beware!
	value.UpdateTime = event.UpdateTime
	_ = value.RestoreCachedInfo()

	value.Name = "" // restored from event anyway
	value.Version = 0
	value.MetricID = 0
	value.NamespaceID = 0
	switch event.Name {
	case format.StatshouseAgentRemoteConfigMetric, format.StatshouseAggregatorRemoteConfigMetric, format.StatshouseAPIRemoteConfig:
		// keep description)
	default:
		value.Description = ""
	}
	value.Resolution = 0
	value.Weight = 0
	value.StringTopDescription = ""
	cutTags := 0
	for ti := range value.Tags {
		tag := &value.Tags[ti]
		tag.Description = ""
		tag.ValueComments = nil
		if tag.Raw || tag.RawKind != "" || tag.Name != "" {
			cutTags = ti + 1 // keep this tag
		}
	}
	value.Tags = value.Tags[:cutTags]
	for k, v := range value.TagsDraft {
		v.Name = ""
		value.TagsDraft[k] = v
	}
	if !value.HasPercentiles {
		value.Kind = ""
	}
	value.MetricType = ""
	value.PreKeyTagID = ""
	value.PreKeyFrom = 0
	value.SkipMinHost = false
	value.SkipMaxHost = false
	value.SkipSumSquare = false
	value.PreKeyOnly = false
	shortData, err := value.MarshalBinary()
	if err != nil {
		return false // discard
	}
	event.ClearMetadata()
	event.Unused = 0
	event.Data = string(shortData)
	event.UpdateTime = 0 // we do not care
	return true
}

func (ms *JournalFast) applyUpdate(src []tlmetadata.Event, aggLog AggLog) {
	if len(src) == 0 {
		return
	}
	explicitNewVersion := src[len(src)-1].Version // compact journal can throw last item out

	var scratch []byte

	if ms.compact {
		pos := 0
		for _, entry := range src {
			if !compactJournalEvent(&entry) {
				continue
			}
			key := journalEventID{typ: entry.EventType, id: entry.Id}
			old, ok := ms.journal[key] // hash is 0 if not there
			if ok && equalWithoutVersionJournalEvent(old.Event, entry) {
				continue
			}
			src[pos] = entry
			pos++
		}
		src = src[:pos]
	}

	// newEntries can contain the same event several times
	// also for all edits old journals contains this event
	// we want all events once and in order in the journal, hence this code
	var oldVersion int64
	var stateHashStr string
	func() {
		ms.modifyMu.Lock()
		defer ms.modifyMu.Unlock()
		ms.mu.Lock()
		defer ms.mu.Unlock()
		oldVersion = ms.versionLocked()
		for _, entry := range src {
			scratch = ms.addEventLocked(scratch, entry)
		}
		ms.finishUpdateLocked(explicitNewVersion)
		stateHashStr = ms.stateHashStr
	}()

	ms.builtinAddValue(&ms.BuiltinJournalUpdateOK, 0)
	for _, f := range ms.applyEvent {
		f(src)
	}

	if len(src) > 0 {
		lastEntry := src[len(src)-1]
		// TODO - remove this printf in tests
		//log.Printf("Version updated from '%d' to '%d', last entity updated is %s, journal hash is '%s'",
		//	oldVersion, lastEntry.Version, lastEntry.Name, stateHash)
		if aggLog != nil {
			aggLog("journal_update", "", "ok",
				strconv.FormatInt(oldVersion, 10),
				strconv.FormatInt(lastEntry.Version, 10),
				lastEntry.Name,
				stateHashStr,
				"")
		}
	}
	ms.broadcastJournal()
}

func (ms *JournalFast) goUpdateMetrics(aggLog AggLog) {
	backoffTimeout := time.Duration(0)
	for {
		err := ms.updateJournal(aggLog)
		if err == nil {
			backoffTimeout = 0
			time.Sleep(ms.journalRequestDelay) // if aggregator invariants are broken and they reply immediately forever
			continue
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		log.Printf("Failed to update metrics from sqliteengine, will retry: %v", err)
		time.Sleep(backoffTimeout)
	}
}
