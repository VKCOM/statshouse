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
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/mailru/easyjson"
	"github.com/zeebo/xxh3"
	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/rpc"
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
	lastKnownVersion    int64
	loaderVersion       int64
	journalRequestDelay time.Duration // to avoid overusing of CPU by handling journal updates

	journal map[journalEventID]journalEvent
	order   *btree.BTreeG[journalOrder]

	clientsMu              sync.Mutex // Always taken after mu
	metricsVersionClients3 map[*rpc.HandlerContext]tlstatshouse.GetMetrics3

	sh2       *agent.Agent
	MetricsMu sync.Mutex

	compact        bool // never changed, not protected by mu
	dumpPathPrefix string

	// periodic saving
	lastSavedVersion     int64
	periodicSaveInterval time.Duration // we run between one and two times this interval

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

func MetricMetaFromEvent(e tlmetadata.Event) (*format.MetricMetaValue, error) {
	value := &format.MetricMetaValue{}
	err := easyjson.Unmarshal([]byte(e.Data), value) // TODO - use unsafe?
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal metric %d %s: %v", e.Id, e.Name, err)
	}
	value.NamespaceID = int32(e.NamespaceId)
	value.Version = e.Version
	value.Name = e.Name
	if e.Id < math.MinInt32 || e.Id > math.MaxInt32 {
		return nil, fmt.Errorf("metric ID %d assigned by metaengine does not fit into int32 for metric %q", e.Id, e.Name)
	}
	value.MetricID = int32(e.Id)
	value.UpdateTime = e.UpdateTime
	_ = value.RestoreCachedInfo() // TODO: older agents can get errors here, so we decided to ignore
	return value, nil
}

func journalOrderLess(a, b journalOrder) bool {
	return a.version < b.version
}

func MakeJournalFast(journalRequestDelay time.Duration, compact bool, applyEvent []ApplyEvent) *JournalFast {
	return &JournalFast{
		periodicSaveInterval:   time.Hour,
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

func LoadJournalFastSlice(fp *[]byte, journalRequestDelay time.Duration, compact bool, applyEvent []ApplyEvent) (*JournalFast, error) {
	c := MakeJournalFast(journalRequestDelay, compact, applyEvent)
	w, t, r, fs := data_model.ChunkedStorageSlice(fp)
	c.writeAt = w
	c.truncate = t
	err := c.load(fs, r)
	return c, err
}

func (ms *JournalFast) SetDumpPathPrefix(dumpPathPrefix string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.dumpPathPrefix = dumpPathPrefix
}

func (ms *JournalFast) applyEvents(src []tlmetadata.Event) {
	if len(src) == 0 {
		return
	}
	for _, f := range ms.applyEvent {
		f(src)
	}
}

func (ms *JournalFast) load(fileSize int64, readAt func(b []byte, offset int64) error) error {
	var loaderVersion, lastEventVersion int64
	src, err := ms.loadImpl(fileSize, readAt, &loaderVersion, &lastEventVersion)     // in case of error, returns events to apply
	if lastEventVersion == ms.currentVersion && loaderVersion >= ms.currentVersion { // fully read journal, so can believe loader version
		ms.loaderVersion = loaderVersion
	} else {
		ms.loaderVersion = ms.currentVersion // load again from the last event we read
	}
	ms.finishUpdateLocked()
	ms.lastKnownVersion = ms.currentVersion
	ms.applyEvents(src)
	return err
}

func (ms *JournalFast) loadImpl(fileSize int64, readAt func(b []byte, offset int64) error, loaderVersion *int64, lastEventVersion *int64) ([]tlmetadata.Event, error) {
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
			if chunk, err = basictl.LongRead(chunk, loaderVersion); err != nil {
				return src, fmt.Errorf("error parsing journal loader version: %v", err)
			}
			if chunk, err = basictl.LongRead(chunk, lastEventVersion); err != nil {
				return src, fmt.Errorf("error parsing journal last event version: %v", err)
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
		ms.finishUpdateLocked()
		ms.applyEvents(src)
		src = src[:0]
	}
	return src, nil
}

func (ms *JournalFast) Save() (bool, int64, error) {
	// We exclude writers so that they do not block on Lock() while code below runs in RLock().
	// If we allow this, all new readers (GetValue) block on RLock(), effectively waiting for Save to finish.
	ms.modifyMu.Lock()
	defer ms.modifyMu.Unlock()

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// If the journal is not changed, do not save it
	if ms.lastSavedVersion == ms.currentVersion {
		return false, ms.currentVersion, nil
	}
	saver := data_model.ChunkedStorageSaver{
		WriteAt:  ms.writeAt,
		Truncate: ms.truncate,
	}
	err := ms.save(&saver, 0)
	if err != nil {
		return false, ms.currentVersion, err
	}
	ms.lastSavedVersion = ms.currentVersion
	return true, ms.currentVersion, nil
}

func (ms *JournalFast) save(saver *data_model.ChunkedStorageSaver, maxChunkSize int) error {
	chunk := saver.StartWrite(data_model.ChunkedMagicJournal, maxChunkSize)
	chunk = basictl.LongWrite(chunk, ms.loaderVersion)  // we need explicit version, because compact journal skips many events, and we must not ask them again from loader
	chunk = basictl.LongWrite(chunk, ms.currentVersion) // we must not use loaderVersion if file tail with some events was corrupted/cut, so we remember it
	// we do not FinishItem after version because version is small
	// we must write in order, because we must deliver them in order during load
	var iteratorError error
	ms.order.Ascend(func(order journalOrder) bool {
		event, ok := ms.journal[order.key]
		if !ok {
			panic(fmt.Sprintf("journal order violation - entry not found %s", order.key.key()))
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

func (ms *JournalFast) dump() error {
	// We exclude writers so that they do not block on Lock() while code below runs in RLock().
	// If we allow this, all new readers (GetValue) block on RLock(), effectively waiting for Save to finish.
	ms.modifyMu.Lock()
	defer ms.modifyMu.Unlock()

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if ms.dumpPathPrefix == "" {
		return nil
	}
	fp, err := os.OpenFile(fmt.Sprintf("%s-%d-%s.dump", ms.dumpPathPrefix, ms.currentVersion, ms.stateHashStr), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()
	w, t, _, _ := data_model.ChunkedStorageFile(fp)

	saver := data_model.ChunkedStorageSaver{
		WriteAt:  w,
		Truncate: t,
	}
	return ms.save(&saver, 0)
}

func (ms *JournalFast) Compare(ms2 *JournalFast) {
	var list1 []journalOrder
	var list2 []journalOrder
	ms.order.Ascend(func(order journalOrder) bool {
		list1 = append(list1, order)
		return true
	})
	ms2.order.Ascend(func(order journalOrder) bool {
		list2 = append(list2, order)
		return true
	})
	if len(list1) != len(list2) {
		fmt.Printf("list1 and list2 length mismatch %d %d\n", len(list1), len(list2))
		return
	}
	found := 0
	for i, l1 := range list1 {
		if found > 10 {
			break
		}
		l2 := list2[i]
		if l1 != l2 {
			fmt.Printf("order %d diff %v %v\n", i, l1, l2)
			found++
		}
		el1 := ms.journal[l1.key]
		el2 := ms2.journal[l2.key]
		if el1 != el2 {
			fmt.Printf("element %d diff %v %v\n", i, el1, el2)
			found++
		}
	}
}

func (ms *JournalFast) Start(sh2 *agent.Agent, aggLog AggLog, metaLoader MetricsStorageLoader) {
	ms.metaLoader = metaLoader
	ms.sh2 = sh2
	ms.parseDiscCache()
	go ms.goUpdateMetrics(aggLog)
}

func (ms *JournalFast) VersionHash() (currentVersion int64, stateHashStr string) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.currentVersion, ms.stateHashStr
}

func (ms *JournalFast) LastKnownVersion() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.lastKnownVersion
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

func hashWithoutVersionJournalEvent(scratch []byte, entry tlmetadata.Event) ([]byte, xxh3.Uint128) {
	entry.Version = 0 // in accordance with code above
	scratch = entry.Write(scratch[:0])
	return scratch, xxh3.Hash128(scratch)
}

func (ms *JournalFast) addEventLocked(scratch []byte, entry tlmetadata.Event) []byte {
	if entry.Version <= ms.currentVersion {
		panic(fmt.Sprintf("journal order invariant violated - adding old element: %d <= %d", entry.Version, ms.currentVersion))
	}
	var hash xxh3.Uint128
	scratch, hash = hashWithoutVersionJournalEvent(scratch, entry)
	key := journalEventID{typ: entry.EventType, id: entry.Id}
	old, ok := ms.journal[key] // hash is 0 if not there
	if ok {
		if _, ok = ms.order.Delete(journalOrder{version: old.Version}); !ok {
			panic(fmt.Sprintf("journal order invariant violation - not found element being removed: %d", old.Version))
		}
	}
	ms.stateHash.Hi ^= old.hash.Hi
	ms.stateHash.Lo ^= old.hash.Lo
	ms.stateHash.Hi ^= hash.Hi
	ms.stateHash.Lo ^= hash.Lo
	ms.journal[key] = journalEvent{Event: entry, hash: hash}
	if _, ok = ms.order.ReplaceOrInsert(journalOrder{key: key, version: entry.Version}); ok {
		panic(fmt.Sprintf("journal order invariant violation - found element being inserted: %s %d", key.key(), entry.Version))
	}
	ms.currentVersion = entry.Version
	return scratch
}

func (ms *JournalFast) finishUpdateLocked() {
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
	loaderVersion := ms.loaderVersion
	ms.mu.RUnlock()

	src, lastKnownVersion, err := ms.metaLoader(context.Background(), loaderVersion, isDead)
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
	loaderFinished := len(src) == 0
	dump := false
	for i, e := range src {
		if e.EventType == format.MetricEvent && e.Name == format.StatshouseJournalDump {
			dump = true
			src = src[:i+1] // cut after dump event (if before, will loop forever). We drop the rest events for simplicity here
			break
		}
	}
	ms.applyUpdate(src, lastKnownVersion, aggLog)
	if dump {
		_ = ms.dump()
	}
	return loaderFinished, nil
}

func compactJournalEvent(event *tlmetadata.Event) (bool, error) {
	switch event.EventType {
	case format.DashboardEvent, format.PromConfigEvent:
		return false, nil // discard
	case format.MetricsGroupEvent, format.NamespaceEvent:
		return true, nil // keep
	case format.MetricEvent:
		break
	default:
		return true, nil // keep future event types, they must be small at first
	}
	value, err := MetricMetaFromEvent(*event)
	if err != nil {
		return true, err // broken, keep original
	}
	valueDeepCopyOriginal, err := MetricMetaFromEvent(*event)
	if err != nil {
		return true, err // broken, keep original
	}
	format.MakeCompactMetric(value)
	// shortData, err := value.MarshalBinary() - easyjson does not sort TagDrafts, so compact journal becomes indeterministic
	shortData, err := json.Marshal(value)
	if err != nil {
		return true, err // broken, keep original
	}
	event2 := *event
	event2.ClearMetadata()
	event2.Unused = 0
	event2.Data = string(shortData)
	event2.UpdateTime = 0 // we do not care
	value, err = MetricMetaFromEvent(event2)
	if err != nil {
		return true, err // broken, keep original
	}
	if !format.SameCompactMetric(value, valueDeepCopyOriginal) {
		return true, err // broken, keep original
	}
	*event = event2
	return true, nil
}

func (ms *JournalFast) applyUpdate(src []tlmetadata.Event, lastKnownVersion int64, aggLog AggLog) {
	if len(src) == 0 {
		return
	}
	newLoaderVersion := src[len(src)-1].Version // compact journal can throw last item out

	var scratch []byte

	if ms.compact {
		pos := 0
		for _, entry := range src {
			ok, err := compactJournalEvent(&entry)
			if err == nil && !ok { // discard
				continue
			}
			if err != nil { // keep, write metrics

			}
			key := journalEventID{typ: entry.EventType, id: entry.Id}
			old, ok := ms.journal[key]
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
		ms.finishUpdateLocked()
		ms.loaderVersion = newLoaderVersion
		ms.lastKnownVersion = lastKnownVersion
		stateHashStr = ms.stateHashStr
	}()

	ms.builtinAddValue(&ms.BuiltinJournalUpdateOK, 0)
	ms.applyEvents(src)
	if len(src) == 0 {
		return
	}
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

func (ms *JournalFast) StartPeriodicSaving() {
	go ms.goPeriodicSaving()
}

func (ms *JournalFast) goPeriodicSaving() {
	for {
		sleepDuration := ms.periodicSaveInterval + time.Duration(rand.Intn(int(ms.periodicSaveInterval)))
		time.Sleep(sleepDuration)
		ok, version, err := ms.Save()
		if err != nil {
			log.Printf("Periodic journal save failed: %v", err)
		} else if ok {
			log.Printf("Periodic journal save completed, new version %d", version)
		}
	}
}
