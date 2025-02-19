// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/zeebo/xxh3"
)

var errDeadMetrics = errors.New("metrics update from storage is dead")

type AggLog func(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string)

type versionClient struct {
	expectedVersion int64
	ch              chan struct{}
}

type journalEventID struct {
	typ int32
	id  int64
}

type MetricsStorageLoader func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error)

type ApplyEvent func(newEntries []tlmetadata.Event)

type Journal struct {
	mu         sync.RWMutex
	dc         *pcache.DiskCache
	metaLoader MetricsStorageLoader
	namespace  string
	applyEvent []ApplyEvent

	metricsDead          bool      // together with this bool
	lastUpdateTime       time.Time // we no more use this information for logic
	stateHashStr         string
	stateXXHash3Str      string
	currentVersion       int64
	stopWriteToDiscCache bool
	journalRequestDelay  time.Duration // to avoid overusing of CPU by handling journal updates

	journal []tlmetadata.Event

	clientsMu              sync.Mutex // Always taken after mu
	metricsVersionClients3 map[*rpc.HandlerContext]tlstatshouse.GetMetrics3

	sh2       *agent.Agent
	MetricsMu sync.Mutex

	BuiltinLongPollImmediateOK    data_model.ItemValue
	BuiltinLongPollImmediateError data_model.ItemValue
	BuiltinLongPollEnqueue        data_model.ItemValue
	BuiltinLongPollDelayedOK      data_model.ItemValue
	BuiltinLongPollDelayedError   data_model.ItemValue

	BuiltinJournalUpdateOK    data_model.ItemValue
	BuiltinJournalUpdateError data_model.ItemValue
}

func MakeJournal(namespaceSuffix string, journalRequestDelay time.Duration, dc *pcache.DiskCache, applyEvent []ApplyEvent) *Journal {
	return &Journal{
		dc:                     dc,
		namespace:              data_model.JournalDiskNamespace + namespaceSuffix,
		journalRequestDelay:    journalRequestDelay,
		applyEvent:             applyEvent,
		metricsVersionClients3: map[*rpc.HandlerContext]tlstatshouse.GetMetrics3{},
		lastUpdateTime:         time.Now(),
	}
}

func (ms *Journal) CancelHijack(hctx *rpc.HandlerContext) {
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	delete(ms.metricsVersionClients3, hctx)
}

func (ms *Journal) Start(sh2 *agent.Agent, aggLog AggLog, metaLoader MetricsStorageLoader) {
	ms.metaLoader = metaLoader
	ms.sh2 = sh2
	ms.parseDiscCache()
	go ms.goUpdateMetrics(aggLog)
}

func (ms *Journal) VersionHash() (int64, string, string) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.currentVersion, ms.stateHashStr, ms.stateXXHash3Str
}

func (ms *Journal) versionLocked() int64 {
	return ms.currentVersion
}

func (ms *Journal) LoadJournal(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
	return ms.metaLoader(ctx, lastVersion, returnIfEmpty)
}

func (ms *Journal) builtinAddValue(m *data_model.ItemValue, value float64) {
	ms.MetricsMu.Lock()
	defer ms.MetricsMu.Unlock()
	m.AddValue(value)
}

func (ms *Journal) parseDiscCache() {
	if ms.dc == nil {
		return
	}
	// TODO - harmonize with goUpdateMetrics
	list, err := ms.dc.List(ms.namespace)
	if err != nil {
		log.Printf("error loading metric journal: %v", err)
		return
	}
	var journal2 []tlmetadata.Event
	for _, l := range list {
		value := tlmetadata.Event{}
		if _, err := value.ReadBoxed(l.Value); err != nil {
			log.Printf("clearing journal, error parsing journal entry: %v", err)
			_ = ms.dc.EraseNamespace(ms.namespace) // we read items out of order, so have to clear both disk
			journal2 = journal2[:0]                // and entities we read so far
			break                                  // we still want correct hash
		}
		journal2 = append(journal2, value)
	}
	sort.Slice(journal2, func(i, j int) bool {
		return journal2[i].Version < journal2[j].Version
	})
	ms.journal = journal2
	ms.currentVersion, ms.stateHashStr, ms.stateXXHash3Str = calculateVersionStateHashLocked(journal2)
	for _, f := range ms.applyEvent {
		f(journal2)
	}
	log.Printf("Loaded metric storage version %d, journal hash is %s xxhash3 is %s", ms.versionLocked(), ms.stateHashStr, ms.stateXXHash3Str)
}

func calculateVersionStateHashLocked(events []tlmetadata.Event) (int64, string, string) {
	var stateHash xxh3.Uint128
	var scratch []byte
	for _, entry := range events {
		var hash xxh3.Uint128
		scratch, hash = hashWithoutVersionJournalEvent(scratch, entry)
		stateHash.Hi ^= hash.Hi
		stateHash.Lo ^= hash.Lo
	}
	hb := stateHash.Bytes()
	stateXXHash3Str := hex.EncodeToString(hb[:])

	version := int64(0)
	if len(events) > 0 {
		version = events[len(events)-1].Version
	}
	r := &tlmetadata.GetJournalResponsenew{Events: events}
	bytes := r.Write(nil, 0)
	hash := sha1.Sum(bytes)
	return version, hex.EncodeToString(hash[:]), stateXXHash3Str
}

func (ms *Journal) updateJournal(aggLog AggLog) error {
	_, err := ms.updateJournalIsFinished(aggLog)
	return err
}

// for tests to stop when all events are played back
func (ms *Journal) updateJournalIsFinished(aggLog AggLog) (bool, error) {
	ms.mu.RLock()
	isDead := ms.metricsDead
	stopWriteToDiscCache := ms.stopWriteToDiscCache
	oldJournal := ms.journal
	oldVersion := ms.versionLocked()
	ms.mu.RUnlock()

	src, _, err := ms.LoadJournal(context.Background(), oldVersion, isDead)
	if err != nil {
		if !isDead {
			ms.mu.Lock()
			ms.metricsDead = true
			ms.mu.Unlock()
			ms.broadcastJournal()
		}
		ms.builtinAddValue(&ms.BuiltinJournalUpdateError, 0)
		log.Printf("error updating journal: %v", err)
		if aggLog != nil {
			aggLog("journal_update", "", "error", "", "", "", "", err.Error())
		}
		return false, err
	}
	newJournal := updateEntriesJournal(oldJournal, src)
	currentVersion, stateHashStr, stateXXHash3Str := calculateVersionStateHashLocked(newJournal)

	// TODO - check invariants here before saving

	ms.builtinAddValue(&ms.BuiltinJournalUpdateOK, 0)
	for _, f := range ms.applyEvent {
		f(src)
	}

	if ms.dc != nil && !stopWriteToDiscCache {
		for _, e := range src {
			buf := e.WriteBoxed(nil)
			key := journalEventID{
				typ: e.EventType,
				id:  e.Id,
			}
			err := ms.dc.Set(ms.namespace, key.key(), buf, time.Now(), 0)
			if err != nil {
				stopWriteToDiscCache = true // we write in order, so events written so far are valid sub journal
				log.Printf("cannot save journal entry %q: %v", e.Name, err)
				break
			}
		}
	}

	if len(newJournal) > 0 {
		lastEntry := newJournal[len(newJournal)-1]
		// TODO - remove this printf in tests
		//log.Printf("Version updated from '%d' to '%d', last entity updated is %s, journal hash is '%s'",
		//	oldVersion, lastEntry.Version, lastEntry.Name, stateHash)
		if aggLog != nil {
			aggLog("journal_update", "", "ok",
				strconv.FormatInt(oldVersion, 10),
				strconv.FormatInt(lastEntry.Version, 10),
				lastEntry.Name,
				stateHashStr,
				stateXXHash3Str)
		}
	}

	ms.mu.Lock()
	ms.journal = newJournal
	ms.stateHashStr = stateHashStr
	ms.stateXXHash3Str = stateXXHash3Str
	ms.currentVersion = currentVersion
	ms.lastUpdateTime = time.Now()
	ms.metricsDead = false
	ms.stopWriteToDiscCache = stopWriteToDiscCache
	ms.mu.Unlock()
	ms.broadcastJournal()
	return len(src) == 0, nil
}

func updateEntriesJournal(oldJournal, newEntries []tlmetadata.Event) []tlmetadata.Event {
	// newEntries can contain the same event several times
	// also for all edits old journals contains this event
	// we want all events once and in order in the journal, hence this code
	var result []tlmetadata.Event
	newEntriesMap := map[journalEventID]int64{}
	for _, entry := range newEntries {
		key := journalEventID{
			typ: entry.EventType,
			id:  entry.Id,
		}
		if old, ok := newEntriesMap[key]; !ok || (ok && old < entry.Version) {
			newEntriesMap[key] = entry.Version
		}
	}
	for _, entry := range oldJournal {
		if _, ok := newEntriesMap[journalEventID{
			typ: entry.EventType,
			id:  entry.Id,
		}]; !ok {
			result = append(result, entry)
		}
	}
	for _, entry := range newEntries {
		key := journalEventID{
			typ: entry.EventType,
			id:  entry.Id,
		}
		if v, ok := newEntriesMap[key]; ok && entry.Version == v {
			result = append(result, entry)
		}
	}
	return result
}

func (ms *Journal) goUpdateMetrics(aggLog AggLog) {
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

func (ms *Journal) getJournalDiffLocked3(verNumb int64) tlmetadata.GetJournalResponsenew {
	result := tlmetadata.GetJournalResponsenew{CurrentVersion: ms.versionLocked()}
	if verNumb >= ms.versionLocked() { // wait until version changes
		return result
	}
	left := sort.Search(len(ms.journal), func(i int) bool {
		return ms.journal[i].Version > verNumb
	})
	right := len(ms.journal)
	result.Events = ms.journal[left:right]
	if len(result.Events) == 0 {
		return result
	}
	if len(result.Events) > data_model.MaxJournalItemsSent {
		result.Events = result.Events[:data_model.MaxJournalItemsSent]
	}
	bytesSize := 0
	for i, event := range result.Events {
		bytesSize += len(event.Name)
		bytesSize += len(event.Data)
		bytesSize += 60
		if bytesSize >= data_model.MaxJournalBytesSent && i > 0 {
			result.Events = result.Events[:i]
			break
		}
	}

	return result
}

func (ms *Journal) broadcastJournal() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	// TODO - most clients wait with the same version, remember response bytes in local map
	for hctx, args := range ms.metricsVersionClients3 {
		if ms.metricsDead {
			delete(ms.metricsVersionClients3, hctx)
			hctx.SendHijackedResponse(errDeadMetrics)
			continue
		}
		result := ms.getJournalDiffLocked3(args.From)
		if len(result.Events) == 0 {
			continue
		}
		delete(ms.metricsVersionClients3, hctx)
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, result)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedOK, float64(len(result.Events)))
		}
		hctx.SendHijackedResponse(err)
	}
}

func prepareResponseToAgent(resp *tlmetadata.GetJournalResponsenew) {
	// TODO skip copy
	cpyArr := make([]tlmetadata.Event, len(resp.Events))
	for i, e := range resp.Events {
		eCpy := tlmetadata.Event{
			Id:          e.Id,
			Name:        e.Name,
			NamespaceId: e.NamespaceId,
			EventType:   e.EventType,
			Version:     e.Version,
			UpdateTime:  e.UpdateTime,
			Data:        e.Data,
		}
		eCpy.SetNamespaceId(e.NamespaceId)
		cpyArr[i] = eCpy

		switch e.EventType {
		case format.DashboardEvent:
			fallthrough
		case format.PromConfigEvent:
		// resp.Events[i].Data = ""
		default:

		}
	}
	resp.Events = cpyArr
}

func (ms *Journal) HandleGetMetrics3(args tlstatshouse.GetMetrics3, hctx *rpc.HandlerContext) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.metricsDead {
		ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		return errDeadMetrics
	}
	result := ms.getJournalDiffLocked3(args.From)
	if len(result.Events) != 0 {
		prepareResponseToAgent(&result)
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, result)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateOK, float64(len(result.Events)))
		}
		return err
	}
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	ms.metricsVersionClients3[hctx] = args
	ms.builtinAddValue(&ms.BuiltinLongPollEnqueue, 1)
	return hctx.HijackResponse(ms)
}

func (j *journalEventID) key() string {
	return strconv.FormatInt(int64(j.typ), 10) + "_" + strconv.FormatInt(j.id, 10)
}
