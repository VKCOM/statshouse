// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"math"
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
)

var errDeadMetrics = errors.New("metrics update from storage is dead")

const longPollTimeout = time.Hour // TODO - remove after rpc.Server tells about disconnected hctx-es

type AggLog func(typ string, key0 string, key1 string, key2 string, key3 string, key4 string, key5 string, message string)

type MetricsVersionClient struct {
	args tlstatshouse.GetMetrics2 // We remember both field mask to correctly serialize response and version
	hctx *rpc.HandlerContext
}

type versionClient struct {
	expectedVersion int64
	ch              chan struct{}
}

type journalEventID struct {
	typ int32
	id  int64
}

type MetricsVersionClient3 struct {
	args tlstatshouse.GetMetrics3 // We remember both field mask to correctly serialize response and version
	hctx *rpc.HandlerContext
}

type MetricsStorageLoader func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error)

type ApplyEvent func(newEntries []tlmetadata.Event)

type Journal struct {
	mu         sync.RWMutex
	dc         *pcache.DiskCache
	metaLoader MetricsStorageLoader
	namespace  string
	applyEvent ApplyEvent

	metricsDead          bool      // together with this bool
	lastUpdateTime       time.Time // we no more use this information for logic
	stateHash            string
	stopWriteToDiscCache bool

	journal    []tlmetadata.Event
	journalOld []*struct {
		version int64
		JSON    string
	}

	clientsMu              sync.Mutex // Always taken after mu
	metricsVersionClients  []MetricsVersionClient
	metricsVersionClients3 []MetricsVersionClient3

	versionClientMu sync.Mutex
	versionClients  []versionClient

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

func MakeJournal(namespaceSuffix string, dc *pcache.DiskCache, applyEvent ApplyEvent) *Journal {
	result := &Journal{
		dc:             dc,
		namespace:      data_model.JournalDiskNamespace + namespaceSuffix,
		applyEvent:     applyEvent,
		lastUpdateTime: time.Now(),
	}
	result.parseDiscCache()
	return result
}

func (ms *Journal) Start(sh2 *agent.Agent, a AggLog, metaLoader MetricsStorageLoader) {
	ms.metaLoader = metaLoader
	ms.sh2 = sh2
	go ms.goUpdateMetrics(a)
	go func() {
		// long poll termination loop
		for {
			<-time.After(longPollTimeout)
			ms.broadcastJournalRPC(true)
			ms.broadcastJournalVersionClient()
		}
	}()
}

func (ms *Journal) Version() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.versionLocked()
}

func (ms *Journal) versionLocked() int64 {
	if len(ms.journal) == 0 {
		return 0
	}
	return ms.journal[len(ms.journal)-1].Version
}

func (ms *Journal) StateHash() string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.stateHash
}

func (ms *Journal) LoadJournal(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
	return ms.metaLoader(ctx, lastVersion, returnIfEmpty)
}

func (ms *Journal) WaitVersion(ctx context.Context, version int64) error {
	select {
	case <-ms.waitVersion(version):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ms *Journal) waitVersion(version int64) chan struct{} {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.versionClientMu.Lock()
	defer ms.versionClientMu.Unlock()
	ch := make(chan struct{})
	if ms.versionLocked() >= version {
		close(ch)
		return ch
	}

	ms.versionClients = append(ms.versionClients, versionClient{
		expectedVersion: version,
		ch:              ch,
	})
	return ch
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
	// TODO - check invariants here before saving
	ms.applyEvent(journal2)
	ms.journal = journal2
	ms.stateHash = calculateStateHashLocked(journal2)
	log.Printf("Loaded metric storage version %d, journal hash is %s", ms.versionLocked(), ms.stateHash)
}

func regenerateOldJSON(src []tlmetadata.Event) (res []*struct {
	version int64
	JSON    string
}) {
	for _, li := range src {
		// todo remove after update all receiver's
		if li.EventType == format.MetricEvent {
			value := &format.MetricMetaValue{}
			err := json.Unmarshal([]byte(li.Data), value)
			if err != nil {
				log.Printf("Cannot marshal MetricMetaValue, skipping")
				continue
			}
			value.Version = li.Version
			value.Name = li.Name
			value.MetricID = int32(li.Id) // TODO - beware!
			value.UpdateTime = li.UpdateTime
			oldValie := &format.MetricMetaValueOld{
				MetricID:             value.MetricID,
				Name:                 value.Name,
				Description:          value.Description,
				Tags:                 value.Tags,
				Visible:              value.Visible,
				Kind:                 value.Kind,
				Weight:               int64(math.Round(value.Weight)),
				Resolution:           value.Resolution,
				StringTopName:        value.StringTopName,
				StringTopDescription: value.StringTopDescription,
				PreKeyTagID:          value.PreKeyTagID,
				PreKeyFrom:           value.PreKeyFrom,
				UpdateTime:           value.UpdateTime,
				Version:              value.Version,
			}
			JSONOld, _ := json.Marshal(oldValie)
			res = append(res, &struct {
				version int64
				JSON    string
			}{
				version: value.Version,
				JSON:    string(JSONOld),
			})
		}
	}
	return res
}

func calculateStateHashLocked(events []tlmetadata.Event) string {
	r := &tlmetadata.GetJournalResponsenew{Events: events}
	bytes, _ := r.Write(nil, 0)
	hash := sha1.Sum(bytes)
	return hex.EncodeToString(hash[:])
}

func (ms *Journal) updateJournal(aggLog AggLog) error {
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
		return err
	}
	newJournal := updateEntriesJournal(oldJournal, src)
	newJournalOld := regenerateOldJSON(newJournal)
	stateHash := calculateStateHashLocked(newJournal)

	// TODO - check invariants here before saving

	ms.builtinAddValue(&ms.BuiltinJournalUpdateOK, 0)
	ms.applyEvent(src)

	if ms.dc != nil && !stopWriteToDiscCache {
		for _, e := range src {
			buf, _ := e.WriteBoxed(nil)
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
		log.Printf("Version updated from '%d' to '%d', last entity updated is %s, journal hash is '%s'", oldVersion, lastEntry.Version, lastEntry.Name, stateHash)
		if aggLog != nil {
			aggLog("journal_update", "", "ok",
				strconv.FormatInt(oldVersion, 10),
				strconv.FormatInt(lastEntry.Version, 10),
				lastEntry.Name,
				stateHash,
				"")
		}
	}

	ms.mu.Lock()
	ms.journal = newJournal
	ms.journalOld = newJournalOld
	ms.stateHash = stateHash
	ms.lastUpdateTime = time.Now()
	ms.metricsDead = false
	ms.stopWriteToDiscCache = stopWriteToDiscCache
	ms.mu.Unlock()
	ms.broadcastJournal()
	return nil
}

func updateEntriesJournal(oldJournal, newEntries []tlmetadata.Event) []tlmetadata.Event {
	var result []tlmetadata.Event
	newEntriesMap := map[journalEventID]struct{}{}
	for _, entry := range newEntries {
		newEntriesMap[journalEventID{
			typ: entry.EventType,
			id:  entry.Id,
		}] = struct{}{}
	}
	for _, entry := range oldJournal {
		if _, ok := newEntriesMap[journalEventID{
			typ: entry.EventType,
			id:  entry.Id,
		}]; !ok {
			result = append(result, entry)
		}
	}
	result = append(result, newEntries...)
	return result
}

func (ms *Journal) goUpdateMetrics(a AggLog) {
	backoffTimeout := time.Duration(0)
	for {
		err := ms.updateJournal(a)
		if err == nil {
			backoffTimeout = 0
			time.Sleep(data_model.JournalDDOSProtectionTimeout) // if aggregator invariants are broken and they reply immediately forever
			continue
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		log.Printf("Failed to update metrics from sqliteengine, will retry: %v", err)
		time.Sleep(backoffTimeout)
	}
}

func (ms *Journal) getJournalDiffLocked(ver string) tlstatshouse.GetMetricsResult {
	curVersion := strconv.FormatInt(ms.versionLocked(), 10)
	verNumb, _ := strconv.ParseInt(ver, 10, 64)

	result := tlstatshouse.GetMetricsResult{Version: curVersion}
	if verNumb >= ms.versionLocked() { // wait until version changes
		return result
	}
	pos := sort.Search(len(ms.journalOld), func(i int) bool {
		return ms.journalOld[i].version > verNumb
	})
	for i := pos; i < len(ms.journalOld); i++ {
		j := ms.journalOld[i]
		result.Metrics = append(result.Metrics, j.JSON)
		if len(result.Metrics) <= data_model.MaxJournalItemsSent {
			continue
		}
		result.Version = strconv.FormatInt(j.version, 10) // keep this logic for legacy clients
	}
	return result
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
	ms.broadcastMetrics()
	ms.broadcastJournalRPC(false)
	ms.broadcastJournalVersionClient()
}

func (ms *Journal) broadcastMetrics() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	// TODO - most clients wait with the same version, store them in hashmap by version, prepare response once, send to many
	keepPos := 0
	for _, c := range ms.metricsVersionClients {
		if ms.metricsDead {
			c.hctx.SendHijackedResponse(errDeadMetrics)
			continue
		}
		result := ms.getJournalDiffLocked(c.args.Version)
		if len(result.Metrics) == 0 { // still waiting, copy to start of array
			ms.metricsVersionClients[keepPos] = c
			keepPos++
			continue
		}
		var err error
		c.hctx.Response, err = c.args.WriteResult(c.hctx.Response, result)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedOK, float64(len(result.Metrics)))
		}
		c.hctx.SendHijackedResponse(err)
	}
	ms.metricsVersionClients = ms.metricsVersionClients[:keepPos]
}

func (ms *Journal) broadcastJournalRPC(sendToAll bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	// TODO - most clients wait with the same version, remember response bytes in local map
	keepPos := 0
	for _, c := range ms.metricsVersionClients3 {
		if ms.metricsDead {
			c.hctx.SendHijackedResponse(errDeadMetrics)
			continue
		}
		result := ms.getJournalDiffLocked3(c.args.From)
		if len(result.Events) == 0 && !sendToAll { // still waiting, copy to start of array
			ms.metricsVersionClients3[keepPos] = c
			keepPos++
			continue
		}
		var err error
		c.hctx.Response, err = c.args.WriteResult(c.hctx.Response, result)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollDelayedOK, float64(len(result.Events)))
		}
		c.hctx.SendHijackedResponse(err)
	}
	ms.metricsVersionClients3 = ms.metricsVersionClients3[:keepPos]
}

func (ms *Journal) broadcastJournalVersionClient() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	ms.versionClientMu.Lock()
	defer ms.versionClientMu.Unlock()
	currentVersion := ms.versionLocked()
	keepPos := 0
	for _, client := range ms.versionClients {
		if client.expectedVersion <= currentVersion {
			close(client.ch)
		} else {
			ms.versionClients[keepPos] = client
			keepPos++
		}
	}
	ms.versionClients = ms.versionClients[:keepPos]
}

func (ms *Journal) HandleGetMetrics(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetMetrics2) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.metricsDead {
		ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		return errDeadMetrics
	}
	result := ms.getJournalDiffLocked(args.Version)
	if len(result.Metrics) != 0 {
		var err error
		hctx.Response, err = args.WriteResult(hctx.Response, result)
		if err != nil {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		} else {
			ms.builtinAddValue(&ms.BuiltinLongPollImmediateOK, float64(len(result.Metrics)))
		}
		return err
	}
	ms.clientsMu.Lock()
	defer ms.clientsMu.Unlock()
	ms.metricsVersionClients = append(ms.metricsVersionClients, MetricsVersionClient{args: args, hctx: hctx})
	ms.builtinAddValue(&ms.BuiltinLongPollEnqueue, 1)
	return hctx.HijackResponse()
}

func (ms *Journal) HandleGetMetrics3(_ context.Context, hctx *rpc.HandlerContext, args tlstatshouse.GetMetrics3) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.metricsDead {
		ms.builtinAddValue(&ms.BuiltinLongPollImmediateError, 0)
		return errDeadMetrics
	}
	result := ms.getJournalDiffLocked3(args.From)
	if len(result.Events) != 0 {
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
	ms.metricsVersionClients3 = append(ms.metricsVersionClients3, MetricsVersionClient3{args: args, hctx: hctx})
	ms.builtinAddValue(&ms.BuiltinLongPollEnqueue, 1)
	return hctx.HijackResponse()
}

func (j *journalEventID) key() string {
	return strconv.FormatInt(int64(j.typ), 10) + "_" + strconv.FormatInt(j.id, 10)
}
