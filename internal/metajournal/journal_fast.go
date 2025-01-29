// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/statshouse/internal/agent"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/zeebo/xxh3"
)

type journalEvent struct {
	tlmetadata.Event
	hash xxh3.Uint128
}

type JournalFast struct {
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

	sh2       *agent.Agent
	MetricsMu sync.Mutex

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

func MakeJournalFast(journalRequestDelay time.Duration, applyEvent []ApplyEvent) *JournalFast {
	return &JournalFast{
		journalRequestDelay: journalRequestDelay,
		journal:             map[journalEventID]journalEvent{},
		applyEvent:          applyEvent,
		lastUpdateTime:      time.Now(),
		writeAt:             func(offset int64, data []byte) error { return nil },
		truncate:            func(offset int64) error { return nil },
	}
}

// fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666) - recommended flags
// if fp nil, then cache works in memory-only mode
func LoadJournalFastFile(fp *os.File, journalRequestDelay time.Duration, applyEvent []ApplyEvent) (*JournalFast, error) {
	c := MakeJournalFast(journalRequestDelay, applyEvent)
	w, t, r, fs := data_model.ChunkedStorageFile(fp)
	c.writeAt = w
	c.truncate = t
	err := c.load(fs, r)
	return c, err
}

func (ms *JournalFast) load(fileSize int64, readAt func(b []byte, offset int64) error) error {
	src, err := ms.loadImpl(fileSize, readAt)
	newVersion, stateHashStr := ms.finishUpdateLocked()
	for _, f := range ms.applyEvent {
		f(src, newVersion, stateHashStr)
	}
	return err
}

func (ms *JournalFast) loadImpl(fileSize int64, readAt func(b []byte, offset int64) error) ([]tlmetadata.Event, error) {
	loader := data_model.ChunkedStorageLoader{ReadAt: readAt}
	loader.StartRead(fileSize, data_model.ChunkedMagicJournal)
	var scratch []byte
	var src []tlmetadata.Event
	for {
		chunk, err := loader.ReadNext()
		if err != nil {
			return src, err
		}
		if len(chunk) == 0 {
			break
		}
		var entry tlmetadata.Event
		for len(chunk) != 0 {
			if chunk, err = entry.ReadBoxed(chunk); err != nil {
				return src, fmt.Errorf("clearing journal, error parsing journal entry: %v", err)
			}
			src = append(src, entry)
			scratch = ms.addEventLocked(scratch, entry)
		}
		newVersion, stateHashStr := ms.finishUpdateLocked()
		for _, f := range ms.applyEvent {
			f(src, newVersion, stateHashStr)
		}
		src = src[:0]
	}
	return src, nil
}

func (ms *JournalFast) Save() error {
	saver := data_model.ChunkedStorageSaver{
		WriteAt:  ms.writeAt,
		Truncate: ms.truncate,
	}

	chunk := saver.StartWrite(data_model.ChunkedMagicJournal)

	// we must write in order, because we must deliver them in order during load
	items := make([]tlmetadata.Event, 0, len(ms.journal))
	for _, event := range ms.journal {
		items = append(items, event.Event)
	}
	slices.SortFunc(items, func(a, b tlmetadata.Event) int {
		return cmp.Compare(a.Version, b.Version)
	})
	for _, event := range items {
		chunk = event.WriteBoxed(chunk)
		var err error
		chunk, err = saver.FinishItem(chunk)
		if err != nil {
			return err
		}
	}
	return saver.FinishWrite(chunk)
}

func (ms *JournalFast) Start(sh2 *agent.Agent, aggLog AggLog, metaLoader MetricsStorageLoader) {
	ms.metaLoader = metaLoader
	ms.sh2 = sh2
	ms.parseDiscCache()
	go ms.goUpdateMetrics(aggLog)
}

func (ms *JournalFast) Version() int64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.versionLocked()
}

func (ms *JournalFast) versionLocked() int64 {
	return ms.currentVersion
}

func (ms *JournalFast) StateHash() string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.stateHashStr
}

func (ms *JournalFast) builtinAddValue(m *data_model.ItemValue, value float64) {
	ms.MetricsMu.Lock()
	defer ms.MetricsMu.Unlock()
	m.AddValue(value)
}

func (ms *JournalFast) parseDiscCache() {
}

func (ms *JournalFast) addEventLocked(scratch []byte, entry tlmetadata.Event) []byte {
	scratch = entry.Write(scratch[:0])
	hash := xxh3.Hash128(scratch)
	key := journalEventID{
		typ: entry.EventType,
		id:  entry.Id,
	}
	old := ms.journal[key] // hash is 0 if not there
	ms.stateHash.Hi ^= old.hash.Hi
	ms.stateHash.Lo ^= old.hash.Lo
	ms.stateHash.Hi ^= hash.Hi
	ms.stateHash.Lo ^= hash.Lo
	ms.journal[key] = journalEvent{Event: entry, hash: hash}
	if entry.Version > ms.currentVersion {
		ms.currentVersion = entry.Version
	}
	return scratch
}

func (ms *JournalFast) finishUpdateLocked() (int64, string) {
	hb := ms.stateHash.Bytes()
	stateHashStr := hex.EncodeToString(hb[:])
	newVersion := ms.currentVersion
	ms.stateHashStr = stateHashStr
	ms.lastUpdateTime = time.Now()
	ms.metricsDead = false
	return newVersion, stateHashStr
}

func (ms *JournalFast) updateJournal(aggLog AggLog) error {
	_, err := ms.updateJournalIsFinished(aggLog)
	return err
}

// for tests to stop when all events are played back
func (ms *JournalFast) updateJournalIsFinished(aggLog AggLog) (bool, error) {
	ms.mu.RLock()
	isDead := ms.metricsDead
	oldVersion := ms.versionLocked()
	ms.mu.RUnlock()

	src, _, err := ms.metaLoader(context.Background(), oldVersion, isDead)
	if err != nil {
		if !isDead {
			ms.mu.Lock()
			ms.metricsDead = true
			ms.mu.Unlock()
		}
		ms.builtinAddValue(&ms.BuiltinJournalUpdateError, 0)
		log.Printf("error updating journal: %v", err)
		if aggLog != nil {
			aggLog("journal_update", "", "error", "", "", "", "", err.Error())
		}
		return false, err
	}
	var scratch []byte

	// newEntries can contain the same event several times
	// also for all edits old journals contains this event
	// we want all events once and in order in the journal, hence this code
	ms.mu.Lock()
	for _, entry := range src {
		scratch = ms.addEventLocked(scratch, entry)
	}
	newVersion, stateHashStr := ms.finishUpdateLocked()
	ms.mu.Unlock()

	// TODO - check invariants here before saving

	ms.builtinAddValue(&ms.BuiltinJournalUpdateOK, 0)
	for _, f := range ms.applyEvent {
		f(src, newVersion, stateHashStr)
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
	return len(src) == 0, nil
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

//sqlite> .once journal.json
//sqlite> SELECT version, id, name, updated_at, type, deleted_at, namespace_id, REPLACE(data, CHAR(10), '\n') FROM metrics_v5 ORDER BY version asc;
//sqlite> .quit

func getJournalFileLoader(fName string) (MetricsStorageLoader, []tlmetadata.Event, error) {
	data, err := os.ReadFile(fName)
	if err != nil {
		return nil, nil, err
	}
	lines := strings.Split(string(data), "\n")
	//var newData bytes.Buffer
	var events []tlmetadata.Event
	for i, l := range lines {
		if l == "" {
			continue
		}
		values := strings.SplitN(l, "|", 8)
		version, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		id, err := strconv.ParseInt(values[1], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		updateTime, err := strconv.ParseUint(values[3], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		tt, err := strconv.ParseInt(values[4], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		_, err = strconv.ParseUint(values[5], 10, 32)
		if err != nil {
			return nil, nil, err
		}
		nID, err := strconv.ParseInt(values[6], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		event := tlmetadata.Event{
			FieldMask:   0,
			Id:          id,
			Name:        values[2],
			NamespaceId: nID,
			UpdateTime:  uint32(updateTime),
			EventType:   int32(tt),
			Version:     version,
			Data:        values[7],
		}
		if len(events) != 0 && version <= events[len(events)-1].Version {
			return nil, nil, fmt.Errorf("events not sorted at %d", i)
		}
		events = append(events, event)
		/*
			switch event.EventType {
			case format.MetricEvent:
				value := &format.MetricMetaValue{}
				err := json.Unmarshal([]byte(event.Data), value)
				if err != nil {
					return nil, nil, err
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
				value.Description = ""
				value.Resolution = 0
				value.Weight = 0
				value.StringTopDescription = ""
				if value.NamespaceID == -5 {
					value.NamespaceID = 0
				}
				value.InVisible = !value.Visible
				value.Visible = false
				cutTags := 0
				for ti := range value.Tags {
					tag := &value.Tags[ti]
					tag.Description = ""
					tag.ID2Value = nil
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
				shortData, err := value.MarshalBinary()
				if err != nil {
					return nil, nil, err
				}
				values[7] = string(shortData)
				newData.WriteString(strings.Join(values, "|"))
				newData.WriteString("\n")
			}
		*/
	}
	// _ = os.WriteFile(fName+".short", newData.Bytes(), 0666)
	return func(ctx context.Context, lastVersion int64, returnIfEmpty bool) ([]tlmetadata.Event, int64, error) {
		var result []tlmetadata.Event
		for _, e := range events {
			if e.Version > lastVersion {
				result = append(result, e)
			}
			if len(result) >= 100 {
				break
			}
		}
		var v int64
		if len(result) > 0 {
			v = result[len(result)-1].Version
		}
		return result, v, nil
	}, events, nil
}

func LoadTestJournalFromFile(journal *JournalFast, fName string) error {
	loader, _, err := getJournalFileLoader("../internal/metajournal/journal.json")
	if err != nil {
		return err
	}
	journal.metaLoader = loader
	for {
		fin, err := journal.updateJournalIsFinished(nil)
		if err != nil {
			return err
		}
		if fin {
			break
		}
	}
	return nil
}
