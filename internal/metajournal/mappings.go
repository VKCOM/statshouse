// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metajournal

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
	"golang.org/x/sync/errgroup"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
	"github.com/VKCOM/statshouse/internal/vkgo/srvfunc"
)

const (
	averagePairSize    = 24
	defaultMappingSize = 150000000
)

// MappingsLoader is a function that loads new mappings from metadata service
// Returns: mappings, currentVersion (max version returned), lastVersion (max version in metadata storage), error
type MappingsLoader func(ctx context.Context, lastVersion int32, returnIfEmpty bool) ([]tlstatshouse.Mapping, int32, int32, error)

type MappingsStorage struct {
	ctx       context.Context
	mu        sync.RWMutex
	storageMu sync.Mutex // disk consistently

	mappingsLoader       MappingsLoader
	mappingsRequestDelay time.Duration

	// Mappings storage - this is the single source of truth
	shards        []*mappingShard // by string xxh3 % len
	reverseEnable bool
	reverseShards []*reverseMappingShard // by reverseEnable, by int % len

	metadataDead     bool  // true if metadata service is dead, under mu
	pendingByteSize  int64 // sum byte size of every shard pendingPairs, under mu
	currentVersion   int32 // maximum mapping value, under mu
	lastKnownVersion int32 // last known version in metadata, under mu

	// metrics
	sh2       *agent.Agent // not available for API
	component int32
}

type mappingShard struct {
	mu       sync.RWMutex
	mappings map[string]int32 // under mu

	// periodic saving
	pendingByteSize int64                            // byte size of pendingPairs, under mu
	pendingPairs    []tlstatshouse.Mapping           // new mappings, under mu
	storage         *data_model.ChunkedStorageShared // under MappingsStorage.storageMu
}

type reverseMappingShard struct {
	mu       sync.RWMutex
	mappings map[int32]string // under mu
}

func MakeMappings(ctx context.Context, mappingsRequestDelay time.Duration, reverseMappingsEnable bool, size int, storages []*data_model.ChunkedStorageShared) *MappingsStorage {
	if size <= 0 {
		size = defaultMappingSize
	}
	shardCnt := len(storages)
	ms := &MappingsStorage{
		ctx:                  ctx,
		mappingsRequestDelay: mappingsRequestDelay,
		shards:               make([]*mappingShard, shardCnt),
		reverseShards:        make([]*reverseMappingShard, shardCnt),
		reverseEnable:        reverseMappingsEnable,
		metadataDead:         true, // for first response
		currentVersion:       -1,
	}
	for i, storage := range storages {
		ms.shards[i] = &mappingShard{
			mappings:     make(map[string]int32, size/shardCnt),
			pendingPairs: make([]tlstatshouse.Mapping, 0, 100),
			storage:      storage,
		}
		if reverseMappingsEnable {
			ms.reverseShards[i] = &reverseMappingShard{mappings: make(map[int32]string, size/shardCnt)}
		}
	}
	return ms
}

// fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666) - recommended flags
// if fp nil, then storage works in memory-only mode
func LoadMappingsFiles(ctx context.Context, fps []*os.File, mappingsRequestDelay time.Duration, reverseMappingsEnable bool) (*MappingsStorage, error) {
	var sumFs int64
	var fss []int64
	var storage []*data_model.ChunkedStorageShared
	for _, fp := range fps {
		w, t, r, fs := data_model.ChunkedStorageFile(fp)
		storage = append(storage, data_model.NewChunkedStorageShared(w, t, r))
		sumFs += fs
		fss = append(fss, fs)
	}
	ms := MakeMappings(ctx, mappingsRequestDelay, reverseMappingsEnable, int(sumFs/averagePairSize), storage)
	err := ms.load(fss)
	return ms, err
}

func (ms *MappingsStorage) load(fileSize []int64) error {
	ms.storageMu.Lock()
	defer ms.storageMu.Unlock()
	log.Printf("Start loading mappings file size %d", fileSize)

	wg := sync.WaitGroup{}
	revChs := ms.goAddReverseShardValues(&wg)

	mu := sync.Mutex{}
	version := int32(math.MaxInt32)
	eg := errgroup.Group{}
	for i, s := range ms.shards {
		num := i
		shard := s
		eg.Go(func() error {
			shard.mu.Lock()
			defer shard.mu.Unlock()

			v := int32(-1)
			var resErr error
			// Use shared storage loader - it will update shared offset automatically
			shard.storage.StartRead(fileSize[num], data_model.ChunkedMagicMappings)
			for {
				select {
				case <-ms.ctx.Done():
					return nil
				default:
				}
				chunk, _, err := shard.storage.ReadNext()
				if err != nil {
					resErr = err
					break
				}
				if len(chunk) == 0 {
					break
				}
				var val int32
				if err = parseMappingChunkInplace(chunk, func(str string, value int32) {
					shard.mappings[str] = value
					if ms.reverseEnable {
						revChs[int(value)%len(revChs)] <- tlstatshouse.Mapping{
							Str:   str,
							Value: value,
						}
					}
					val = value
				}); err != nil {
					resErr = err
					break
				}
				v = max(v, val)
				shard.storage.FinishReadChunk()
			}
			mu.Lock()
			defer mu.Unlock()
			version = min(version, v) // in case files damaged
			return resErr
		})
	}
	err := eg.Wait()
	for _, ch := range revChs {
		close(ch)
	}
	wg.Wait()

	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.currentVersion = version
	ms.lastKnownVersion = version
	log.Printf("Finish loading mappings file size %d", fileSize)
	return err
}

func (ms *MappingsStorage) Save() (bool, error) {
	ms.storageMu.Lock()
	defer ms.storageMu.Unlock()

	allSize := int64(0)
	mu := sync.Mutex{}
	eg := errgroup.Group{}
	for _, s := range ms.shards {
		shard := s
		eg.Go(func() error {
			sizeDone := int64(0)
			defer func() {
				mu.Lock()
				defer mu.Unlock()
				allSize += sizeDone
			}()

			shard.mu.Lock()
			pairs := append([]tlstatshouse.Mapping{}, shard.pendingPairs...)
			size := shard.pendingByteSize
			shard.pendingPairs = shard.pendingPairs[:0]
			shard.pendingByteSize = 0
			shard.mu.Unlock()

			if len(pairs) == 0 {
				return nil
			}
			rollback := func() {
				shard.mu.Lock()
				defer shard.mu.Unlock()
				shard.pendingPairs = append(pairs, shard.pendingPairs...)
				shard.pendingByteSize += size - sizeDone
			}

			chunk := shard.storage.StartWriteWithOffset(data_model.ChunkedMagicMappings, 0)

			appendItem := func(k string, v int32) error {
				// we have big key lens
				chunk = basictl.StringWriteTL2(chunk, k)
				chunk = basictl.IntWrite(chunk, v)
				var err error
				chunk, err = shard.storage.FinishItem(chunk)
				return err
			}
			for len(pairs) > 0 {
				if err := appendItem(pairs[0].Str, pairs[0].Value); err != nil {
					rollback()
					return err
				}
				sizeDone += int64(len(pairs[0].Str) + 4)
				pairs = pairs[1:]
			}
			err := shard.storage.FinishWrite(chunk)
			if err != nil {
				rollback()
				return err
			}
			return nil
		})
	}
	err := eg.Wait()

	ms.mu.Lock()
	ms.pendingByteSize -= allSize
	ms.mu.Unlock()

	if ms.sh2 != nil {
		ms.sh2.AddValueCounter(0, format.BuiltinMetricMappingStoragePending, []int32{0, ms.component}, float64(allSize), 1)
	} else {
		statshouse.Value(format.BuiltinMetricMappingStoragePending.Name, statshouse.Tags{1: strconv.Itoa(int(ms.component)), 2: srvfunc.HostnameForStatshouse()}, float64(allSize))
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ms *MappingsStorage) Start(componentTag int32, sh2 *agent.Agent, mappingsLoader MappingsLoader, preload bool) {
	ms.component = componentTag
	ms.mappingsLoader = mappingsLoader
	ms.sh2 = sh2

	if mappingsLoader == nil {
		return
	}
	for preload {
		select {
		case <-ms.ctx.Done():
			return
		default:
		}
		if err := ms.updateMappings(); err != nil {
			log.Printf("failed to full update mapping storage: %v. So start anyway", err)
			break
		}
		ms.mu.RLock()
		if ms.currentVersion == ms.lastKnownVersion {
			ms.mu.RUnlock()
			break
		}
		ms.mu.RUnlock()
		// without sleep, fast load
	}
	go ms.goUpdateMappings()
}

func (ms *MappingsStorage) StartPeriodicSaving() {
	go ms.goPeriodicSaving()
}

func (ms *MappingsStorage) goPeriodicSaving() {
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
		}
		ms.mu.RLock()
		if ms.pendingByteSize/int64(len(ms.shards)) < data_model.ChunkSize {
			ms.mu.RUnlock()
			time.Sleep(2 * time.Second)
			continue
		}
		ms.mu.RUnlock()

		log.Printf("start saving mapping storage")
		if _, err := ms.Save(); err != nil {
			log.Printf("Periodic mappings storage save failed: %v", err)
			continue
		}
		log.Printf("Periodic mappings storage save completed")
	}
}

func (ms *MappingsStorage) updateMappings() error {
	ms.mu.RLock()
	isDead := ms.metadataDead
	curVersion := ms.currentVersion
	ms.mu.RUnlock()

	mappings, newCurV, newLastV, err := ms.mappingsLoader(ms.ctx, curVersion, isDead)
	if err != nil {
		if !isDead {
			ms.mu.Lock()
			defer ms.mu.Unlock()
			ms.metadataDead = true
		}
		return fmt.Errorf("failed to load new mappings: %w", err)
	}
	if len(mappings) == 0 {
		if isDead {
			ms.mu.Lock()
			ms.metadataDead = false
			ms.mu.Unlock()
		}
		return nil
	}
	var wg sync.WaitGroup
	chs := ms.goAddShardValues(&wg)
	revChs := ms.goAddReverseShardValues(&wg)
	size := int64(0)
	for _, m := range mappings {
		size += int64(len(m.Str) + 4)
		shard := int(xxh3.HashString(m.Str) % uint64(len(ms.shards)))
		chs[shard] <- m
		if ms.reverseEnable {
			revChs[int(m.Value)%len(ms.reverseShards)] <- m
		}
	}
	for _, ch := range chs {
		close(ch)
	}
	for _, ch := range revChs {
		close(ch)
	}
	wg.Wait()

	ms.mu.Lock()
	ms.currentVersion = newCurV
	ms.lastKnownVersion = newLastV
	ms.pendingByteSize += size
	ms.mu.Unlock()

	if ms.sh2 != nil {
		ms.sh2.AddValueCounter(0, format.BuiltinMetricMappingStorageVersion,
			[]int32{0, ms.component, 0, format.TagValueIDMappingStorageVersionCurrent},
			float64(newCurV), 1)
		ms.sh2.AddValueCounter(0, format.BuiltinMetricMappingStorageVersion,
			[]int32{0, ms.component, 0, format.TagValueIDMappingStorageVersionLastKnown},
			float64(newLastV), 1)
	} else {
		statshouse.Value(format.BuiltinMetricMappingStorageVersion.Name, statshouse.Tags{
			1: strconv.Itoa(int(ms.component)),
			2: srvfunc.HostnameForStatshouse(),
			3: strconv.Itoa(format.TagValueIDMappingStorageVersionCurrent),
		}, float64(newCurV))
		statshouse.Value(format.BuiltinMetricMappingStorageVersion.Name, statshouse.Tags{
			1: strconv.Itoa(int(ms.component)),
			2: srvfunc.HostnameForStatshouse(),
			3: strconv.Itoa(format.TagValueIDMappingStorageVersionLastKnown),
		}, float64(newLastV))
	}
	return nil
}

func (ms *MappingsStorage) goAddShardValues(wg *sync.WaitGroup) []chan tlstatshouse.Mapping {
	shardChans := make([]chan tlstatshouse.Mapping, len(ms.shards))
	for i := 0; i < len(shardChans); i++ {
		shardChans[i] = make(chan tlstatshouse.Mapping)
		wg.Add(1)
		go func(shard *mappingShard, ch chan tlstatshouse.Mapping) {
			defer wg.Done()
			shard.mu.Lock()
			defer shard.mu.Unlock()
			for p := range ch {
				if _, ok := shard.mappings[p.Str]; !ok {
					shard.pendingPairs = append(shard.pendingPairs, p)
					shard.pendingByteSize += int64(len(p.Str) + 4)
				}
				shard.mappings[p.Str] = p.Value
			}
		}(ms.shards[i], shardChans[i])
	}
	return shardChans
}

func (ms *MappingsStorage) goAddReverseShardValues(wg *sync.WaitGroup) []chan tlstatshouse.Mapping {
	if !ms.reverseEnable {
		return nil
	}
	reverseShardChans := make([]chan tlstatshouse.Mapping, len(ms.reverseShards))
	for i := 0; i < len(reverseShardChans); i++ {
		reverseShardChans[i] = make(chan tlstatshouse.Mapping)
		wg.Add(1)
		go func(shard *reverseMappingShard, ch <-chan tlstatshouse.Mapping) {
			defer wg.Done()
			shard.mu.Lock()
			defer shard.mu.Unlock()
			for p := range ch {
				shard.mappings[p.Value] = p.Str
			}
		}(ms.reverseShards[i], reverseShardChans[i])
	}
	return reverseShardChans
}

func (ms *MappingsStorage) goUpdateMappings() {
	backoffTimeout := time.Duration(0)
	for {
		select {
		case <-ms.ctx.Done():
			return
		default:
		}
		err := ms.updateMappings()
		if err == nil {
			backoffTimeout = 0
			time.Sleep(ms.mappingsRequestDelay)
			continue
		}
		backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
		log.Printf("Failed to update mappings from metadata, will retry: %v", err)
		time.Sleep(backoffTimeout)
	}
}

func (ms *MappingsStorage) GetValue(str string) (int32, bool) {
	shard := int(xxh3.HashString(str) % uint64(len(ms.shards)))
	ms.shards[shard].mu.RLock()
	defer ms.shards[shard].mu.RUnlock()
	val, ok := ms.shards[shard].mappings[str]
	return val, ok
}

func (ms *MappingsStorage) GetString(value int32) (string, bool) {
	if !ms.reverseEnable {
		return "", false
	}
	shard := int(value) % len(ms.reverseShards)
	ms.reverseShards[shard].mu.RLock()
	defer ms.reverseShards[shard].mu.RUnlock()
	str, ok := ms.reverseShards[shard].mappings[value]
	return str, ok
}

func parseMappingChunkInplace(chunk []byte, f func(str string, value int32)) error {
	if len(chunk) == 0 {
		return nil
	}
	var err error
	// allocate once for all strings at chunk
	chunkStr := string(chunk)
	cutL := func(l int) {
		chunk = chunk[l:]
		chunkStr = chunkStr[l:]
	}
	readSize := func() int {
		var size int
		curL := len(chunk)
		chunk, err = basictl.TL2ReadSize(chunk, &size)
		newL := len(chunk)
		chunkStr = chunkStr[curL-newL:]
		return size
	}
	for len(chunkStr) > 0 {
		l := readSize()
		if err != nil {
			return err
		}

		str := chunkStr[:l]
		cutL(l)
		v := int32(binary.LittleEndian.Uint32(chunk))
		cutL(4)
		f(str, v)
	}
	return nil
}
