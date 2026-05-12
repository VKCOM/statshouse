package mappings_tracker

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

type Tracker struct {
	ids    []int32
	loaded bool // not sync under the assumption that updating the var only happens once and so does loading the set

	// TODO: sharding?
	mu          sync.Mutex
	usageCounts map[int32]uint64
}

func New() *Tracker {
	t := &Tracker{}
	t.usageCounts = make(map[int32]uint64)
	t.loaded = false // explicit false for semantics
	return t
}

func (t *Tracker) LoadFromFile(path string) error {
	if t.loaded {
		return fmt.Errorf("error loading from %s: set is already loaded", path)
	}
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("error opening mappings file: %s: %w", path, err)
	}
	defer f.Close()

	ids := make([]int32, 0, 1024)

	log.Printf("loading tracked mappings from file %s", path)
	for {
		var x int32
		_, err = fmt.Fscanln(f, &x)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		ids = append(ids, x)
	}

	log.Printf("loaded tracked mappings from file %s, sorting...", path)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	t.ids = ids
	log.Printf("%d tracked mappings loaded", len(ids))
	t.loaded = true
	return nil
}

func (t *Tracker) Len() int {
	return len(t.ids)
}

// IsRedundant reports whether id is in the loaded set. Implemented as binsearch instead of map lookup for optimal storage
func (t *Tracker) IsRedundant(id int32) bool {
	if id <= 0 {
		return false
	}
	if !t.loaded {
		return false
	}
	i := sort.Search(len(t.ids), func(i int) bool { return t.ids[i] >= id })
	return i < len(t.ids) && t.ids[i] == id
}

func (t *Tracker) RecordUsage(id int32) {
	t.mu.Lock()
	if _, ok := t.usageCounts[id]; !ok {
		t.usageCounts[id] = 0
	}
	t.usageCounts[id]++
	t.mu.Unlock()
}

func (t *Tracker) DrainUsage() map[int32]uint64 {
	t.mu.Lock()
	drained := t.usageCounts
	t.usageCounts = make(map[int32]uint64)
	t.mu.Unlock()
	return drained
}

func (t *Tracker) Check(id int32) bool {
	if t.IsRedundant(id) {
		t.RecordUsage(id)
		return true
	}
	return false
}
