package mappings_tracker

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"

	"go.uber.org/atomic"
)

type Tracker struct {
	ids    []int32
	loaded atomic.Bool // not sync under the assumption that updating the var only happens once and so does loading the set

	// TODO: sharding?
	mu          sync.Mutex
	usageCounts map[int32]uint64
}

func New() *Tracker {
	t := &Tracker{}
	t.usageCounts = make(map[int32]uint64)
	t.loaded.Store(false) // explicit false for semantics
	return t
}

func (t *Tracker) LoadFromBinFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open mappings file %s: %w", path, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat mappings file %s: %w", path, err)
	}

	size := st.Size()
	if size%4 != 0 {
		return fmt.Errorf("invalid mappings file size %d: not divisible by 4", size)
	}

	buf := make([]byte, int(size))

	if _, err := io.ReadFull(f, buf); err != nil {
		return fmt.Errorf("read mappings file %s: %w", path, err)
	}

	// safe []int32 parse + sort check
	ids := make([]int32, int(size/4))
	lastId := int32(-1)
	for i := range ids {
		id := int32(binary.LittleEndian.Uint32(buf[i*4:]))
		if id <= lastId {
			return fmt.Errorf("invalid mappings file (parsing error or not sorted in ascending order) i: %d, ids[i]: %d, ids[i-1]: %d", i, id, lastId)
		}
		ids[i] = id
		lastId = id
	}

	if len(ids) == 0 {
		log.Printf("loaded empty tracked mappings from file %s, len=0, range=[]", path)
	} else {
		log.Printf("loaded sorted tracked mappings from file %s, len=%d, range=[%d - %d]", path, len(ids), ids[0], ids[len(ids)-1])
	}
	t.ids = ids
	t.loaded.Store(true)

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
	if !t.loaded.Load() {
		return false
	}
	i := sort.Search(len(t.ids), func(i int) bool { return t.ids[i] >= id })
	return i < len(t.ids) && t.ids[i] == id
}

func (t *Tracker) RecordUsage(id int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usageCounts[id]++
}

func (t *Tracker) DrainUsage() map[int32]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	drained := t.usageCounts
	t.usageCounts = make(map[int32]uint64)
	return drained
}

func (t *Tracker) Check(id int32) bool {
	if t.IsRedundant(id) {
		t.RecordUsage(id)
		return true
	}
	return false
}
