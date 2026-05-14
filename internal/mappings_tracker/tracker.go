package mappings_tracker

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"sync"
	"unsafe"

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

// NOTE: deprecated, slow (~5 minutes for 50m mappings)
func (t *Tracker) LoadFromFile(path string) error {
	if t.loaded.Load() {
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
	t.loaded.Store(true)
	return nil
}

func nativeLittleEndian() bool {
	var x uint16 = 1
	return *(*byte)(unsafe.Pointer(&x)) == 1
}

func bytesAsInt32Slice(b []byte) []int32 {
	if len(b) == 0 {
		return nil
	}

	return unsafe.Slice(
		(*int32)(unsafe.Pointer(unsafe.SliceData(b))),
		len(b)/4,
	)
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

	if !nativeLittleEndian() {
		ids := make([]int32, len(buf)/4)
		for i := range ids {
			ids[i] = int32(binary.LittleEndian.Uint32(buf[i*4:]))
		}
		return nil
	}

	ids := bytesAsInt32Slice(buf)

	// safety check
	if !slices.IsSorted(ids) {
		log.Printf("[WARNING] mappings file isn't sorted correctly, sorting...")
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}

	if len(ids) == 0 {
		log.Printf("loaded empty tracked mappings from file %s, len=0, range=[]", path)
	} else {
		if ids[0] < 0 {
			// extra safety check for conversion correctness
			return fmt.Errorf("invalid mappings file, contains negative numbers: %d", ids[0])
		}
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
