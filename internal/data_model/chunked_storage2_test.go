package data_model_test

import (
	"fmt"
	"testing"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/basictl"
)

const chunkedMagicTest = 0x11223344

type TestModel struct {
	value uint32

	lastSavedVal uint32
}

func (tm *TestModel) addMapping() {
	tm.value++
}

func (tm *TestModel) load(storage *data_model.ChunkedStorage2) error {
	for {
		chunk, err := storage.ReadNext(chunkedMagicTest)
		if err != nil {
			return err
		}
		tm.lastSavedVal = tm.value
		if len(chunk) == 0 {
			break
		}
		if storage.IsFirst() {
			if chunk, err = basictl.NatReadExactTag(chunk, 0x123); err != nil {
				return fmt.Errorf("error parsing first chunk: %v", err)
			}
		}
		for len(chunk) != 0 {
			var nextVal uint32
			if chunk, err = basictl.NatRead(chunk, &nextVal); err != nil {
				return fmt.Errorf("error parsing test entry: %v", err)
			}
			if nextVal != tm.value+1 {
				return fmt.Errorf("unexpected test entry: %d (must be %d)", nextVal, tm.value)
			}
			tm.value++
		}
	}
	return nil
}

func (tm *TestModel) save(storage *data_model.ChunkedStorage2) (err error) {
	if tm.value <= tm.lastSavedVal {
		return nil
	}
	chunk := storage.StartWriteChunk(chunkedMagicTest, 0)
	if storage.IsFirst() {
		chunk = basictl.NatWrite(chunk, 0x123)
	}
	for ; tm.lastSavedVal < tm.value; tm.lastSavedVal++ {
		chunk = basictl.NatWrite(chunk, tm.lastSavedVal+1)
		if chunk, err = storage.FinishItem(chunk); err != nil {
			return err
		}
	}
	return storage.FinishWriteChunk(chunk)
}

func TestChunkedStorageSlice(t *testing.T) {
	var fp []byte
	storage := data_model.NewChunkedStorage2Slice(&fp)
	{
		var tm TestModel
		err := tm.load(storage)
		t.Logf("tm: %d %d err: %v\n", tm.value, tm.lastSavedVal, err)
		tm.addMapping()
		tm.addMapping()
		_ = tm.save(storage)
		_ = tm.save(storage)
		tm.addMapping()
		err = tm.save(storage)
		t.Logf("tm: %d %d err: %v\n", tm.value, tm.lastSavedVal, err)
	}
	storage = data_model.NewChunkedStorage2Slice(&fp)
	{
		var tm TestModel
		err := tm.load(storage)
		t.Logf("tm: %d %d err: %v\n", tm.value, tm.lastSavedVal, err)
		tm.addMapping()
		err = tm.save(storage)
		t.Logf("tm: %d %d err: %v\n", tm.value, tm.lastSavedVal, err)
	}
	storage = data_model.NewChunkedStorage2Slice(&fp)
	{
		var tm TestModel
		err := tm.load(storage)
		t.Logf("tm: %d %d err: %v\n", tm.value, tm.lastSavedVal, err)
	}
}
