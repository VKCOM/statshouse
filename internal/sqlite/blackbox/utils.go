package blackbox

import (
	"os"

	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog"
)

func getBinlogPosition(tempDir string, magic uint32) int64 {
	fs, err := fsbinlog.ScanForFilesFromPos(0, tempDir, magic, nil)
	if err != nil {
		panic(err)
	}
	if len(fs) == 0 {
		return 0
	}
	var maxF *fsbinlog.FileHeader
	for _, f := range fs {
		if maxF == nil || maxF.Position < f.Position {
			fCopy := f
			maxF = &fCopy
		}
	}
	fi, err := os.Stat(maxF.FileName)
	if err != nil {
		panic(err)
	}
	return maxF.Position + fi.Size()
}
