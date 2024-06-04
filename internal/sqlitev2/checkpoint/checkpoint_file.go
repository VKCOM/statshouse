package checkpoint

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/vkcom/statshouse/internal/sqlitev2/checkpoint/gen2/tlsqlite"
	"github.com/zeebo/xxh3"
	"go.uber.org/multierr"
)

/*
Используется доп. файл для хранения оффсетазакомиченной позиции
В данный файл происходит запись перед выполнением чекпоинта.
*/
type RestartFile struct {
	mx                 sync.Mutex
	buffer             []byte
	metainfo           tlsqlite.Metainfo
	syncedCommitOffset int64
	f                  *os.File
}

const (
	commitFileSuffix = "-barsic-commit"
	defaultFilePerm  = os.FileMode(0640)
)

func CommitFileName(dbPath string) string {
	return dbPath + commitFileSuffix
}

func OpenAndLock(dbPath string) (*RestartFile, error) {
	re := &RestartFile{buffer: make([]byte, 4096)}
	filePath := CommitFileName(dbPath)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	re.f = f

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		closeErr := f.Close()
		return nil, fmt.Errorf("databas checkpoint file is locked: %w", multierr.Append(err, closeErr))
	}

	data, err := io.ReadAll(f)
	if len(data) == 0 {
		return re, nil
	}
	if err != nil {
		return nil, multierr.Append(err, re.Close())
	}
	metainfo, err := decode(data)
	if err != nil {
		return re, multierr.Append(err, re.Close())
	}
	re.metainfo = metainfo
	re.syncedCommitOffset = metainfo.Offset
	return re, nil
}

func (f *RestartFile) Close() (err error) {
	err = multierr.Append(err, syscall.Flock(int(f.f.Fd()), syscall.LOCK_UN))
	return multierr.Append(err, f.f.Close())
}

func (f *RestartFile) SetCommitOffset(offset int64) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.metainfo.Offset = offset
}

func (f *RestartFile) SetCommitOffsetAndSync(offset int64) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.metainfo.Offset = offset
	if offset == f.syncedCommitOffset {
		return nil
	}
	data := encode(f.metainfo, f.buffer[:0])
	_, err := f.f.WriteAt(data, 0)
	if err != nil {
		return err
	}
	err = f.f.Sync()
	if err != nil {
		return err
	}
	f.syncedCommitOffset = offset
	return nil
}

func (f *RestartFile) GetCommitOffset() int64 {
	f.mx.Lock()
	defer f.mx.Unlock()
	return f.metainfo.Offset
}

func encode(metainfo tlsqlite.Metainfo, buffer []byte) []byte {
	buffer = metainfo.WriteBoxed(buffer)
	hash := xxh3.Hash(buffer)
	buffer = binary.BigEndian.AppendUint64(buffer, hash)
	return buffer
}

func decode(data []byte) (metainfo tlsqlite.Metainfo, err error) {
	tail, err := metainfo.ReadBoxed(data)
	if err != nil {
		return metainfo, fmt.Errorf("failed to decode checkpoint file: %w", err)
	}
	dataLen := len(data) - len(tail)
	hashCalculated := xxh3.Hash(data[:dataLen])
	hashFromFile := binary.BigEndian.Uint64(tail)
	if hashFromFile != hashCalculated {
		return metainfo, fmt.Errorf("helper file is corrupted")
	}
	return metainfo, nil
}
