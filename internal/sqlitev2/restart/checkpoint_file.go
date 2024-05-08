package restart

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/zeebo/xxh3"
	"go.uber.org/multierr"
)

/*
Используется доп. файл для хранения оффсетазакомиченной позиции
В данный файл происходит запись перед выполнением чекпоинта.

Формат:
8 байт версия
8 байт коммит позиция
488 байт для будущего использования
8 байт чексумма
*/
type RestartFile struct {
	mx                 sync.Mutex
	commitOffset       int64
	syncedCommitOffset int64
	f                  *os.File
}

const (
	commitFileSuffix = "-barsic-commit"
	defaultFilePerm  = os.FileMode(0640)
	fileLen          = 512
	version          = 0x0faf1000000
)

func CommitFileName(dbPath string) string {
	return dbPath + commitFileSuffix
}

func OpenAndLock(dbPath string) (*RestartFile, error) {
	re := &RestartFile{}
	filePath := CommitFileName(dbPath)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, err
	}

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, fmt.Errorf("databas checkpoint file is locked: %w", err)
	}
	re.f = f
	var data [fileLen]byte
	_, err = f.ReadAt(data[:], 0)
	if errors.Is(err, io.EOF) {
		return re, nil
	}
	if err != nil {
		return nil, err
	}
	offs, err := decode(data)
	if err != nil {
		return re, nil
	}
	re.commitOffset = offs
	re.syncedCommitOffset = offs
	return re, nil
}

func (f *RestartFile) Close() (err error) {
	err = multierr.Append(err, syscall.Flock(int(f.f.Fd()), syscall.LOCK_UN))
	return multierr.Append(err, f.f.Close())
}

func (f *RestartFile) SetCommitOffset(offset int64) {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.commitOffset = offset
}

func (f *RestartFile) SetCommitOffsetAndSync(offset int64) error {
	f.mx.Lock()
	defer f.mx.Unlock()
	f.commitOffset = offset
	if offset == f.syncedCommitOffset {
		return nil
	}
	data := encode(offset)
	_, err := f.f.WriteAt(data[:], 0)
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
	return f.commitOffset
}

func encode(commitOffset int64) (ret [512]byte) {
	const dataLen = 512 - 8
	binary.BigEndian.PutUint64(ret[0:], uint64(version))
	binary.BigEndian.PutUint64(ret[8:], uint64(commitOffset))
	hash := xxh3.Hash(ret[:dataLen])
	binary.BigEndian.PutUint64(ret[dataLen:], hash)
	return ret
}

func decode(data [512]byte) (commitOffset int64, err error) {
	const dataLen = 512 - 8
	versionR := binary.BigEndian.Uint64(data[:8])
	if versionR != version {
		return 0, fmt.Errorf("helper file is corrupted")
	}
	commitOffset = int64(binary.BigEndian.Uint64(data[8:16]))
	hashCalculated := xxh3.Hash(data[:dataLen])
	hash := binary.BigEndian.Uint64(data[dataLen:])
	if hash != hashCalculated {
		return 0, fmt.Errorf("helper file is corrupted")
	}
	return commitOffset, nil
}
