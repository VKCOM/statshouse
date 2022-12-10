// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"fmt"
	"os"
	"time"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

var testEnvTurnOffFSync bool

type binlogWriter struct {
	logger             binlog.Logger
	engine             binlog.Engine
	stat               *stat
	fd                 *os.File
	BinlogChunkMaxSize int64
	curFileHeader      *fileHeader
	PrefixPath         string
	buffEx             *buffExchange

	close chan struct{}

	// Канал сигнализирующий о наличии данных. Протокол следующий:
	// если передано nil, то мы можем забрать (steal) данные, если в данный момент не заняты
	// если переданы буфферы, это значит, что клиент уперся в HardMemLimit и мы обязанны их взять как можно скорее
	ch chan struct{}
}

func newBinlogWriter(
	logger binlog.Logger,
	engine binlog.Engine,
	options binlog.Options,
	posAfterRead int64,
	lastFileHdr *fileHeader,
	buffEx *buffExchange,
	stat *stat,
) (*binlogWriter, error) {
	bw := &binlogWriter{
		logger:             logger,
		engine:             engine,
		stat:               stat,
		BinlogChunkMaxSize: int64(options.MaxChunkSize),
		curFileHeader:      lastFileHdr,
		PrefixPath:         options.PrefixPath,
		buffEx:             buffEx,
		close:              make(chan struct{}),
		ch:                 make(chan struct{}, 1),
	}

	expectedSize := posAfterRead - lastFileHdr.Position
	err := bw.initChunk(false, lastFileHdr.FileName, expectedSize)
	if err != nil {
		return nil, err
	}
	return bw, nil
}

func (bw *binlogWriter) Close() {
	bw.close <- struct{}{}
}

func (bw *binlogWriter) initChunk(createNew bool, filename string, expectedSize int64) error {
	flags := os.O_WRONLY | os.O_APPEND
	if createNew {
		flags |= os.O_CREATE | os.O_EXCL
	}
	fd, err := os.OpenFile(filename, flags, defaultFilePerm)
	if err != nil {
		return err
	}

	fi, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if fi.Size() != expectedSize {
		return fmt.Errorf("current position in file is not equal file size, expected: %d, file size: %d", expectedSize, fi.Size())
	}

	bw.fd = fd
	return nil
}

func (bw *binlogWriter) loop() (PositionInfo, error) {
	var rd replaceData
	flushCh := time.NewTimer(flushInterval)
	buff := make([]byte, 0, defaultBuffSize)

	defer func() {
		flushCh.Stop()

		if bw.fd == nil {
			return
		}

		if bw.logger != nil {
			if fi, err := bw.fd.Stat(); err != nil {
				bw.logger.Infof(`fsBinlogImpl stop writing loop: last filename:%q (could not get stats:%s)`, bw.fd.Name(), err)
			} else {
				bw.logger.Infof(`fsBinlogImpl stop writing loop: last filename:%q file_size=%d pos_in_file=%d global_pos=%d crc=0x%x`,
					bw.fd.Name(), fi.Size(), rd.offsetLocal, rd.offsetGlobal, rd.crc)
			}
		}

		if !testEnvTurnOffFSync {
			if err := bw.fd.Sync(); err != nil {
				if bw.logger != nil {
					bw.logger.Errorf("Cannot flush data to file: %s", err)
				}
			}
		}
		if err := bw.fd.Close(); err != nil {
			if bw.logger != nil {
				bw.logger.Warnf("Error while closing file: %s", err)
			}
		}
	}()

	var (
		stop    bool
		dirty   bool
		loopErr error
	)
loop:
	for !stop {
		hitTimer := false

		select {
		case _, ok := <-bw.ch:
			if !ok {
				stop = true
			}

		case <-flushCh.C:
			hitTimer = true
			flushCh.Reset(flushInterval)

		case <-bw.close:
			stop = true
		}

		buff, rd = bw.buffEx.replaceBuff(buff)
		if len(buff) != 0 {
			if err := bw.writeBuffer(buff, &rd); err != nil {
				loopErr = err
				break loop
			}
			dirty = true
		}

		if dirty && (rd.commitASAP || hitTimer || stop) {
			var err error
			if !testEnvTurnOffFSync {
				err = bw.fd.Sync()
			}

			if err != nil {
				if bw.logger != nil {
					bw.logger.Warnf("Binlog: cannot fsync: %s", err)
				}
			} else {
				bw.stat.lastPosition.Store(rd.offsetGlobal)
				snapData := prepareSnapMeta(rd.offsetGlobal, rd.crc, bw.stat.lastTimestamp.Load())
				bw.engine.Commit(rd.offsetGlobal, snapData, rd.offsetGlobal)
				dirty = false
			}
		}
	}
	return PositionInfo{
		Offset:         rd.offsetGlobal,
		Crc:            rd.crc,
		LastFileHeader: *bw.curFileHeader,
	}, loopErr
}

func (bw *binlogWriter) writeBuffer(buff []byte, rd *replaceData) error {
	prevPos := int64(0)
	for _, pos := range rd.rotatePos {
		to := pos - levRotateSize
		if _, err := bw.fd.Write(buff[prevPos:to]); err != nil {
			return err
		}
		if err := bw.rotate(buff[to:pos], buff[pos:pos+levRotateSize]); err != nil {
			return err
		}
		prevPos = pos + levRotateSize
	}
	if _, err := bw.fd.Write(buff[prevPos:]); err != nil {
		return err
	}
	return nil
}

func (bw *binlogWriter) rotate(rotateTo, rotateFrom []byte) error {
	// Нужно сначала создать новый файл, записать в него ROTATE_FROM, синкнуть на диск, а потом уже писать ROTATE_TO и закрывать старый.
	// Так работают сишные движки и такое поведение ожидает репликатор.

	if !testEnvTurnOffFSync {
		if err := bw.fd.Sync(); err != nil {
			return err
		}
	}

	var newHeader fileHeader
	if _, err := readLevRotateFrom(&newHeader.LevRotateFrom, rotateFrom); err != nil {
		return err
	}
	newHeader.Position = newHeader.LevRotateFrom.CurLogPos
	newHeader.Timestamp = uint64(newHeader.LevRotateFrom.Timestamp)
	newHeader.FileName = chooseFilenameForChunk(newHeader.Position, bw.PrefixPath)

	bw.logger.Tracef("rotate binlog file on position %d, new binlog filename: %s", newHeader.Position, newHeader.FileName)

	prevChunkFd := bw.fd // Сохраняю старый файл для дозаписи события

	if err := bw.initChunk(true, newHeader.FileName, 0); err != nil {
		return err
	}

	// Сначала запись RotateFrom в новый бинлог
	if _, err := bw.fd.Write(rotateFrom); err != nil {
		return err
	}

	bw.curFileHeader = &newHeader
	bw.stat.lastTimestamp.Store(uint32(newHeader.LevRotateFrom.Timestamp))
	bw.stat.currentBinlogPath.Store(newHeader.FileName)

	if !testEnvTurnOffFSync {
		if err := bw.fd.Sync(); err != nil {
			if bw.logger != nil {
				bw.logger.Warnf(`could not sync new binlog file: %s`, err)
			}
		}
	}

	// Теперь записываю ROTATE_TO в прошлый	файл и закрываю его
	if _, err := prevChunkFd.Write(rotateTo); err != nil {
		return fmt.Errorf(`could not write ROTATE_TO event: %s`, err)
	}

	if !testEnvTurnOffFSync {
		if err := prevChunkFd.Sync(); err != nil {
			if bw.logger != nil {
				bw.logger.Warnf(`could not sync previous bw file after rotation: %s`, err)
			}
		}
	}

	if err := prevChunkFd.Chmod(0440); err != nil {
		if bw.logger != nil {
			bw.logger.Warnf("Can't set read only mode for the slice %s", prevChunkFd.Name())
		}
	}

	if err := prevChunkFd.Close(); err != nil {
		if bw.logger != nil {
			bw.logger.Warnf(`could not close previous bw file after rotation: %s`, err)
		}
	}

	return nil
}
