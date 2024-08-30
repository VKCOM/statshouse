// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"fmt"
	"os"
	"time"

	"github.com/myxo/gofs"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
)

type binlogWriter struct {
	logger             binlog.Logger
	fs                 gofs.FS
	engine             binlog.Engine
	stat               *stat
	fp                 *gofs.File
	BinlogChunkMaxSize int64
	curFileHeader      *FileHeader
	PrefixPath         string
	buffEx             *buffExchange
	writeCallDelay     time.Duration

	stop           chan struct{}
	reindexEventCh chan bool
	dataCh         chan struct{}
}

func newBinlogWriter(
	fs gofs.FS,
	logger binlog.Logger,
	engine binlog.Engine,
	options Options,
	posAfterRead int64,
	lastFileHdr *FileHeader,
	buffEx *buffExchange,
	stat *stat,
	stop chan struct{},
	reindexEventCh chan bool,
	writeCallDelay time.Duration,
) (*binlogWriter, error) {
	if logger == nil {
		logger = &binlog.EmptyLogger{}
	}
	bw := &binlogWriter{
		logger:             logger,
		fs:                 fs,
		engine:             engine,
		stat:               stat,
		BinlogChunkMaxSize: int64(options.MaxChunkSize),
		curFileHeader:      lastFileHdr,
		PrefixPath:         options.PrefixPath,
		buffEx:             buffEx,
		stop:               stop,
		reindexEventCh:     reindexEventCh,
		dataCh:             make(chan struct{}, 1),
		writeCallDelay:     writeCallDelay,
	}

	expectedSize := posAfterRead - lastFileHdr.Position
	err := bw.initChunk(false, lastFileHdr.FileName, expectedSize)
	if err != nil {
		return nil, err
	}
	return bw, nil
}

func (bw *binlogWriter) initChunk(createNew bool, filename string, expectedSize int64) error {
	flags := os.O_WRONLY | os.O_APPEND
	if createNew {
		flags |= os.O_CREATE | os.O_EXCL
	}
	fd, err := bw.fs.OpenFile(filename, flags, defaultFilePerm)
	if err != nil {
		return err
	}

	fi, err := bw.fs.Stat(filename)
	if err != nil {
		return err
	}
	if fi.Size() != expectedSize {
		return fmt.Errorf("current position in file is not equal file size, expected: %d, file size: %d", expectedSize, fi.Size())
	}

	bw.fp = fd
	return nil
}

func (bw *binlogWriter) loop(lastFsyncPos int64) (PositionInfo, error) {
	var rd replaceData
	flushCh := time.NewTimer(flushInterval)
	buff := make([]byte, 0, defaultBuffSize)

	defer func() {
		flushCh.Stop()

		if bw.fp == nil {
			return
		}

		if fi, err := bw.fp.Stat(); err != nil {
			bw.logger.Infof(`fsBinlogImpl stop writing loop: last filename:%q (could not get stats:%s)`, bw.fp.Name(), err)
		} else {
			bw.logger.Infof(`fsBinlogImpl stop writing loop: last filename:%q file_size=%d pos_in_file=%d global_pos=%d crc=0x%x`,
				bw.fp.Name(), fi.Size(), rd.offsetLocal, rd.offsetGlobal, rd.crc)
		}

		if err := bw.fp.Sync(); err != nil {
			bw.logger.Errorf("Cannot flush data to file: %s", err)
		}
		if err := bw.fp.Close(); err != nil {
			bw.logger.Warnf("Error while closing file: %s", err)
		}
	}()

	var (
		stop    bool
		dirty   bool
		loopErr error
	)
	lastWrite := time.Now()
loop:
	for !stop {
		hitTimer := false

		select {
		case _, ok := <-bw.dataCh:
			if !ok {
				stop = true
			}

		case <-flushCh.C:
			hitTimer = true
			flushCh.Reset(flushInterval)

		case <-bw.stop:
			bw.buffEx.stopAccept()
			stop = true

		case <-bw.reindexEventCh:
			bw.engine.StartReindex(&EmptyReindexOperator{})
			continue
		}

		buff, rd = bw.buffEx.replaceBuff(buff)
		if len(buff) != 0 {
			now := time.Now()
			sinceLastWrite := now.Sub(lastWrite)
			lastWrite = now
			if sinceLastWrite < bw.writeCallDelay {
				// We don't want to write every small event without batching (it may have unpredictable effects in replicator),
				// but don't want to over pessimize replication lag. Sleep for e.g. millisecond is a reasonable tradeoff
				time.Sleep(bw.writeCallDelay - sinceLastWrite)
				lastWrite = time.Now()
			}

			if err := bw.writeBuffer(buff, &rd); err != nil {
				_ = bw.engine.ChangeRole(binlog.ChangeRoleInfo{
					IsMaster: false,
					IsReady:  false,
				})
				_, _ = bw.engine.Revert(lastFsyncPos)
				loopErr = err
				break loop
			}
			dirty = true
		}

		bigUncommittedTail := rd.offsetGlobal-lastFsyncPos > uncommittedMaxSize

		if dirty && (rd.commitASAP || hitTimer || bigUncommittedTail || stop) {
			err := bw.fp.Sync()

			if err != nil {
				bw.logger.Errorf("Binlog: abort binlog writing, cannot fsync, error: %q, last fsync position: %d", err, lastFsyncPos)
				// on this stage we do not care about errors anymore
				_ = bw.engine.ChangeRole(binlog.ChangeRoleInfo{
					IsMaster: false,
					IsReady:  false,
				})
				_, _ = bw.engine.Revert(lastFsyncPos)
				loopErr = fmt.Errorf("fsync error: %w", err)
				break loop
			} else {
				bw.stat.lastPosition.Store(rd.offsetGlobal)
				lastFsyncPos = rd.offsetGlobal
				snapData := prepareSnapMeta(rd.offsetGlobal, rd.crc, bw.stat.lastTimestamp.Load())
				if err := bw.engine.Commit(rd.offsetGlobal, snapData, rd.offsetGlobal); err != nil {
					loopErr = err
					break loop
				}
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
		if _, err := bw.fp.Write(buff[prevPos:to]); err != nil {
			return err
		}
		if err := bw.rotate(buff[to:pos], buff[pos:pos+levRotateSize]); err != nil {
			return err
		}
		prevPos = pos + levRotateSize
	}
	if _, err := bw.fp.Write(buff[prevPos:]); err != nil {
		return err
	}
	return nil
}

func (bw *binlogWriter) rotate(rotateTo, rotateFrom []byte) error {
	// Нужно сначала создать новый файл, записать в него ROTATE_FROM, синкнуть на диск, а потом уже писать ROTATE_TO и закрывать старый.
	// Так работают сишные движки и такое поведение ожидает репликатор.

	if err := bw.fp.Sync(); err != nil {
		return err
	}

	var newHeader FileHeader
	if _, err := readLevRotateFrom(&newHeader.LevRotateFrom, rotateFrom); err != nil {
		return err
	}
	newHeader.Position = newHeader.LevRotateFrom.CurLogPos
	newHeader.Timestamp = uint64(newHeader.LevRotateFrom.Timestamp)
	newHeader.FileName = chooseFilenameForChunk(bw.fs, newHeader.Position, bw.PrefixPath)

	if bw.logger != nil {
		bw.logger.Tracef("rotate binlog file on position %d, new binlog filename: %s", newHeader.Position, newHeader.FileName)
	}

	prevChunkFd := bw.fp // Сохраняю старый файл для дозаписи события

	if err := bw.initChunk(true, newHeader.FileName, 0); err != nil {
		return err
	}

	// Сначала запись RotateFrom в новый бинлог
	if _, err := bw.fp.Write(rotateFrom); err != nil {
		return err
	}

	bw.curFileHeader = &newHeader
	bw.stat.lastTimestamp.Store(uint32(newHeader.LevRotateFrom.Timestamp))
	bw.stat.currentBinlogPath.Store(newHeader.FileName)

	if err := bw.fp.Sync(); err != nil {
		bw.logger.Warnf(`could not sync new binlog file: %s`, err)
	}

	// Теперь записываю ROTATE_TO в прошлый	файл и закрываю его
	if _, err := prevChunkFd.Write(rotateTo); err != nil {
		return fmt.Errorf(`could not write ROTATE_TO event: %s`, err)
	}

	if err := prevChunkFd.Sync(); err != nil {
		bw.logger.Warnf(`could not sync previous bw file after rotation: %s`, err)
	}

	if err := prevChunkFd.Chmod(0440); err != nil {
		bw.logger.Warnf("Can't set read only mode for the slice %s", prevChunkFd.Name())
	}

	if err := prevChunkFd.Close(); err != nil {
		bw.logger.Warnf(`could not close previous bw file after rotation: %s`, err)
	}

	return nil
}
