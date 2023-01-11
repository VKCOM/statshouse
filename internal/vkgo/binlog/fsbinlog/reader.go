// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/vkcom/statshouse/internal/vkgo/algo"
	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/constants"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

type seekInfo = tlfsbinlog.SnapshotMeta

type binlogReader struct {
	fsWatcher           *fsnotify.Watcher
	stat                *stat
	readyCallbackCalled bool
	fileHeaders         []fileHeader
	logger              binlog.Logger
	commitDuration      time.Duration

	stop       *chan struct{}
	pidChanged *chan struct{}
}

func newBinlogReader(pidChanged *chan struct{}, commitDuration time.Duration, logger binlog.Logger, stat *stat, stopCh *chan struct{}) (*binlogReader, error) {
	return &binlogReader{
		stop:           stopCh,
		pidChanged:     pidChanged,
		stat:           stat,
		logger:         logger,
		commitDuration: commitDuration,
	}, nil
}

func (b *binlogReader) readAllFromPosition(fromPosition int64, prefixPath string, expectedMagic uint32, engine binlog.Engine, sm *seekInfo, isMaster bool) (int64, uint32, error) {
	var (
		err          error
		posAfterRead int64
		crcAfterRead uint32
	)

	if !isMaster {
		b.fsWatcher, err = fsnotify.NewWatcher()
		if err != nil {
			return 0, 0, err
		}
		defer func() {
			_ = b.fsWatcher.Close()
			b.fsWatcher = nil
		}()
	}

	b.fileHeaders, err = scanForFilesFromPos(0, prefixPath, expectedMagic, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan directory for binlog files: %w", err)
	}
	if len(b.fileHeaders) == 0 {
		return 0, 0, fmt.Errorf("binlog not found")
	}

	fileIndex := getBinlogIndexByPosition(fromPosition, b.fileHeaders)
	firstIter := true
	if sm != nil {
		commitFileIndex := getBinlogIndexByPosition(sm.CommitPosition, b.fileHeaders)
		if commitFileIndex != fileIndex || fromPosition < sm.CommitPosition {
			sm = nil
		}
	}

loop:
	for {
		for i := fileIndex; i < len(b.fileHeaders); i++ {
			fileHdr := &b.fileHeaders[i]
			b.stat.currentBinlogPath.Store(fileHdr.FileName)

			if !firstIter {
				sm = nil
				fromPosition = 0
			}
			firstIter = false

			posAfterRead, crcAfterRead, err = b.readBinlogFromFile(fileHdr, fromPosition, engine, sm, isMaster)
			if err != nil {
				return posAfterRead, crcAfterRead, err
			}
		}

		if isMaster {
			break loop
		}

		fileIndex = len(b.fileHeaders)

		lastBinlogPosition := b.fileHeaders[fileIndex-1].Position
		var newFileHeaders []fileHeader
		newFileHeaders, err = b.waitForNewBinlogFile(lastBinlogPosition, prefixPath, b.fileHeaders)
		if err != nil {
			return posAfterRead, crcAfterRead, fmt.Errorf("waitForNewBinlogFile failed at pos %d: %w", lastBinlogPosition, err)
		}
		b.fileHeaders = append(b.fileHeaders, newFileHeaders...)
	}

	return posAfterRead, crcAfterRead, nil
}

func (b *binlogReader) waitForNewBinlogFile(lastBinlogPosition int64, prefixPath string, currHdrs []fileHeader) ([]fileHeader, error) {
	for {
		// no point to specify expected magic, since we already read first file
		fileHeaders, err := scanForFilesFromPos(lastBinlogPosition, prefixPath, 0, currHdrs)
		if err != nil {
			return nil, err
		}

		if len(fileHeaders) > 0 {
			return fileHeaders, nil
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-*b.stop:
			return nil, errStopped
		case <-timer.C:
		}
	}
}

func (b *binlogReader) readBinlogFromFile(header *fileHeader, fromPos int64, engine binlog.Engine, si *seekInfo, isMaster bool) (int64, uint32, error) {
	fd, err := os.Open(header.FileName)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = fd.Close() }()

	if !isMaster {
		if err = b.fsWatcher.Add(header.FileName); err != nil {
			return 0, 0, err
		}
		defer func() { _ = b.fsWatcher.Remove(header.FileName) }()
	}

	var r io.Reader
	r = fd

	if header.CompressInfo.Compressed {
		fi, err := fd.Stat()
		if err != nil {
			return 0, 0, err
		}

		_, err = fd.Seek(header.CompressInfo.headerSize, 0)
		if err != nil {
			return 0, 0, err
		}

		r = newDecompressor(
			fd,
			uint64(fi.Size()),
			header.CompressInfo.Algo,
			header.CompressInfo.ChunkOffsets,
		)
	}

	return b.readUncompressedFile(
		r,
		header.LevRotateFrom.CurLogPos,
		header.LevRotateFrom.Crc32,
		fromPos,
		engine,
		si,
		isMaster,
	)
}

func isEOFErr(err error) bool {
	return err == binlog.ErrorNotEnoughData ||
		err == io.EOF ||
		err == io.ErrUnexpectedEOF ||
		errors.Is(err, binlog.ErrorNotEnoughData) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func isExpectedError(err error) bool {
	return isEOFErr(err) || err == binlog.ErrorUnknownMagic || errors.Is(err, binlog.ErrorUnknownMagic)
}

func readToAndUpdateCrc(r io.Reader, bytes int64, crc uint32) (uint32, error) {
	const bufSize = 64 * 1024
	buff := make([]byte, bufSize)
	for bytes > 0 {
		to := algo.MinInt64(bytes, bufSize)
		n, err := r.Read(buff[:to])
		if err != nil {
			// if we seek, file should already exist. EOF is an error
			return crc, err
		}
		crc = crc32.Update(crc, crc32.IEEETable, buff[:n])
		bytes -= int64(n)
	}
	return crc, nil
}

func (b *binlogReader) readUncompressedFile(r io.Reader, curPos int64, curCrc32 uint32, startPos int64, engine binlog.Engine, si *seekInfo, isMaster bool) (int64, uint32, error) {
	var (
		readBytes           int
		clientDontKnowMagic bool
		processErr          error
		finish              bool
		hasUncommitted      bool
		err                 error
	)
	b.stat.positionInCurFile.Store(levRotateSize)

	buffer := newReadBuffer(64 * 1024)
	commitTimer := time.NewTimer(b.commitDuration)
	makeCommit := func(int64, uint32) error {
		if hasUncommitted {
			snapMeta := prepareSnapMeta(curPos, curCrc32, b.stat.lastTimestamp.Load())
			if err := engine.Commit(curPos, snapMeta, curPos); err != nil {
				return fmt.Errorf("Engine.Commit return error %w", err)
			}
			hasUncommitted = false
		}
		return nil
	}

	curPos, curCrc32, err = b.readAndUpdateCRCIfNeed(r, curPos, curCrc32, startPos, si)
	if err != nil {
		return curPos, curCrc32, err
	}

loop:
	for !finish {
		curPos += int64(readBytes)
		curCrc32 = crc32.Update(curCrc32, crc32.IEEETable, buffer.Bytes()[:readBytes])

		b.stat.lastPosition.Store(curPos)
		b.stat.positionInCurFile.Add(int64(readBytes))

		if processErr != nil && !isExpectedError(processErr) {
			return curPos, curCrc32, fmt.Errorf("Engine.Apply return error %w", processErr)
		}

		select {
		case <-*b.stop:
			return curPos, curCrc32, errStopped
		case <-commitTimer.C:
			// Фейковый коммит. fsync делает репликатор и мы не знаем когда он произошел, поэтому так.
			// Делаем это для того, чтобы быть похожим на Barsic
			if err := makeCommit(curPos, curCrc32); err != nil {
				return curPos, curCrc32, err
			}
			commitTimer.Reset(b.commitDuration)
		default:
		}

		{
			buffer.RemoveProcessed(readBytes)
			readBytes = 0

			var n int
			if isEOFErr(processErr) || buffer.IsLowFilled() {
				var readError error
				n, readError = buffer.TryReadFrom(r)
				if readError != nil && !isEOFErr(readError) {
					return curPos, curCrc32, readError
				}
			}

			if buffer.size == 0 || (isEOFErr(processErr) && n == 0) {
				// if masterNotReady, we may be in change pid procedure
				if b.pidChanged != nil {
					// Нотифицируем клиента, что мы дошли до какого-то конца
					if err := engine.ChangeRole(binlog.ChangeRoleInfo{
						IsMaster: isMaster,
						IsReady:  false,
					}); err != nil {
						return curPos, curCrc32, fmt.Errorf("Engine.ChangeRole return error %w", err)
					}

					// Ждем пока он нотифицирует нас, что предыдущий процесс грохнут
					<-*b.pidChanged

					// Теперь файлы на диске гарантированно актуальные, больше в эту ветку заходить не нужно
					b.pidChanged = nil
					continue
				}

				if isMaster {
					break loop
				}

				if b.fsWatcher == nil {
					return 0, 0, fmt.Errorf("internal error: fsWatcher is nil in replica mode")
				}

				if !b.readyCallbackCalled {
					b.readyCallbackCalled = true
					if err := engine.ChangeRole(binlog.ChangeRoleInfo{
						IsMaster: false,
						IsReady:  true,
					}); err != nil {
						return curPos, curCrc32, fmt.Errorf("Engine.ChangeRole return error %w", err)
					}
				}

				select {
				case <-b.fsWatcher.Events:
					continue
				case err = <-b.fsWatcher.Errors:
					// can't really do anything here... Just clean up channel
					if b.logger != nil {
						b.logger.Warnf("Error while fsWatcher: %s", err)
					}
					continue
				case <-commitTimer.C:
					if err := makeCommit(curPos, curCrc32); err != nil {
						return curPos, curCrc32, err

					}
					commitTimer.Reset(b.commitDuration)
					continue
				case <-*b.stop:
					break loop
				}
			}
		}

		buff := buffer.Bytes()
		crc32BeforeEvent := curCrc32
		serviceLev := true

		if len(buff) < 4 {
			processErr = binlog.ErrorNotEnoughData
			continue
		}

		levType := binary.LittleEndian.Uint32(buff)

		switch levType {
		case constants.FsbinlogLevStart:
			// just read bytes
			var lev tlfsbinlog.LevStart
			readBytes, processErr = readLevStart(&lev, buff)

		case magicLevRotateFrom:
			var lev levRotateFrom
			readBytes, processErr = readLevRotateFrom(&lev, buff)
			if processErr == nil {
				b.updateTimestamp(uint32(lev.Timestamp))
			}

		case magicKfsBinlogZipMagic:
			// в не сжатых бинлогах не может быть такого типа
			return curPos, curCrc32, fmt.Errorf("unexpected magic magicKfsBinlogZipMagic (%x)", magicKfsBinlogZipMagic)

		case magicLevTag:
			// just read bytes
			var lev levTag
			readBytes, processErr = readLevTag(&lev, buff)

		case magicLevCrc32:
			var lev levCrc32
			readBytes, processErr = readLevCrc32(&lev, buff)
			if processErr == nil {
				b.updateTimestamp(uint32(lev.Timestamp))
				if lev.Crc32 != crc32BeforeEvent {
					return 0, 0, fmt.Errorf(`crc32 mismatch. levCrc32.crc32:%08x my:%08x position:%d`, lev.Crc32, crc32BeforeEvent, curPos)
				}
			}

		case magicLevTimestamp:
			var lev levTimestamp
			readBytes, processErr = readLevTimestamp(&lev, buff)
			if processErr == nil {
				b.updateTimestamp(uint32(lev.Timestamp))
			}

		case magicLevRotateTo:
			var lev levRotateTo
			readBytes, processErr = readLevRotateTo(&lev, buff)
			if processErr == nil {
				finish = true
			}

		case magicLevSetPersistentConfigValue:
			var lev levSetPersistentConfigValue
			readBytes, processErr = readLevSetPersistentConfigValue(&lev, buff)
			readBytes = AddPadding(readBytes)

		case magicLevSetPersistentConfigArray:
			// реализовать при необходимости lev_set_persistent_config_array
			return curPos, curCrc32, fmt.Errorf("unexpected magic magicLevSetPersistentConfigArray (%x)", magicLevSetPersistentConfigArray)

		case constants.FsbinlogLevUpgradeToGms:
			var lev tlfsbinlog.LevUpgradeToGms
			var leftover []byte
			leftover, processErr = lev.ReadBoxed(buff)
			readBytes = len(buff) - len(leftover)
			if processErr == nil {
				processErr = ErrUpgradeToBarsicLev
			}

		default:
			serviceLev = false

			if clientDontKnowMagic {
				// Опять зашли сюда => клиент уже не знает этот magic и это не служебное событие
				return curPos, curCrc32, binlog.ErrorUnknownMagic
			}

			// never give unaligned buffer to user, because it will be ambiguous what to do with the padding
			alignedBuff := getAlignedBuffer(buff)
			var newPos int64
			newPos, processErr = engine.Apply(alignedBuff)
			if newPos < curPos {
				return curPos, curCrc32, fmt.Errorf("apply lev: new position (%d) is less than preceous (%d)", newPos, curPos)
			}
			readBytes = int(newPos - curPos)

			if readBytes > len(alignedBuff) {
				return curPos, curCrc32, fmt.Errorf("engine declared to read %d bytes, but payload buffer only have %d bytes, abort reading",
					readBytes, len(alignedBuff))
			}

			if readBytes <= 0 && processErr == nil {
				return curPos, curCrc32, fmt.Errorf("engine does not read eny bytes and does not return eny error")
			}

			// Если колбек не вычитал данные с выравниванием, то делаем это за него. // TODO: нужно ли это делать?
			readBytes = AddPadding(readBytes)

			if processErr != nil && errors.Is(processErr, binlog.ErrorUnknownMagic) {
				clientDontKnowMagic = true
				continue
			}
		}

		clientDontKnowMagic = false
		hasUncommitted = true

		if serviceLev && (processErr == nil || processErr == ErrUpgradeToBarsicLev) {
			newPos, skipErr := engine.Skip(int64(readBytes))
			if skipErr != nil {
				return curPos, curCrc32, fmt.Errorf("Engine.Skip return error %w", skipErr)
			}
			if newPos != curPos+int64(readBytes) {
				return curPos, curCrc32, fmt.Errorf("Engine.Skip return new position %d, expect %d", newPos, curPos+int64(readBytes))
			}
		}
	}
	return curPos, curCrc32, nil
}

func (b *binlogReader) updateTimestamp(timestamp uint32) {
	b.stat.lastTimestamp.Store(timestamp)
	if b.stat.firstReadTimestamp.Load() == 0 {
		b.stat.firstReadTimestamp.Store(timestamp)
	}
}

func (b *binlogReader) readAndUpdateCRCIfNeed(r io.Reader, curPos int64, curCrc32 uint32, startPos int64, si *seekInfo) (int64, uint32, error) {
	if si != nil {
		// Barsic have same restriction
		if si.CommitPosition > startPos {
			return 0, 0, fmt.Errorf("start offset position is lesser than commit position in SnapshotMeta")
		}

		if curPos <= si.CommitPosition {
			realCrc, err := readToAndUpdateCrc(r, si.CommitPosition-curPos, curCrc32)
			if err != nil {
				return 0, 0, fmt.Errorf("cannot seek to commit position %d: %w", startPos, err)
			}
			if realCrc != si.CommitCrc {
				return 0, 0, fmt.Errorf("tryed to seek to pos %d, expected crc=0x%x, got=0x%x", si.CommitPosition, si.CommitCrc, realCrc)
			}
			curPos = si.CommitPosition
			curCrc32 = si.CommitCrc
			b.updateTimestamp(si.CommitTs)
		}
	}

	if curPos < startPos {
		realCrc, err := readToAndUpdateCrc(r, startPos-curPos, curCrc32)
		if err != nil {
			return 0, 0, fmt.Errorf("cannot seek to position %d: %w", startPos, err)
		}
		curPos = startPos
		curCrc32 = realCrc
	}
	return curPos, curCrc32, nil
}

func readBinlogHeaderFile(header *fileHeader, expectedMagic uint32) error {
	fd, err := os.OpenFile(header.FileName, os.O_RDONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	defer func() {
		_ = fd.Close()
	}()

	buff := make([]byte, 1024*10) // should be big enough
	n, err := fd.Read(buff)
	if n == 0 && err != nil {
		return err
	}
	buff = buff[:n]
	return readBinlogHeader(header, buff, expectedMagic)
}

func readBinlogHeader(header *fileHeader, buff []byte, expectedMagic uint32) error {
	levType := binary.LittleEndian.Uint32(buff)

	switch levType {
	case constants.FsbinlogLevStart:
		var lev tlfsbinlog.LevStart
		if _, err := lev.ReadBoxed(buff); err != nil {
			return err
		}
		if expectedMagic != 0 && expectedMagic != uint32(lev.SchemaId) {
			return fmt.Errorf("expect magic %x got %x", expectedMagic, uint32(lev.SchemaId))
		}

	case magicLevRotateFrom:
		if _, err := readLevRotateFrom(&header.LevRotateFrom, buff); err != nil {
			return err
		}
		header.Position = header.LevRotateFrom.CurLogPos
		header.Timestamp = uint64(header.LevRotateFrom.Timestamp)

	case magicKfsBinlogZipMagic:
		var lev kfsBinlogZipHeader
		hdrSize, err := readKfsBinlogZipHeader(&lev, buff)
		if err != nil {
			return err
		}

		header.CompressInfo.Compressed = true
		header.CompressInfo.headerSize = hdrSize
		header.CompressInfo.ChunkOffsets = append(header.CompressInfo.ChunkOffsets[:0], lev.ChunkOffset...)
		header.CompressInfo.Algo = lev.getCompressAlgo()
		if header.CompressInfo.Algo == compressAlgoUnknown {
			return fmt.Errorf(`unknown compression algorithm (kfsBinlogZipHeader.Format: %x)`, lev.Format)
		}

		return readBinlogHeader(header, lev.First36Bytes[:], expectedMagic)

	default:
		return fmt.Errorf("unexpected magic %x", levType)
	}

	return nil
}
