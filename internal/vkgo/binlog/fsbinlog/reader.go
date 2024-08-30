// Copyright 2024 V Kontakte LLC
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
	"github.com/myxo/gofs"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/constants"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

type seekInfo = tlfsbinlog.SnapshotMeta

type ReaderSync interface {
	io.Reader
	Sync() error
}

type binlogReader struct {
	fs                  gofs.FS
	fsWatcher           *fsnotify.Watcher
	stat                *stat
	readyCallbackCalled bool
	fileHeaders         []FileHeader
	logger              binlog.Logger
	commitDuration      time.Duration

	stop         chan struct{}
	pidChanged   chan struct{}
	reindexEvent chan bool // make event bus if we have more events?
}

func newBinlogReader(fs gofs.FS, pidChanged chan struct{}, reindexEvent chan bool, commitDuration time.Duration, logger binlog.Logger, stat *stat, stopCh chan struct{}) (*binlogReader, error) {
	if logger == nil {
		logger = &binlog.EmptyLogger{}
	}
	return &binlogReader{
		fs:             fs,
		stop:           stopCh,
		reindexEvent:   reindexEvent,
		pidChanged:     pidChanged,
		stat:           stat,
		logger:         logger,
		commitDuration: commitDuration,
	}, nil
}

func (b *binlogReader) readAllFromPosition(fromPosition int64, prefixPath string, expectedMagic uint32, engine binlog.Engine, sm *seekInfo, endlessMode bool) (int64, uint32, error) {
	var (
		err          error
		posAfterRead int64
		crcAfterRead uint32
	)

	if endlessMode {
		b.fsWatcher, err = fsnotify.NewWatcher()
		if err != nil {
			return 0, 0, err
		}
		defer func() {
			_ = b.fsWatcher.Close()
			b.fsWatcher = nil
		}()
	}

	b.fileHeaders, err = ScanForFilesFromPos(b.fs, 0, prefixPath, expectedMagic, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan directory for binlog files: %w", err)
	}
	if len(b.fileHeaders) == 0 {
		return 0, 0, fmt.Errorf("binlog not found")
	}

	if fromPosition < b.fileHeaders[0].Position {
		return 0, 0, fmt.Errorf("cannot start from offset %d, oldest binlog file (%s) has offset %d",
			fromPosition, b.fileHeaders[0].FileName, b.fileHeaders[0].Position)
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
		rotated := false
		for i := fileIndex; i < len(b.fileHeaders); i++ {
			fileHdr := &b.fileHeaders[i]
			b.stat.currentBinlogPath.Store(fileHdr.FileName)

			if !firstIter {
				sm = nil
				fromPosition = 0
			}
			firstIter = false

			var pos int64
			var crc uint32
			pos, crc, rotated, err = b.readBinlogFromFile(fileHdr, fromPosition, engine, sm, endlessMode)
			// ErrUpgradeToBarsicLev is allowed to pup up for correct switching
			if err == ErrUpgradeToBarsicLev {
				return pos, crc, err
			}

			if err != nil {
				return posAfterRead, crcAfterRead, err
			}
			posAfterRead, crcAfterRead = pos, crc
		}

		fileIndex = len(b.fileHeaders)
		lastBinlogPosition := b.fileHeaders[fileIndex-1].Position

		newFileHeaders, err := ScanForFilesFromPos(b.fs, lastBinlogPosition, prefixPath, 0, b.fileHeaders)
		if err != nil {
			return posAfterRead, crcAfterRead, fmt.Errorf("ScanForFilesFromPos failed at pos %d: %w", lastBinlogPosition, err)
		}
		if len(newFileHeaders) != 0 {
			b.fileHeaders = append(b.fileHeaders, newFileHeaders...)
			continue
		}

		if b.pidChanged == nil && !endlessMode {
			if rotated {
				b.logger.Warnf("Binlog reading finished, but last read file (%s) have RotateTo event. "+
					"This mean that there is some other binlog file and may indicate an error",
					b.fileHeaders[len(b.fileHeaders)-1].FileName)
			}
			break loop
		}

		newFileHeaders, err = b.waitForNewBinlogFile(lastBinlogPosition, prefixPath, b.fileHeaders)
		if err != nil {
			return posAfterRead, crcAfterRead, fmt.Errorf("waitForNewBinlogFile failed at pos %d: %w", lastBinlogPosition, err)
		}
		b.fileHeaders = append(b.fileHeaders, newFileHeaders...)
	}

	return posAfterRead, crcAfterRead, nil
}

func (b *binlogReader) waitForNewBinlogFile(lastBinlogPosition int64, prefixPath string, currHdrs []FileHeader) ([]FileHeader, error) {
	for {
		// no point to specify expected magic, since we already read first file
		fileHeaders, err := ScanForFilesFromPos(b.fs, lastBinlogPosition, prefixPath, 0, currHdrs)
		if err != nil {
			return nil, err
		}

		if len(fileHeaders) > 0 {
			return fileHeaders, nil
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-b.stop:
			return nil, errStopped
		case <-timer.C:
		}
	}
}

func (b *binlogReader) readBinlogFromFile(header *FileHeader, fromPos int64, engine binlog.Engine, si *seekInfo, endlessMode bool) (int64, uint32, bool, error) {
	fd, err := b.fs.Open(header.FileName)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = fd.Close() }()

	if endlessMode {
		if err = b.fsWatcher.Add(header.FileName); err != nil {
			return 0, 0, false, err
		}
		defer func() { _ = b.fsWatcher.Remove(header.FileName) }()
	}

	var r ReaderSync
	r = fd

	if header.CompressInfo.Compressed {
		fi, err := fd.Stat()
		if err != nil {
			return 0, 0, false, err
		}

		_, err = fd.Seek(header.CompressInfo.headerSize, 0)
		if err != nil {
			return 0, 0, false, err
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
		endlessMode,
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
		to := min(bytes, bufSize)
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

func (b *binlogReader) readUncompressedFile(r ReaderSync, curPos int64, curCrc32 uint32, startPos int64, engine binlog.Engine, si *seekInfo, endlessMode bool) (_ int64, _ uint32, rotated bool, err error) {
	var (
		readBytes           int
		clientDontKnowMagic bool
		processErr          error
		finish              bool
		commitPos           int64
	)
	b.stat.positionInCurFile.Store(levRotateSize)

	buffer := newReadBuffer(64 * 1024)
	commitTimer := time.NewTimer(b.commitDuration)
	makeCommit := func(int64, uint32) error {
		if commitPos == curPos {
			return nil
		}
		commitPos = curPos
		if err := r.Sync(); err != nil {
			return fmt.Errorf("failed fsync in reader: %w", err)
		}
		snapMeta := prepareSnapMeta(curPos, curCrc32, b.stat.lastTimestamp.Load())
		if err := engine.Commit(curPos, snapMeta, curPos); err != nil {
			return fmt.Errorf("Engine.Commit return error %w", err)
		}
		return nil
	}

	curPos, curCrc32, err = b.readAndUpdateCRCIfNeed(r, curPos, curCrc32, startPos, si)
	if err != nil {
		return curPos, curCrc32, false, err
	}

loop:
	for !finish {
		curPos += int64(readBytes)
		curCrc32 = crc32.Update(curCrc32, crc32.IEEETable, buffer.Bytes()[:readBytes])

		b.stat.lastPosition.Store(curPos)
		b.stat.positionInCurFile.Add(int64(readBytes))

		if processErr != nil && !isExpectedError(processErr) {
			// ErrUpgradeToBarsicLev is allowed to pup up for correct switching
			if processErr == ErrUpgradeToBarsicLev {
				return curPos, curCrc32, true, ErrUpgradeToBarsicLev
			}

			return curPos, curCrc32, false, fmt.Errorf("Engine.Apply return error %w", processErr)
		}

		select {
		case <-b.stop:
			return curPos, curCrc32, false, errStopped
		case <-commitTimer.C:
			// Фейковый коммит. fsync делает репликатор и мы не знаем когда он произошел, поэтому так.
			// Делаем это для того, чтобы быть похожим на Barsic
			if err := makeCommit(curPos, curCrc32); err != nil {
				return curPos, curCrc32, false, err
			}
			commitTimer.Reset(b.commitDuration)
		case <-b.reindexEvent:
			engine.StartReindex(&EmptyReindexOperator{})
		default:
		}

		if curPos-commitPos > int64(uncommittedMaxSize) {
			if err := makeCommit(curPos, curCrc32); err != nil {
				return curPos, curCrc32, false, err
			}
		}

		{
			buffer.RemoveProcessed(readBytes)
			readBytes = 0

			var n int
			if isEOFErr(processErr) || buffer.IsLowFilled() {
				var readError error
				n, readError = buffer.TryReadFrom(r)
				if readError != nil && !isEOFErr(readError) {
					return curPos, curCrc32, false, readError
				}
			}

			if buffer.size == 0 || (isEOFErr(processErr) && n == 0) {
				// if masterNotReady, we may be in change pid procedure
				if b.pidChanged != nil {
					// Нотифицируем клиента, что мы дошли до какого-то конца
					if err := engine.ChangeRole(binlog.ChangeRoleInfo{
						IsMaster: !endlessMode,
						IsReady:  false,
					}); err != nil {
						return curPos, curCrc32, false, fmt.Errorf("Engine.ChangeRole return error %w", err)
					}

					b.logger.Infof("Binlog: running in subst mode, wait for previous process to be killed")
					<-b.pidChanged

					b.logger.Infof("Binlog: previous process is dead, continue to read binlog")

					// Теперь файлы на диске гарантированно актуальные, больше в эту ветку заходить не нужно
					b.pidChanged = nil
					continue
				}

				if !endlessMode {
					if err := makeCommit(curPos, curCrc32); err != nil {
						return curPos, curCrc32, false, err
					}
					break loop
				}

				if b.fsWatcher == nil {
					return 0, 0, false, fmt.Errorf("internal error: fsWatcher is nil in replica mode")
				}

				if !b.readyCallbackCalled {
					b.readyCallbackCalled = true
					if err := engine.ChangeRole(binlog.ChangeRoleInfo{IsMaster: false, IsReady: true}); err != nil {
						return curPos, curCrc32, false, fmt.Errorf("Engine.ChangeRole return error %w", err)
					}
				}

				select {
				case <-b.fsWatcher.Events:
					continue
				case err = <-b.fsWatcher.Errors:
					// can't really do anything here... Just clean up channel
					b.logger.Warnf("Error while fsWatcher: %s", err)
					continue
				case <-commitTimer.C:
					if err := makeCommit(curPos, curCrc32); err != nil {
						return curPos, curCrc32, false, err
					}
					commitTimer.Reset(b.commitDuration)
					continue
				case <-b.reindexEvent:
					engine.StartReindex(&EmptyReindexOperator{})
				case <-b.stop:
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
			return curPos, curCrc32, false, fmt.Errorf("unexpected magic magicKfsBinlogZipMagic (%x)", magicKfsBinlogZipMagic)

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
					return 0, 0, false, fmt.Errorf(`crc32 mismatch. levCrc32.crc32:%08x my:%08x position:%d`, lev.Crc32, crc32BeforeEvent, curPos)
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
			return curPos, curCrc32, false, fmt.Errorf("unexpected magic magicLevSetPersistentConfigArray (%x)", magicLevSetPersistentConfigArray)

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
				return curPos, curCrc32, false, binlog.ErrorUnknownMagic
			}

			// never give unaligned buffer to user, because it will be ambiguous what to do with the padding
			alignedBuff := getAlignedBuffer(buff)
			var newPos int64
			newPos, processErr = engine.Apply(alignedBuff)
			if newPos < curPos {
				return curPos, curCrc32, false, fmt.Errorf("apply lev: new position (%d) is less than preceous (%d)", newPos, curPos)
			}
			readBytes = int(newPos - curPos)

			if readBytes > len(alignedBuff) {
				return curPos, curCrc32, false, fmt.Errorf("engine declared to read %d bytes, but payload buffer only have %d bytes, abort reading",
					readBytes, len(alignedBuff))
			}

			if readBytes <= 0 && processErr == nil {
				return curPos, curCrc32, false, fmt.Errorf("engine does not read eny bytes and does not return eny error")
			}

			// Если колбек не вычитал данные с выравниванием, то делаем это за него. // TODO: нужно ли это делать?
			readBytes = AddPadding(readBytes)

			if processErr != nil && errors.Is(processErr, binlog.ErrorUnknownMagic) {
				clientDontKnowMagic = true
				continue
			}
		}

		clientDontKnowMagic = false

		if serviceLev && (processErr == nil || processErr == ErrUpgradeToBarsicLev) {
			newPos, skipErr := engine.Skip(int64(readBytes))
			if skipErr != nil {
				return curPos, curCrc32, false, fmt.Errorf("Engine.Skip return error %w", skipErr)
			}
			if newPos != curPos+int64(readBytes) {
				return curPos, curCrc32, false, fmt.Errorf("Engine.Skip return new position %d, expect %d", newPos, curPos+int64(readBytes))
			}
		}
	}
	if err := makeCommit(curPos, curCrc32); err != nil {
		return curPos, curCrc32, false, err
	}
	return curPos, curCrc32, finish, nil
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

func readBinlogHeaderFile(fs gofs.FS, header *FileHeader, expectedMagic uint32) error {
	fd, err := fs.OpenFile(header.FileName, os.O_RDONLY, defaultFilePerm)
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

func readBinlogHeader(header *FileHeader, buff []byte, expectedMagic uint32) error {
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
