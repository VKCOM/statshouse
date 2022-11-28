// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fsbinlog

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/vkcom/statshouse/internal/vkgo/binlog"
	"github.com/vkcom/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
)

type Options struct {
	PrefixPath      string // путь до бинлога, включающий префикс для файлов финлога (например `some/path/gopusher_binlog`)
	Magic           uint32 // Ожидаемый magic (scheme) движка (0 - если можно открывать любой тип)
	ReadOnly        bool
	ReplicaMode     bool   // Бинлог открывается в режиме читающей реплики (постоянно читаем файл, если EOF, ждем новой записи)
	WriteCrcEveryKB int32  // Как часто нужно писать в бинлог LEV_CRC32 события (по умолчанию defaultWriteCrcEveryKB)
	MaxChunkSize    uint32 // Примерный порог (в байтах) для срабатывания ротации бинлога
	BuffSizeLimit   int    // Лимит буффера, при котором он будет передаваться воркеру на запись, если он свободен (по умолчанию равен WriteCrcEveryKB * 1024)
	HardMemLimit    int    // Лимит буффера, при котором включается механизм back pressure: Append начнет блокироваться, пока буффер не передастся на запись (по умолчанию defaultHardMemoryLimitKB)

	EngineIDInCluster uint // Номер данного шарда в кластере, для статы
	ClusterSize       uint // Размер кластера для данного шарда, для статы
	FlushInterval     time.Duration
}

const (
	mb                     = 1024 * 1024
	flushInterval          = 500 * time.Millisecond
	defaultWriteCrcEveryKB = 64
	defaultMaxChunkSize    = 1024 * mb
	defaultHardMemoryLimit = 100 * mb
	defaultBuffSize        = mb / 2

	defaultFilePerm = os.FileMode(0640)
)

var (
	errStopped = fmt.Errorf("already stopped")
)

type (
	// stat содержит переменные нужные только для передачи статистики
	stat struct {
		firstReadTimestamp atomic.Uint32
		lastReadTimestamp  atomic.Uint32
		lastTimestamp      atomic.Uint32
		lastPosition       atomic.Int64
		positionInCurFile  atomic.Int64
		loadTimeSec        atomic.Float64
		readStartPos       atomic.Uint64
		writeStartPos      atomic.Uint64
		currentBinlogPath  atomic.String
	}

	readFinishInfo struct {
		readPos        int64
		crc            uint32
		lastFileHeader fileHeader
	}

	fsBinlog struct {
		options      Options
		stat         stat
		engine       binlog.Engine
		logger       Logger
		writerInitMu sync.Mutex
		writer       *binlogWriter
		stop         chan struct{}
		pidChanged   *chan struct{}

		predict struct {
			lastPosForCrc int64
			fileStartPos  int64
			firstFile     bool
			currFileHash  uint64
		}

		buffEx *buffExchange
	}
)

// NewFsBinlog создает объект бинлога с правильными дефолтами
func NewFsBinlog(logger Logger, options Options) (binlog.Binlog, error) {
	return doCreateBinlog(logger, options)
}

// NewFsBinlogMasterChange используется в сценарии replace pid мастера (при апдейте движка). В этом сценарии
// может получится, что бинлоги дойдут до EOF, но потом что-то ещё запишется потому-что предыдущий мастер ещё жив.
// Для корректной обработки сценария есть эта функция.
//
// В этом режиме бинлог при получении ошибки EOF вызывает Binlog.ChangeRole с параметром Ready == false.
// После этого он блокируется на канале, который возвращается из этой функции. Обязанность клиента - дернуть
// канал, когда станет известно, что предыдущий мастер завершил работу (см. vkd.Options.ReadyHandler).
// После этого бинлог возвращается к обычной работе и при получении EOF будет вызван Binlog.ChangeRole с Ready == true
func NewFsBinlogMasterChange(logger Logger, options Options) (binlog.Binlog, *chan struct{}, error) {
	bl, err := doCreateBinlog(logger, options)
	if err != nil {
		return nil, nil, err
	}
	pidChanged := make(chan struct{}, 1)
	bl.pidChanged = &pidChanged
	return bl, &pidChanged, nil
}

func doCreateBinlog(logger Logger, options Options) (*fsBinlog, error) {
	if options.WriteCrcEveryKB == 0 {
		options.WriteCrcEveryKB = defaultWriteCrcEveryKB
	}
	if options.MaxChunkSize == 0 {
		options.MaxChunkSize = defaultMaxChunkSize
	}
	if options.BuffSizeLimit == 0 {
		options.BuffSizeLimit = int(options.WriteCrcEveryKB) * 1024
	}
	if options.HardMemLimit == 0 {
		options.HardMemLimit = defaultHardMemoryLimit
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = flushInterval
	}
	return &fsBinlog{
		options: options,
		logger:  logger,
		stop:    make(chan struct{}),
	}, nil
}

// CreateEmptyFsBinlog создает новый бинлог.
// Обычная схема такая: движок запускается с флагом --create-binlog и завершает работу, после его запускают на уже существующих файлах
func CreateEmptyFsBinlog(options Options) (string, error) {
	root := path.Dir(options.PrefixPath)
	basename := path.Base(options.PrefixPath)

	fileName := filepath.Join(root, basename+`.000000.bin`)

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		return fileName, err
	}

	if err := writeEmptyBinlog(options, fd); err != nil {
		_ = fd.Close()
		return fileName, err
	}

	if err = fd.Sync(); err != nil {
		return fileName, err
	}

	return fileName, fd.Close()
}

// AddPadding высчитывает размер событий бинлога с учетом alignment (по умолчанию все события выровнены по 4 байта)
// Типичное использование:
//
//	read := 0
//	for {
//		...
//		lev, n, err := deserializeLev(payload)
//		n = binlog2.AddPadding(n)
//		payload = payload(n:)
//		read += n
//		...
//	}
//	return read, err
//
// TODO: get rid of
func AddPadding(readBytes int) int {
	left := readBytes % 4
	if left == 0 {
		return readBytes
	}
	return readBytes + 4 - left
}

func (b *fsBinlog) Start(offset int64, snapshotMeta []byte, engine binlog.Engine) error {
	var si *tlfsbinlog.SnapshotMeta
	if len(snapshotMeta) > 0 {
		si = &tlfsbinlog.SnapshotMeta{}
		_, err := si.ReadBoxed(snapshotMeta)
		if err != nil {
			return fmt.Errorf("wrong snapshot meta format: %w", err)
		}
	}

	if engine == nil {
		return fmt.Errorf("engine pointer is nil")
	}
	b.engine = engine

	ri, err := b.readAll(offset, si)
	if err != nil {
		return fmt.Errorf("binlog reading failed: %w", err)
	}

	if b.options.ReplicaMode {
		return nil
	}

	if err = b.setupWriterWorker(ri); err != nil {
		return fmt.Errorf("binlog writer loop finished with error: %w", err)
	}

	b.predict.firstFile = ri.lastFileHeader.Position == 0
	if b.predict.firstFile {
		// We should save file content for hash calc in Rotate levs (only on first file)
		fp, err := os.Open(ri.lastFileHeader.FileName)
		if err != nil {
			return err
		}
		_, _ = fp.ReadAt(b.buffEx.hashBuff1[:], 0)
	} else {
		b.predict.currFileHash = ri.lastFileHeader.LevRotateFrom.CurLogHash
	}

	snapMeta := prepareSnapMeta(ri.readPos, ri.crc, b.stat.lastTimestamp.Load())
	engine.Commit(ri.readPos, snapMeta)

	engine.ChangeRole(binlog.ChangeRoleInfo{
		IsMaster: true,
		Ready:    true,
	})

	return b.writer.loop()
}

func (b *fsBinlog) Append(onOffset int64, payload []byte) (int64, error) {
	return b.doAppend(onOffset, payload, false)
}

func (b *fsBinlog) AppendASAP(onOffset int64, payload []byte) (int64, error) {
	return b.doAppend(onOffset, payload, true)
}

func (b *fsBinlog) doAppend(onOffset int64, body []byte, asap bool) (int64, error) {
	if !b.isWriterInitialized() {
		return 0, fmt.Errorf("writer is not initialized (still reading?)")
	}

	select {
	case <-b.stop:
		return -1, errStopped
	default:
	}

	curBuffSize, nextPos, err := b.putLevToBuffer(onOffset, body, asap)
	if err != nil {
		return nextPos, err
	}

	if asap || curBuffSize >= b.options.BuffSizeLimit {
		if curBuffSize >= b.options.HardMemLimit {
			if b.logger != nil {
				b.logger.Infof("Binlog: buffer size exceed hard memory limit (%d byte), start back pressure procedure", b.options.HardMemLimit)
			}

			b.writer.ch <- struct{}{}
			return nextPos, nil
		}

		// Продолжаем писать, writer заберет буффер, когда освободится
		select {
		case b.writer.ch <- struct{}{}:
		default:
		}
	}

	return nextPos, nil
}

func (b *fsBinlog) putLevToBuffer(incomeOffset int64, body []byte, asap bool) (int, int64, error) {
	b.buffEx.mu.Lock()
	defer b.buffEx.mu.Unlock()

	if incomeOffset != b.buffEx.rd.offsetGlobal {
		return b.buffEx.getSizeUnsafe(), b.buffEx.rd.offsetGlobal, fmt.Errorf("append get wrong offset, expect: %d, got: %d", b.buffEx.rd.offsetGlobal, incomeOffset)
	}

	b.buffEx.appendLevUnsafe(body)

	// Add Crc32 Lev if need
	if b.buffEx.rd.offsetGlobal-b.predict.lastPosForCrc >= int64(b.options.WriteCrcEveryKB*1024) {
		lev := levCrc32{
			Type:      magicLevCrc32,
			Timestamp: int32(time.Now().Unix()),
			Pos:       b.buffEx.rd.offsetGlobal,
			Crc32:     b.buffEx.rd.crc,
		}
		b.buffEx.appendLevUnsafe(writeLevCrc32(&lev))
		b.predict.lastPosForCrc = b.buffEx.rd.offsetGlobal

		b.stat.lastTimestamp.Store(uint32(lev.Timestamp))
	}

	// Add Rotate Levs if need
	if b.buffEx.rd.offsetGlobal-b.predict.fileStartPos >= int64(b.options.MaxChunkSize) {
		levRotateTo := levRotateTo{
			Type:        magicLevRotateTo,
			Timestamp:   int32(time.Now().Unix()),
			NextLogPos:  b.buffEx.rd.offsetGlobal + levRotateSize,
			Crc32:       b.buffEx.rd.crc,
			CurLogHash:  0, // fill later
			NextLogHash: 0, // fill later
		}

		if b.predict.firstFile {
			// If file size is less than 32K, calc md5 from whole file.
			// If more than 32K, calc md5 from first 16K and last 16K
			hash := md5.New()
			if b.buffEx.rd.offsetLocal <= hashDataSize {
				_, _ = hash.Write(b.buffEx.hashBuff1[:b.buffEx.rd.offsetLocal])
			} else {
				_, _ = hash.Write(b.buffEx.hashBuff1[:])

				if b.buffEx.rd.offsetLocal < 2*hashDataSize-levRotateSize {
					_, _ = hash.Write(b.buffEx.hashBuff2)
				} else {
					_, _ = hash.Write(b.buffEx.hashBuff2[len(b.buffEx.hashBuff2)-(hashDataSize-levRotateSize):])
				}
			}

			// Calc hash with rotateTo lev
			_, _ = hash.Write(writeLevRotateTo(&levRotateTo))
			levRotateTo.CurLogHash = binary.LittleEndian.Uint64(hash.Sum(nil))
			b.buffEx.hashBuff2 = nil // need only in first file
		} else {
			levRotateTo.CurLogHash = b.predict.currFileHash
		}

		levRotateTo.NextLogHash = calcNextLogHash(levRotateTo.CurLogHash, levRotateTo.NextLogPos, levRotateTo.Crc32)

		b.buffEx.appendLevUnsafe(writeLevRotateTo(&levRotateTo))
		b.buffEx.rotateFile()

		levRotateFrom := levRotateFrom{
			Type:        magicLevRotateFrom,
			Timestamp:   levRotateTo.Timestamp,
			CurLogPos:   levRotateTo.NextLogPos,
			Crc32:       b.buffEx.rd.crc,
			PrevLogHash: levRotateTo.CurLogHash,
			CurLogHash:  levRotateTo.NextLogHash,
		}
		b.buffEx.appendLevUnsafe(writeLevRotateFrom(&levRotateFrom))

		b.predict.currFileHash = levRotateFrom.CurLogHash

		b.predict.fileStartPos = b.buffEx.rd.offsetGlobal - levRotateSize
		b.stat.positionInCurFile.Store(b.buffEx.rd.offsetLocal)
		b.predict.firstFile = false
	}

	if asap {
		b.buffEx.rd.commitASAP = true
	}
	return b.buffEx.getSizeUnsafe(), b.buffEx.rd.offsetGlobal, nil
}

func (b *fsBinlog) AddStats(stats map[string]string) {
	// Все имена и формат значений приближены к сишной репе

	if b.options.ClusterSize > 0 {
		stats["slice_id"] = strconv.FormatUint(uint64(b.options.EngineIDInCluster), 10)
		stats["slices_count"] = strconv.FormatUint(uint64(b.options.ClusterSize), 10)
	}

	stats["binlog_load_time (s)"] = fmt.Sprintf("%.6f", b.stat.loadTimeSec.Load())
	stats["max_binlog_size"] = strconv.FormatUint(uint64(b.options.MaxChunkSize), 10)

	binlogOriginalSize := b.stat.writeStartPos.Load()
	if binlogOriginalSize == 0 {
		binlogOriginalSize = b.stat.readStartPos.Load()
	}
	stats["binlog_original_size"] = strconv.FormatUint(binlogOriginalSize, 10)

	binlogLoadedBytes := uint64(0)
	if b.stat.writeStartPos.Load() >= b.stat.readStartPos.Load() {
		binlogLoadedBytes = b.stat.writeStartPos.Load() - b.stat.readStartPos.Load()
	}
	stats["binlog_loaded_bytes"] = strconv.FormatUint(binlogLoadedBytes, 10)

	stats["current_binlog_size"] = strconv.FormatUint(uint64(b.stat.lastPosition.Load()), 10)

	stats["binlog_path"] = b.stat.currentBinlogPath.Load()

	stats["binlog_first_timestamp"] = strconv.FormatUint(uint64(b.stat.firstReadTimestamp.Load()), 10)
	stats["binlog_read_timestamp"] = strconv.FormatUint(uint64(b.stat.lastReadTimestamp.Load()), 10)
	stats["binlog_last_timestamp"] = strconv.FormatUint(uint64(b.stat.lastTimestamp.Load()), 10)

	if b.options.ReplicaMode {
		stats["binlog_last_file_size"] = strconv.FormatUint(uint64(b.stat.positionInCurFile.Load()), 10)
	}
}

func (b *fsBinlog) readAll(fromPosition int64, si *seekInfo) (readFinishInfo, error) {
	reader, err := newBinlogReader(b.pidChanged, b.options.FlushInterval, b.logger, &b.stat, &b.stop)
	if err != nil {
		return readFinishInfo{}, err
	}

	b.stat.readStartPos.Store(uint64(fromPosition))

	from := time.Now()
	posAfterRead, crcAfterRead, err := reader.readAllFromPosition(
		fromPosition,
		b.options.PrefixPath,
		b.options.Magic,
		b.engine,
		si,
		!b.options.ReplicaMode,
	)
	b.stat.loadTimeSec.Store(time.Since(from).Seconds())

	if errors.Is(err, errStopped) {
		err = nil // Not really an error
	}
	if err != nil {
		return readFinishInfo{}, err
	}

	readInfo := readFinishInfo{
		readPos:        posAfterRead,
		crc:            crcAfterRead,
		lastFileHeader: reader.fileHeaders[len(reader.fileHeaders)-1], // should have at least one
	}

	if b.logger != nil {
		if err != nil {
			b.logger.Errorf("fsBinlog reading error: read from pos %d to %d, current crc: 0x%x, error: %s",
				fromPosition,
				posAfterRead,
				crcAfterRead,
				err,
			)
		} else {
			b.logger.Infof("fsBinlog reading finished: read from pos %d to %d, current crc: 0x%x",
				fromPosition,
				posAfterRead,
				crcAfterRead,
			)
		}
	}

	return readInfo, err
}

func (b *fsBinlog) Shutdown() error {
	close(b.stop)

	b.writerInitMu.Lock()
	if b.writer != nil {
		b.writer.Close()
	}
	b.writerInitMu.Unlock()
	return nil
}

func (b *fsBinlog) isWriterInitialized() bool {
	b.writerInitMu.Lock()
	defer b.writerInitMu.Unlock()

	return b.writer != nil
}

func (b *fsBinlog) setupWriterWorker(readInfo readFinishInfo) error {
	if b.options.ReadOnly || b.options.ReplicaMode {
		return fmt.Errorf("cannot init writer: not in master mode")
	}

	b.writerInitMu.Lock()
	defer b.writerInitMu.Unlock()

	if b.writer != nil {
		return nil
	}

	b.stat.writeStartPos.Store(uint64(readInfo.readPos))

	b.predict.lastPosForCrc = readInfo.readPos
	b.predict.fileStartPos = readInfo.lastFileHeader.Position

	processedInFile := readInfo.readPos - readInfo.lastFileHeader.Position
	b.buffEx = newBuffEx(readInfo.crc, processedInFile, readInfo.readPos, int64(b.options.MaxChunkSize))

	var err error
	b.writer, err = newBinlogWriter(
		b.logger,
		b.engine,
		b.options,
		readInfo.readPos,
		&readInfo.lastFileHeader,
		b.buffEx,
		&b.stat,
	)
	return err
}
