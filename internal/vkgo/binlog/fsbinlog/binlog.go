// Copyright 2025 V Kontakte LLC
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

	"github.com/myxo/gofs"
	"go.uber.org/atomic"

	"github.com/VKCOM/statshouse/internal/vkgo/binlog"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/constants"
	"github.com/VKCOM/statshouse/internal/vkgo/binlog/fsbinlog/internal/gen/tlfsbinlog"
	"github.com/VKCOM/statshouse/internal/vkgo/tlbarsic"
)

const (
	mb                     = 1024 * 1024
	flushInterval          = 500 * time.Millisecond
	uncommittedMaxSize     = 50 * mb // may be set in Options if need
	writeCrcEveryBytes     = 64 * 1024
	defaultMaxChunkSize    = 1024 * mb
	defaultHardMemoryLimit = 100 * mb
	defaultBuffSize        = mb / 2
	defaultWriteCallDelay  = time.Millisecond

	defaultFilePerm = os.FileMode(0640)

	MagicFsbinlogSnapshotMeta = constants.FsbinlogSnapshotMeta
)

type LevUpgradeToBarsic = tlfsbinlog.LevUpgradeToGms

var (
	errStopped            = fmt.Errorf("already stopped")
	ErrUpgradeToBarsicLev = fmt.Errorf("need upgrade to barsic")
)

var runSimpleMode = false

// RunInSimpleMode return if binlog was created without subst mode.
// We often run in situation then engine use master subst, but
// not use NewFsBinlogMasterChange function. This function used by
// vkd to log this situation.
func RunInSimpleMode() bool {
	return runSimpleMode
}

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

	PositionInfo struct {
		Offset         int64
		Crc            uint32
		LastFileHeader FileHeader
	}

	fsBinlog struct {
		fs           gofs.FS
		options      Options
		stat         stat
		engine       binlog.Engine
		logger       binlog.Logger
		writerInitMu sync.Mutex
		writer       *binlogWriter
		stop         chan struct{}
		stopOnce     sync.Once
		pidChanged   chan struct{}
		reindexEvent chan bool

		predict struct {
			lastPosForCrc int64
			fileStartPos  int64
			firstFile     bool
			currFileHash  uint64
		}

		buffEx *buffExchange
	}

	// Настройки для fsBinlog имплементации, барсик настраиваится в конфиге барсика
	Options struct {
		PrefixPath        string // путь до бинлога, включающий префикс для файлов финлога (например `some/path/gopusher_binlog`)
		Magic             uint32 // Ожидаемый magic (scheme) движка (0 - если можно открывать любой тип)
		ReplicaMode       bool   // Бинлог открывается в режиме читающей реплики (постоянно читаем файл, если наткнулись на EOF, ждем новой записи)
		ReadAndExit       bool   // Может быть полезен для инструментария (например дампилки бинлога), не имеет смысла вместе с ReplicaMode
		MaxChunkSize      uint32 // Примерный порог (в байтах) для срабатывания ротации бинлога
		HardMemLimit      int    // Лимит буффера, при котором включается механизм back pressure: Append начнет блокироваться, пока буффер не передастся на запись
		EngineIDInCluster uint   // Номер данного шарда в кластере, для статы
		ClusterSize       uint   // Размер кластера для данного шарда, для статы

		// Файловая система, которую должен использовать fsBinlog.
		// Для прода должен быть nil (тогда будет использована реальная fs).
		// В тестах можно передавать fs := gofs.NewMemoryFs, тогда не будет обращений к диску и тесты будут побыстрее.
		Fs gofs.FS

		WriteCallDelay *time.Duration // Sleep duration between writes. Defaults to defaultWriteCallDelay. May be explicitly set to zero
	}
)

var _ binlog.Binlog = &fsBinlog{}

// BinlogReadWrite is an internal interface for migration
// TODO: move this interface to internal folder
type BinlogReadWrite interface {
	binlog.Binlog
	ReadAll(offset int64, snapshotMeta []byte, engine binlog.Engine) (PositionInfo, error)
	WriteLoop(ri PositionInfo) (PositionInfo, error)
}

// NewFsBinlog создает объект бинлога с правильными дефолтами
func NewFsBinlog(logger binlog.Logger, options Options) (BinlogReadWrite, error) {
	runSimpleMode = true
	return doCreateBinlog(logger, options), nil
}

// NewFsBinlogMasterChange используется в сценарии replace pid мастера (при апдейте движка). В этом сценарии
// может получится, что бинлоги дойдут до EOF, но потом что-то ещё запишется потому-что предыдущий мастер ещё жив.
// Для корректной обработки сценария есть эта функция.
//
// В этом режиме бинлог при получении ошибки EOF вызывает Binlog.ChangeRole с параметром IsReady == false.
// После этого он блокируется на канале, который возвращается из этой функции. Обязанность клиента - дернуть
// канал, когда станет известно, что предыдущий мастер завершил работу (см. vkd.Options.ReadyHandler).
// После этого бинлог возвращается к обычной работе и при получении EOF будет вызван Binlog.ChangeRole с IsReady == true
func NewFsBinlogMasterChange(logger binlog.Logger, options Options) (BinlogReadWrite, chan struct{}, error) {
	runSimpleMode = false
	bl := doCreateBinlog(logger, options)
	bl.pidChanged = make(chan struct{}, 1)
	return bl, bl.pidChanged, nil
}

func doCreateBinlog(logger binlog.Logger, options Options) *fsBinlog {
	if options.MaxChunkSize == 0 {
		options.MaxChunkSize = defaultMaxChunkSize
	}
	if options.HardMemLimit == 0 {
		options.HardMemLimit = defaultHardMemoryLimit
	}
	if options.WriteCallDelay == nil {
		dur := defaultWriteCallDelay
		options.WriteCallDelay = &dur
	}
	fs := options.Fs
	if fs == nil {
		fs = gofs.OsFs()
	}
	return &fsBinlog{
		fs:           fs,
		options:      options,
		logger:       logger,
		stop:         make(chan struct{}),
		reindexEvent: make(chan bool, 1),
	}
}

// CreateEmptyFsBinlog создает новый бинлог.
// Обычная схема такая: движок запускается с флагом --create-binlog и завершает работу, после его запускают на уже существующих файлах
func CreateEmptyFsBinlog(options Options) (string, error) {
	if len(options.PrefixPath) == 0 {
		return "", fmt.Errorf("PrefixPath is empty, cannot create binlog file")
	}
	root := path.Dir(options.PrefixPath)
	basename := path.Base(options.PrefixPath)

	fileName := filepath.Join(root, basename+`.000000.bin`)
	fs := options.Fs
	if fs == nil {
		fs = gofs.OsFs()
	}

	fd, err := fs.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, defaultFilePerm)
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

func (b *fsBinlog) ReadAll(offset int64, snapshotMeta []byte, engine binlog.Engine) (PositionInfo, error) {
	var si *tlfsbinlog.SnapshotMeta
	if len(snapshotMeta) > 0 {
		si = &tlfsbinlog.SnapshotMeta{}
		_, err := si.ReadBoxed(snapshotMeta)
		if err != nil {
			return PositionInfo{}, fmt.Errorf("wrong snapshot meta format: %w", err)
		}
	}

	if engine == nil {
		return PositionInfo{}, fmt.Errorf("engine pointer is nil")
	}
	b.engine = engine

	return b.readAll(offset, si)
}

func (b *fsBinlog) WriteLoop(ri PositionInfo) (PositionInfo, error) {
	if b.options.ReplicaMode {
		return ri, fmt.Errorf("cannot start write loop in replica mode")
	}

	if err := b.setupWriterWorker(ri); err != nil {
		return ri, fmt.Errorf("binlog writer loop finished with error: %w", err)
	}

	b.predict.firstFile = ri.LastFileHeader.Position == 0
	if b.predict.firstFile {
		// We should save file content for hash calc in Rotate levs (only on first file)
		fp, err := b.fs.Open(ri.LastFileHeader.FileName)
		if err != nil {
			return ri, err
		}
		_, _ = fp.ReadAt(b.buffEx.hashBuff1[:], 0)
	} else {
		b.predict.currFileHash = ri.LastFileHeader.LevRotateFrom.CurLogHash
	}

	// TODO: this is probably redundant
	snapMeta := prepareSnapMeta(ri.Offset, ri.Crc, b.stat.lastTimestamp.Load())
	b.engine.Commit(ri.Offset, snapMeta, ri.Offset)

	if err := b.engine.ChangeRole(binlog.ChangeRoleInfo{IsMaster: true, IsReady: true}); err != nil {
		return ri, fmt.Errorf("ChangeRole return error: %w", err)
	}

	return b.writer.loop(ri.Offset)
}

func (b *fsBinlog) Run(offset int64, snapshotMeta []byte, controlMeta []byte, engine binlog.Engine) error {
	readInfo, err := b.ReadAll(offset, snapshotMeta, engine)
	if errors.Is(err, errStopped) {
		return nil
	}
	if err != nil {
		return err
	}

	if b.options.ReplicaMode {
		return nil
	}

	if b.options.ReadAndExit {
		return nil
	}

	_, err = b.WriteLoop(readInfo)
	return err
}

func (b *fsBinlog) RequestShutdown() {
	b.stopOnce.Do(func() {
		close(b.stop)
	})
}

func (b *fsBinlog) RequestReindex(diff bool, fast bool) {
	select {
	case b.reindexEvent <- true:
	default:
	}
}

func (b *fsBinlog) EngineStatus(status binlog.EngineStatus) {} // Status will appear only after upgrade to Barsic

func (b *fsBinlog) GetStartCmd() (tlbarsic.Start, bool) {
	return tlbarsic.Start{}, false
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

	if b.logger != nil {
		var magic uint32
		if len(body) > 4 {
			magic = binary.LittleEndian.Uint32(body)
		}
		b.logger.Tracef("binlog.Append, offset=%d len=%d magic=0x%x, asap=%t", onOffset, len(body), magic, asap)
	}

	// return errStopper if write cycle is finished
	curBuffSize, nextPos, err := b.putLevToBuffer(onOffset, body, asap)
	if err != nil {
		return nextPos, err
	}

	if curBuffSize >= b.options.HardMemLimit {
		if b.logger != nil {
			b.logger.Infof("Binlog: buffer size exceed hard memory limit (%d byte), start back pressure procedure", b.options.HardMemLimit)
		}

		// Ждем пока writer реально не заберет
		b.writer.dataCh <- struct{}{}
		return nextPos, nil
	}

	// Продолжаем писать, writer заберет буффер, когда освободится
	select {
	case b.writer.dataCh <- struct{}{}:
	default:
	}

	return nextPos, nil
}

func (b *fsBinlog) putLevToBuffer(incomeOffset int64, body []byte, asap bool) (int, int64, error) {
	b.buffEx.mu.Lock()
	defer b.buffEx.mu.Unlock()

	if b.buffEx.finishAccept {
		return b.buffEx.getSizeUnsafe(), b.buffEx.rd.offsetGlobal, errStopped
	}

	if incomeOffset != b.buffEx.rd.offsetGlobal {
		return b.buffEx.getSizeUnsafe(), b.buffEx.rd.offsetGlobal, fmt.Errorf("append get wrong offset, expect: %d, got: %d", b.buffEx.rd.offsetGlobal, incomeOffset)
	}

	b.buffEx.appendLevUnsafe(body)

	// Add Crc32 Lev
	if b.buffEx.rd.offsetGlobal-b.predict.lastPosForCrc >= writeCrcEveryBytes {
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

	// Add Rotate Levs
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

	stats[binlog.StatSwitchStatus] = "no"
}

func (b *fsBinlog) readAll(fromPosition int64, si *seekInfo) (PositionInfo, error) {
	reader, err := newBinlogReader(b.fs, b.pidChanged, b.reindexEvent, flushInterval, b.logger, &b.stat, b.stop)
	if err != nil {
		return PositionInfo{}, err
	}

	b.stat.readStartPos.Store(uint64(fromPosition))

	from := time.Now()
	posAfterRead, crcAfterRead, err := reader.readAllFromPosition(
		fromPosition,
		b.options.PrefixPath,
		b.options.Magic,
		b.engine,
		si,
		b.options.ReplicaMode,
	)
	b.stat.loadTimeSec.Store(time.Since(from).Seconds())

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

	if err != nil && err != ErrUpgradeToBarsicLev {
		return PositionInfo{}, err
	}

	readInfo := PositionInfo{
		Offset:         posAfterRead,
		Crc:            crcAfterRead,
		LastFileHeader: reader.fileHeaders[len(reader.fileHeaders)-1], // should have at least one
	}

	return readInfo, err
}

func (b *fsBinlog) isWriterInitialized() bool {
	b.writerInitMu.Lock()
	defer b.writerInitMu.Unlock()

	return b.writer != nil
}

func (b *fsBinlog) setupWriterWorker(readInfo PositionInfo) error {
	if b.options.ReplicaMode {
		return fmt.Errorf("cannot init writer: not in master mode")
	}

	b.writerInitMu.Lock()
	defer b.writerInitMu.Unlock()

	if b.writer != nil {
		return nil
	}

	b.stat.writeStartPos.Store(uint64(readInfo.Offset))

	b.predict.lastPosForCrc = readInfo.Offset
	b.predict.fileStartPos = readInfo.LastFileHeader.Position

	processedInFile := readInfo.Offset - readInfo.LastFileHeader.Position
	b.buffEx = newBuffEx(readInfo.Crc, processedInFile, readInfo.Offset, int64(b.options.MaxChunkSize))

	var err error
	b.writer, err = newBinlogWriter(
		b.fs,
		b.logger,
		b.engine,
		b.options,
		readInfo.Offset,
		&readInfo.LastFileHeader,
		b.buffEx,
		&b.stat,
		b.stop,
		b.reindexEvent,
		*b.options.WriteCallDelay,
	)
	return err
}
