package sqlitev2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"

	restart2 "github.com/VKCOM/statshouse/internal/sqlitev2/checkpoint"
)

type walHdr struct {
	chkpt uint32
}

type walInfo struct {
	hdr        walHdr
	iWal       bool // 0=wal, -1=wal2
	path       string
	restartPah string
}

func runRestart(re *restart2.RestartFile, opt Options, log *log.Logger) (err error) {
	isExistsDb, err := checkFileExist(opt.Path)
	if err != nil {
		return fmt.Errorf("faied to check db existance: %w", err)
	}
	if !isExistsDb {
		log.Println("db not exists")
		return nil
	}
	wals, err := loadWalsInfo(opt.Path)
	if err != nil {
		return fmt.Errorf("failed to load wals: %w", err)
	}
	if len(wals) == 0 {
		log.Println("0 WALS found")
		return nil
	}
	if len(wals) == 1 {
		log.Println("one wal found")
		w := wals[0]
		iWal2 := !w.iWal
		wal2Path := walPath(iWal2, opt.Path)
		wal2, wal2IsExists, err := loadWal(iWal2, restartPath(wal2Path))
		if err != nil {
			return err
		}
		if !wal2IsExists {
			log.Println("one wal found remove it")
			err = os.Remove(w.path)
			if err != nil {
				return fmt.Errorf("failed to remove wal %s: %w", w.path, err)
			}
			return nil
		}
		wal2.path = wal2Path
		wal2.restartPah = restartPath(wal2Path)
		wals = append(wals, wal2)
		err = os.Rename(wal2.restartPah, wal2.path)
		if err != nil {
			return fmt.Errorf("failed to raname wal %s -> %s: %w", wal2.restartPah, wal2.path, err)
		}
	}

	var commitOffset = re.GetCommitOffset()
	log.Println("load commit offset", commitOffset)

	err = os.Rename(wals[1].path, wals[1].restartPah)
	if err != nil {
		return fmt.Errorf("failed to rename second wal to restart path: %w", err)
	}

	conn, err := newSqliteRWWALConn(opt.Path, opt.APPID, 100, opt.PageSize, opt.StatsOptions, log)
	if err != nil {
		return fmt.Errorf("failed to open 1 wal db: %w", err)
	}
	var withoutSecondWalDBOffset int64
	isExists := false
	rows := conn.queryLocked(context.Background(), query, "__select_binlog_pos_from_1_wal", nil, "SELECT offset from __binlog_offset")
	for rows.Next() {
		isExists = true
		withoutSecondWalDBOffset = rows.ColumnInteger(0)
	}
	if rows.Error() != nil {
		return fmt.Errorf("failed to select binlog pos from 1 wal: %w", rows.Error())
	}
	log.Println("db offset 1 wal:", withoutSecondWalDBOffset)

	var skipCheckpoint = !isExists || withoutSecondWalDBOffset == 0

	if !skipCheckpoint && withoutSecondWalDBOffset <= commitOffset {
		log.Println("do checkpoint to sync 1 wal")
		err = conn.conn.Checkpoint()
		if err != nil {
			return fmt.Errorf("failed to checkpoint 1 wal: %w", err)
		}
	}
	err = conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close 1 wal conn: %w", err)
	}

	// База консистента, удаляем оба вала
	// сначала удаляем новый файл, если упадем после этой строки, то при рестарте надо будет просто удалить старый вал
	_ = os.Remove(wals[1].path)
	_ = os.Remove(wals[1].restartPah)
	_ = os.Remove(wals[0].path)

	return nil
}

func walPath(iWal bool, path string) string {
	if !iWal {
		return path + "-wal"
	} else {
		return path + "-wal2"
	}
}

func restartPath(path string) string {
	return path + ".runRestart"
}

func readChkpt(f *os.File) (uint32, error) {
	hdr := [24]byte{}
	n, err := f.ReadAt(hdr[:], 0)
	if err != nil {
		return 0, err
	}
	if n != 24 {
		return 0, fmt.Errorf("expect 24")
	}
	chkpt := binary.BigEndian.Uint32(hdr[12:16])
	return chkpt, nil
}

func checkFileExist(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, err
		}
	}
	_ = f.Close()
	return true, nil
}

func checkFollowAndOrder(wals []walInfo) error {
	chkpt1 := wals[0].hdr.chkpt
	chkpt2 := wals[1].hdr.chkpt
	// The case where *-wal2 may follow *-wal
	if chkpt1 <= 0x0F && chkpt2 == chkpt1+1 {
		// ok
	} else // When *-wal may follow *-wal2
	if (chkpt2 == 0x0F && chkpt1 == 0) || (chkpt2 < 0x0F && chkpt2 == chkpt1-1) {
		slices.Reverse(wals)
	} else {
		panic("???")
	}
	return nil
}

func checkWal(f *os.File) (delete bool, _ error) {
	var hdr [24]byte
	n, err := f.ReadAt(hdr[:], 0)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read hdr: %w", err)
	}
	if n < 24 {
		return true, nil
	}
	return false, nil
}

func loadWal(iWal bool, path string) (i walInfo, walExists bool, _ error) {
	wal, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return i, false, nil
		} else {
			return i, false, fmt.Errorf("failed to open wal %s: %w", path, err)
		}
	}
	defer wal.Close()
	deleteWal, err := checkWal(wal)
	if err != nil {
		return i, false, fmt.Errorf("failed to check wal %s: %w", path, err)
	}
	if deleteWal {
		err := os.Remove(path)
		if err != nil {
			return i, false, fmt.Errorf("failed to delete wal %s: %w", path, err)
		}
		return i, false, nil
	}
	chkpt, err := readChkpt(wal)
	if err != nil {
		return i, false, fmt.Errorf("failed to read chkpot %s: %s", path, err)
	}
	return walInfo{
		hdr:        walHdr{chkpt: chkpt},
		iWal:       iWal,
		path:       path,
		restartPah: restartPath(path),
	}, true, nil
}

// TODO надо отсеивать фреймы которые не были закомиченны и проверять чексуммы
func loadWalsInfo(path string) (wals []walInfo, err error) {
	wal1, wal1Exists, err := loadWal(false, walPath(false, path))
	if err != nil {
		return wals, fmt.Errorf("failed to load wal1: %w", err)
	}
	wal2, wal2Exists, err := loadWal(true, walPath(true, path))
	if err != nil {
		return wals, fmt.Errorf("failed to load wal2: %w", err)
	}
	if wal1Exists {
		wals = append(wals, wal1)
	}
	if wal2Exists {
		wals = append(wals, wal2)
	}

	if !wal1Exists || !wal2Exists {
		return wals, nil
	}
	err = checkFollowAndOrder(wals)
	if err != nil {
		return wals, fmt.Errorf("failed to check wals consistency: %w", err)
	}
	return wals, nil
}
