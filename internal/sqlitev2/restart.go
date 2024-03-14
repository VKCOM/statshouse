package sqlitev2

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
)

type walHdr struct {
	chkpt uint32
}

type walInfo struct {
	hdr        walHdr
	iWal       int // 0=wal, 1=wal2
	path       string
	restartPah string
}

func restart(opt Options, log *log.Logger) error {
	// todo нужен какой-то лок допольнительный, чтобы другой процесс не мог ничего сделать на время рестарта
	isExists, err := checkFileExist(opt.Path)
	if err != nil {
		return fmt.Errorf("faied to check db existance: %w", err)
	}
	if !isExists {
		return nil
	}
	wals, err := loadWalsInfo(opt.Path)
	if err != nil {
		return err
	}
	if len(wals) == 0 {
		// удалить restart файлы если есть
		return nil
	}
	if len(wals) == 1 {
		w := wals[0]
		iWal2 := ^w.iWal
		wal2Path := walPath(iWal2, opt.Path)
		wal2, isExists, err := loadWal(iWal2, restartPath(wal2Path))
		if err != nil {
			return err
		}
		if !isExists {
			// todo???
		}
		wal2.path = wal2Path
		wal2.restartPah = restartPath(wal2Path)
		wals = append(wals, wal2)
		err = os.Rename(wal2.restartPah, wal2.path)
		if err != nil {
			panic(err)
		}
	}

	var commitOffset int64
	var dbOffset int64

	conn, err := newSqliteRWWALConn(opt.Path, opt.APPID, false, 100, log)
	if err != nil {
		panic(err)
	}
	rows := conn.queryLocked(context.Background(), query, "__select_binlog_committed_offset", nil, "SELECT offset FROM __binlog_commit_offset")
	if rows.err != nil {
		panic(rows.err)
	}
	for rows.Next() {
		isExists = true
		commitOffset = rows.ColumnInt64(0)
		return nil
	}
	if rows.err != nil {
		panic(err)
	}
	rows = conn.queryLocked(context.Background(), query, "__select_binlog_pos", nil, "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return rows.err
	}
	for rows.Next() {
		isExists = true
		dbOffset = rows.ColumnInt64(0)
		return nil
	}
	if rows.err != nil {
		return rows.err
	}
	if commitOffset > 0 && dbOffset <= commitOffset {
		err = conn.conn.Checkpoint() // все окей, база с 2 валами на уровне с бинлогом делаем чекпоинт
		if err != nil {
			panic(err)
		}
		return nil
	}
	err = conn.Close()
	if err != nil {
		panic(err)
	}
	if commitOffset > 0 && dbOffset <= commitOffset {
		return nil
	}
	if !isExists || commitOffset == 0 {
		return nil
	}
	err = os.Rename(wals[1].path, wals[1].restartPah)
	if err != nil {
		panic(err)
	}

	conn, err = newSqliteRWWALConn(opt.Path, opt.APPID, false, 100, log)
	if err != nil {
		panic(err) // TODO точно ли будет всегда корректно открываться с одним валом
	}
	var withoutWalOffset int64
	isExists = false
	rows = conn.queryLocked(context.Background(), query, "__select_binlog_pos_from_1_wal", nil, "SELECT offset from __binlog_offset")
	if rows.err != nil {
		return rows.err
	}
	for rows.Next() {
		isExists = true
		withoutWalOffset = rows.ColumnInt64(0)
		return nil
	}
	if rows.err != nil {
		return rows.err
	}
	err = conn.Close()
	if err != nil {
		panic(err)
	}
	if !isExists || withoutWalOffset == 0 {
		return nil // TODO?
	}

	if withoutWalOffset <= commitOffset {
		err = conn.conn.Checkpoint()
		if err != nil {
			panic(err)
		}
	}
	err = conn.Close()
	if err != nil {
		panic(err)
	}
	// База консистента, удаляем оба вала
	_ = os.Remove(wals[1].restartPah) // сначала удаляем новый файл, если упадем после этой строки, то при рестарте надо будет просто удалить старый вал
	_ = os.Remove(wals[1].path)       // RO/RW соединение создает недостающий вал
	_ = os.Remove(wals[0].path)
	return nil
}

func walPath(iWal int, path string) string {
	if iWal == 0 {
		return path + "-wal"
	} else {
		return path + "-wal2"
	}
}

func restartPath(path string) string {
	return path + ".restart"
}

func readChkpt(f *os.File) (uint32, error) {
	hdr := [24]byte{}
	n, err := f.Read(hdr[:])
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

func loadWal(iWal int, path string) (i walInfo, walExists bool, _ error) {
	wal, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return i, false, nil
		} else {
			panic(err)
		}
	}
	defer wal.Close()
	chkpt, err := readChkpt(wal)
	if err != nil {
		panic(err)
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

	wal1, wal1Exists, err := loadWal(0, walPath(0, path))
	if err != nil {
		panic(err)
	}
	wal2, wal2Exists, err := loadWal(1, walPath(1, path))
	if err != nil {
		panic(err)
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
		panic(err)
	}
	return wals, nil
}
