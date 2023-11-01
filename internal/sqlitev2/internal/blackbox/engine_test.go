package blackbox

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

type engineState struct {
	e           *engine
	testCase    *Case
	backupIndex int
}

func (s *engineState) init(tempDir string) {
	prefix := tempDir
	if err := createBinlog(prefix, "1,0"); err != nil {
		panic(err)
	}
	db := path.Join(prefix, "db")
	e, err := runEngine(db, tempDir)
	if err != nil {
		panic(err)
	}
	s.e = e
	c := rpc.NewClient(rpc.ClientWithLogf(log.Printf))
	client := &tlkv_engine.Client{
		Client:  c,
		Network: "tcp4",
		Address: "127.0.0.1:2442",
	}
	s.testCase = NewCase(tempDir, &kvEngine{client: client})
}

const BinlogMagic = 123

func (s *engineState) clientLoop() {
	for {
		n := rand.Int() % 4
		switch n {
		case 0:
			fmt.Println("PUT")
			s.testCase.Put()
		case 1:
			fmt.Println("INCR")
			s.testCase.Incr()
		case 2:
			s.testCase.Backup()
		case 3:
			if s.testCase.LastBackupPath == "" || s.testCase.log[len(s.testCase.log)-1].offset == s.testCase.lastBackupOffset {
				continue
			}
			fmt.Println("KILL")

			_, err := s.e.kill()
			if err != nil {
				panic(err)
			}
			if s.testCase.LastBackupPath != "" {
				//s.e.db = s.testCase.LastBackupPath
				sOld, _ := os.Stat(s.e.db)
				fmt.Println("DB SIZE BEFORE REPLACE", sOld.Size())
				content, err := os.ReadFile(s.testCase.LastBackupPath)
				if err != nil {
					panic(err)
				}
				err = os.WriteFile(s.e.db, content, 0)
				if err != nil {
					panic(err)
				}
				fmt.Println("COPY", s.testCase.LastBackupPath, "TO", s.e.db)
				s1, _ := os.Stat(s.e.db)
				fmt.Println("DB SIZE", s1.Size())
				s2, _ := os.Stat(s.testCase.LastBackupPath)
				fmt.Println("SNAPSHOT SIZE", s2.Size())
				_ = os.Remove(s.e.db + "-shm")
				_ = os.Remove(s.e.db + "-wal")
				_ = os.Remove(s.e.db + "-wal2")
			}
			binlogPosition := getBinlogPosition(s.e.prefix, BinlogMagic)
			s.testCase.Revert(binlogPosition)
			err = s.e.restart(s.e.db)
			if err != nil {
				panic(err)
			}
		}
		s.testCase.Check()
		time.Sleep(time.Millisecond)
	}
}

func TestEngine(t *testing.T) {
	dir := t.TempDir()
	s := engineState{}
	s.init(dir)
	time.Sleep(time.Second)
	s.clientLoop()
}
