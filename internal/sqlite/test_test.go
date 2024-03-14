package sqlite

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"pgregory.net/rand"
)

/*
** The WAL header is 32 bytes in size and consists of the following eight
** big-endian 32-bit unsigned integer values:
**
**     0: Magic number.  0x377f0682 or 0x377f0683
**     4: File format version.  Currently 3007000
**     8: Database page size.  Example: 1024
**    12: Checkpoint sequence number
**    16: Salt-1, random integer incremented with each checkpoint
**    20: Salt-2, a different random integer changing with each ckpt
**    24: Checksum-1 (first part of checksum for first 24 bytes of header).
**    28: Checksum-2 (second part of checksum for first 24 bytes of header).

** Immediately following the wal-header are zero or more frames. Each
** frame consists of a 24-byte frame-header followed by a <page-size> bytes
** of page data. The frame-header is six big-endian 32-bit unsigned
** integer values, as follows:
**
**     0: Page number.
**     4: For commit records, the size of the database image in pages
**        after the commit. For all other records, zero.
**     8: Salt-1 (copied from the header)
**    12: Salt-2 (copied from the header)
**    16: Checksum-1.
**    20: Checksum-2.



wal-index: header first_index_block [index_block]
first_index_block|index_block: page_mapping hash_table

page_mapping_first_index_block: [HASHTABLE_NPAGE_ONE]
page_mapping_other_index_block: [HASHTABLE_NPAGE]

HASHTABLE_NPAGE: [32-bit page number] 4096 elements?
HASHTABLE_NPAGE_ONE: [32-bit page number] total 4062 bytes?

HASHTABLE_NSLOT: 16-bit
-- HASHTABLE_NSLOT: [16-bit] = 2*HASHTABLE_NPAGE


Each index block
** holds over 4000 entries.

So two or three index blocks are sufficient
** to cover a typical 10 megabyte WAL file, assuming 1K pages.

The wal-index is divided into pages of WALINDEX_PGSZ bytes each.
#define WALINDEX_PGSZ   (                                         \
sizeof(ht_slot)*HASHTABLE_NSLOT + HASHTABLE_NPAGE*sizeof(u32) \
)
2 * 8192 + 4096 * 4 = 32768

WALINDEX_PGSZ: [4096]int32 [8192]int16


WALINDEX_HDR_SIZE    (sizeof(WalIndexHdr)*2+sizeof(WalCkptInfo))

/* A block of WALINDEX_LOCK_RESERVED bytes beginning at
** WALINDEX_LOCK_OFFSET is reserved for locks. Since some systems
** only support mandatory file-locks, we do not read or write data
** from the region of the file on which locks are applied.

#define WALINDEX_LOCK_OFFSET (sizeof(WalIndexHdr)*2+offsetof(WalCkptInfo,aLock))
#define WALINDEX_HDR_SIZE    (sizeof(WalIndexHdr)*2+sizeof(WalCkptInfo))

/* Size of header before each frame in wal
#define WAL_FRAME_HDRSIZE 24

/* Size of write ahead log header, including checksum.
#define WAL_HDRSIZE 32

*/

func TestTest(t *testing.T) {
	for i := 0; i < 10000; i++ {
		if i&(i-1) == 0 {
			fmt.Println(i)
		}
	}
}

func Test_Engine_NoBinlog_Close1(t *testing.T) {
	f := func(n, k int) {
		schema := "CREATE TABLE IF NOT EXISTS test_db (data INTEGER PRIMARY KEY);"
		dir := t.TempDir()
		engine, err := OpenEngine(Options{
			Path:                   dir + "/db",
			APPID:                  32,
			Scheme:                 schema,
			DurabilityMode:         NoBinlog,
			CacheMaxSizePerConnect: 100,
			CommitOnEachWrite:      true,
		}, nil, nil, nil)
		require.NoError(t, err)
		wg := sync.WaitGroup{}
		ch := make(chan struct{})
		for i := 0; i < 0; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
			L:
				for {
					select {
					case _, _ = <-ch:
						break L
					default:
					}
					err := engine.View(context.Background(), "qqq", func(conn Conn) error {
						r := conn.Query("adas", "SELECT count(*) FROM test_db")
						for r.Next() {
						}
						return nil
					})
					require.NoError(t, err)
				}
			}()
		}
		for i := 0; i < n; i++ {
			err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
				for j := 0; j < k; j++ {
					_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", Int64("$data", rand.Int63()))
					if err != nil {
						return cache, err
					}
				}
				return cache, err
			})
			require.NoError(t, err)
			wal1, _ := os.ReadFile(dir + "/db-wal")
			wal2, _ := os.ReadFile(dir + "/db-wal2")
			if len(wal1) > 1000 {
				//fmt.Println("OK wal1")
				frame := wal1[len(wal1)-(4096+24)*1:]
				prefix := frame[:24]
				n := binary.BigEndian.Uint32(prefix[4:8])
				//fmt.Println(n)
				if n == 0 {
					fmt.Println(n)
				}
				require.Greater(t, int(n), 0)
			}
			if len(wal2) > 1000 {
				fmt.Println("OK wal2")
				prefix := wal2[len(wal2)-(4096+24)*1:][:24]
				n := binary.BigEndian.Uint32(prefix[4:8])
				require.Greater(t, int(n), 0)
			}
		}
		close(ch)
		wg.Wait()
		require.NoError(t, engine.Close(context.Background()))
		_ = os.RemoveAll(dir)
	}
	for i := 0; i < 1; i++ {
		f(int(1), 10000000)
	}
}

func Test_Engine_NoBinlog_Close2(t *testing.T) {
	schema := "CREATE TABLE IF NOT EXISTS test_db (data INTEGER PRIMARY KEY);"
	dir := t.TempDir()
	engine, err := OpenEngine(Options{
		Path:                   dir + "/db",
		APPID:                  32,
		Scheme:                 schema,
		DurabilityMode:         NoBinlog,
		CacheMaxSizePerConnect: 100,
		CommitOnEachWrite:      true,
	}, nil, nil, nil)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	check := func() {
		wal1, _ := os.ReadFile(dir + "/db-wal")
		wal2, _ := os.ReadFile(dir + "/db-wal2")
		if len(wal1) > 1000 {
			//fmt.Println("OK wal1")
			frame := wal1[len(wal1)-(4096+24)*1:]
			prefix := frame[:24]
			n := binary.BigEndian.Uint32(prefix[4:8])
			//fmt.Println(n)
			if n == 0 {
				fmt.Println(n)
			}
			require.Greater(t, int(n), 0)
		}
		if len(wal2) > 1000 {
			fmt.Println("OK wal2")
			prefix := wal2[len(wal2)-(4096+24)*1:][:24]
			n := binary.BigEndian.Uint32(prefix[4:8])
			require.Greater(t, int(n), 0)
		}
	}
	ok := atomic.NewBool(false)
	view := func() {
		defer wg.Done()
		for {
			err := engine.View(context.Background(), "aaaa", func(conn Conn) error {
				r := conn.Query("aaa", "SELECT data FROM test_db")
				for r.Next() {
					if !ok.Load() {
						time.Sleep(time.Second)
					}
				}
				return nil
			})
			require.NoError(t, err)
		}
	}
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		k := int(10000000 / 2.5)
		for j := 0; j < k; j++ {
			if j == k/2 {
				wg.Add(1)
				go view()
				//}
				time.Sleep(time.Second * 3)
			}
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", Int64("$data", rand.Int63()))
			if err != nil {
				return cache, err
			}
		}
		return cache, err
	})
	require.NoError(t, err)
	check()
	fmt.Println("!!!!!")
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		k := int(100000)
		for j := 0; j < k; j++ {
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", Int64("$data", rand.Int63()))
			if err != nil {
				return cache, err
			}
		}
		return cache, err
	})
	fmt.Println("??????")
	check()
	err = engine.Do(context.Background(), "test", func(conn Conn, cache []byte) ([]byte, error) {
		k := int(100000)
		for j := 0; j < k; j++ {
			_, err = conn.Exec("test", "INSERT INTO test_db(data) VALUES ($data)", Int64("$data", rand.Int63()))
			if err != nil {
				return cache, err
			}
		}
		return cache, err
	})
	check()
	fmt.Println("CLOSE")
	require.NoError(t, engine.Close(context.Background()))
	_ = os.RemoveAll(dir)
}
