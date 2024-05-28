package blackbox

import (
	"log"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"pgregory.net/rapid"
)

type engineState struct {
	testCase *Case
	clients  []*client
	eng      *engine
}

type client struct {
	r *rapid.T
	//start   chan struct{}
	//restart chan *sync.WaitGroup
	stop chan struct{}
	//offset      chan int64 // revert offset
	testCase *Case
}

//func (s *engineState) wakeupClients() {
//	for _, c := range s.clients {
//		c.start <- struct{}{}
//	}
//}
//
//func (s *engineState) sleepAndWaitClients() {
//	wg := &sync.WaitGroup{}
//	wg.Add(len(s.clients))
//	for _, c := range s.clients {
//		c.restart <- wg
//	}
//	wg.Wait()
//}

func (c *client) clientLoop() {
	for {
		_ = c.testCase.Put()
		select {
		case <-c.stop:
			return
		default:

		}
		//_ = c.testCase.Check(c.r)
		//if err != nil {
		//	panic(err)
		//}
	}
}

const n = 8

func (s *engineState) init(r *rapid.T, tempDir string) {
	var i int64
	for i = 1; i <= n; i++ {
		c := rpc.NewClient(rpc.ClientWithLogf(func(format string, args ...any) {
			log.Println(format, args)
		}))
		cc := &tlkv_engine.Client{
			Client:  c,
			Network: "tcp4",
			Address: "127.0.0.1:2442",
		}
		tc := NewCase(i*10, i*10+10, tempDir, &kvEngine{client: cc})
		client := &client{
			stop:     make(chan struct{}),
			testCase: tc,
			r:        r,
		}
		s.clients = append(s.clients, client)
	}
	prefix := tempDir
	if err := createBinlog(prefix, "1,0"); err != nil {
		panic(err)
	}
	db := path.Join(prefix, "db")
	e, err := runEngine(db, tempDir)
	if err != nil {
		panic(err)
	}
	s.eng = e
	c := rpc.NewClient(rpc.ClientWithLogf(log.Printf))
	client := &tlkv_engine.Client{
		Client:  c,
		Network: "tcp4",
		Address: "127.0.0.1:2442",
	}
	time.Sleep(time.Second * 5)
	s.testCase = NewCase(1000, 9999999999, tempDir, &kvEngine{client: client})
	for _, c := range s.clients {
		go c.clientLoop()
	}
}

const BinlogMagic = 123

func (s *engineState) Backup(r *rapid.T) {
	//s.testCase.Backup(r)
}

func (s *engineState) Put(r *rapid.T) {
	for i := 0; i < 500; i++ {
		err := s.testCase.Put()
		require.NoError(r, err)
	}
}

func (s *engineState) Kill(r *rapid.T) {
	//log.Println("KILL")
	_, err := s.eng.kill()
	if err != nil {
		r.Errorf(err.Error())
		return
	}
	err = s.eng.restart(s.eng.db)
	if err != nil {
		panic(err)
	}
	s.testCase.HealchCheck(r)
}

func (s *engineState) Shutdown(r *rapid.T) {
	state, err := s.eng.shutdown()
	if err != nil {
		r.Errorf(err.Error())
		return
	}
	if !state.Success() {
		r.Fatal("state.Success() must be true")
	}
	err = s.eng.restart(s.eng.db)
	if err != nil {
		panic(err)
	}
	s.testCase.HealchCheck(r)
}

func (s *engineState) Check(r *rapid.T) {
}

func (s *engineState) stop() {
	for _, c := range s.clients {
		c.stop <- struct{}{}
	}
	_, _ = s.eng.kill()
}

func TestEngine(t *testing.T) {
	rapid.Check(t, func(r *rapid.T) {
		state := &engineState{}
		defer func() {
			state.stop()
		}()
		dir := t.TempDir()
		state.init(r, dir)
		r.Repeat(rapid.StateMachineActions(state))
	})
}
