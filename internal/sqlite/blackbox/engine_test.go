package blackbox

import (
	"log"
	"path"
	"sync"
	"testing"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlkv_engine"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"pgregory.net/rapid"
)

type engineState struct {
	testCase *Case
	clients  []*client
	pushers  []*client // эти клиенты не проверяют консистентность, нужны для того чтобы создать большую хаотичность в работе движка
	eng      *engine
}

type client struct {
	r           *rapid.T
	start       chan struct{}
	restart     chan *sync.WaitGroup
	stop        chan struct{}
	offset      chan int64 // revert offset
	testCase    *Case
	shouldCheck bool
}

func (s *engineState) revertClient(offset int64) {
	for _, c := range s.clients {
		c.offset <- offset
	}
}

func (s *engineState) wakeupClients() {
	for _, c := range s.clients {
		c.start <- struct{}{}
	}
}

func (s *engineState) sleepAndWaitClients() {
	wg := &sync.WaitGroup{}
	wg.Add(len(s.clients))
	for _, c := range s.clients {
		c.restart <- wg
	}
	wg.Wait()
}

func (c *client) clientLoop() {
	for {
		_ = c.testCase.Put()
		if !c.shouldCheck {
			select {
			case <-c.stop:
				return
			default:
				continue
			}
		}
		select {
		case <-c.stop:
			return
		case wg := <-c.restart:
			wg.Done()
		default:
			continue
		}
		offset := <-c.offset
		c.testCase.Revert(c.r, offset)
		<-c.start
		c.testCase.Check(c.r)
	}
}

const n = 5
const k = 5

func (s *engineState) init(r *rapid.T, tempDir string) {
	var i int64
	for i = 1; i <= n+k; i++ {
		c := rpc.NewClient(rpc.ClientWithLogf(func(format string, args ...any) {
			// log.Printf("CLIENT"+strconv.FormatInt(i, 10)+": "+format, args)
		}))
		cc := &tlkv_engine.Client{
			Client:  c,
			Network: "tcp4",
			Address: "127.0.0.1:2442",
		}
		tc := NewCase(i*10, i*10+10, tempDir, &kvEngine{client: cc})
		client := &client{
			offset:   make(chan int64, 1),
			restart:  make(chan *sync.WaitGroup, 1),
			start:    make(chan struct{}, 1),
			stop:     make(chan struct{}),
			testCase: tc,
			r:        r,
		}
		if i <= n {
			client.shouldCheck = true
			s.clients = append(s.clients, client)
		} else {
			client.stop = make(chan struct{}, 1)
			s.pushers = append(s.pushers, client)
		}
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
	s.testCase = NewCase(0, 10, tempDir, &kvEngine{client: client})
	for _, c := range s.clients {
		go c.clientLoop()
	}
	for _, c := range s.pushers {
		go c.clientLoop()
	}
}

const BinlogMagic = 123

func (s *engineState) Put(r *rapid.T) {
	err := s.testCase.Put()
	if err != nil {
		r.Errorf(err.Error())
	}
}

func (s *engineState) Kill(r *rapid.T) {
	s.sleepAndWaitClients()
	_, err := s.eng.kill()
	if err != nil {
		r.Errorf(err.Error())
		return
	}

	binlogPosition := getBinlogPosition(s.eng.prefix, BinlogMagic)
	log.Println("BINLOG POSITION", binlogPosition)
	s.testCase.Revert(r, binlogPosition)
	err = s.eng.restart(s.eng.db)
	if err != nil {
		panic(err)
	}
	s.revertClient(binlogPosition)
	s.wakeupClients()
}

func (s *engineState) Check(r *rapid.T) {
	s.testCase.Check(r)
}

func (s *engineState) stop() {
	for _, c := range s.clients {
		c.stop <- struct{}{}
	}
	for _, c := range s.pushers {
		c.stop <- struct{}{}
	}
	_, _ = s.eng.kill()
}

func TestEngine(t *testing.T) {
	t.SkipNow()
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
