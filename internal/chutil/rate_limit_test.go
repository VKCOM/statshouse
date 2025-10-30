package chutil

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rand"
)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.FailNow(t, "timeout waiting for condition")
}

func isHealthy(rl *RateLimit) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.stage == StageHealth
}

func newCfg() RateLimitConfig {
	return RateLimitConfig{
		WindowDuration:     5 * time.Second,
		MaxErrorRate:       20,
		MaxInflightWeight:  10,
		RecoverWeightStep:  1,
		RecoverGapDuration: 100 * time.Millisecond,
		SleepMultiplier:    1.5,
		MaxSleepDuration:   2 * time.Second,
		MinSleepDuration:   100 * time.Millisecond,
		CheckCount:         3,
		RecalcInterval:     50 * time.Millisecond,
	}
}

func TestRateLimit_BasicFlow(t *testing.T) {
	rl := NewRateLimit(newCfg(), 0, 0)
	rl.Start()
	defer rl.Close()

	rl.AddInflightCount()
	rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 10 * time.Millisecond})
	waitFor(t, 500*time.Millisecond, func() bool { return isHealthy(rl) })
}

func TestRateLimit_SleepOnHighError(t *testing.T) {
	cfg := newCfg()
	rl := NewRateLimit(cfg, 0, 0)
	rl.Start()
	defer rl.Close()

	for i := 0; i < 50; i++ {
		rl.AddInflightCount()
		st := StatusError
		if i%10 == 0 {
			st = StatusSuccess
		}
		rl.RecordEvent(Event{Timestamp: time.Now(), Status: st, Duration: 20 * time.Millisecond})
	}

	waitFor(t, time.Second, func() bool { return !isHealthy(rl) && !rl.ShouldCheck() })
}

func TestRateLimit_WeightIncreasesWithErrorsAndLatency(t *testing.T) {
	cfg := newCfg()
	rl := NewRateLimit(cfg, 0, 0)
	rl.Start()
	defer rl.Close()

	for i := 0; i < 10; i++ {
		rl.AddInflightCount()
		rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 30 * time.Millisecond})
	}
	rl.AddInflightCount()
	rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusError, Duration: 30 * time.Millisecond})

	time.Sleep(2 * cfg.RecalcInterval)
	require.Equal(t, cfg.MaxInflightWeight/2, rl.healthState.InflightWeight)

	for i := 0; i < 10; i++ {
		rl.AddInflightCount()
		rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 60 * time.Millisecond})
	}
	rl.AddInflightCount()
	rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusError, Duration: 60 * time.Millisecond})

	time.Sleep(2 * cfg.RecalcInterval)
	require.LessOrEqual(t, cfg.MaxInflightWeight-1, rl.healthState.InflightWeight)

}

func TestRateLimit_WeightRecovery(t *testing.T) {
	cfg := newCfg()
	rl := NewRateLimit(cfg, 0, 0)
	rl.Start()
	defer rl.Close()

	for i := 0; i < 30; i++ {
		rl.AddInflightCount()
		rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusError, Duration: 20 * time.Millisecond})
	}
	waitFor(t, 3*time.Second, func() bool { return rl.ShouldCheck() })
	for i := 0; i < int(cfg.CheckCount); i++ {
		rl.RecordCheck(func() Event {
			return Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 20 * time.Millisecond}
		})
		time.Sleep(10 * time.Millisecond)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, ok := rl.GetInflightCount()
		return ok
	})
	rl.AddInflightCount()
	rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 20 * time.Millisecond})

	waitFor(t, 6*time.Second, func() bool {
		return rl.healthState.InflightWeight == 1
	})
}

func TestRateLimit_CheckFlowToHealth(t *testing.T) {
	cfg := newCfg()
	cfg.CheckCount = 3
	cfg.MinSleepDuration = 150 * time.Millisecond
	rl := NewRateLimit(cfg, 0, 0)
	rl.Start()
	defer rl.Close()

	for i := 0; i < 20; i++ {
		rl.AddInflightCount()
		rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusError, Duration: 10 * time.Millisecond})
	}
	waitFor(t, 3*time.Second, func() bool { return rl.ShouldCheck() })

	for i := 0; i < int(cfg.CheckCount); i++ {
		rl.RecordCheck(func() Event { return Event{Timestamp: time.Now(), Status: StatusSuccess} })
		time.Sleep(20 * time.Millisecond)
	}
	waitFor(t, 2*time.Second, func() bool { return isHealthy(rl) })
}

func TestRateLimit_InflightCounterSafety(t *testing.T) {
	cfg := newCfg()
	rl := NewRateLimit(cfg, 0, 0)
	rl.Start()
	defer rl.Close()

	n := runtime.GOMAXPROCS(0) * 4
	loops := 2000
	wg := sync.WaitGroup{}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				rl.AddInflightCount()
				rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 0})
			}
		}()
	}
	wg.Wait()
	require.True(t, isHealthy(rl))
}

func TestRateLimit_HighLoadRace(t *testing.T) {
	cfg := newCfg()
	cfg.RecalcInterval = 25 * time.Millisecond
	cfg.WindowDuration = 2 * time.Second
	cfg.RecoverGapDuration = 80 * time.Millisecond
	cfg.CheckCount = 5

	rl := NewRateLimit(cfg, 1, 1)
	rl.Start()
	defer rl.Close()

	n := runtime.GOMAXPROCS(0) * 8
	loops := 50000 / n
	wg := sync.WaitGroup{}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				op := (id + j) % 100
				switch {
				case op < 55:
					rl.AddInflightCount()
					rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: time.Duration(5+op%20) * time.Millisecond})
				case op < 80:
					rl.AddInflightCount()
					rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusError, Duration: time.Duration(10+op%30) * time.Millisecond})
				case op < 90:
					rl.AddInflightCount()
					rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: time.Duration(100+op%100) * time.Millisecond})
				default:
					if rl.ShouldCheck() {
						rl.RecordCheck(func() Event { return Event{Timestamp: time.Now(), Status: StatusSuccess} })
					}
				}
				_ = rl.ShouldCheck()
				_, _ = rl.GetInflightCount()
				time.Sleep(cfg.RecalcInterval)
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(2 * cfg.MaxSleepDuration)
	if rl.ShouldCheck() {
		for i := 0; i < int(cfg.CheckCount); i++ {
			rl.RecordCheck(func() Event { return Event{Timestamp: time.Now(), Status: StatusSuccess} })
			time.Sleep(20 * time.Millisecond)
		}
	}
	time.Sleep(2 * cfg.RecalcInterval)
	rl.AddInflightCount()
	rl.RecordEvent(Event{Timestamp: time.Now(), Status: StatusSuccess, Duration: 50 * time.Millisecond})
	time.Sleep(12 * cfg.RecoverGapDuration)

	require.Equal(t, uint64(0), rl.healthState.InflightCnt)
	require.Equal(t, uint64(1), rl.healthState.InflightWeight)
	rl.mu.RLock()
	st := rl.stage
	rl.mu.RUnlock()
	require.True(t, st == StageHealth)
}

func TestRateLimit_StrategyComparison(t *testing.T) {
	t.Parallel()
	nServers := 5
	cfg := newCfg()
	cfg.RecalcInterval = 10 * time.Millisecond
	cfg.WindowDuration = 1500 * time.Millisecond
	cfg.RecoverGapDuration = 50 * time.Millisecond
	cfg.MinSleepDuration = 50 * time.Millisecond
	cfg.MaxSleepDuration = 150 * time.Millisecond
	cfg.CheckCount = 5

	type server struct {
		rl      *RateLimit
		baseLat time.Duration
		errPct  int
		dPeriod time.Duration
		dLen    time.Duration
		start   time.Time
	}

	newServer := func(lat time.Duration, errPct int, period, dlen time.Duration) *server {
		s := &server{rl: NewRateLimit(cfg, 0, 0), baseLat: lat, errPct: errPct, dPeriod: period, dLen: dlen, start: time.Now()}
		s.rl.Start()
		return s
	}

	srv := []*server{
		newServer(5*time.Millisecond, 1, 1200*time.Millisecond, 300*time.Millisecond),   // fast, rare errors
		newServer(8*time.Millisecond, 1, 0, 0),                                          // stable
		newServer(22*time.Millisecond, 5, 1600*time.Millisecond, 400*time.Millisecond),  // sometimes degraded
		newServer(70*time.Millisecond, 3, 3000*time.Millisecond, 800*time.Millisecond),  // slowest
		newServer(35*time.Millisecond, 20, 2500*time.Millisecond, 700*time.Millisecond), // most errors
		newServer(100*time.Millisecond, 80, 0, 0),                                       // dead
	}
	defer func() {
		for _, s := range srv {
			s.rl.Close()
		}
	}()

	rng := rand.New()

	rateLimiterPickHealthServer := func(ss []*server) int {
		// mirror pickHealthServer logic over test servers
		i1, i2 := -1, -1
		var minInflight = ^uint64(0)
		for i, item := range ss {
			if count, ok := item.rl.GetInflightCount(); ok {
				if minInflight > count {
					minInflight = count
					i1 = i
					i2 = -1
				} else if minInflight == count {
					i2 = i
				}
			}
		}
		if i1 == -1 {
			return rng.Intn(len(ss))
		}
		if i2 == -1 {
			return i1
		}
		if rng.Intn(2) == 0 {
			return i1
		}
		return i2
	}

	rateLimiterPickCheckServer := func(ss []*server, f func(s *server) Event) {
		// mirror pickCheckServer but without pool: just trigger RecordCheck on a random ShouldCheck server
		var checks []*server
		for i := 0; i < len(ss); i++ {
			if ss[i].rl.ShouldCheck() {
				checks = append(checks, ss[i])
			}
		}
		if len(checks) == 0 {
			return
		}
		ind := rng.Intn(len(checks))
		checks[ind].rl.RecordCheck(func() Event {
			return f(ss[ind])
		})
	}

	percentile := func(vals []time.Duration, p float64) time.Duration {
		if len(vals) == 0 {
			return 0
		}
		s := make([]time.Duration, len(vals))
		copy(s, vals)
		for i := 1; i < len(s); i++ {
			v := s[i]
			j := i - 1
			for j >= 0 && s[j] > v {
				s[j+1] = s[j]
				j--
			}
			s[j+1] = v
		}
		k := int(float64(len(s)-1) * p)
		return s[k]
	}

	doQuery := func(s *server, seed int) Event {
		now := time.Now()
		degraded := false
		if s.dPeriod > 0 {
			elapsed := now.Sub(s.start) % s.dPeriod
			if elapsed < s.dLen {
				degraded = true
			}
		}
		lat := s.baseLat
		err := s.errPct
		if degraded {
			lat *= 5
			err = min(25, err*5)
		}

		start := time.Now()
		time.Sleep(lat)
		status := StatusSuccess
		if (start.UnixNano()+int64(seed))%100 < int64(err) {
			status = StatusError
		}
		return Event{Timestamp: start, Status: status, Duration: time.Since(start)}
	}

	type metrics struct {
		sync.Mutex
		picks     int
		top1Picks int
		top2Picks int
		success   int
		errors    int
		latencies []time.Duration
	}

	runScenario := func(label string, pick func([]*server) int, dur time.Duration, useShadow bool) metrics {
		m := metrics{}
		stopAt := time.Now().Add(dur)
		wg := sync.WaitGroup{}
		clients := runtime.GOMAXPROCS(0) * 4
		for c := 0; c < clients; c++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				for time.Now().Before(stopAt) {
					// pick using provided strategy
					idx := pick(srv)
					if idx < 0 || idx >= nServers {
						idx = 0
					}

					i1, i2 := -1, -1
					var minInflight = ^uint64(0)
					healthy := make([]int, 0, nServers)
					for i := range srv {
						if count, ok := srv[i].rl.GetInflightCount(); ok {
							healthy = append(healthy, i)
							if minInflight > count {
								minInflight = count
								i1 = i
								i2 = -1
							} else if minInflight == count {
								i2 = i
							}
						}
					}
					s := srv[idx]
					s.rl.AddInflightCount()
					eventRes := doQuery(s, seed)
					s.rl.RecordEvent(eventRes)

					if useShadow {
						for _, sv := range srv {
							if sv.rl.ShouldCheck() {
								rateLimiterPickCheckServer([]*server{sv}, func(s *server) Event {
									return doQuery(s, seed)
								})
								break
							}
						}
					}

					m.Lock()
					m.picks++
					if i1 >= 0 && idx == i1 {
						m.top1Picks++
					}
					if i1 >= 0 && i2 >= 0 {
						if idx == i1 || idx == i2 {
							m.top2Picks++
						}
					} else if i1 >= 0 && i2 == -1 {
						if idx == i1 {
							m.top2Picks++
						}
					}
					if eventRes.Status == StatusSuccess {
						m.success++
					} else {
						m.errors++
					}
					m.latencies = append(m.latencies, time.Since(eventRes.Timestamp))
					m.Unlock()
				}
			}(c)
		}
		wg.Wait()
		return m
	}

	// strategies
	rr := func(ss []*server) int {
		return int(time.Now().UnixNano()&0x7fffffff) % len(ss)
	}
	rand2 := func(ss []*server) int {
		if len(ss) == 1 {
			return 0
		}
		i := int(time.Now().UnixNano() & 0x7fffffff)
		a := (i*48271 + 1) % len(ss)
		b := (i*69621 + 7) % len(ss)
		if a == b {
			b = (b + 1) % len(ss)
		}
		if i%2 == 0 {
			return a
		}
		return b
	}
	rateLimiterMin := func(ss []*server) int {
		return rateLimiterPickHealthServer(ss)
	}

	duration := 3 * time.Second
	mRR := runScenario("RR", rr, duration, false)
	mRand2 := runScenario("RAND2", rand2, duration, false)
	mMin := runScenario("MIN", rateLimiterMin, duration, true)

	p := func(m metrics) (time.Duration, time.Duration) {
		return percentile(m.latencies, 0.50), percentile(m.latencies, 0.95)
	}
	p50RR, p95RR := p(mRR)
	p50Rand2, p95Rand2 := p(mRand2)
	p50Min, p95Min := p(mMin)

	logSummary := func(name string, m metrics, p50, p95 time.Duration) {
		best1Rate := 0.0
		best2Rate := 0.0
		if m.picks > 0 {
			best1Rate = float64(m.top1Picks) / float64(m.picks) * 100
			best2Rate = float64(m.top2Picks) / float64(m.picks) * 100
		}
		throughput := float64(m.success) / duration.Seconds()
		t.Logf("[%s] succ=%d err=%d thr=%.1f rps top1=%.1f%% top2=%.1f%% p50=%v p95=%v", name, m.success, m.errors, throughput, best1Rate, best2Rate, p50, p95)
	}
	logSummary("RR  ", mRR, p50RR, p95RR)
	logSummary("RAND2", mRand2, p50Rand2, p95Rand2)
	logSummary("MIN  ", mMin, p50Min, p95Min)

	betterCnt := 0
	if float64(mMin.success)/duration.Seconds() >= float64(mRR.success)/duration.Seconds() &&
		float64(mMin.success)/duration.Seconds() >= float64(mRand2.success)/duration.Seconds() {
		betterCnt++
	}
	if mMin.top2Picks >= mRR.top2Picks && mMin.top2Picks >= mRand2.top2Picks {
		betterCnt++
	}
	if p95Min <= p95RR && p95Min <= p95Rand2 {
		betterCnt++
	}
	require.GreaterOrEqual(t, betterCnt, 2)
}
