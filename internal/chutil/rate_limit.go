package chutil

import (
	"log"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/util/buffer"
)

type Event struct {
	Timestamp time.Time
	Status    Status
	Duration  time.Duration
}

type Status int

const (
	StatusSuccess Status = iota
	StatusError
)

type Stage string

const (
	StageHealth  Stage = "health"
	StageCheck   Stage = "check"
	StageSleep   Stage = "sleep"
	StageDisable Stage = "disable"
)

type HealthState struct {
	SuccessCnt      uint64
	ErrorCnt        uint64
	TotalDuration   time.Duration
	InflightCnt     uint64
	InflightWeight  uint64
	WeightUpdatedAt time.Time
	Events          *buffer.DoubleRingBuffer[Event]
}

type SleepState struct {
	Duration  time.Duration
	StartedAt time.Time
}

type CheckState struct {
	SuccessCount uint64
	Failed       bool
	BusyCh       chan struct{}
}

type RateLimitConfig struct {
	RateLimitDisable   bool
	WindowDuration     time.Duration
	MaxErrorRate       uint64
	MaxInflightWeight  uint64
	RecoverWeightStep  uint64
	RecoverGapDuration time.Duration
	SleepMultiplier    float64
	MaxSleepDuration   time.Duration
	MinSleepDuration   time.Duration
	CheckCount         uint64
	RecalcInterval     time.Duration
}

type RateLimitMetric struct {
	ShardKey       int
	ReplicaKey     int
	Stage          string
	InflightWeight uint64
	InflightCnt    uint64
}

type RateLimit struct {
	config      RateLimitConfig
	stage       Stage
	mu          sync.RWMutex
	healthState HealthState
	sleepState  SleepState
	checkState  CheckState
	shardKey    int
	replicaKey  int
	stopCh      chan struct{}
}

func NewRateLimit(config RateLimitConfig, shardKey, replicaKey int) *RateLimit {
	return &RateLimit{
		config: config,
		stage:  StageHealth,
		healthState: HealthState{
			InflightWeight: 1,
			Events:         buffer.NewDoubleRingBuffer[Event](100),
		},
		checkState: CheckState{
			BusyCh: make(chan struct{}, 1),
		},
		shardKey:   shardKey,
		replicaKey: replicaKey,
		stopCh:     make(chan struct{}),
	}
}

func (r *RateLimit) SetConfig(config RateLimitConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.config = config
	if config.RateLimitDisable {
		r.stage = StageDisable
	} else if r.stage == StageDisable {
		r.stage = StageHealth
		r.healthState.InflightWeight = 1
	}
}

func (r *RateLimit) GetMetrics() RateLimitMetric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return RateLimitMetric{
		ShardKey:       r.shardKey,
		ReplicaKey:     r.replicaKey,
		Stage:          string(r.stage),
		InflightWeight: r.healthState.InflightWeight,
		InflightCnt:    r.healthState.InflightCnt,
	}
}

// GetReplicaKey only concurrent read operations
func (r *RateLimit) GetReplicaKey() int {
	return r.replicaKey
}

func (r *RateLimit) Start() {
	go func() {
		defer log.Printf("[RateLimit] recalc worker quit")
		ticker := time.NewTicker(r.config.RecalcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				r.recalc()
			}
		}
	}()
}

func (r *RateLimit) Close() {
	close(r.stopCh)
}

func (r *RateLimit) ShouldCheck() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stage == StageCheck && !r.checkState.Failed
}

func (r *RateLimit) GetInflightCount() (uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.stage != StageHealth {
		return 0, false
	}
	return r.healthState.InflightCnt * r.healthState.InflightWeight, true
}

func (r *RateLimit) DoInflight(f func() error) error {
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.healthState.InflightCnt++
	}()
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.healthState.InflightCnt--
	}()
	return f()
}

func (r *RateLimit) RecordEvent(event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stage == StageDisable {
		return
	}
	r.healthState.Events.PushUncommited(event)
}

func (r *RateLimit) RecordCheck(f func() Event) {
	select {
	case r.checkState.BusyCh <- struct{}{}:
		go func() {
			defer func() { <-r.checkState.BusyCh }()
			res := f()

			r.mu.Lock()
			defer r.mu.Unlock()
			if res.Status == StatusSuccess {
				r.checkState.SuccessCount++
			} else {
				r.checkState.Failed = true
			}
		}()
	default:
	}
}

func (r *RateLimit) recalc() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()

	if r.stage == StageHealth {
		switch r.healthState.recalc(r.config, now) {
		case StageSleep:
			r.stage = StageSleep
			r.sleepState.reset(r.config, r.config.MinSleepDuration)
		default:
		}
	}
	if r.stage == StageSleep {
		switch r.sleepState.recalc(now) {
		case StageCheck:
			r.stage = StageCheck
			r.checkState.reset()
		default:
		}
	}
	if r.stage == StageCheck {
		switch r.checkState.recalc(r.config) {
		case StageHealth:
			r.stage = StageHealth
			r.healthState.reset(r.config)
		case StageSleep:
			r.stage = StageSleep
			newDuration := float64(r.sleepState.Duration) * r.config.SleepMultiplier
			r.sleepState.reset(r.config, time.Duration(newDuration))
		default:
		}
	}
}

func (s *HealthState) recalc(cfg RateLimitConfig, now time.Time) Stage {
	var prevAvgDuration time.Duration
	if s.SuccessCnt+s.ErrorCnt > 0 {
		prevAvgDuration = s.TotalDuration / time.Duration(s.SuccessCnt+s.ErrorCnt)
	}
	cutoff := now.Add(-cfg.WindowDuration)

	s.Events.ReadAndCommit(func(item Event) {
		switch item.Status {
		case StatusSuccess:
			s.SuccessCnt++
		case StatusError:
			s.ErrorCnt++
		default:
		}
		s.TotalDuration += item.Duration
	})
	err := s.Events.PopF(func(item Event) bool {
		if item.Timestamp.After(cutoff) {
			return false
		}
		switch item.Status {
		case StatusSuccess:
			s.SuccessCnt--
		case StatusError:
			s.ErrorCnt--
		default:
		}
		s.TotalDuration -= item.Duration
		return true
	})
	if err != nil {
		log.Printf("[RateLimit] popF err: %v", err)
	}

	var weight uint64 = 1
	if s.SuccessCnt+s.ErrorCnt == 0 {
		return StageHealth
	}
	errorRate := s.ErrorCnt * 100 / (s.SuccessCnt + s.ErrorCnt)
	if errorRate >= cfg.MaxErrorRate {
		return StageSleep
	}
	weight += cfg.MaxInflightWeight * errorRate / cfg.MaxErrorRate

	avgDuration := s.TotalDuration / time.Duration(s.SuccessCnt+s.ErrorCnt)
	if prevAvgDuration > 0 && prevAvgDuration < avgDuration {
		step := uint64((avgDuration - prevAvgDuration) * 100 / prevAvgDuration)
		weight += cfg.MaxInflightWeight * step / 100
	}
	weight = max(1, weight)
	weight = min(cfg.MaxInflightWeight, weight)

	if s.InflightWeight < weight {
		s.WeightUpdatedAt = now
		s.InflightWeight = weight
	}
	if s.InflightWeight > weight && now.Sub(s.WeightUpdatedAt) > cfg.RecoverGapDuration {
		s.WeightUpdatedAt = now
		s.InflightWeight = max(1, s.InflightWeight-cfg.RecoverWeightStep)
	}
	return StageHealth
}

func (s *SleepState) recalc(now time.Time) Stage {
	if now.After(s.StartedAt.Add(s.Duration)) {
		return StageCheck
	}
	return StageSleep
}

func (s *CheckState) recalc(cfg RateLimitConfig) Stage {
	if s.Failed {
		return StageSleep
	}
	if s.SuccessCount >= cfg.CheckCount {
		return StageHealth
	}
	return StageCheck
}

func (s *HealthState) reset(cfg RateLimitConfig) {
	s.SuccessCnt = 0
	s.ErrorCnt = 0
	s.TotalDuration = 0
	s.InflightWeight = cfg.MaxInflightWeight
	s.Events.Reset()
}

func (s *SleepState) reset(cfg RateLimitConfig, duration time.Duration) {
	duration = max(cfg.MinSleepDuration, duration)
	duration = min(cfg.MaxSleepDuration, duration)
	s.Duration = duration
	s.StartedAt = time.Now()
}

func (s *CheckState) reset() {
	s.SuccessCount = 0
	s.Failed = false
}
