package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type secondCacheSimple struct {
	sec map[int64]int64
	now func() time.Time
}

func (c *secondCacheSimple) updateTime(invalidatedAtNano, sec int64) {
	c.sec[sec] = invalidatedAtNano
}

func (c *secondCacheSimple) checkInvalidation(loadAt, from, to int64) bool {
	now := c.now()
	cacheImmutable := now.Add(invalidateFrom)
	if time.Unix(from, 0).Before(cacheImmutable) {
		from = cacheImmutable.Unix()
	}
	if time.Unix(to, 0).Before(cacheImmutable) {
		return true
	}
	for s, at := range c.sec {
		if s >= from && s <= to && loadAt <= at+int64(invalidateLinger) {
			return false
		}
	}
	return true
}

type cacheTestState struct {
	cache       *secondsCache
	cacheSimple *secondCacheSimple
	currentTime int64
}

func (s *cacheTestState) Init(t *rapid.T) {
	s.currentTime = 100_000_000
	now := func() time.Time {
		return time.Unix(s.currentTime, 0)
	}
	s.cache = newSecondsCache(now)
	s.cacheSimple = &secondCacheSimple{sec: map[int64]int64{}, now: now}
}
func (s *cacheTestState) Tick(r *rapid.T) {
	s.currentTime++
}

func (s *cacheTestState) Push(r *rapid.T) {
	max := s.currentTime
	loadedAtNano := time.Unix(max, 0).UnixNano()
	invalidatedSec := int64(invalidateFrom.Abs().Seconds())
	sec := rapid.Int64Range(max-1-invalidatedSec, max-1).Draw(r, "invalidated sec")
	s.cache.updateTimeLocked(loadedAtNano, sec, 0)
	s.cacheSimple.updateTime(loadedAtNano, sec)
}

func (s *cacheTestState) CheckInvalidation(r *rapid.T) {
	max := s.currentTime
	rang := int64((30 * 24 * time.Hour).Seconds())
	from := rapid.Int64Range(max-rang, max-1).Draw(r, "from")
	to := rapid.Int64Range(from+1, max).Draw(r, "to")
	loadedAtRange := int64(invalidateFrom.Abs().Seconds())
	loadedAt := rapid.Int64Range(max-loadedAtRange, max).Draw(r, "loaded_at")
	loadedAtNano := time.Unix(loadedAt, 0).UnixNano()
	expected := s.cacheSimple.checkInvalidation(loadedAtNano, from, to)
	got := s.cache.checkInvalidationLocked(loadedAtNano, from, to, 0)
	require.Equal(r, expected, got)
}

func (s *cacheTestState) Check(r *rapid.T) {

}

func TestSecondsCache(t *testing.T) {
	rapid.Check(t, rapid.Run[*cacheTestState]())
}
