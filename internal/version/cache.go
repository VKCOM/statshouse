package version

import (
	"time"

	"github.com/vkcom/statshouse/internal/pcache"
)

const cacheNamespace = "agent_commits"

type Cache struct {
	dc        *pcache.DiskCache
	oldAgents map[string]uint32 // host -> ts in seconds
}

func NewVersionCache(dc *pcache.DiskCache) *Cache {
	return &Cache{dc: dc, oldAgents: make(map[string]uint32)}
}

func (c *Cache) Get(host string) uint32 {
	// check mem cache
	if savedTs, found := c.oldAgents[host]; found {
		return savedTs
	}
	data, _, _, err, found := c.dc.Get(cacheNamespace, host)
	if err != nil || !found {
		return 0
	}
	var i32Value pcache.Int32Value
	var value pcache.Value = &i32Value
	err = value.UnmarshalBinary(data)
	if err != nil {
		return 0
	}
	result := uint32(pcache.ValueToInt32(value))
	c.oldAgents[host] = result
	return result
}

func (c *Cache) Set(host string, now time.Time) {
	c.oldAgents[host] = uint32(now.Unix())
	value := pcache.Int32ToValue(int32(now.Unix()))
	data, err := value.MarshalBinary()
	if err == nil {
		_ = c.dc.Set(cacheNamespace, host, data, now, 0)
	}
}

func (c *Cache) Cleanup(host string) {
	// it's possible for host to be in disk cache but not in memory cache,
	// but it's unlikely, and not a big problem because it will be cleaned up after next update
	if _, found := c.oldAgents[host]; found {
		delete(c.oldAgents, host)
		_ = c.dc.Erase(cacheNamespace, host)
	}
}
