package version

import (
	"time"
)

// 30 days grace period to switch to a new agent version
const grace = uint32(time.Hour / time.Second * 24 * 30)

// AllowAgent assumes that if agents are within 30 days from aggregator, they are fresh enough
// and if they are older they are given 30 days grace to update
func AllowAgent(host string, aggregatorCommitTs uint32, agentCommitTs uint32, c *Cache) (allow bool, outdated bool) {
	// good case when agent is fresh enough
	outdated = aggregatorCommitTs > agentCommitTs+grace
	if !outdated {
		c.Cleanup(host)
		allow = true
		return allow, outdated
	}
	// when agent is outdated we give a grace period to switch to a new agent version
	now := time.Now()
	firstSeen := c.Get(host)
	if firstSeen == 0 || uint32(now.Unix()) < firstSeen || uint32(now.Unix())-firstSeen < grace {
		if firstSeen == 0 {
			c.Set(host, now)
		}
		allow = true
		return allow, outdated
	}
	// grace period passed
	allow = false
	return allow, outdated
}
