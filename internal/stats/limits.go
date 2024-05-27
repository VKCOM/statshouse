package stats

// workaround: some environment could report trash
const (
	memFreeUpperLimit   = 64 * (1024 * 1024 * 1024 * 1024)
	processBlockedLimit = 10e10
)
