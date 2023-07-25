package stats

func diff(new, old uint64) float64 {
	if new >= old {
		return float64(new - old)
	}
	return 0
}
