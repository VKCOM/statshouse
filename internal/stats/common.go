package stats

func diff[A float64 | uint64](new, old A) float64 {
	if new >= old {
		return float64(new) - float64(old)
	}
	return 0
}
