package stats

func diff[A float64 | uint64](new, old A) float64 {
	if new >= old {
		return float64(new) - float64(old)
	}
	return 0
}

func nonNegative[A float64 | int64](v A) float64 {
	if v < 0 {
		return 0
	}
	return float64(v)
}
