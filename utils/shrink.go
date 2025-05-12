package utils

// See "A fast alternative to the modulo reduction" (Lemire, 2016)
// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func FastMod(x, m uint64) uint32 {
	return uint32((x * m) >> 32)
}

func FastRangeReduction(key, num uint64) uint64 {
	return 0
}
