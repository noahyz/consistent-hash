package utils

func SymmetricDifference[T comparable](a, b []T) map[T]struct{} {
	inA := make(map[T]struct{}, len(a))
	inB := make(map[T]struct{}, len(b))

	for _, item := range a {
		inA[item] = struct{}{}
	}
	for _, item := range b {
		inB[item] = struct{}{}
	}

	diff := make(map[T]struct{})
	for _, item := range a {
		if _, found := inB[item]; !found {
			diff[item] = struct{}{}
		}
	}
	for _, item := range b {
		if _, found := inA[item]; !found {
			diff[item] = struct{}{}
		}
	}
	return diff
}

func CountCommon[T comparable](a, b []T) int {
	seen := make(map[T]struct{}, len(a))
	for _, v := range a {
		seen[v] = struct{}{}
	}

	count := 0
	// 防止重复计数
	visited := make(map[T]struct{}, len(b))
	for _, v := range b {
		if _, inA := seen[v]; inA {
			if _, done := visited[v]; !done {
				count++
				visited[v] = struct{}{}
			}
		}
	}
	return count
}
