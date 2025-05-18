package utils

type Set[T comparable] struct {
	data map[T]struct{}
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{data: make(map[T]struct{})}
}

func (s *Set[T]) Add(val T) {
	s.data[val] = struct{}{}
}

func (s *Set[T]) Remove(val T) {
	delete(s.data, val)
}

func (s *Set[T]) Contains(val T) bool {
	_, ok := s.data[val]
	return ok
}

func (s *Set[T]) Size() int {
	return len(s.data)
}

func (s *Set[T]) Items() []T {
	items := make([]T, 0, len(s.data))
	for v := range s.data {
		items = append(items, v)
	}
	return items
}

func (s *Set[T]) Empty() bool {
	return len(s.data) == 0
}
