package utils

import (
	"cmp"
	"fmt"
	"sort"
	"strings"
)

type Set[K cmp.Ordered] struct {
	*BaseSet[K]
}

func NewSet[K cmp.Ordered]() *Set[K] {
	return &Set[K]{
		BaseSet: NewBaseSet[K](),
	}
}

func NewSetWithData[K cmp.Ordered](data []K) *Set[K] {
	s := NewSet[K]()
	s.PushList(data)
	return s
}

func (s *Set[K]) DeepCopy() *Set[K] {
	s.lock.Lock()
	defer s.lock.Unlock()

	result := NewSet[K]()
	result.cnt = s.cnt

	data := make(map[K]struct{})
	for k, v := range s.data {
		data[k] = v
	}
	result.data = data

	return result
}

func (s *Set[K]) SortList(ascend bool) []K {
	s.lock.RLock()
	defer s.lock.RUnlock()

	data := make([]K, 0, len(s.data))
	for e := range s.data {
		data = append(data, e)
	}

	sort.SliceStable(data, func(i, j int) bool {
		if ascend {
			return data[i] < data[j]
		}
		return data[i] > data[j]
	})

	return data
}

func (s *Set[K]) PushSet(ds *Set[K]) {
	if ds == nil || ds.Empty() {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	ds.lock.RLock()
	defer ds.lock.RUnlock()

	for k := range ds.data {
		if _, ok := s.data[k]; !ok {
			s.data[k] = struct{}{}
			s.cnt++
		}
	}
}

func (s *Set[K]) String() string {
	items := s.SortList(true)

	itemStrs := make([]string, 0, len(items)+1)
	itemStrs = append(itemStrs, fmt.Sprintf("%d", len(items)))
	for _, item := range items {
		itemStrs = append(itemStrs, fmt.Sprintf("%v", item))
	}

	return strings.Join(itemStrs, ", ")
}
