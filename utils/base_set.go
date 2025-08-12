package utils

import (
	"sync"
	"sync/atomic"
)

type BaseSet[K comparable] struct {
	data map[K]struct{}
	cnt  int64
	lock *sync.RWMutex
}

func NewBaseSet[K comparable]() *BaseSet[K] {
	return &BaseSet[K]{
		data: make(map[K]struct{}),
		cnt:  0,
		lock: &sync.RWMutex{},
	}
}

func NewBaseSetWithData[K comparable](data []K) *BaseSet[K] {
	s := NewBaseSet[K]()
	s.PushList(data)
	return s
}

func (s *BaseSet[K]) DeepCopy() *BaseSet[K] {
	s.lock.Lock()
	defer s.lock.Unlock()

	result := NewBaseSet[K]()
	result.cnt = s.cnt

	data := make(map[K]struct{})
	for k, v := range s.data {
		data[k] = v
	}
	result.data = data

	return result
}

func (s *BaseSet[K]) Clear() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data = make(map[K]struct{})
	s.cnt = 0
}

func (s *BaseSet[K]) Has(e K) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, ok := s.data[e]
	return ok
}

func (s *BaseSet[K]) ExistOne(items []K) bool {
	if len(items) <= 0 {
		return false
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, e := range items {
		if _, ok := s.data[e]; ok {
			return true
		}
	}

	return false
}

func (s *BaseSet[K]) FilterExists(items []K) []K {
	if len(items) <= 0 {
		return []K{}
	}

	s.lock.RLock()
	defer s.lock.RUnlock()

	result := make([]K, 0)
	for _, e := range items {
		if _, ok := s.data[e]; ok {
			result = append(result, e)
		}
	}

	return result
}

func (s *BaseSet[K]) TryPush(e K) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.data[e]; ok {
		return false
	}

	s.data[e] = struct{}{}
	s.cnt++
	return true
}

func (s *BaseSet[K]) Push(e K) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.data[e]; !ok {
		s.data[e] = struct{}{}
		s.cnt++
	}
}

func (s *BaseSet[K]) PushList(list []K) {
	if len(list) <= 0 {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	for index := 0; index < len(list); index++ {
		if _, ok := s.data[list[index]]; !ok {
			s.data[list[index]] = struct{}{}
			s.cnt++
		}
	}
}

func (s *BaseSet[K]) PushSet(bs *BaseSet[K]) {
	if bs == nil || bs.Empty() {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	bs.lock.RLock()
	defer bs.lock.RUnlock()

	for k := range bs.data {
		if _, ok := s.data[k]; !ok {
			s.data[k] = struct{}{}
			s.cnt++
		}
	}
}

func (s *BaseSet[K]) List() []K {
	s.lock.RLock()
	defer s.lock.RUnlock()

	data := make([]K, 0, len(s.data))
	for e := range s.data {
		data = append(data, e)
	}

	return data
}

func (s *BaseSet[K]) Len() int64 {
	return atomic.LoadInt64(&s.cnt)
}

func (s *BaseSet[K]) Empty() bool {
	return atomic.LoadInt64(&s.cnt) <= 0
}

func (s *BaseSet[K]) NotEmpty() bool {
	return atomic.LoadInt64(&s.cnt) > 0
}

func (s *BaseSet[K]) Remove(e K) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.data[e]; ok {
		delete(s.data, e)
		s.cnt--
	}
}

func BaseInter[K comparable](a, b *BaseSet[K]) *BaseSet[K] {
	if a == nil || a.Len() <= 0 || b == nil || b.Len() <= 0 {
		return NewBaseSet[K]()
	}

	inter := NewBaseSet[K]()
	for _, v := range a.List() {
		if b.Has(v) {
			inter.Push(v)
		}
	}

	return inter
}

func BaseDiff[K comparable](a, b *BaseSet[K]) *BaseSet[K] {
	if a == nil || a.Len() <= 0 {
		return BaseSub(b, a)
	}
	if b == nil || b.Len() <= 0 {
		return BaseSub(a, b)
	}

	diff := NewBaseSet[K]()
	for _, v := range a.List() {
		if !b.Has(v) {
			diff.Push(v)
		}
	}
	for _, v := range b.List() {
		if !a.Has(v) {
			diff.Push(v)
		}
	}

	return diff
}

func BaseSub[K comparable](a, b *BaseSet[K]) *BaseSet[K] {
	if a == nil || a.Len() <= 0 {
		return NewBaseSet[K]()
	}

	sub := NewBaseSet[K]()
	for _, v := range a.List() {
		if b == nil || !b.Has(v) {
			sub.Push(v)
		}
	}

	return sub
}
