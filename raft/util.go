package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

type IntSet struct {
	values map[int]bool
}

func NewIntSet() *IntSet {
	return &IntSet{
		values: make(map[int]bool),
	}
}

func (s *IntSet) Containes(key int) bool {
	_, ok := s.values[key]
	return ok
}

func (s *IntSet) Add(key int) {
	s.values[key] = true
}

func (s *IntSet) Delete(key int) {
	delete(s.values, key)
}

func (s *IntSet) Size() int {
	return len(s.values)
}
