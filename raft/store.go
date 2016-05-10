package raft

import (
	"fmt"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogStore interface {
	Append(*LogEntry) error
	AppendAll([]*LogEntry) error
	Write(idx int, e *LogEntry) error // write to idx
	WriteAll(idx int, es []*LogEntry) error
	Delete(idx int) error
	DeleteAll(idxFrom, idxTo int) error
	GetLastIndex() int // log index starts from 1
	Len() int
	Get(idx int) *LogEntry
	GetAll(idxFrom, idxTo int) []*LogEntry
	IsMatch(idx int, e *LogEntry) bool // if real entry matches the given entry
}

// for test purpose only
type InMemLogStore struct {
	entries []*LogEntry
}

func NewInMemLogStore() LogStore {
	return &InMemLogStore{make([]*LogEntry, 0)}
}

func (s *InMemLogStore) Append(e *LogEntry) error {
	s.entries = append(s.entries, e)
	return nil
}

func (s *InMemLogStore) AppendAll(es []*LogEntry) error {
	s.entries = append(s.entries, es...)
	return nil
}

func (s *InMemLogStore) Write(idx int, e *LogEntry) error {
	return s.WriteAll(idx, []*LogEntry{e})
}

func (s *InMemLogStore) WriteAll(idx int, es []*LogEntry) error {
	last := s.GetLastIndex()

	if idx <= 0 || idx > last+1 {
		return fmt.Errorf("log index out of bound")
	}

	if idx == last+1 {
		return s.AppendAll(es)
	}

	for i := 0; i < len(es); i++ {
		nextIdx := idx + i
		if nextIdx <= last {
			s.entries[nextIdx-1] = es[i]
		} else {
			s.Append(es[i])
		}
	}

	return nil
}

func (s *InMemLogStore) Delete(idx int) error {
	if idx <= 0 || idx > s.GetLastIndex() {
		return fmt.Errorf("index out of bound")
	}

	s.entries = append(s.entries[:idx-1], s.entries[idx:]...)
	return nil
}

func (s *InMemLogStore) DeleteAll(fromIdx, toIdx int) error {
	if fromIdx <= 0 {
		fromIdx = 1
	}

	if toIdx > s.GetLastIndex() {
		toIdx = s.GetLastIndex()
	}

	if fromIdx > toIdx {
		return nil
	}

	s.entries = append(s.entries[:fromIdx-1], s.entries[toIdx:]...)
	return nil
}

func (s *InMemLogStore) IsMatch(idx int, e *LogEntry) bool {
	r := s.Get(idx)
	if r == nil {
		return idx == 0 && e == nil
	} else {
		return r.Term == e.Term
	}
}

func (s *InMemLogStore) GetLastIndex() int {
	return len(s.entries)
}

func (s *InMemLogStore) Len() int {
	return len(s.entries)
}

func (s *InMemLogStore) Get(idx int) *LogEntry {
	if idx <= 0 || idx > s.GetLastIndex() {
		return nil
	}
	return s.entries[idx-1]
}

func (s *InMemLogStore) GetAll(from, to int) []*LogEntry {
	if from <= 0 {
		from = 1
	}

	if to > s.GetLastIndex() {
		to = s.GetLastIndex()
	}

	if from > to {
		return make([]*LogEntry, 0)
	}

	return s.entries[from-1 : to]
}
