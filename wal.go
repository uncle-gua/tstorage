package tstorage

import (
	"os"
	"sync"
)

type walOperation byte

const (
	// The record format for operateInsert is as shown below:
	/*
	   +--------+---------------------+--------+--------------------+----------------+
	   | op(1b) | len metric(varints) | metric | timestamp(varints) | value(varints) |
	   +--------+---------------------+--------+--------------------+----------------+
	*/
	operationInsert walOperation = iota
)

// wal represents a write-ahead log, which offers durability guarantees.
type wal[T any] interface {
	append(op walOperation, rows []Row[T]) error
	flush() error
	punctuate() error
	removeOldest() error
	removeAll() error
	refresh() error
}

type nopWAL[T any] struct {
	filename string
	f        *os.File
	mu       sync.Mutex
}

func (f *nopWAL[T]) append(_ walOperation, _ []Row[T]) error {
	return nil
}

func (f *nopWAL[T]) flush() error {
	return nil
}

func (f *nopWAL[T]) punctuate() error {
	return nil
}

func (f *nopWAL[T]) removeOldest() error {
	return nil
}

func (f *nopWAL[T]) removeAll() error {
	return nil
}

func (f *nopWAL[T]) refresh() error {
	return nil
}
