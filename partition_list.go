package tstorage

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// partitionList represents a linked list for partitions.
// Each partition is arranged in order order of newest to oldest.
// That is, the head node is always the newest, the tail node is the oldest.
//
// Head and its next partitions must be writable to accept out-of-order data points
// even if it's inactive.
type partitionList[T any] interface {
	// insert appends a new node to the head.
	insert(partition partition[T])
	// remove eliminates the given partition from the list.
	remove(partition partition[T]) error
	// swap replaces the old partition with the new one.
	swap(old, new partition[T]) error
	// getHead gives back the head node which is the newest one.
	getHead() partition[T]
	// size returns the number of partitions of itself.
	size() int
	// newIterator gives back the iterator object fot this list.
	// If you need to inspect all nodes within the list, use this one.
	newIterator() partitionIterator[T]

	String() string
}

// Iterator represents an iterator for partition list. The basic usage is:
/*
  for iterator.next() {
    partition, err := iterator.value()
    // Do something with partition
  }
*/
type partitionIterator[T any] interface {
	// next positions the iterator at the next node in the list.
	// It will be positioned at the head on the first call.
	// The return value will be true if a value can be read from the list.
	next() bool
	// value gives back the current partition in the iterator.
	// If it was called even though next() returns false, it will return nil.
	value() partition[T]

	currentNode() *partitionNode[T]
}

type partitionListImpl[T any] struct {
	numPartitions int64
	head          *partitionNode[T]
	tail          *partitionNode[T]
	mu            sync.RWMutex
}

func newPartitionList[T any]() partitionList[T] {
	return &partitionListImpl[T]{}
}

func (p *partitionListImpl[T]) getHead() partition[T] {
	if p.size() <= 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.head.value()
}

func (p *partitionListImpl[T]) insert(partition partition[T]) {
	node := &partitionNode[T]{
		val: partition,
	}
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	if head != nil {
		node.next = head
	}

	p.setHead(node)
	atomic.AddInt64(&p.numPartitions, 1)
}

func (p *partitionListImpl[T]) remove(target partition[T]) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode[T]
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), target) {
			prev = current
			continue
		}

		// remove the current node.

		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// removing the head node
			p.setHead(next)
		case next == nil:
			// removing the tail node
			prev.setNext(nil)
			p.setTail(prev)
		default:
			// removing the middle node
			prev.setNext(next)
		}
		atomic.AddInt64(&p.numPartitions, -1)

		if err := current.value().clean(); err != nil {
			return fmt.Errorf("failed to clean resources managed by partition to be removed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func (p *partitionListImpl[T]) swap(old, new partition[T]) error {
	if p.size() <= 0 {
		return fmt.Errorf("empty partition")
	}

	// Iterate over itself from the head.
	var prev, next *partitionNode[T]
	iterator := p.newIterator()
	for iterator.next() {
		current := iterator.currentNode()
		if !samePartitions(current.value(), old) {
			prev = current
			continue
		}

		// swap the current node.

		newNode := &partitionNode[T]{
			val:  new,
			next: current.getNext(),
		}
		iterator.next()
		next = iterator.currentNode()
		switch {
		case prev == nil:
			// swapping the head node
			p.setHead(newNode)
		case next == nil:
			// swapping the tail node
			prev.setNext(newNode)
			p.setTail(newNode)
		default:
			// swapping the middle node
			prev.setNext(newNode)
		}
		return nil
	}

	return fmt.Errorf("the given partition was not found")
}

func samePartitions[T any](x, y partition[T]) bool {
	return x.minTimestamp() == y.minTimestamp()
}

func (p *partitionListImpl[T]) size() int {
	return int(atomic.LoadInt64(&p.numPartitions))
}

func (p *partitionListImpl[T]) newIterator() partitionIterator[T] {
	p.mu.RLock()
	head := p.head
	p.mu.RUnlock()
	// Put a dummy node so that it positions the head on the first next() call.
	dummy := &partitionNode[T]{
		next: head,
	}
	return &partitionIteratorImpl[T]{
		current: dummy,
	}
}

func (p *partitionListImpl[T]) setHead(node *partitionNode[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = node
}

func (p *partitionListImpl[T]) setTail(node *partitionNode[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tail = node
}

func (p *partitionListImpl[T]) String() string {
	b := &strings.Builder{}
	iterator := p.newIterator()
	for iterator.next() {
		p := iterator.value()
		if _, ok := p.(*memoryPartition[T]); ok {
			b.WriteString("[Memory Partition]")
		} else if _, ok := p.(*diskPartition[T]); ok {
			b.WriteString("[Disk Partition]")
		} else {
			b.WriteString("[Unknown Partition]")
		}
		b.WriteString("->")
	}
	return strings.TrimSuffix(b.String(), "->")
}

// partitionNode wraps a partition to hold the pointer to the next one.
type partitionNode[T any] struct {
	// val is immutable
	val  partition[T]
	next *partitionNode[T]
	mu   sync.RWMutex
}

// value gives back the actual partition of the node.
func (p *partitionNode[T]) value() partition[T] {
	return p.val
}

func (p *partitionNode[T]) setNext(node *partitionNode[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = node
}

func (p *partitionNode[T]) getNext() *partitionNode[T] {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.next
}

type partitionIteratorImpl[T any] struct {
	current *partitionNode[T]
}

func (i *partitionIteratorImpl[T]) next() bool {
	if i.current == nil {
		return false
	}
	next := i.current.getNext()
	i.current = next
	return i.current != nil
}

func (i *partitionIteratorImpl[T]) value() partition[T] {
	if i.current == nil {
		return nil
	}
	return i.current.value()
}

func (i *partitionIteratorImpl[T]) currentNode() *partitionNode[T] {
	return i.current
}
