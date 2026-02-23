package scanner

import (
	"container/heap"

	"github.com/dethi/riverbed/hfile"
)

type scannerEntry struct {
	s     *hfile.Scanner
	order int64 // lower = newer file = higher priority
}

// entryHeap implements container/heap.Interface for []*scannerEntry.
type entryHeap []*scannerEntry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	return CompareCellWithOrder(h[i].s.Cell(), h[j].s.Cell(), h[i].order, h[j].order) < 0
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) { *h = append(*h, x.(*scannerEntry)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// kvHeap is a min-heap of HFile scanners, ordered by current cell.
// Mirrors HBase KeyValueHeap: keeps "current" (the minimum) outside the heap
// to reduce heap operations.
type kvHeap struct {
	h      entryHeap
	cur    *scannerEntry
	errVal error
}

// newKVHeap initialises the heap from a slice of scanners.
// Each scanner is advanced once (Next() called); those that have cells are
// added to the heap. The minimum is immediately placed in cur so that peek()
// is valid before the first next() call.
// Callers should pass slices newest-file-first so that lower order values
// correspond to newer (higher-priority) files.
func newKVHeap(scanners []*hfile.Scanner) (*kvHeap, error) {
	kv := &kvHeap{}
	for i, s := range scanners {
		if s.Next() {
			kv.h = append(kv.h, &scannerEntry{s: s, order: int64(i)})
		} else if err := s.Err(); err != nil {
			return nil, err
		}
	}
	heap.Init(&kv.h)
	if len(kv.h) > 0 {
		kv.cur = heap.Pop(&kv.h).(*scannerEntry)
	}
	return kv, nil
}

// peek returns the cell at the current heap minimum (nil when exhausted).
func (kv *kvHeap) peek() *hfile.Cell {
	if kv.cur == nil {
		return nil
	}
	return kv.cur.s.Cell()
}

// next advances the minimum scanner and repositions the heap.
// Returns false when all scanners are exhausted or on error.
func (kv *kvHeap) next() bool {
	if kv.cur == nil {
		return false
	}
	cur := kv.cur
	if cur.s.Next() {
		heap.Push(&kv.h, cur)
	} else if err := cur.s.Err(); err != nil {
		kv.errVal = err
		kv.cur = nil
		return false
	}
	// cur scanner exhausted without error: just don't push it back.
	if len(kv.h) > 0 {
		kv.cur = heap.Pop(&kv.h).(*scannerEntry)
		return true
	}
	kv.cur = nil
	return false
}

func (kv *kvHeap) err() error { return kv.errVal }
