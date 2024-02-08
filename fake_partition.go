package tstorage

type fakePartition[T any] struct {
	minT      int64
	maxT      int64
	numPoints int
	IsActive  bool

	err error
}

func (f *fakePartition[T]) insertRows(_ []Row[T]) ([]Row[T], error) {
	return nil, f.err
}

func (f *fakePartition[T]) selectDataPoints(_ string, _ []Label, _, _ int64) ([]*DataPoint[T], error) {
	return nil, f.err
}

func (f *fakePartition[T]) minTimestamp() int64 {
	return f.minT
}

func (f *fakePartition[T]) maxTimestamp() int64 {
	return f.maxT
}

func (f *fakePartition[T]) size() int {
	return f.numPoints
}

func (f *fakePartition[T]) active() bool {
	return f.IsActive
}

func (f *fakePartition[T]) clean() error {
	return nil
}

func (f *fakePartition[T]) expired() bool {
	return false
}
