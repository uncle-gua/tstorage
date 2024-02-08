package tstorage

type fakeEncoder[T any] struct {
	encodePointFunc func(*DataPoint[T]) error
	flushFunc       func() error
}

func (f *fakeEncoder[T]) encodePoint(p *DataPoint[T]) error {
	if f.encodePointFunc == nil {
		return nil
	}
	return f.encodePointFunc(p)
}

func (f *fakeEncoder[T]) flush() error {
	if f.flushFunc == nil {
		return nil
	}
	return f.flushFunc()
}
