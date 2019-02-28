package offheap

import (
	"sync/atomic"
	"unsafe"
)

const (
	LSharedPointerUninited   = int32(0)
	LSharedPointerIniteded   = int32(1)
	LSharedPointerReleasable = int32(2)
)

type LSharedPointerUPtr uintptr

func (u LSharedPointerUPtr) Ptr() *LSharedPointer {
	return (*LSharedPointer)(unsafe.Pointer(u))
}

// Heavy SharedPointer
type LSharedPointer struct {
	Accessor int32
}

func (p *LSharedPointer) GetAccessor() int32 {
	return atomic.LoadInt32(&p.Accessor)
}

func (p *LSharedPointer) ReadAcquire() {
	atomic.AddInt32(&p.Accessor, 1)
}

func (p *LSharedPointer) ReadRelease() {
	atomic.AddInt32(&p.Accessor, -1)
}
