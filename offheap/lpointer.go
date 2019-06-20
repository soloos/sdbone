package offheap

import (
	"sync/atomic"
	"unsafe"
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

func (p *LSharedPointer) Acquire() int32 {
	return atomic.AddInt32(&p.Accessor, 1)
}

func (p *LSharedPointer) Release() int32 {
	return atomic.AddInt32(&p.Accessor, -1)
}

func (p *LSharedPointer) Reset() {
	atomic.StoreInt32(&p.Accessor, 0)
}
