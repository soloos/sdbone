package offheap

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	HSharedPointerUninited   = int32(0)
	HSharedPointerIniteded   = int32(1)
	HSharedPointerReleasable = int32(2)
)

type HSharedPointerUPtr uintptr

func (u HSharedPointerUPtr) Ptr() *HSharedPointer {
	return (*HSharedPointer)(unsafe.Pointer(u))
}

// Heavy SharedPointer
type HSharedPointer struct {
	accessRWMutex sync.RWMutex
	Accessor      int32
	Status        int32
}

func (p *HSharedPointer) SetReleasable() {
	atomic.StoreInt32(&p.Status, HSharedPointerReleasable)
}

func (p *HSharedPointer) Reset() {
	atomic.StoreInt32(&p.Status, HSharedPointerUninited)
}

func (p *HSharedPointer) CompleteInit() {
	atomic.StoreInt32(&p.Status, HSharedPointerIniteded)
}

func (p *HSharedPointer) IsInited() bool {
	return atomic.LoadInt32(&p.Status) > HSharedPointerUninited
}

func (p *HSharedPointer) IsShouldRelease() bool {
	return atomic.LoadInt32(&p.Status) == HSharedPointerReleasable
}

func (p *HSharedPointer) GetAccessor() int32 {
	return atomic.LoadInt32(&p.Accessor)
}

func (p *HSharedPointer) ReadAcquire() {
	atomic.AddInt32(&p.Accessor, 1)
	p.accessRWMutex.RLock()
}

func (p *HSharedPointer) ReadRelease() {
	p.accessRWMutex.RUnlock()
	atomic.AddInt32(&p.Accessor, -1)
}

func (p *HSharedPointer) WriteAcquire() {
	atomic.AddInt32(&p.Accessor, 1)
	p.accessRWMutex.Lock()
}

func (p *HSharedPointer) WriteRelease() {
	p.accessRWMutex.Unlock()
	atomic.AddInt32(&p.Accessor, -1)
}
