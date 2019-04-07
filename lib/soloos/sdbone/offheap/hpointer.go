package offheap

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	HSharedPointerUniniteded = int32(iota)
	HSharedPointerIniteded
	HSharedPointerReleasable
	HSharedPointerRelease
)

type HSharedPointerUPtr uintptr

func (u HSharedPointerUPtr) Ptr() *HSharedPointer {
	return (*HSharedPointer)(unsafe.Pointer(u))
}

// Heavy SharedPointer
type HSharedPointer struct {
	accessRWMutex sync.RWMutex
	accessor      int32
	status        int32
}

func (p *HSharedPointer) SetReleasable() {
	atomic.StoreInt32(&p.status, HSharedPointerReleasable)
}

func (p *HSharedPointer) EnsureRelease() bool {
	return atomic.CompareAndSwapInt32(&p.status, HSharedPointerReleasable, HSharedPointerRelease)
}
func (p *HSharedPointer) Reset() {
	atomic.StoreInt32(&p.status, SharedPointerUninited)
}

func (p *HSharedPointer) CompleteInit() {
	atomic.StoreInt32(&p.status, SharedPointerIniteded)
}

func (p *HSharedPointer) IsInited() bool {
	return atomic.LoadInt32(&p.status) > SharedPointerUninited
}

func (p *HSharedPointer) IsShouldRelease() bool {
	return atomic.LoadInt32(&p.status) == HSharedPointerReleasable
}

func (p *HSharedPointer) GetAccessor() int32 {
	return atomic.LoadInt32(&p.accessor)
}

func (p *HSharedPointer) ReadAcquire() {
	atomic.AddInt32(&p.accessor, 1)
	p.accessRWMutex.RLock()
}

func (p *HSharedPointer) ReadRelease() {
	p.accessRWMutex.RUnlock()
	atomic.AddInt32(&p.accessor, -1)
}

func (p *HSharedPointer) WriteAcquire() {
	atomic.AddInt32(&p.accessor, 1)
	p.accessRWMutex.Lock()
}

func (p *HSharedPointer) WriteRelease() {
	p.accessRWMutex.Unlock()
	atomic.AddInt32(&p.accessor, -1)
}
