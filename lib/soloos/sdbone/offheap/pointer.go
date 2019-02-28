package offheap

import (
	"sync"
	"sync/atomic"
)

const (
	SharedPointerUninited   = int32(0)
	SharedPointerIniteded   = int32(1)
	SharedPointerReleasable = int32(2)
)

type SharedPointer struct {
	accessRWMutex sync.RWMutex
	Accessor      int32
	Status        int32
}

func (p *SharedPointer) SetReleasable() {
	atomic.StoreInt32(&p.Status, SharedPointerReleasable)
}

func (p *SharedPointer) Reset() {
	atomic.StoreInt32(&p.Status, SharedPointerUninited)
}

func (p *SharedPointer) CompleteInit() {
	atomic.StoreInt32(&p.Status, SharedPointerIniteded)
}

func (p *SharedPointer) IsInited() bool {
	return atomic.LoadInt32(&p.Status) > SharedPointerUninited
}

func (p *SharedPointer) IsShouldRelease() bool {
	return atomic.LoadInt32(&p.Status) == SharedPointerReleasable
}

func (p *SharedPointer) ReadAcquire() {
	atomic.AddInt32(&p.Accessor, 1)
	p.accessRWMutex.RLock()
}

func (p *SharedPointer) ReadRelease() {
	p.accessRWMutex.RUnlock()
	atomic.AddInt32(&p.Accessor, -1)
}

func (p *SharedPointer) WriteAcquire() {
	atomic.AddInt32(&p.Accessor, 1)
	p.accessRWMutex.Lock()
}

func (p *SharedPointer) WriteRelease() {
	p.accessRWMutex.Unlock()
	atomic.AddInt32(&p.Accessor, -1)
}
