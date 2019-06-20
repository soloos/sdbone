package offheap

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// A LinkPool is a set of temporary objects that may be individually saved and
// retrieved.
//
// Any item stored in the LinkPool may be removed automatically at any time without
// notification. If the LinkPool holds the only reference when this happens, the
// item might be deallocated.
//
// A LinkPool is safe for use by multiple goroutines simultaneously.
//
// LinkPool's purpose is to cache allocated but unused items for later reuse,
// relieving pressure on the garbage collector. That is, it makes it easy to
// build efficient, thread-safe free lists. However, it is not suitable for all
// free lists.
//
// An appropriate use of a LinkPool is to manage a group of temporary items
// silently shared among and potentially reused by concurrent independent
// clients of a package. LinkPool provides a way to amortize allocation overhead
// across many clients.
//
// An example of good use of a LinkPool is in the fmt package, which maintains a
// dynamically-sized store of temporary output buffers. The store scales under
// load (when many goroutines are actively printing) and shrinks when
// quiescent.
//
// On the other hand, a free list maintained as part of a short-lived object is
// not a suitable use for a LinkPool, since the overhead does not amortize well in
// that scenario. It is more efficient to have such objects implement their own
// free list.
//
// A LinkPool must not be copied after first use.
type LinkPool struct {
	// noCopy noCopy

	local     unsafe.Pointer // local fixed-size per-P pool, actual type is [P]linkPoolLocal
	localSize uintptr        // size of the local array

	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	New func() uintptr
}

// Local per-P Pool appendix.
type linkPoolLocalInternal struct {
	private    uintptr // Can be used only by the respective P.
	sharedFree uintptr // Can be used by any P.
	sharedLast uintptr // Can be used by any P.
	sync.Mutex         // Protects shared.
}

type linkPoolLocal struct {
	linkPoolLocalInternal

	// Prevents false sharing on widespread platforms with
	// 128 mod (cache line size) = 0 .
	pad [128 - unsafe.Sizeof(linkPoolLocalInternal{})%128]byte
}

// Put adds x to the pool.
func (p *LinkPool) Put(x uintptr) {
	if x == 0 {
		return
	}
	l := p.pin()
	if l.private == 0 {
		l.private = x
		x = 0
	}
	runtime.UnsafeProcUnpin()
	if x != 0 {
		l.Lock()
		if l.sharedFree == 0 {
			l.sharedFree = x
			l.sharedLast = x
			*(*uintptr)(unsafe.Pointer(l.sharedLast)) = 0
		} else {
			*(*uintptr)(unsafe.Pointer(l.sharedLast)) = x
			l.sharedLast = x
		}
		l.Unlock()
	}
}

// Get selects an arbitrary item from the LinkPool, removes it from the
// LinkPool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to Put and
// the values returned by Get.
//
// If Get would otherwise return nil and p.New is non-nil, Get returns
// the result of calling p.New.
func (p *LinkPool) Get() uintptr {
	l := p.pin()
	x := l.private
	l.private = 0
	runtime.UnsafeProcUnpin()
	if x == 0 {
		l.Lock()
		if l.sharedFree != 0 {
			x = l.sharedFree
			l.sharedFree = *(*uintptr)(unsafe.Pointer(l.sharedFree))
		}
		l.Unlock()
		if x == 0 {
			x = p.getSlow()
		}
	}
	if x == 0 && p.New != nil {
		x = p.New()
	}
	*(*uintptr)(unsafe.Pointer(x)) = 0
	return x
}

func (p *LinkPool) getSlow() (x uintptr) {
	// See the comment in pin regarding ordering of the loads.
	size := atomic.LoadUintptr(&p.localSize) // load-acquire
	local := p.local                         // load-consume
	// Try to steal one element from other procs.
	pid := runtime.UnsafeProcPin()
	runtime.UnsafeProcUnpin()
	for i := 0; i < int(size); i++ {
		l := LinkPoolIndexLocal(local, (pid+i+1)%int(size))
		l.Lock()
		if l.sharedFree != 0 {
			x = l.sharedFree
			l.sharedFree = *(*uintptr)(unsafe.Pointer(l.sharedFree))
			l.Unlock()
			break
		}
		l.Unlock()
	}
	return x
}

// pin pins the current goroutine to P, disables preemption and returns linkPoolLocal pool for the P.
// Caller must call runtime.UnsafeProcUnpin() when done with the pool.
func (p *LinkPool) pin() *linkPoolLocal {
	pid := runtime.UnsafeProcPin()
	// In pinSlow we store to localSize and then to local, here we load in opposite order.
	// Since we've disabled preemption, GC cannot happen in between.
	// Thus here we must observe local at least as large localSize.
	// We can observe a newer/larger local, it is fine (we must observe its zero-initialized-ness).
	s := atomic.LoadUintptr(&p.localSize) // load-acquire
	l := p.local                          // load-consume
	if uintptr(pid) < s {
		return LinkPoolIndexLocal(l, pid)
	}
	return p.pinSlow()
}

func (p *LinkPool) pinSlow() *linkPoolLocal {
	// Retry under the mutex.
	// Can not lock the mutex while pinned.
	runtime.UnsafeProcUnpin()
	allLinkPoolsMu.Lock()
	defer allLinkPoolsMu.Unlock()
	pid := runtime.UnsafeProcPin()
	s := p.localSize
	l := p.local
	if uintptr(pid) < s {
		return LinkPoolIndexLocal(l, pid)
	}
	if p.local == nil {
		allLinkPools = append(allLinkPools, p)
	}
	// If GOMAXPROCS changes between GCs, we re-allocate the array and lose the old one.
	size := runtime.GOMAXPROCS(0)
	local := make([]linkPoolLocal, size)
	for i := uintptr(0); i < p.localSize; i++ {
		local[i] = (*(*[]linkPoolLocal)(p.local))[i]
	}
	atomic.StorePointer(&p.local, unsafe.Pointer(&local[0])) // store-release
	atomic.StoreUintptr(&p.localSize, uintptr(size))         // store-release
	return &local[pid]
}

var (
	allLinkPoolsMu sync.Mutex
	allLinkPools   []*LinkPool
)

func LinkPoolIndexLocal(l unsafe.Pointer, i int) *linkPoolLocal {
	lp := unsafe.Pointer(uintptr(l) + uintptr(i)*unsafe.Sizeof(linkPoolLocal{}))
	return (*linkPoolLocal)(lp)
}
