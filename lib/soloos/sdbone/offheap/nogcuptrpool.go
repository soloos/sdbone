package offheap

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

var poolRaceHash [128]uint64

// poolRaceAddr returns an address to use as the synchronization point
// for race detector logic. We don't use the actual pointer stored in x
// directly, for fear of conflicting with other synchronization on that address.
// Instead, we hash the pointer to get an index into poolRaceHash.
// See discussion on golang.org/cl/31589.
func poolRaceAddr(x interface{}) unsafe.Pointer {
	ptr := uintptr((*[2]unsafe.Pointer)(unsafe.Pointer(&x))[1])
	h := uint32((uint64(uint32(ptr)) * 0x85ebca6b) >> 16)
	return unsafe.Pointer(&poolRaceHash[h%uint32(len(poolRaceHash))])
}

// A NoGCUintptrPool is a set of temporary objects that may be individually saved and
// retrieved.
//
// Any item stored in the NoGCUintptrPool may be removed automatically at any time without
// notification. If the NoGCUintptrPool holds the only reference when this happens, the
// item might be deallocated.
//
// A NoGCUintptrPool is safe for use by multiple goroutines simultaneously.
//
// NoGCUintptrPool's purpose is to cache allocated but unused items for later reuse,
// relieving pressure on the garbage collector. That is, it makes it easy to
// build efficient, thread-safe free lists. However, it is not suitable for all
// free lists.
//
// An appropriate use of a NoGCUintptrPool is to manage a group of temporary items
// silently shared among and potentially reused by concurrent independent
// clients of a package. NoGCUintptrPool provides a way to amortize allocation overhead
// across many clients.
//
// An example of good use of a NoGCUintptrPool is in the fmt package, which maintains a
// dynamically-sized store of temporary output buffers. The store scales under
// load (when many goroutines are actively printing) and shrinks when
// quiescent.
//
// On the other hand, a free list maintained as part of a short-lived object is
// not a suitable use for a NoGCUintptrPool, since the overhead does not amortize well in
// that scenario. It is more efficient to have such objects implement their own
// free list.
//
// A NoGCUintptrPool must not be copied after first use.
type NoGCUintptrPool struct {
	// noCopy noCopy

	local     unsafe.Pointer // local fixed-size per-P pool, actual type is [P]uintptrPoolLocal
	localSize uintptr        // size of the local array

	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	New func() uintptr
}

// var poolRaceHash [128]uint64

// poolRaceAddr returns an address to use as the synchronization point
// for race detector logic. We don't use the actual pointer stored in x
// directly, for fear of conflicting with other synchronization on that address.
// Instead, we hash the pointer to get an index into poolRaceHash.
// See discussion on golang.org/cl/31589.
// func poolRaceAddr(x interface{}) unsafe.Pointer {
// ptr := uintptr((*[2]unsafe.Pointer)(unsafe.Pointer(&x))[1])
// h := uint32((uint64(uint32(ptr)) * 0x85ebca6b) >> 16)
// return unsafe.Pointer(&poolRaceHash[h%uint32(len(poolRaceHash))])
// }

// Local per-P Pool appendix.
type uintptrPoolLocalInternal struct {
	private    uintptr   // Can be used only by the respective P.
	shared     []uintptr // Can be used by any P.
	sync.Mutex           // Protects shared.
}

type uintptrPoolLocal struct {
	uintptrPoolLocalInternal

	// Prevents false sharing on widespread platforms with
	// 128 mod (cache line size) = 0 .
	pad [128 - unsafe.Sizeof(uintptrPoolLocalInternal{})%128]byte
}

// Put adds x to the pool.
func (p *NoGCUintptrPool) Put(x uintptr) {
	if x == 0 {
		return
	}
	if runtime.UnsafeRaceEnabled {
		if runtime.UnsafeFastrand()%4 == 0 {
			// Randomly drop x on floor.
			return
		}
		runtime.UnsafeRaceReleaseMerge(poolRaceAddr(x))
		runtime.UnsafeRaceDisable()
	}
	l := p.pin()
	if l.private == 0 {
		l.private = x
		x = 0
	}
	runtime.UnsafeProcUnpin()
	if x != 0 {
		l.Lock()
		l.shared = append(l.shared, x)
		l.Unlock()
	}
	if runtime.UnsafeRaceEnabled {
		runtime.UnsafeRaceEnable()
	}
}

// Get selects an arbitrary item from the NoGCUintptrPool, removes it from the
// NoGCUintptrPool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to Put and
// the values returned by Get.
//
// If Get would otherwise return nil and p.New is non-nil, Get returns
// the result of calling p.New.
func (p *NoGCUintptrPool) Get() uintptr {
	if runtime.UnsafeRaceEnabled {
		runtime.UnsafeRaceDisable()
	}
	l := p.pin()
	x := l.private
	l.private = 0
	runtime.UnsafeProcUnpin()
	if x == 0 {
		l.Lock()
		last := len(l.shared) - 1
		if last >= 0 {
			x = l.shared[last]
			l.shared = l.shared[:last]
		}
		l.Unlock()
		if x == 0 {
			x = p.getSlow()
		}
	}
	if runtime.UnsafeRaceEnabled {
		runtime.UnsafeRaceEnable()
		if x != 0 {
			runtime.UnsafeRaceAcquire(poolRaceAddr(x))
		}
	}
	if x == 0 && p.New != nil {
		x = p.New()
	}
	return x
}

func (p *NoGCUintptrPool) getSlow() (x uintptr) {
	// See the comment in pin regarding ordering of the loads.
	size := atomic.LoadUintptr(&p.localSize) // load-acquire
	local := p.local                         // load-consume
	// Try to steal one element from other procs.
	pid := runtime.UnsafeProcPin()
	runtime.UnsafeProcUnpin()
	for i := 0; i < int(size); i++ {
		l := NoGCUintptrPoolIndexLocal(local, (pid+i+1)%int(size))
		l.Lock()
		last := len(l.shared) - 1
		if last >= 0 {
			x = l.shared[last]
			l.shared = l.shared[:last]
			l.Unlock()
			break
		}
		l.Unlock()
	}
	return x
}

// pin pins the current goroutine to P, disables preemption and returns uintptrPoolLocal pool for the P.
// Caller must call runtime.UnsafeProcUnpin() when done with the pool.
func (p *NoGCUintptrPool) pin() *uintptrPoolLocal {
	pid := runtime.UnsafeProcPin()
	// In pinSlow we store to localSize and then to local, here we load in opposite order.
	// Since we've disabled preemption, GC cannot happen in between.
	// Thus here we must observe local at least as large localSize.
	// We can observe a newer/larger local, it is fine (we must observe its zero-initialized-ness).
	s := atomic.LoadUintptr(&p.localSize) // load-acquire
	l := p.local                          // load-consume
	if uintptr(pid) < s {
		return NoGCUintptrPoolIndexLocal(l, pid)
	}
	return p.pinSlow()
}

func (p *NoGCUintptrPool) pinSlow() *uintptrPoolLocal {
	// Retry under the mutex.
	// Can not lock the mutex while pinned.
	runtime.UnsafeProcUnpin()
	allNoGCUintptrPoolsMu.Lock()
	defer allNoGCUintptrPoolsMu.Unlock()
	pid := runtime.UnsafeProcPin()
	s := p.localSize
	l := p.local
	if uintptr(pid) < s {
		return NoGCUintptrPoolIndexLocal(l, pid)
	}
	if p.local == nil {
		allNoGCUintptrPools = append(allNoGCUintptrPools, p)
	}
	// If GOMAXPROCS changes between GCs, we re-allocate the array and lose the old one.
	size := runtime.GOMAXPROCS(0)
	local := make([]uintptrPoolLocal, size)
	atomic.StorePointer(&p.local, unsafe.Pointer(&local[0])) // store-release
	atomic.StoreUintptr(&p.localSize, uintptr(size))         // store-release
	return &local[pid]
}

var (
	allNoGCUintptrPoolsMu sync.Mutex
	allNoGCUintptrPools   []*NoGCUintptrPool
)

func NoGCUintptrPoolIndexLocal(l unsafe.Pointer, i int) *uintptrPoolLocal {
	lp := unsafe.Pointer(uintptr(l) + uintptr(i)*unsafe.Sizeof(uintptrPoolLocal{}))
	return (*uintptrPoolLocal)(lp)
}
