package offheap

import (
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

const TStructSize = int(unsafe.Sizeof(T{}))

type T struct {
	i    byte
	Data [1024]byte
}

func (p *T) test() {
	p.Data[0] = p.i
	p.i += 1
}

type TUintptr uintptr

func (u TUintptr) Ptr() *T { return (*T)(unsafe.Pointer(u)) }

type TPool struct {
	rawObjectPool RawObjectPool
}

func (p *TPool) Init(structSize int, objectsLimit int32) {
	p.rawObjectPool.Init(structSize, -1, nil, nil)
}

func BenchmarkOffheapRawObjectPool(b *testing.B) {
	runtime.GC()
	var tPool TPool
	tPool.Init(TStructSize, int32(b.N))
	var t TUintptr

	for run := 0; run < 2; run++ {
		for n := 0; n < b.N; n++ {
			if n%10000 == 0 {
				runtime.GC()
			}
			t = TUintptr(tPool.rawObjectPool.AllocRawObject())
			t.Ptr().test()
			tPool.rawObjectPool.ReleaseRawObject(uintptr(t))
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n%10000 == 0 {
			runtime.GC()
		}
		tPool.rawObjectPool.AllocRawObject()
	}
}

func BenchmarkOffheapSyncPool(b *testing.B) {
	runtime.GC()
	var pool sync.Pool
	pool.New = func() interface{} {
		return new(T)
	}
	var t *T

	for run := 0; run < 2; run++ {
		for n := 0; n < b.N; n++ {
			if n%10000 == 0 {
				runtime.GC()
			}
			t = pool.Get().(*T)
			t.test()
			pool.Put(t)
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n%10000 == 0 {
			runtime.GC()
		}
		pool.Get()
	}
}
