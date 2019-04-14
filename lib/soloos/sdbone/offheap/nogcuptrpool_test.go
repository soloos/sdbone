// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// NoGCUintptrPool is no-op under race detector, so all these tests do not work.
// +build !race

package offheap

import (
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
)

func TestNoGCUintptrPool(t *testing.T) {
	// disable GC so we can control when it happens.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	var p NoGCUintptrPool
	if p.Get() != 0 {
		t.Fatal("expected empty")
	}

	// Make sure that the goroutine doesn't migrate to another P
	// between Put and Get calls.
	runtime.UnsafeProcPin()
	p.Put(61)
	p.Put(62)
	if g := p.Get(); g != 61 {
		t.Fatalf("got %#v; want a", g)
	}
	if g := p.Get(); g != 62 {
		t.Fatalf("got %#v; want b", g)
	}
	if g := p.Get(); g != 0 {
		t.Fatalf("got %#v; want nil", g)
	}
	runtime.UnsafeProcUnpin()

	p.Put(63)
	debug.SetGCPercent(100) // to allow following GC to actually run
}

func TestNoGCUintptrPoolNew(t *testing.T) {
	// disable GC so we can control when it happens.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	i := uintptr(0)
	p := NoGCUintptrPool{
		New: func() uintptr {
			i++
			return i
		},
	}
	if v := p.Get(); v != 1 {
		t.Fatalf("got %v; want 1", v)
	}
	if v := p.Get(); v != 2 {
		t.Fatalf("got %v; want 2", v)
	}

	// Make sure that the goroutine doesn't migrate to another P
	// between Put and Get calls.
	runtime.UnsafeProcPin()
	p.Put(42)
	if v := p.Get(); v != 42 {
		t.Fatalf("got %v; want 42", v)
	}
	runtime.UnsafeProcUnpin()

	if v := p.Get(); v != 3 {
		t.Fatalf("got %v; want 3", v)
	}
}

func TestNoGCUintptrPoolStress(t *testing.T) {
	const P = 10
	N := int(1e6)
	if testing.Short() {
		N /= 100
	}
	var p NoGCUintptrPool
	done := make(chan bool)
	for i := 0; i < P; i++ {
		go func() {
			var v uintptr = 0
			for j := 0; j < N; j++ {
				if v == 0 {
					v = 0
				}
				p.Put(v)
				v = p.Get()
				if v != 0 {
					t.Errorf("expect 0, got %v", v)
					break
				}
			}
			done <- true
		}()
	}
	for i := 0; i < P; i++ {
		<-done
	}
}

func BenchmarkNoGCUintptrPool(b *testing.B) {
	var p NoGCUintptrPool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Put(1)
			p.Get()
		}
	})
}

func BenchmarkNoGCUintptrPoolOverflow(b *testing.B) {
	var p NoGCUintptrPool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for b := 0; b < 100; b++ {
				p.Put(1)
			}
			for b := 0; b < 100; b++ {
				p.Get()
			}
		}
	})
}

func BenchmarkPool(b *testing.B) {
	var p sync.Pool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Put(1)
			p.Get()
		}
	})
}

func BenchmarkPoolOverflow(b *testing.B) {
	var p sync.Pool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for b := 0; b < 100; b++ {
				p.Put(1)
			}
			for b := 0; b < 100; b++ {
				p.Get()
			}
		}
	})
}
