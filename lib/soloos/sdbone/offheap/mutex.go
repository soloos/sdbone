package offheap

import (
	"sync"
	"unsafe"
)

const (
	MutexStrutSize = unsafe.Sizeof(sync.Mutex{})
)

type MutexUintptr uintptr

func (p MutexUintptr) Ptr() *sync.Mutex { return (*sync.Mutex)(unsafe.Pointer(p)) }
