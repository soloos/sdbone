package offheap

import (
	"unsafe"
)

const (
	ChunkStructSize = unsafe.Sizeof(Chunk{})
)

type ChunkUintptr uintptr

func (p ChunkUintptr) Ptr() *Chunk { return (*Chunk)(unsafe.Pointer(p)) }

type Chunk struct {
	SharedPointer
	ID   int64
	Data uintptr
}
