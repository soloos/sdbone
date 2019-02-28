package offheap

import (
	"unsafe"
)

type HKVTableObjectUPtrByString uintptr

func (u HKVTableObjectUPtrByString) Ptr() *HKVTableObjectByString {
	return (*HKVTableObjectByString)(unsafe.Pointer(u))
}

type HKVTableObjectByString struct {
	HSharedPointer
	Key string
}

type HKVTableObjectUPtrByInt32 uintptr

func (u HKVTableObjectUPtrByInt32) Ptr() *HKVTableObjectByInt32 {
	return (*HKVTableObjectByInt32)(unsafe.Pointer(u))
}

type HKVTableObjectByInt32 struct {
	HSharedPointer
	Key int32
}

type HKVTableObjectUPtrByInt64 uintptr

func (u HKVTableObjectUPtrByInt64) Ptr() *HKVTableObjectByInt64 {
	return (*HKVTableObjectByInt64)(unsafe.Pointer(u))
}

type HKVTableObjectByInt64 struct {
	HSharedPointer
	Key int64
}

type HKVTableObjectUPtrByBytes12 uintptr

func (u HKVTableObjectUPtrByBytes12) Ptr() *HKVTableObjectByBytes12 {
	return (*HKVTableObjectByBytes12)(unsafe.Pointer(u))
}

type HKVTableObjectByBytes12 struct {
	HSharedPointer
	Key [12]byte
}

type HKVTableObjectUPtrByBytes64 uintptr

func (u HKVTableObjectUPtrByBytes64) Ptr() *HKVTableObjectByBytes64 {
	return (*HKVTableObjectByBytes64)(unsafe.Pointer(u))
}

type HKVTableObjectByBytes64 struct {
	HSharedPointer
	Key [64]byte
}
