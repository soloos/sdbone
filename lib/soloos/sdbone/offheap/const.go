package offheap

import "unsafe"

const (
	UintptrSize            = int(unsafe.Sizeof(uintptr(0)))
	Int32Size              = int(unsafe.Sizeof(int32(0)))
	MaskArrayElementsLimit = 128
)
